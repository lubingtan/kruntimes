// Package sandbox provides the agent-facing Go API for Session-mode Runs.
package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const defaultPollInterval = 500 * time.Millisecond

// HTTPDoer is the HTTP transport used for Runtime gateway requests.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Client creates and manages Sandboxes backed by Session-mode Runs.
type Client struct {
	runs         client.Client
	httpClient   HTTPDoer
	bearerToken  string
	pollInterval time.Duration
}

// Config supplies the explicit Kubernetes and gateway dependencies for Client.
// Callers may use a Kubernetes-configured HTTP client that injects credentials
// instead of BearerToken.
type Config struct {
	Runs         client.Client
	HTTPClient   HTTPDoer
	BearerToken  string
	PollInterval time.Duration
}

// New constructs a Sandbox client. Kubernetes and gateway dependencies are
// explicit so the same client works for in-cluster, local, and test transports.
func New(config Config) (*Client, error) {
	if config.Runs == nil {
		return nil, errors.New("sandbox Run client is required")
	}
	if config.HTTPClient == nil {
		return nil, errors.New("sandbox HTTP client is required")
	}
	interval := config.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	return &Client{runs: config.Runs, httpClient: config.HTTPClient, bearerToken: config.BearerToken, pollInterval: interval}, nil
}

// CreateOptions defines a new Session-mode Run.
type CreateOptions struct {
	Name           string
	GenerateName   string
	Namespace      string
	Runtime        string
	Source         *v1alpha1.CodeSource
	ArtifactInputs []v1alpha1.ArtifactInput
	Env            map[string]string
	Timeout        *metav1.Duration
	Session        *v1alpha1.RunSessionMode
}

// Create creates a Session Run. It does not wait for Runtime capacity or
// registration; callers use Wait to obtain a ready Sandbox.
func (c *Client) Create(ctx context.Context, options CreateOptions) (*Sandbox, error) {
	if options.Namespace == "" || options.Runtime == "" {
		return nil, errors.New("sandbox namespace and runtime are required")
	}
	if options.Name == "" && options.GenerateName == "" {
		return nil, errors.New("sandbox name or generateName is required")
	}
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: options.Name, GenerateName: options.GenerateName, Namespace: options.Namespace},
		Spec: v1alpha1.RunSpec{
			Runtime:        options.Runtime,
			Source:         options.Source,
			ArtifactInputs: options.ArtifactInputs,
			Env:            environmentVariables(options.Env),
			Timeout:        options.Timeout,
			Mode:           v1alpha1.RunMode{Session: options.Session},
		},
	}
	if run.Spec.Mode.Session == nil {
		run.Spec.Mode.Session = &v1alpha1.RunSessionMode{}
	}
	if err := c.runs.Create(ctx, run); err != nil {
		return nil, fmt.Errorf("create Session Run: %w", err)
	}
	return &Sandbox{client: c, run: run}, nil
}

// Open reads an existing Session-mode Run. It never creates or re-registers a
// sandbox as a side effect.
func (c *Client) Open(ctx context.Context, namespace, name string) (*Sandbox, error) {
	run := &v1alpha1.Run{}
	if err := c.runs.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, run); err != nil {
		return nil, fmt.Errorf("get Session Run: %w", err)
	}
	if run.Spec.Mode.Session == nil {
		return nil, &StateError{Run: run.DeepCopy(), Message: "Run is not a Session Run"}
	}
	return &Sandbox{client: c, run: run}, nil
}

// Sandbox is an opened Session-mode Run.
type Sandbox struct {
	client *Client
	run    *v1alpha1.Run
}

// Run returns a copy of the latest known Kubernetes Run object.
func (s *Sandbox) Run() *v1alpha1.Run { return s.run.DeepCopy() }

// Refresh reads the current Run status.
func (s *Sandbox) Refresh(ctx context.Context) error {
	if err := s.client.runs.Get(ctx, client.ObjectKeyFromObject(s.run), s.run); err != nil {
		return fmt.Errorf("get Session Run: %w", err)
	}
	return nil
}

// Wait waits until the Sandbox is Ready or becomes terminal.
func (s *Sandbox) Wait(ctx context.Context) error {
	for {
		if err := s.Refresh(ctx); err != nil {
			return err
		}
		switch s.run.Status.Phase {
		case v1alpha1.RunReady:
			return nil
		case v1alpha1.RunSucceeded, v1alpha1.RunFailed, v1alpha1.RunTimeout, v1alpha1.RunCancelled:
			return &StateError{Run: s.run.DeepCopy(), Message: "Session Run became terminal before it was ready"}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.client.pollInterval):
		}
	}
}

// Close requests normal Session Run cancellation and waits for its terminal
// lifecycle. It never calls Runtime Server CloseSession directly.
func (s *Sandbox) Close(ctx context.Context) error {
	if err := s.Refresh(ctx); err != nil {
		return err
	}
	if !s.run.Spec.CancelRequested {
		s.run.Spec.CancelRequested = true
		if err := s.client.runs.Update(ctx, s.run); err != nil {
			return fmt.Errorf("cancel Session Run: %w", err)
		}
	}
	for {
		if err := s.Refresh(ctx); err != nil {
			return err
		}
		if isTerminal(s.run.Status.Phase) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.client.pollInterval):
		}
	}
}

// Command defines one workspace-relative process execution request.
type Command struct {
	Argv             []string          `json:"argv,omitempty"`
	Shell            string            `json:"shell,omitempty"`
	WorkingDirectory string            `json:"workingDirectory,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
	Stdin            []byte            `json:"stdin,omitempty"`
	TimeoutMillis    int64             `json:"timeoutMillis,omitempty"`
}

// CommandResult is the bounded result of one command operation.
type CommandResult struct {
	ExitCode int32  `json:"exitCode"`
	Stdout   []byte `json:"stdout,omitempty"`
	Stderr   []byte `json:"stderr,omitempty"`
	TimedOut bool   `json:"timedOut,omitempty"`
}

// Execute runs exactly one command. Transport errors have unknown execution
// outcome and are intentionally never retried by this SDK.
func (s *Sandbox) Execute(ctx context.Context, command Command) (CommandResult, error) {
	var response struct {
		Command *CommandResult `json:"command"`
	}
	err := s.operation(ctx, map[string]any{"command": command}, &response)
	if err != nil {
		return CommandResult{}, err
	}
	if response.Command == nil {
		return CommandResult{}, errors.New("gateway response did not include a command result")
	}
	return *response.Command, nil
}

// WriteFile writes bounded content at a workspace-relative path.
func (s *Sandbox) WriteFile(ctx context.Context, path string, contents []byte, createParents bool) error {
	return s.operation(ctx, map[string]any{"writeFile": map[string]any{"path": path, "contents": contents, "createParents": createParents}}, nil)
}

// ReadFile returns bounded content at a workspace-relative path.
func (s *Sandbox) ReadFile(ctx context.Context, path string, maxBytes int64) ([]byte, bool, error) {
	endpoint, err := s.endpoint("files/" + url.PathEscape(path))
	if err != nil {
		return nil, false, err
	}
	if maxBytes > 0 {
		endpoint += "?maxBytes=" + fmt.Sprint(maxBytes)
	}
	var response struct {
		Contents  []byte `json:"contents"`
		Truncated bool   `json:"truncated"`
	}
	if err := s.request(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, false, err
	}
	return response.Contents, response.Truncated, nil
}

// FileInfo identifies one direct child of a workspace-relative directory.
type FileInfo struct {
	Path      string `json:"path"`
	Directory bool   `json:"directory"`
	SizeBytes int64  `json:"sizeBytes"`
}

// ListFiles lists bounded direct children below a workspace-relative path.
func (s *Sandbox) ListFiles(ctx context.Context, directory string) ([]FileInfo, error) {
	endpoint, err := s.endpoint("files")
	if err != nil {
		return nil, err
	}
	if directory != "" {
		endpoint += "?path=" + url.QueryEscape(directory)
	}
	var response struct {
		Entries []FileInfo `json:"entries"`
	}
	if err := s.request(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.Entries, nil
}

// APIError reports a non-success Runtime gateway response.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Runtime gateway returned HTTP %d: %s", e.StatusCode, e.Message)
}

// StateError reports an invalid Session Run lifecycle state.
type StateError struct {
	Run     *v1alpha1.Run
	Message string
}

func (e *StateError) Error() string { return e.Message }

func (s *Sandbox) operation(ctx context.Context, operation any, response any) error {
	endpoint, err := s.endpoint("operations:execute")
	if err != nil {
		return err
	}
	return s.request(ctx, http.MethodPost, endpoint, operation, response)
}

func (s *Sandbox) endpoint(suffix string) (string, error) {
	if s.run.Status.Phase != v1alpha1.RunReady || s.run.Status.Endpoint == nil || s.run.Status.Endpoint.URL == "" {
		return "", &StateError{Run: s.run.DeepCopy(), Message: "Session Run is not ready"}
	}
	endpoint, err := url.Parse(s.run.Status.Endpoint.URL)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return "", &StateError{Run: s.run.DeepCopy(), Message: "Session Run has an invalid endpoint"}
	}
	parts := strings.Split(strings.Trim(endpoint.Path, "/"), "/")
	if len(parts) < 7 || parts[len(parts)-2] != "sessions" || parts[len(parts)-1] != string(s.run.UID) {
		return "", &StateError{Run: s.run.DeepCopy(), Message: "Session Run endpoint does not match its UID"}
	}
	endpoint.Path = path.Join(endpoint.Path, suffix)
	return endpoint.String(), nil
}

func (s *Sandbox) request(ctx context.Context, method, endpoint string, body, response any) error {
	var content io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode gateway request: %w", err)
		}
		content = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, content)
	if err != nil {
		return fmt.Errorf("build gateway request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if s.client.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+s.client.bearerToken)
	}
	result, err := s.client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call Runtime gateway: %w", err)
	}
	defer result.Body.Close()
	if result.StatusCode < http.StatusOK || result.StatusCode >= http.StatusMultipleChoices {
		var gatewayError struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(result.Body, 1<<20)).Decode(&gatewayError)
		return &APIError{StatusCode: result.StatusCode, Message: gatewayError.Error}
	}
	if response != nil {
		if err := json.NewDecoder(io.LimitReader(result.Body, 1<<20)).Decode(response); err != nil {
			return fmt.Errorf("decode gateway response: %w", err)
		}
	}
	return nil
}

func isTerminal(phase v1alpha1.RunPhase) bool {
	return phase == v1alpha1.RunSucceeded || phase == v1alpha1.RunFailed || phase == v1alpha1.RunTimeout || phase == v1alpha1.RunCancelled
}

func environmentVariables(values map[string]string) []corev1.EnvVar {
	if len(values) == 0 {
		return nil
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	variables := make([]corev1.EnvVar, 0, len(names))
	for _, name := range names {
		variables = append(variables, corev1.EnvVar{Name: name, Value: values[name]})
	}
	return variables
}
