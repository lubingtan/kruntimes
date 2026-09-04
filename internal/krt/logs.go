package krt

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const (
	defaultRunLogTailLines = 100
	maxRunLogTailLines     = 500
)

type runLogEntry struct {
	Stream  string `json:"stream"`
	Message string `json:"message"`
}

// runLogGateway reads one Run's structured records through the operator-managed
// Runtime Gateway. Its HTTP transport retains kubeconfig bearer, exec, or
// client-certificate authentication, but has independent Gateway TLS trust.
type runLogGateway struct {
	baseURL *url.URL
	client  *http.Client
}

func newRunLogGateway(restConfig *rest.Config, rawURL, caFile string, insecureSkipTLSVerify bool) (*runLogGateway, error) {
	endpoint, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, errors.New("gateway URL must be an absolute HTTP URL")
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, errors.New("gateway URL must use http or https")
	}
	if restConfig == nil {
		return nil, errors.New("Kubernetes REST configuration is required for Gateway authentication")
	}

	// Build kubeconfig authentication wrappers first. TransportConfig resolves
	// exec-provider cluster information from the Kubernetes API configuration.
	// Replace the Kubernetes API trust roots afterwards, so Gateway trust is
	// explicit. Retain user client-certificate material for an operator-enabled
	// Gateway mTLS deployment.
	transportConfig, err := restConfig.TransportConfig()
	if err != nil {
		return nil, fmt.Errorf("configure Gateway authentication: %w", err)
	}
	if insecureSkipTLSVerify && endpoint.Scheme != "https" {
		return nil, errors.New("gateway insecure TLS verification can only be used with an HTTPS URL")
	}
	userTLS := transportConfig.TLS
	transportConfig.TLS = transport.TLSConfig{
		Insecure: insecureSkipTLSVerify,
		CAFile:   caFile,
		CertFile: userTLS.CertFile,
		CertData: userTLS.CertData,
		KeyFile:  userTLS.KeyFile,
		KeyData:  userTLS.KeyData,
	}
	roundTripper, err := transport.New(transportConfig)
	if err != nil {
		return nil, fmt.Errorf("configure Gateway HTTP transport: %w", err)
	}
	return &runLogGateway{baseURL: endpoint, client: &http.Client{Transport: roundTripper}}, nil
}

func (g *runLogGateway) logs(ctx context.Context, namespace, runtimeName, runUID string, tailLines int, follow bool) (*http.Response, error) {
	if g == nil || g.baseURL == nil || g.client == nil {
		return nil, errors.New("Runtime Gateway client is not configured")
	}
	endpoint := g.baseURL.JoinPath("v1", "namespaces", namespace, "runtimes", runtimeName, "runs", runUID, "logs")
	query := endpoint.Query()
	query.Set("tailLines", fmt.Sprintf("%d", tailLines))
	if follow {
		query.Set("follow", "true")
	}
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Runtime Gateway log request: %w", err)
	}
	response, err := g.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call Runtime Gateway log API: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer response.Body.Close()
		return nil, gatewayLogError(response)
	}
	return response, nil
}

func gatewayLogError(response *http.Response) error {
	var body struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&body)
	if body.Error != "" {
		return fmt.Errorf("Runtime Gateway log API returned %s: %s", response.Status, body.Error)
	}
	return fmt.Errorf("Runtime Gateway log API returned %s", response.Status)
}

func newLogsCmd(getter genericclioptions.RESTClientGetter, scheme *runtime.Scheme) *cobra.Command {
	var (
		follow                       bool
		tailLines                    int
		gatewayURL                   string
		gatewayCAFile                string
		gatewayInsecureSkipTLSVerify bool
	)

	cmd := &cobra.Command{
		Use:   "logs <run-name>",
		Short: "Show logs from a Run through the Runtime Gateway.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if tailLines < 1 || tailLines > maxRunLogTailLines {
				return fmt.Errorf("tail must be an integer from 1 through %d", maxRunLogTailLines)
			}
			k8sClient, err := clientFromConfig(getter, scheme)
			if err != nil {
				return err
			}
			restConfig, err := restConfigFromConfig(getter)
			if err != nil {
				return err
			}
			gateway, err := newRunLogGateway(restConfig, gatewayURL, gatewayCAFile, gatewayInsecureSkipTLSVerify)
			if err != nil {
				return err
			}
			namespace := namespaceFromConfig(getter)
			runName := args[0]
			run := &v1alpha1.Run{}
			if err := k8sClient.Get(cmd.Context(), client.ObjectKey{Name: runName, Namespace: namespace}, run); err != nil {
				return fmt.Errorf("get run: %w", err)
			}
			response, err := gateway.logs(cmd.Context(), run.Namespace, run.Spec.Runtime, string(run.UID), tailLines, follow)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if follow {
				return writeFollowRunLogs(response.Body, cmd.OutOrStdout(), cmd.ErrOrStderr())
			}
			return writeSnapshotRunLogs(response.Body, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log output")
	cmd.Flags().IntVar(&tailLines, "tail", defaultRunLogTailLines, "Number of recent structured log records to show (1-500)")
	cmd.Flags().StringVar(&gatewayURL, "gateway-url", "", "Operator-managed Runtime Gateway base URL")
	cmd.Flags().StringVar(&gatewayCAFile, "gateway-ca-file", "", "PEM trust bundle file for an HTTPS Runtime Gateway")
	cmd.Flags().BoolVar(&gatewayInsecureSkipTLSVerify, "gateway-insecure-skip-tls-verify", false, "Allow an HTTPS Runtime Gateway with an unverified certificate")
	return cmd
}

func writeSnapshotRunLogs(reader io.Reader, stdout, stderr io.Writer) error {
	var response struct {
		Items []runLogEntry `json:"items"`
	}
	if err := json.NewDecoder(reader).Decode(&response); err != nil {
		return fmt.Errorf("decode Runtime Gateway log response: %w", err)
	}
	return writeRunLogEntries(response.Items, stdout, stderr)
}

func writeFollowRunLogs(reader io.Reader, stdout, stderr io.Writer) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		var entry runLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("decode Runtime Gateway log record: %w", err)
		}
		if err := writeRunLogEntries([]runLogEntry{entry}, stdout, stderr); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read Runtime Gateway log stream: %w", err)
	}
	return nil
}

func writeRunLogEntries(entries []runLogEntry, stdout, stderr io.Writer) error {
	for _, entry := range entries {
		var output io.Writer
		switch entry.Stream {
		case "stdout":
			output = stdout
		case "stderr":
			output = stderr
		default:
			continue
		}
		if err := writeLogOutput(output, entry.Message); err != nil {
			return fmt.Errorf("write %s: %w", entry.Stream, err)
		}
	}
	return nil
}

func writeLogOutput(w io.Writer, output string) error {
	if output == "" {
		return nil
	}
	if !strings.HasSuffix(output, "\n") {
		output += "\n"
	}
	_, err := io.WriteString(w, output)
	return err
}
