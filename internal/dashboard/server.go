package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const (
	defaultRunListLimit int64 = 50
	maxRunListLimit     int64 = 200
	defaultLogTailLines int64 = 100
	maxLogTailLines     int64 = 500
	maxLogBytes         int64 = 1 << 20
)

// RequestClientProvider resolves a Kubernetes client for one HTTP request.
// RequestClientFactory is the production implementation.
type RequestClientProvider interface {
	ClientForRequest(*http.Request) (client.Client, error)
}

// PodLogReader exposes the narrowly-scoped Kubernetes log operation required
// by the Dashboard. RequestClientFactory is the production implementation.
type PodLogReader interface {
	ReadPodLogs(context.Context, *http.Request, string, string, corev1.PodLogOptions) (io.ReadCloser, error)
}

// Server exposes the Dashboard's internal, read-only HTTP API.
type Server struct {
	Clients RequestClientProvider

	routesOnce sync.Once
	routes     http.Handler
}

type errorResponse struct {
	Error string `json:"error"`
}

type namespaceListResponse struct {
	Items []string `json:"items"`
}

type runListResponse struct {
	Items    []runSummary `json:"items"`
	Continue string       `json:"continue,omitempty"`
}

type runSummary struct {
	Name                 string            `json:"name"`
	Namespace            string            `json:"namespace"`
	UID                  types.UID         `json:"uid"`
	Runtime              string            `json:"runtime"`
	Phase                v1alpha1.RunPhase `json:"phase"`
	AssignedPod          string            `json:"assignedPod,omitempty"`
	Attempt              int32             `json:"attempt,omitempty"`
	CreationTimestamp    metav1.Time       `json:"creationTimestamp"`
	StartTime            *metav1.Time      `json:"startTime,omitempty"`
	CompletionTime       *metav1.Time      `json:"completionTime,omitempty"`
	LastTransitionReason string            `json:"lastTransitionReason,omitempty"`
}

type runDetailResponse struct {
	runSummary
	Message      string                 `json:"message,omitempty"`
	Endpoint     *v1alpha1.RunEndpoint  `json:"endpoint,omitempty"`
	Conditions   []metav1.Condition     `json:"conditions,omitempty"`
	Outputs      map[string]string      `json:"outputs,omitempty"`
	ArtifactRefs []v1alpha1.ArtifactRef `json:"artifactRefs,omitempty"`
}

type runLogResponse struct {
	Items []runLogEntry `json:"items"`
}

// runLogEntry is the safe portion of one structured runtimed log record.
type runLogEntry struct {
	Stream               string `json:"stream"`
	Message              string `json:"message"`
	InvocationID         string `json:"invocationId,omitempty"`
	Operation            string `json:"operation,omitempty"`
	Outcome              string `json:"outcome,omitempty"`
	StatusCode           string `json:"statusCode,omitempty"`
	ExitCode             *int32 `json:"exitCode,omitempty"`
	TimedOut             bool   `json:"timedOut,omitempty"`
	DurationMilliseconds int64  `json:"durationMilliseconds,omitempty"`
}

type runtimedLogRecord struct {
	RunUID               string `json:"run_uid"`
	Stream               string `json:"stream"`
	Message              string `json:"message"`
	InvocationID         string `json:"invocation_id,omitempty"`
	Operation            string `json:"operation,omitempty"`
	Outcome              string `json:"outcome,omitempty"`
	StatusCode           string `json:"status_code,omitempty"`
	ExitCode             *int32 `json:"exit_code,omitempty"`
	TimedOut             bool   `json:"timed_out,omitempty"`
	DurationMilliseconds int64  `json:"duration_milliseconds,omitempty"`
}

func (record runtimedLogRecord) entry() runLogEntry {
	return runLogEntry{
		Stream:               record.Stream,
		Message:              record.Message,
		InvocationID:         record.InvocationID,
		Operation:            record.Operation,
		Outcome:              record.Outcome,
		StatusCode:           record.StatusCode,
		ExitCode:             record.ExitCode,
		TimedOut:             record.TimedOut,
		DurationMilliseconds: record.DurationMilliseconds,
	}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		s.writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.routesOnce.Do(s.registerRoutes)
	s.routes.ServeHTTP(writer, request)
}

func (s *Server) registerRoutes() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /api/namespaces", s.withRequestClient(s.listNamespaces))
	mux.HandleFunc("GET /api/namespaces/{namespace}/runs", s.withRequestClient(s.listRuns))
	mux.HandleFunc("GET /api/namespaces/{namespace}/runs/{name}", s.withRequestClient(s.getRun))
	mux.HandleFunc("GET /api/namespaces/{namespace}/runs/{name}/logs", s.withRequestClient(s.getRunLogs))
	mux.HandleFunc("GET /", func(writer http.ResponseWriter, _ *http.Request) {
		s.writeError(writer, http.StatusNotFound, "endpoint not found")
	})
	s.routes = mux
}

func (s *Server) withRequestClient(handler func(http.ResponseWriter, *http.Request, client.Client)) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if s.Clients == nil {
			s.writeError(writer, http.StatusServiceUnavailable, "dashboard Kubernetes client is not configured")
			return
		}
		kubernetesClient, err := s.Clients.ClientForRequest(request)
		if err != nil {
			s.writeKubernetesError(writer, err)
			return
		}
		handler(writer, request, kubernetesClient)
	}
}

func (s *Server) listNamespaces(writer http.ResponseWriter, request *http.Request, kubernetesClient client.Client) {
	var namespaces corev1.NamespaceList
	if err := kubernetesClient.List(request.Context(), &namespaces); err != nil {
		s.writeKubernetesError(writer, err)
		return
	}
	items := make([]string, 0, len(namespaces.Items))
	for _, namespace := range namespaces.Items {
		items = append(items, namespace.Name)
	}
	s.writeJSON(writer, http.StatusOK, namespaceListResponse{Items: items})
}

func (s *Server) listRuns(writer http.ResponseWriter, request *http.Request, kubernetesClient client.Client) {
	options, err := parseRunListOptions(request)
	if err != nil {
		s.writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	var runs v1alpha1.RunList
	if err := kubernetesClient.List(request.Context(), &runs, append(options, client.InNamespace(request.PathValue("namespace")))...); err != nil {
		s.writeKubernetesError(writer, err)
		return
	}
	items := make([]runSummary, 0, len(runs.Items))
	for i := range runs.Items {
		items = append(items, summaryForRun(&runs.Items[i]))
	}
	s.writeJSON(writer, http.StatusOK, runListResponse{Items: items, Continue: runs.Continue})
}

func parseRunListOptions(request *http.Request) ([]client.ListOption, error) {
	query := request.URL.Query()
	if query.Get("phase") != "" || query.Get("runtime") != "" {
		return nil, errors.New("phase and runtime filters are not supported yet")
	}
	limit := defaultRunListLimit
	if rawLimit := query.Get("limit"); rawLimit != "" {
		parsed, err := strconv.ParseInt(rawLimit, 10, 64)
		if err != nil || parsed < 1 || parsed > maxRunListLimit {
			return nil, fmt.Errorf("limit must be between 1 and %d", maxRunListLimit)
		}
		limit = parsed
	}
	options := []client.ListOption{client.Limit(limit)}
	if continuation := query.Get("continue"); continuation != "" {
		options = append(options, client.Continue(continuation))
	}
	if selector := query.Get("labelSelector"); selector != "" {
		parsed, err := labels.Parse(selector)
		if err != nil {
			return nil, fmt.Errorf("invalid labelSelector: %w", err)
		}
		options = append(options, client.MatchingLabelsSelector{Selector: parsed})
	}
	return options, nil
}

func (s *Server) getRun(writer http.ResponseWriter, request *http.Request, kubernetesClient client.Client) {
	run := &v1alpha1.Run{}
	if err := kubernetesClient.Get(request.Context(), client.ObjectKey{Namespace: request.PathValue("namespace"), Name: request.PathValue("name")}, run); err != nil {
		s.writeKubernetesError(writer, err)
		return
	}
	s.writeJSON(writer, http.StatusOK, runDetailResponse{
		runSummary:   summaryForRun(run),
		Message:      run.Status.Message,
		Endpoint:     run.Status.Endpoint,
		Conditions:   append([]metav1.Condition(nil), run.Status.Conditions...),
		Outputs:      copyStringMap(run.Status.Outputs),
		ArtifactRefs: append([]v1alpha1.ArtifactRef(nil), run.Status.ArtifactRefs...),
	})
}

func (s *Server) getRunLogs(writer http.ResponseWriter, request *http.Request, kubernetesClient client.Client) {
	tailLines, follow, err := parseLogOptions(request)
	if err != nil {
		s.writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	logReader, ok := s.Clients.(PodLogReader)
	if !ok {
		s.writeError(writer, http.StatusServiceUnavailable, "dashboard Pod log reader is not configured")
		return
	}
	run := &v1alpha1.Run{}
	if err := kubernetesClient.Get(request.Context(), client.ObjectKey{Namespace: request.PathValue("namespace"), Name: request.PathValue("name")}, run); err != nil {
		s.writeKubernetesError(writer, err)
		return
	}
	if run.Status.AssignedPod == "" {
		s.writeError(writer, http.StatusConflict, "Run has not been assigned to a Runtime Pod")
		return
	}
	stream, err := logReader.ReadPodLogs(request.Context(), request, run.Namespace, run.Status.AssignedPod, corev1.PodLogOptions{
		Container:  "runtimed",
		Follow:     follow,
		TailLines:  pointerTo(maxLogTailLines),
		LimitBytes: pointerTo(maxLogBytes),
	})
	if err != nil {
		s.writeKubernetesError(writer, err)
		return
	}
	defer stream.Close()
	if follow {
		s.streamRunLogs(writer, stream, string(run.UID))
		return
	}
	entries, err := readRunLogs(stream, string(run.UID), tailLines)
	if err != nil {
		s.writeError(writer, http.StatusInternalServerError, "dashboard log request failed")
		return
	}
	s.writeJSON(writer, http.StatusOK, runLogResponse{Items: entries})
}

func parseLogOptions(request *http.Request) (int64, bool, error) {
	query := request.URL.Query()
	tailLines := defaultLogTailLines
	if rawTail := query.Get("tail"); rawTail != "" {
		parsed, err := strconv.ParseInt(rawTail, 10, 64)
		if err != nil || parsed < 1 || parsed > maxLogTailLines {
			return 0, false, fmt.Errorf("tail must be between 1 and %d", maxLogTailLines)
		}
		tailLines = parsed
	}
	follow := false
	if rawFollow := query.Get("follow"); rawFollow != "" {
		parsed, err := strconv.ParseBool(rawFollow)
		if err != nil {
			return 0, false, errors.New("follow must be true or false")
		}
		follow = parsed
	}
	return tailLines, follow, nil
}

func readRunLogs(reader io.Reader, runUID string, tailLines int64) ([]runLogEntry, error) {
	entries := make([]runLogEntry, 0, tailLines)
	if err := scanRunLogs(reader, runUID, func(entry runLogEntry) error {
		entries = append(entries, entry)
		if int64(len(entries)) > tailLines {
			copy(entries, entries[len(entries)-int(tailLines):])
			entries = entries[:tailLines]
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *Server) streamRunLogs(writer http.ResponseWriter, reader io.Reader, runUID string) {
	writer.Header().Set("Content-Type", "application/x-ndjson")
	writer.WriteHeader(http.StatusOK)
	flusher, _ := writer.(http.Flusher)
	_ = scanRunLogs(reader, runUID, func(entry runLogEntry) error {
		if err := json.NewEncoder(writer).Encode(entry); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
}

func scanRunLogs(reader io.Reader, runUID string, handle func(runLogEntry) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), int(maxLogBytes))
	for scanner.Scan() {
		var record runtimedLogRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil || record.RunUID != runUID {
			continue
		}
		if err := handle(record.entry()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func pointerTo[T any](value T) *T {
	return &value
}

func summaryForRun(run *v1alpha1.Run) runSummary {
	return runSummary{
		Name:                 run.Name,
		Namespace:            run.Namespace,
		UID:                  run.UID,
		Runtime:              run.Spec.Runtime,
		Phase:                run.Status.Phase,
		AssignedPod:          run.Status.AssignedPod,
		Attempt:              run.Status.Attempt,
		CreationTimestamp:    run.CreationTimestamp,
		StartTime:            run.Status.StartTime,
		CompletionTime:       run.Status.CompletionTime,
		LastTransitionReason: lastTransitionReason(run.Status.Conditions),
	}
}

func lastTransitionReason(conditions []metav1.Condition) string {
	var latest *metav1.Condition
	for i := range conditions {
		condition := &conditions[i]
		if latest == nil || condition.LastTransitionTime.After(latest.LastTransitionTime.Time) {
			latest = condition
		}
	}
	if latest == nil {
		return ""
	}
	return latest.Reason
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func (s *Server) writeKubernetesError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrMissingBearerToken), apierrors.IsUnauthorized(err):
		s.writeError(writer, http.StatusUnauthorized, "Kubernetes bearer token is required")
	case apierrors.IsForbidden(err):
		s.writeError(writer, http.StatusForbidden, "Kubernetes authorization denied")
	case apierrors.IsNotFound(err):
		s.writeError(writer, http.StatusNotFound, "resource not found")
	default:
		s.writeError(writer, http.StatusInternalServerError, "dashboard request failed")
	}
}

func (s *Server) writeError(writer http.ResponseWriter, status int, message string) {
	s.writeJSON(writer, status, errorResponse{Error: message})
}

func (s *Server) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

var _ http.Handler = (*Server)(nil)
