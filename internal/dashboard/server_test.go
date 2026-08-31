package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

type staticRequestClients struct {
	client     client.Client
	err        error
	logs       string
	logsErr    error
	logRequest struct {
		namespace string
		pod       string
		options   corev1.PodLogOptions
	}
}

func (s staticRequestClients) ClientForRequest(*http.Request) (client.Client, error) {
	return s.client, s.err
}

func (s *staticRequestClients) ReadPodLogs(_ context.Context, _ *http.Request, namespace, pod string, options corev1.PodLogOptions) (io.ReadCloser, error) {
	s.logRequest.namespace = namespace
	s.logRequest.pod = pod
	s.logRequest.options = options
	if s.logsErr != nil {
		return nil, s.logsErr
	}
	return io.NopCloser(strings.NewReader(s.logs)), nil
}

func TestServerListsNamespacesAndRuns(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC))
	readyRun := dashboardRun("ready", "team-a", "python", v1alpha1.RunReady, now)
	readyRun.Labels = map[string]string{"app": "dashboard"}
	taskRun := dashboardRun("task", "team-a", "bash", v1alpha1.RunSucceeded, now)
	otherNamespace := dashboardRun("other", "team-b", "python", v1alpha1.RunReady, now)
	server := dashboardTestServer(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b"}},
		readyRun, taskRun, otherNamespace,
	)

	response := requestDashboard(t, server, http.MethodGet, "/api/namespaces", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("list namespaces status = %d, body = %s", response.Code, response.Body.String())
	}
	var namespaces namespaceListResponse
	decodeDashboardResponse(t, response, &namespaces)
	if len(namespaces.Items) != 2 || namespaces.Items[0] != "team-a" || namespaces.Items[1] != "team-b" {
		t.Fatalf("namespaces = %#v, want team-a and team-b", namespaces.Items)
	}

	response = requestDashboard(t, server, http.MethodGet, "/api/namespaces/team-a/runs?labelSelector=app%3Ddashboard", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("list Runs status = %d, body = %s", response.Code, response.Body.String())
	}
	var runs runListResponse
	decodeDashboardResponse(t, response, &runs)
	if len(runs.Items) != 1 {
		t.Fatalf("Run list = %#v, want one matching Run", runs.Items)
	}
	if got := runs.Items[0]; got.Name != "ready" || got.Namespace != "team-a" || got.Runtime != "python" || got.Phase != v1alpha1.RunReady || got.LastTransitionReason != "FunctionRegistered" {
		t.Fatalf("Run summary = %#v", got)
	}
}

func TestServerGetsSafeRunDetail(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC))
	run := dashboardRun("detail", "team-a", "python", v1alpha1.RunReady, now)
	run.Spec.Env = []corev1.EnvVar{{Name: "SECRET", Value: "must-not-leak"}}
	run.Status.Message = "ready for requests"
	run.Status.Outputs = map[string]string{"answer": "42"}
	run.Status.ArtifactRefs = []v1alpha1.ArtifactRef{{Name: "result", Driver: v1alpha1.ArtifactDriverS3}}
	server := dashboardTestServer(t, run)

	response := requestDashboard(t, server, http.MethodGet, "/api/namespaces/team-a/runs/detail", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("get Run status = %d, body = %s", response.Code, response.Body.String())
	}
	var body map[string]json.RawMessage
	decodeDashboardResponse(t, response, &body)
	if _, exists := body["spec"]; exists {
		t.Fatalf("Dashboard Run detail must not expose Run spec: %s", response.Body.String())
	}
	var outputs map[string]string
	if err := json.Unmarshal(body["outputs"], &outputs); err != nil {
		t.Fatalf("decode outputs: %v", err)
	}
	if outputs["answer"] != "42" {
		t.Fatalf("outputs = %#v", outputs)
	}
	var artifacts []v1alpha1.ArtifactRef
	if err := json.Unmarshal(body["artifactRefs"], &artifacts); err != nil {
		t.Fatalf("decode artifact refs: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Name != "result" {
		t.Fatalf("artifact refs = %#v", artifacts)
	}
}

func TestServerRejectsMissingTokenAndMapsKubernetesErrors(t *testing.T) {
	server := &Server{Clients: staticRequestClients{err: ErrMissingBearerToken}}
	response := requestDashboard(t, server, http.MethodGet, "/api/namespaces", nil)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want %d", response.Code, http.StatusUnauthorized)
	}

	server.Clients = staticRequestClients{err: apierrors.NewForbidden(schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "runs"}, "run", errors.New("denied"))}
	response = requestDashboard(t, server, http.MethodGet, "/api/namespaces/team-a/runs", nil)
	if response.Code != http.StatusForbidden {
		t.Fatalf("forbidden status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestServerRejectsInvalidListRequests(t *testing.T) {
	server := dashboardTestServer(t)
	for _, path := range []string{
		"/api/namespaces/team-a/runs?limit=0",
		"/api/namespaces/team-a/runs?limit=201",
		"/api/namespaces/team-a/runs?labelSelector=app%20in%20(",
		"/api/namespaces/team-a/runs?phase=Ready",
		"/api/namespaces/team-a/runs?runtime=python",
	} {
		response := requestDashboard(t, server, http.MethodGet, path, nil)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusBadRequest)
		}
	}
}

func TestServerRoutesMethodsAndMissingEndpoints(t *testing.T) {
	server := dashboardTestServer(t)

	response := requestDashboard(t, server, http.MethodPost, "/api/namespaces", nil)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	var methodError errorResponse
	decodeDashboardResponse(t, response, &methodError)
	if methodError.Error != "method not allowed" {
		t.Fatalf("POST error = %q", methodError.Error)
	}

	response = requestDashboard(t, server, http.MethodGet, "/api/unknown", nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing route status = %d, want %d", response.Code, http.StatusNotFound)
	}
	var routeError errorResponse
	decodeDashboardResponse(t, response, &routeError)
	if routeError.Error != "endpoint not found" {
		t.Fatalf("missing route error = %q", routeError.Error)
	}
}

func TestServerGetsFilteredRunLogs(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC))
	run := dashboardRun("logs", "team-a", "python", v1alpha1.RunRunning, now)
	clients := &staticRequestClients{logs: strings.Join([]string{
		`{"run_uid":"other","stream":"stdout","message":"ignore"}`,
		`{"run_uid":"logs-uid","stream":"stdout","message":"first"}`,
		`{"run_uid":"logs-uid","stream":"stderr","message":"second"}`,
		`{"run_uid":"logs-uid","stream":"audit","message":"finished","invocation_id":"invoke-1","duration_milliseconds":12}`,
	}, "\n")}
	server := dashboardTestServerWithClients(t, clients, run)

	response := requestDashboard(t, server, http.MethodGet, "/api/namespaces/team-a/runs/logs/logs?tail=2", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("get logs status = %d, body = %s", response.Code, response.Body.String())
	}
	var body runLogResponse
	decodeDashboardResponse(t, response, &body)
	if len(body.Items) != 2 || body.Items[0].Message != "second" || body.Items[1].InvocationID != "invoke-1" || body.Items[1].DurationMilliseconds != 12 {
		t.Fatalf("log entries = %#v", body.Items)
	}
	if clients.logRequest.namespace != "team-a" || clients.logRequest.pod != "runtime-pod" || clients.logRequest.options.Container != "runtimed" || clients.logRequest.options.Follow || clients.logRequest.options.TailLines == nil || *clients.logRequest.options.TailLines != maxLogTailLines {
		t.Fatalf("Pod log request = %#v", clients.logRequest)
	}
}

func TestServerStreamsFilteredRunLogs(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC))
	run := dashboardRun("follow", "team-a", "python", v1alpha1.RunRunning, now)
	clients := &staticRequestClients{logs: strings.Join([]string{
		`{"run_uid":"other","stream":"stdout","message":"ignore"}`,
		`{"run_uid":"follow-uid","stream":"stdout","message":"followed"}`,
	}, "\n")}
	server := dashboardTestServerWithClients(t, clients, run)

	response := requestDashboard(t, server, http.MethodGet, "/api/namespaces/team-a/runs/follow/logs?follow=true", nil)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("follow status/content type = %d/%q", response.Code, response.Header().Get("Content-Type"))
	}
	var entry runLogEntry
	if err := json.Unmarshal(response.Body.Bytes(), &entry); err != nil || entry.Message != "followed" {
		t.Fatalf("follow entry = %q, %v", response.Body.String(), err)
	}
	if !clients.logRequest.options.Follow {
		t.Fatal("follow Pod log request must set Follow")
	}
}

func TestServerRejectsInvalidLogRequests(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC))
	unassigned := dashboardRun("unassigned", "team-a", "python", v1alpha1.RunPending, now)
	unassigned.Status.AssignedPod = ""
	server := dashboardTestServer(t, unassigned)
	for _, path := range []string{
		"/api/namespaces/team-a/runs/unassigned/logs?tail=0",
		"/api/namespaces/team-a/runs/unassigned/logs?tail=501",
		"/api/namespaces/team-a/runs/unassigned/logs?follow=maybe",
	} {
		response := requestDashboard(t, server, http.MethodGet, path, nil)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusBadRequest)
		}
	}
	response := requestDashboard(t, server, http.MethodGet, "/api/namespaces/team-a/runs/unassigned/logs", nil)
	if response.Code != http.StatusConflict {
		t.Fatalf("unassigned logs status = %d, want %d", response.Code, http.StatusConflict)
	}
}

func dashboardTestServer(t *testing.T, objects ...client.Object) *Server {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kruntimes scheme: %v", err)
	}
	return dashboardTestServerWithClients(t, &staticRequestClients{client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()})
}

func dashboardTestServerWithClients(t *testing.T, clients *staticRequestClients, objects ...client.Object) *Server {
	t.Helper()
	if clients.client == nil {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatalf("add core scheme: %v", err)
		}
		if err := v1alpha1.AddToScheme(scheme); err != nil {
			t.Fatalf("add kruntimes scheme: %v", err)
		}
		clients.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	}
	return &Server{Clients: clients}
}

func dashboardRun(name, namespace, runtimeName string, phase v1alpha1.RunPhase, now metav1.Time) *v1alpha1.Run {
	return &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(name + "-uid"), CreationTimestamp: now},
		Spec:       v1alpha1.RunSpec{Runtime: runtimeName},
		Status: v1alpha1.RunStatus{
			Phase:       phase,
			AssignedPod: "runtime-pod",
			Attempt:     1,
			Conditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "FunctionRegistered",
				LastTransitionTime: now,
			}},
		},
	}
}

func requestDashboard(t *testing.T, server http.Handler, method, path string, headers http.Header) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	for key, values := range headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func decodeDashboardResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}
