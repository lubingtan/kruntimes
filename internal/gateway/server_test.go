package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestGatewayGetsSessionThroughRuntimeService(t *testing.T) {
	run := readySessionRun()
	client := &fakeSessionRuntimeClient{status: func(_ context.Context, request *pb.GetSessionStatusRequest, _ ...grpc.CallOption) (*pb.SessionStatus, error) {
		if request.GetIdentity().GetRunUid() != string(run.UID) || request.GetIdentity().GetAssignedPodUid() != "pod-uid" {
			t.Fatalf("identity = %#v", request.GetIdentity())
		}
		return &pb.SessionStatus{State: pb.SessionState_SESSION_STATE_READY}, nil
	}}
	dialer := &fakeDialer{client: client}
	server := testServer(t, run, allowAuthorizer{}, dialer)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/namespaces/default/runtimes/bash/sessions/session-uid", nil)
	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if dialer.address != "runtime-bash.default:9093" {
		t.Fatalf("Runtime Service address = %q", dialer.address)
	}
	if !strings.Contains(response.Body.String(), `"state":"SESSION_STATE_READY"`) {
		t.Fatalf("response = %s", response.Body.String())
	}
}

func TestGatewayRejectsUnauthorizedRequestBeforeDialingRuntime(t *testing.T) {
	dialer := &fakeDialer{client: &fakeSessionRuntimeClient{}}
	server := testServer(t, readySessionRun(), denyAuthorizer{}, dialer)

	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/namespaces/default/runtimes/bash/sessions/session-uid", nil))

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if dialer.address != "" {
		t.Fatalf("dialed Runtime Service %q for denied request", dialer.address)
	}
}

func TestGatewayExecutesExactlyOneOperation(t *testing.T) {
	run := readySessionRun()
	client := &fakeSessionRuntimeClient{execute: func(_ context.Context, request *pb.ExecuteSessionOperationRequest, _ ...grpc.CallOption) (*pb.ExecuteSessionOperationResponse, error) {
		if got := request.GetCommand().GetArgv(); len(got) != 2 || got[0] != "echo" || got[1] != "hello" {
			t.Fatalf("command = %#v", request.GetCommand())
		}
		return &pb.ExecuteSessionOperationResponse{Command: &pb.SessionCommandResult{ExitCode: 0, Stdout: []byte("hello\n")}}, nil
	}}
	server := testServer(t, run, allowAuthorizer{}, &fakeDialer{client: client})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/runtimes/bash/sessions/session-uid/operations:execute", strings.NewReader(`{"command":{"argv":["echo","hello"]}}`))
	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"stdout":"aGVsbG8K"`) {
		t.Fatalf("response = %s", response.Body.String())
	}
}

func TestGatewayRejectsRequestBodyOverConfiguredLimitBeforeDialingRuntime(t *testing.T) {
	dialer := &fakeDialer{client: &fakeSessionRuntimeClient{}}
	server := testServer(t, readySessionRun(), allowAuthorizer{}, dialer)
	server.MaxRequestBodyBytes = 64
	payload := `{"command":{"argv":["echo","` + strings.Repeat("x", 80) + `"]}}`

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/runtimes/bash/sessions/session-uid/operations:execute", strings.NewReader(payload))
	server.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if dialer.address != "" {
		t.Fatalf("dialed Runtime Service %q for oversized request", dialer.address)
	}
}

func TestGatewayRejectsResponseOverConfiguredLimitWithoutPartialJSON(t *testing.T) {
	client := &fakeSessionRuntimeClient{execute: func(_ context.Context, _ *pb.ExecuteSessionOperationRequest, _ ...grpc.CallOption) (*pb.ExecuteSessionOperationResponse, error) {
		return &pb.ExecuteSessionOperationResponse{Command: &pb.SessionCommandResult{ExitCode: 0, Stdout: []byte(strings.Repeat("x", 128))}}, nil
	}}
	server := testServer(t, readySessionRun(), allowAuthorizer{}, &fakeDialer{client: client})
	server.MaxResponseBodyBytes = 64

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/runtimes/bash/sessions/session-uid/operations:execute", strings.NewReader(`{"command":{"argv":["true"]}}`))
	server.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got, want := response.Body.String(), "{\"error\":\"gateway response exceeds configured limit\"}\n"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
	if strings.Contains(response.Body.String(), "stdout") {
		t.Fatalf("response contains partial successful JSON: %s", response.Body.String())
	}
}

func TestGatewayListsSessionFilesInPages(t *testing.T) {
	run := readySessionRun()
	client := &fakeSessionRuntimeClient{list: func(_ context.Context, request *pb.ListSessionFilesRequest, _ ...grpc.CallOption) (*pb.ListSessionFilesResponse, error) {
		if request.GetIdentity().GetRunUid() != string(run.UID) || request.GetPath() != "notes" || request.GetLimit() != 2 || request.GetPageToken() != "after-notes" {
			t.Fatalf("list request = %#v", request)
		}
		return &pb.ListSessionFilesResponse{
			Entries:       []*pb.SessionFileInfo{{Path: "build.log", SizeBytes: 12}},
			NextPageToken: "next-notes",
		}, nil
	}}
	server := testServer(t, run, allowAuthorizer{}, &fakeDialer{client: client})

	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/namespaces/default/runtimes/bash/sessions/session-uid/files?path=notes&limit=2&pageToken=after-notes", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got, want := response.Body.String(), `{"entries":[{"path":"build.log","directory":false,"sizeBytes":12}],"nextPageToken":"next-notes"}`+"\n"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}

	invalid := httptest.NewRecorder()
	server.ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, "/v1/namespaces/default/runtimes/bash/sessions/session-uid/files?limit=0", nil))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid limit status = %d, body = %s", invalid.Code, invalid.Body.String())
	}
}

func TestGatewayRejectsInvalidOperationShape(t *testing.T) {
	server := testServer(t, readySessionRun(), allowAuthorizer{}, &fakeDialer{client: &fakeSessionRuntimeClient{}})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/runtimes/bash/sessions/session-uid/operations:execute", strings.NewReader(`{"command":{"shell":"true"},"deleteFile":{"path":"x"}}`))
	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestGatewayLimitsConcurrentRequests(t *testing.T) {
	run := readySessionRun()
	started := make(chan struct{})
	release := make(chan struct{})
	client := &fakeSessionRuntimeClient{status: func(_ context.Context, _ *pb.GetSessionStatusRequest, _ ...grpc.CallOption) (*pb.SessionStatus, error) {
		close(started)
		<-release
		return &pb.SessionStatus{State: pb.SessionState_SESSION_STATE_READY}, nil
	}}
	server := testServer(t, run, allowAuthorizer{}, &fakeDialer{client: client})
	server.MaxConcurrentRequests = 1

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/namespaces/default/runtimes/bash/sessions/session-uid", nil))
		firstDone <- response
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach the Runtime Service")
	}

	health := httptest.NewRecorder()
	server.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", health.Code, http.StatusOK)
	}

	second := httptest.NewRecorder()
	server.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/v1/namespaces/default/runtimes/bash/sessions/session-uid", nil))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, body = %s", second.Code, second.Body.String())
	}

	close(release)
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK {
			t.Fatalf("first request status = %d, body = %s", first.Code, first.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
}

func TestGatewayUsesConfiguredHeaderLimit(t *testing.T) {
	server := &Server{MaxHeaderBytes: 4096}
	if got := server.httpServer().MaxHeaderBytes; got != 4096 {
		t.Fatalf("MaxHeaderBytes = %d, want 4096", got)
	}
	if got := (&Server{}).httpServer().MaxHeaderBytes; got != DefaultMaxHeaderBytes {
		t.Fatalf("default MaxHeaderBytes = %d, want %d", got, DefaultMaxHeaderBytes)
	}
}

func TestGatewayTLSConfigRejectsIncompleteFiles(t *testing.T) {
	for _, server := range []*Server{
		{TLSCertificateFile: "certificate.pem"},
		{TLSPrivateKeyFile: "key.pem"},
	} {
		if _, err := server.tlsConfig(); err == nil || !strings.Contains(err.Error(), "both Runtime gateway TLS certificate and private key files are required") {
			t.Fatalf("tlsConfig() error = %v, want incomplete TLS file error", err)
		}
	}
}

func TestGatewayTLSConfigRejectsInvalidCertificate(t *testing.T) {
	directory := t.TempDir()
	certificate := directory + "/tls.crt"
	privateKey := directory + "/tls.key"
	if err := os.WriteFile(certificate, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(privateKey, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &Server{TLSCertificateFile: certificate, TLSPrivateKeyFile: privateKey}
	if _, err := server.tlsConfig(); err == nil || !strings.Contains(err.Error(), "load Runtime gateway TLS certificate") {
		t.Fatalf("tlsConfig() error = %v, want certificate load error", err)
	}
}

func testServer(t *testing.T, run *v1alpha1.Run, authorizer Authorizer, dialer SessionRuntimeDialer) *Server {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithIndex(&v1alpha1.Run{}, runtimeIndexField, func(object client.Object) []string {
		return []string{object.(*v1alpha1.Run).Spec.Runtime}
	}).WithObjects(run).Build()
	return &Server{Runs: reader, Authorizer: authorizer, Dialer: dialer}
}

func readySessionRun() *v1alpha1.Run {
	return &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "session", Namespace: "default", UID: types.UID("session-uid")},
		Spec:       v1alpha1.RunSpec{Runtime: "bash", Mode: v1alpha1.RunMode{Session: &v1alpha1.RunSessionMode{}}},
		Status:     v1alpha1.RunStatus{Phase: v1alpha1.RunReady, AssignedPod: "runtime-pod", AssignedPodUID: "pod-uid"},
	}
}

type allowAuthorizer struct{}

func (allowAuthorizer) Authorize(context.Context, *http.Request, *v1alpha1.Run) error { return nil }

type denyAuthorizer struct{}

func (denyAuthorizer) Authorize(context.Context, *http.Request, *v1alpha1.Run) error {
	return status.Error(codes.PermissionDenied, "denied")
}

type fakeDialer struct {
	client  pb.SessionRuntimeClient
	address string
}

func (d *fakeDialer) Dial(_ context.Context, address string) (pb.SessionRuntimeClient, io.Closer, error) {
	d.address = address
	return d.client, nopCloser{}, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

type fakeSessionRuntimeClient struct {
	pb.SessionRuntimeClient
	status  func(context.Context, *pb.GetSessionStatusRequest, ...grpc.CallOption) (*pb.SessionStatus, error)
	execute func(context.Context, *pb.ExecuteSessionOperationRequest, ...grpc.CallOption) (*pb.ExecuteSessionOperationResponse, error)
	list    func(context.Context, *pb.ListSessionFilesRequest, ...grpc.CallOption) (*pb.ListSessionFilesResponse, error)
}

func (c *fakeSessionRuntimeClient) GetSessionStatus(ctx context.Context, request *pb.GetSessionStatusRequest, options ...grpc.CallOption) (*pb.SessionStatus, error) {
	if c.status == nil {
		return nil, status.Error(codes.Unimplemented, "GetSessionStatus")
	}
	return c.status(ctx, request, options...)
}
func (c *fakeSessionRuntimeClient) ExecuteSessionOperation(ctx context.Context, request *pb.ExecuteSessionOperationRequest, options ...grpc.CallOption) (*pb.ExecuteSessionOperationResponse, error) {
	if c.execute == nil {
		return nil, status.Error(codes.Unimplemented, "ExecuteSessionOperation")
	}
	return c.execute(ctx, request, options...)
}

func (c *fakeSessionRuntimeClient) ListSessionFiles(ctx context.Context, request *pb.ListSessionFilesRequest, options ...grpc.CallOption) (*pb.ListSessionFilesResponse, error) {
	if c.list == nil {
		return nil, status.Error(codes.Unimplemented, "ListSessionFiles")
	}
	return c.list(ctx, request, options...)
}
