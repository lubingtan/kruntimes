package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func TestGatewayRejectsInvalidOperationShape(t *testing.T) {
	server := testServer(t, readySessionRun(), allowAuthorizer{}, &fakeDialer{client: &fakeSessionRuntimeClient{}})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/runtimes/bash/sessions/session-uid/operations:execute", strings.NewReader(`{"command":{"shell":"true"},"deleteFile":{"path":"x"}}`))
	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
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
