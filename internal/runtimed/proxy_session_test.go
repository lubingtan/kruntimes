package runtimed

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestSessionRuntimeProxyCallsLocalRuntimeServerForOwner(t *testing.T) {
	run := proxySessionRun("session-run", "bash", "pod-a", "pod-a-uid")
	reader := newSessionProxyReader(t, run)
	local := &sessionRuntimeClient{execute: func(_ context.Context, request *pb.ExecuteSessionOperationRequest) (*pb.ExecuteSessionOperationResponse, error) {
		if request.GetIdentity().GetRunUid() != string(run.UID) {
			t.Fatalf("Run UID = %q, want %q", request.GetIdentity().GetRunUid(), run.UID)
		}
		return &pb.ExecuteSessionOperationResponse{Command: &pb.SessionCommandResult{ExitCode: 0, Stdout: []byte("done")}}, nil
	}}
	proxy := newSessionRuntimeProxy(reader, local, run.Namespace, "bash", "pod-a", "9093")

	response, err := proxy.ExecuteSessionOperation(t.Context(), &pb.ExecuteSessionOperationRequest{
		Identity:  &pb.SessionIdentity{RunUid: string(run.UID), AssignedPodUid: "pod-a-uid"},
		Operation: &pb.ExecuteSessionOperationRequest_Command{Command: &pb.SessionCommand{Argv: []string{"echo", "done"}}},
	})
	if err != nil {
		t.Fatalf("ExecuteSessionOperation() error = %v", err)
	}
	if string(response.GetCommand().GetStdout()) != "done" {
		t.Fatalf("stdout = %q, want done", response.GetCommand().GetStdout())
	}
}

func TestSessionRuntimeProxyForwardsToAssignedRuntimePod(t *testing.T) {
	run := proxySessionRun("session-run", "bash", "pod-b", "pod-b-uid")
	owner := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-b",
			Namespace: run.Namespace,
			UID:       types.UID("pod-b-uid"),
			Labels:    map[string]string{"runtime": "bash"},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.2"},
	}
	reader := newSessionProxyReader(t, run, owner)
	peer := &sessionRuntimeClient{execute: func(ctx context.Context, _ *pb.ExecuteSessionOperationRequest) (*pb.ExecuteSessionOperationResponse, error) {
		values, ok := metadata.FromOutgoingContext(ctx)
		if !ok || len(values.Get(sessionForwardedMetadataKey)) == 0 {
			t.Fatal("forwarded request did not include the forwarding marker")
		}
		return &pb.ExecuteSessionOperationResponse{}, nil
	}}
	proxy := newSessionRuntimeProxy(reader, &sessionRuntimeClient{}, run.Namespace, "bash", "pod-a", "9093")
	proxy.dialPeer = func(_ context.Context, address string) (pb.SessionRuntimeClient, io.Closer, error) {
		if address != net.JoinHostPort(owner.Status.PodIP, "9093") {
			t.Fatalf("peer address = %q, want %q", address, net.JoinHostPort(owner.Status.PodIP, "9093"))
		}
		return peer, nopCloser{}, nil
	}

	_, err := proxy.ExecuteSessionOperation(t.Context(), &pb.ExecuteSessionOperationRequest{
		Identity:  &pb.SessionIdentity{RunUid: string(run.UID), AssignedPodUid: "pod-b-uid"},
		Operation: &pb.ExecuteSessionOperationRequest_Command{Command: &pb.SessionCommand{Argv: []string{"true"}}},
	})
	if err != nil {
		t.Fatalf("ExecuteSessionOperation() error = %v", err)
	}
}

func TestSessionRuntimeProxyRejectsOwnerFromAnotherRuntime(t *testing.T) {
	run := proxySessionRun("session-run", "bash", "pod-b", "pod-b-uid")
	owner := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-b",
			Namespace: run.Namespace,
			UID:       types.UID("pod-b-uid"),
			Labels:    map[string]string{"runtime": "python"},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.2"},
	}
	proxy := newSessionRuntimeProxy(newSessionProxyReader(t, run, owner), &sessionRuntimeClient{}, run.Namespace, "bash", "pod-a", "9093")
	proxy.dialPeer = func(context.Context, string) (pb.SessionRuntimeClient, io.Closer, error) {
		t.Fatal("dialPeer must not be called for a different Runtime")
		return nil, nil, nil
	}

	_, err := proxy.GetSessionStatus(t.Context(), &pb.GetSessionStatusRequest{
		Identity: &pb.SessionIdentity{RunUid: string(run.UID), AssignedPodUid: "pod-b-uid"},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("GetSessionStatus() code = %s, want %s", status.Code(err), codes.Unavailable)
	}
}

func TestSessionRuntimeProxyRejectsStaleAssignment(t *testing.T) {
	run := proxySessionRun("session-run", "bash", "pod-a", "pod-a-uid")
	proxy := newSessionRuntimeProxy(newSessionProxyReader(t, run), &sessionRuntimeClient{}, run.Namespace, "bash", "pod-a", "9093")

	_, err := proxy.GetSessionStatus(t.Context(), &pb.GetSessionStatusRequest{
		Identity: &pb.SessionIdentity{RunUid: string(run.UID), AssignedPodUid: "old-pod-uid"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetSessionStatus() code = %s, want %s", status.Code(err), codes.FailedPrecondition)
	}
}

func proxySessionRun(name, runtimeName, podName, podUID string) *v1alpha1.Run {
	return &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")},
		Spec: v1alpha1.RunSpec{
			Runtime: runtimeName,
			Mode:    v1alpha1.RunMode{Session: &v1alpha1.RunSessionMode{}},
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunReady, AssignedPod: podName, AssignedPodUID: podUID},
	}
}

func newSessionProxyReader(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kruntimes scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&v1alpha1.Run{}, sessionRunRuntimeIndexField, func(object client.Object) []string {
			run, ok := object.(*v1alpha1.Run)
			if !ok || run.Spec.Runtime == "" {
				return nil
			}
			return []string{run.Spec.Runtime}
		}).
		WithRuntimeObjects(objects...).
		Build()
}

type sessionRuntimeClient struct {
	execute func(context.Context, *pb.ExecuteSessionOperationRequest) (*pb.ExecuteSessionOperationResponse, error)
}

func (c *sessionRuntimeClient) RegisterSession(context.Context, *pb.RegisterSessionRequest, ...grpc.CallOption) (*pb.SessionStatus, error) {
	return nil, status.Error(codes.Unimplemented, "RegisterSession")
}

func (c *sessionRuntimeClient) GetSessionStatus(context.Context, *pb.GetSessionStatusRequest, ...grpc.CallOption) (*pb.SessionStatus, error) {
	return nil, status.Error(codes.Unimplemented, "GetSessionStatus")
}

func (c *sessionRuntimeClient) ExecuteSessionOperation(ctx context.Context, request *pb.ExecuteSessionOperationRequest, _ ...grpc.CallOption) (*pb.ExecuteSessionOperationResponse, error) {
	if c.execute == nil {
		return nil, status.Error(codes.Unimplemented, "ExecuteSessionOperation")
	}
	return c.execute(ctx, request)
}

func (c *sessionRuntimeClient) ReadSessionFile(context.Context, *pb.ReadSessionFileRequest, ...grpc.CallOption) (*pb.ReadSessionFileResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ReadSessionFile")
}

func (c *sessionRuntimeClient) ListSessionFiles(context.Context, *pb.ListSessionFilesRequest, ...grpc.CallOption) (*pb.ListSessionFilesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListSessionFiles")
}

func (c *sessionRuntimeClient) CloseSession(context.Context, *pb.CloseSessionRequest, ...grpc.CallOption) (*pb.CloseSessionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CloseSession")
}

func TestForwardedRecognizesIncomingMarker(t *testing.T) {
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(sessionForwardedMetadataKey, "true"))
	if !forwarded(ctx) {
		t.Fatal("forwarded() = false, want true")
	}
}
