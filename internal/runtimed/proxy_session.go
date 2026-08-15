package runtimed

import (
	"context"
	"fmt"
	"io"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const (
	sessionForwardedMetadataKey = "kruntimes-session-forwarded"
	sessionRunRuntimeIndexField = "spec.runtime"
)

// sessionRuntimeProxy serves gateway-originated SessionRuntime requests on a
// runtimed Pod. The owner Pod proxies accepted calls to its local Runtime
// Server; another Pod forwards the request once to that owner.
type sessionRuntimeProxy struct {
	pb.UnimplementedSessionRuntimeServer

	reader      client.Reader
	local       pb.SessionRuntimeClient
	namespace   string
	runtimeName string
	podName     string
	statusPort  string
	dialPeer    func(context.Context, string) (pb.SessionRuntimeClient, io.Closer, error)
}

func newSessionRuntimeProxy(
	reader client.Reader,
	local pb.SessionRuntimeClient,
	namespace, runtimeName, podName, statusPort string,
) *sessionRuntimeProxy {
	return &sessionRuntimeProxy{
		reader:      reader,
		local:       local,
		namespace:   namespace,
		runtimeName: runtimeName,
		podName:     podName,
		statusPort:  statusPort,
		dialPeer:    dialSessionRuntimePeer,
	}
}

func (s *sessionRuntimeProxy) GetSessionStatus(ctx context.Context, req *pb.GetSessionStatusRequest) (*pb.SessionStatus, error) {
	callCtx, client, closer, err := s.ownerClient(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return client.GetSessionStatus(callCtx, req)
}

func (s *sessionRuntimeProxy) ExecuteSessionOperation(ctx context.Context, req *pb.ExecuteSessionOperationRequest) (*pb.ExecuteSessionOperationResponse, error) {
	callCtx, client, closer, err := s.ownerClient(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return client.ExecuteSessionOperation(callCtx, req)
}

func (s *sessionRuntimeProxy) ReadSessionFile(ctx context.Context, req *pb.ReadSessionFileRequest) (*pb.ReadSessionFileResponse, error) {
	callCtx, client, closer, err := s.ownerClient(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return client.ReadSessionFile(callCtx, req)
}

func (s *sessionRuntimeProxy) ListSessionFiles(ctx context.Context, req *pb.ListSessionFilesRequest) (*pb.ListSessionFilesResponse, error) {
	callCtx, client, closer, err := s.ownerClient(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return client.ListSessionFiles(callCtx, req)
}

func (s *sessionRuntimeProxy) ownerClient(ctx context.Context, identity *pb.SessionIdentity) (context.Context, pb.SessionRuntimeClient, io.Closer, error) {
	run, err := s.sessionRun(ctx, identity)
	if err != nil {
		return nil, nil, nil, err
	}
	if run.Status.AssignedPod == s.podName {
		return ctx, s.local, nopCloser{}, nil
	}
	if forwarded(ctx) {
		return nil, nil, nil, status.Error(codes.FailedPrecondition, "forwarded session request did not reach its assigned Runtime Pod")
	}

	owner := &corev1.Pod{}
	ownerKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Status.AssignedPod}
	if err := s.reader.Get(ctx, ownerKey, owner); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil, status.Error(codes.Unavailable, "assigned Runtime Pod is not available")
		}
		return nil, nil, nil, status.Errorf(codes.Internal, "get assigned Runtime Pod: %v", err)
	}
	if string(owner.UID) != identity.GetAssignedPodUid() ||
		owner.Labels["runtime"] != s.runtimeName ||
		owner.Status.PodIP == "" {
		return nil, nil, nil, status.Error(codes.Unavailable, "assigned Runtime Pod is not available")
	}

	forwardedCtx := withForwardedMarker(ctx)
	peer, closer, err := s.dialPeer(forwardedCtx, net.JoinHostPort(owner.Status.PodIP, s.statusPort))
	if err != nil {
		return nil, nil, nil, status.Errorf(codes.Unavailable, "dial assigned Runtime Pod: %v", err)
	}
	return forwardedCtx, peer, closer, nil
}

func (s *sessionRuntimeProxy) sessionRun(ctx context.Context, identity *pb.SessionIdentity) (*v1alpha1.Run, error) {
	if identity == nil || identity.GetRunUid() == "" || identity.GetAssignedPodUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "session run uid and assigned pod uid are required")
	}
	if s.reader == nil || s.local == nil || s.namespace == "" || s.runtimeName == "" || s.podName == "" {
		return nil, status.Error(codes.FailedPrecondition, "SessionRuntime proxy is not configured")
	}

	var runs v1alpha1.RunList
	if err := s.reader.List(ctx, &runs,
		client.InNamespace(s.namespace),
		client.MatchingFields{sessionRunRuntimeIndexField: s.runtimeName},
	); err != nil {
		return nil, status.Errorf(codes.Internal, "list Session Runs: %v", err)
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if string(run.UID) != identity.GetRunUid() {
			continue
		}
		if run.Spec.Runtime != s.runtimeName || run.Spec.Mode.Session == nil {
			return nil, status.Error(codes.NotFound, "session Run is not served by this Runtime")
		}
		if run.Status.AssignedPodUID != identity.GetAssignedPodUid() || run.Status.AssignedPod == "" {
			return nil, status.Error(codes.FailedPrecondition, "session assignment is stale")
		}
		if run.Status.Phase != v1alpha1.RunReady {
			return nil, status.Errorf(codes.FailedPrecondition, "session Run is %s, not Ready", run.Status.Phase)
		}
		return run, nil
	}
	return nil, status.Error(codes.NotFound, "session Run not found")
}

func forwarded(ctx context.Context) bool {
	values := metadata.ValueFromIncomingContext(ctx, sessionForwardedMetadataKey)
	return len(values) > 0
}

func withForwardedMarker(ctx context.Context) context.Context {
	values, _ := metadata.FromIncomingContext(ctx)
	forwardedValues := values.Copy()
	forwardedValues.Append(sessionForwardedMetadataKey, "true")
	return metadata.NewOutgoingContext(ctx, forwardedValues)
}

func dialSessionRuntimePeer(ctx context.Context, address string) (pb.SessionRuntimeClient, io.Closer, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("create SessionRuntime client: %w", err)
	}
	return pb.NewSessionRuntimeClient(connection), connection, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }
