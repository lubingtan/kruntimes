package runtimed

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

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
	operations  *SessionOperationQueue
	dialPeer    func(context.Context, string) (pb.SessionRuntimeClient, io.Closer, error)
	logWriter   io.Writer
	logMu       sync.Mutex
}

type sessionRoute struct {
	run    *v1alpha1.Run
	ctx    context.Context
	client pb.SessionRuntimeClient
	closer io.Closer
	owner  bool
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
		operations:  NewSessionOperationQueue(0, 0),
		dialPeer:    dialSessionRuntimePeer,
		logWriter:   os.Stdout,
	}
}

func (s *sessionRuntimeProxy) GetSessionStatus(ctx context.Context, req *pb.GetSessionStatusRequest) (*pb.SessionStatus, error) {
	route, err := s.route(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer route.closer.Close()
	return route.client.GetSessionStatus(route.ctx, req)
}

func (s *sessionRuntimeProxy) ExecuteSessionOperation(ctx context.Context, req *pb.ExecuteSessionOperationRequest) (*pb.ExecuteSessionOperationResponse, error) {
	route, err := s.route(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer route.closer.Close()
	if !route.owner {
		return route.client.ExecuteSessionOperation(route.ctx, req)
	}
	started := time.Now()
	response, operationErr := s.operations.Execute(route.ctx, route.run, func(operationCtx context.Context) (*pb.ExecuteSessionOperationResponse, error) {
		return route.client.ExecuteSessionOperation(operationCtx, req)
	})
	s.emitSessionOperationLog(route.run, req, response, operationErr, time.Since(started))
	return response, operationErr
}

func (s *sessionRuntimeProxy) emitSessionOperationLog(
	run *v1alpha1.Run,
	request *pb.ExecuteSessionOperationRequest,
	response *pb.ExecuteSessionOperationResponse,
	operationErr error,
	duration time.Duration,
) {
	if run == nil || s.logWriter == nil {
		return
	}
	operation := sessionOperationName(request)
	outcome, statusCode := sessionOperationOutcome(response, operationErr)
	var exitCode *int32
	timedOut := false
	if command := response.GetCommand(); command != nil {
		exitCode = &command.ExitCode
		timedOut = command.TimedOut
	}

	s.logMu.Lock()
	defer s.logMu.Unlock()
	if command := response.GetCommand(); command != nil {
		s.emitSessionStream(run, "stdout", string(command.Stdout), operation, outcome, statusCode, exitCode, timedOut, duration)
		s.emitSessionStream(run, "stderr", string(command.Stderr), operation, outcome, statusCode, exitCode, timedOut, duration)
	}
	audit := executionLogLineFor(run, s.podName, "audit", "session operation completed")
	audit.Operation = operation
	audit.Outcome = outcome
	audit.StatusCode = statusCode
	audit.ExitCode = exitCode
	audit.TimedOut = timedOut
	audit.DurationMilliseconds = duration.Milliseconds()
	writeExecutionLogLine(s.logWriter, audit)
}

func (s *sessionRuntimeProxy) emitSessionStream(
	run *v1alpha1.Run,
	stream, content, operation, outcome, statusCode string,
	exitCode *int32,
	timedOut bool,
	duration time.Duration,
) {
	for _, message := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
		if message == "" {
			continue
		}
		line := executionLogLineFor(run, s.podName, stream, strings.TrimSuffix(message, "\r"))
		line.Operation = operation
		line.Outcome = outcome
		line.StatusCode = statusCode
		line.ExitCode = exitCode
		line.TimedOut = timedOut
		line.DurationMilliseconds = duration.Milliseconds()
		writeExecutionLogLine(s.logWriter, line)
	}
}

func sessionOperationName(request *pb.ExecuteSessionOperationRequest) string {
	switch {
	case request.GetCommand() != nil:
		return "command"
	case request.GetWriteFile() != nil:
		return "write_file"
	case request.GetCreateDirectory() != nil:
		return "create_directory"
	case request.GetDeleteFile() != nil:
		return "delete_file"
	case request.GetRenameFile() != nil:
		return "rename_file"
	default:
		return "unknown"
	}
}

func sessionOperationOutcome(response *pb.ExecuteSessionOperationResponse, operationErr error) (string, string) {
	if operationErr != nil {
		code := status.Code(operationErr)
		switch code {
		case codes.Canceled:
			return "cancelled", code.String()
		case codes.DeadlineExceeded:
			return "timed_out", code.String()
		default:
			return "failed", code.String()
		}
	}
	if command := response.GetCommand(); command != nil {
		switch {
		case command.TimedOut:
			return "timed_out", ""
		case command.ExitCode != 0:
			return "failed", ""
		}
	}
	return "succeeded", ""
}

func (s *sessionRuntimeProxy) ReadSessionFile(ctx context.Context, req *pb.ReadSessionFileRequest) (*pb.ReadSessionFileResponse, error) {
	route, err := s.route(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer route.closer.Close()
	return route.client.ReadSessionFile(route.ctx, req)
}

func (s *sessionRuntimeProxy) ListSessionFiles(ctx context.Context, req *pb.ListSessionFilesRequest) (*pb.ListSessionFilesResponse, error) {
	route, err := s.route(ctx, req.GetIdentity())
	if err != nil {
		return nil, err
	}
	defer route.closer.Close()
	return route.client.ListSessionFiles(route.ctx, req)
}

func (s *sessionRuntimeProxy) route(ctx context.Context, identity *pb.SessionIdentity) (sessionRoute, error) {
	run, err := s.sessionRun(ctx, identity)
	if err != nil {
		return sessionRoute{}, err
	}
	if run.Status.AssignedPod == s.podName {
		return sessionRoute{run: run, ctx: ctx, client: s.local, closer: nopCloser{}, owner: true}, nil
	}
	if forwarded(ctx) {
		return sessionRoute{}, status.Error(codes.FailedPrecondition, "forwarded session request did not reach its assigned Runtime Pod")
	}

	owner := &corev1.Pod{}
	ownerKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Status.AssignedPod}
	if err := s.reader.Get(ctx, ownerKey, owner); err != nil {
		if apierrors.IsNotFound(err) {
			return sessionRoute{}, status.Error(codes.Unavailable, "assigned Runtime Pod is not available")
		}
		return sessionRoute{}, status.Errorf(codes.Internal, "get assigned Runtime Pod: %v", err)
	}
	if string(owner.UID) != identity.GetAssignedPodUid() ||
		owner.Labels["runtime"] != s.runtimeName ||
		owner.Status.PodIP == "" {
		return sessionRoute{}, status.Error(codes.Unavailable, "assigned Runtime Pod is not available")
	}

	forwardedCtx := withForwardedMarker(ctx)
	peer, closer, err := s.dialPeer(forwardedCtx, net.JoinHostPort(owner.Status.PodIP, s.statusPort))
	if err != nil {
		return sessionRoute{}, status.Errorf(codes.Unavailable, "dial assigned Runtime Pod: %v", err)
	}
	return sessionRoute{run: run, ctx: forwardedCtx, client: peer, closer: closer}, nil
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
