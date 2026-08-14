package bash

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sessionEntry is Runtime Server-local state. runtimed owns the operation
// queue, so this entry only records the fenced workspace lifecycle.
type sessionEntry struct {
	mu       sync.RWMutex
	identity *pb.SessionIdentity
	workDir  string
	state    pb.SessionState
	activity time.Time
}

func (s *Server) RegisterSession(_ context.Context, req *pb.RegisterSessionRequest) (*pb.SessionStatus, error) {
	identity, workingDir, err := s.validateSessionRegistration(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	s.operationMu.Lock()
	defer s.operationMu.Unlock()

	if existing := s.session(identity.RunUid); existing != nil {
		if !existing.matches(identity) {
			return nil, status.Error(codes.FailedPrecondition, "session assignment is stale")
		}
		return existing.status(), nil
	}

	entry := &sessionEntry{
		identity: cloneSessionIdentity(identity),
		workDir:  workingDir,
		state:    pb.SessionState_SESSION_STATE_READY,
		activity: time.Now(),
	}
	s.mu.Lock()
	s.sessions[identity.RunUid] = entry
	s.mu.Unlock()
	return entry.status(), nil
}

func (s *Server) GetSessionStatus(_ context.Context, req *pb.GetSessionStatusRequest) (*pb.SessionStatus, error) {
	entry, err := s.matchSession(req.GetIdentity())
	if err != nil {
		return nil, err
	}
	return entry.status(), nil
}

func (s *Server) CloseSession(_ context.Context, req *pb.CloseSessionRequest) (*pb.CloseSessionResponse, error) {
	identity := req.GetIdentity()
	if err := validateSessionIdentity(identity); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	entry := s.session(identity.RunUid)
	if entry == nil {
		return &pb.CloseSessionResponse{Identity: cloneSessionIdentity(identity)}, nil
	}
	if !entry.matches(identity) {
		return nil, status.Error(codes.FailedPrecondition, "session assignment is stale")
	}
	entry.close()
	s.mu.Lock()
	if s.sessions[identity.RunUid] == entry {
		delete(s.sessions, identity.RunUid)
	}
	s.mu.Unlock()
	return &pb.CloseSessionResponse{Identity: cloneSessionIdentity(identity)}, nil
}

func (s *Server) validateSessionRegistration(req *pb.RegisterSessionRequest) (*pb.SessionIdentity, string, error) {
	if req == nil {
		return nil, "", fmt.Errorf("session registration is required")
	}
	if err := validateSessionIdentity(req.GetIdentity()); err != nil {
		return nil, "", err
	}
	workingDir, err := s.runtimeWorkingDir(req.WorkingDir)
	if err != nil {
		return nil, "", err
	}
	return cloneSessionIdentity(req.Identity), workingDir, nil
}

func validateSessionIdentity(identity *pb.SessionIdentity) error {
	if identity == nil || identity.RunUid == "" || identity.AssignedPodUid == "" {
		return fmt.Errorf("session run uid and assigned pod uid are required")
	}
	return nil
}

func (s *Server) session(runUID string) *sessionEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[runUID]
}

func (s *Server) matchSession(identity *pb.SessionIdentity) (*sessionEntry, error) {
	if err := validateSessionIdentity(identity); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	entry := s.session(identity.RunUid)
	if entry == nil {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	if !entry.matches(identity) {
		return nil, status.Error(codes.FailedPrecondition, "session assignment is stale")
	}
	return entry, nil
}

func (e *sessionEntry) matches(identity *pb.SessionIdentity) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.identity.RunUid == identity.RunUid && e.identity.AssignedPodUid == identity.AssignedPodUid
}

func (e *sessionEntry) status() *pb.SessionStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return &pb.SessionStatus{
		Identity:             cloneSessionIdentity(e.identity),
		State:                e.state,
		LastActivityUnixNano: e.activity.UnixNano(),
	}
}

func (e *sessionEntry) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state = pb.SessionState_SESSION_STATE_CLOSED
	e.activity = time.Now()
}

func cloneSessionIdentity(identity *pb.SessionIdentity) *pb.SessionIdentity {
	if identity == nil {
		return nil
	}
	return &pb.SessionIdentity{RunUid: identity.RunUid, AssignedPodUid: identity.AssignedPodUid}
}
