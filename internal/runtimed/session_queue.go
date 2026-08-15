package runtimed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const (
	defaultSessionQueueSize        = 32
	defaultSessionOperationTimeout = 5 * time.Minute
)

// SessionOperationQueue serializes mutating operations for each Session Run.
// The owning runtimed is its sole caller. A Run-specific limit can only reduce
// the process-wide queue and timeout limits supplied at construction.
type SessionOperationQueue struct {
	mu                  sync.Mutex
	maxQueueSize        int
	maxOperationTimeout time.Duration
	sessions            map[string]*sessionOperationQueueEntry
}

type sessionOperationQueueEntry struct {
	jobs   chan sessionOperationJob
	closed bool

	mu           sync.Mutex
	activeCancel context.CancelFunc
}

type sessionOperationJob struct {
	ctx     context.Context
	execute func(context.Context) (*pb.ExecuteSessionOperationResponse, error)
	result  chan sessionOperationResult
}

type sessionOperationResult struct {
	response *pb.ExecuteSessionOperationResponse
	err      error
}

// NewSessionOperationQueue creates the per-runtimed operation queue manager.
// Non-positive limits use the v0 defaults.
func NewSessionOperationQueue(maxQueueSize int, maxOperationTimeout time.Duration) *SessionOperationQueue {
	if maxQueueSize <= 0 {
		maxQueueSize = defaultSessionQueueSize
	}
	if maxOperationTimeout <= 0 {
		maxOperationTimeout = defaultSessionOperationTimeout
	}
	return &SessionOperationQueue{
		maxQueueSize:        maxQueueSize,
		maxOperationTimeout: maxOperationTimeout,
		sessions:            make(map[string]*sessionOperationQueueEntry),
	}
}

// Execute waits for the Session Run's FIFO turn, then invokes execute with the
// effective per-operation deadline. The caller's context can cancel work while
// it is queued or running.
func (q *SessionOperationQueue) Execute(
	ctx context.Context,
	run *v1alpha1.Run,
	execute func(context.Context) (*pb.ExecuteSessionOperationResponse, error),
) (*pb.ExecuteSessionOperationResponse, error) {
	if q == nil {
		return nil, status.Error(codes.FailedPrecondition, "Session operation queue is not configured")
	}
	if ctx == nil {
		return nil, status.Error(codes.InvalidArgument, "operation context is required")
	}
	if execute == nil {
		return nil, status.Error(codes.InvalidArgument, "session operation is required")
	}
	uid, queueSize, timeout, err := q.limits(run)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	q.mu.Lock()
	entry := q.sessions[uid]
	if entry == nil {
		entry = &sessionOperationQueueEntry{jobs: make(chan sessionOperationJob, queueSize)}
		q.sessions[uid] = entry
		go entry.run(timeout)
	}
	if entry.closed {
		q.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "session is closing")
	}
	job := sessionOperationJob{
		ctx:     ctx,
		execute: execute,
		result:  make(chan sessionOperationResult, 1),
	}
	select {
	case entry.jobs <- job:
		q.mu.Unlock()
	default:
		q.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "session operation queue is full")
	}

	select {
	case result := <-job.result:
		return result.response, result.err
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// Close prevents new work for a Session Run, cancels the active operation, and
// rejects each queued operation. It is idempotent and is called before runtimed
// closes the corresponding Runtime Server session.
func (q *SessionOperationQueue) Close(runUID string) {
	if q == nil || runUID == "" {
		return
	}
	q.mu.Lock()
	entry := q.sessions[runUID]
	if entry == nil || entry.isClosed() {
		q.mu.Unlock()
		return
	}
	entry.markClosed()
	delete(q.sessions, runUID)
	close(entry.jobs)
	q.mu.Unlock()
	entry.cancelActive()
}

func (q *SessionOperationQueue) limits(run *v1alpha1.Run) (string, int, time.Duration, error) {
	if run == nil || run.UID == "" || run.Spec.Mode.Session == nil {
		return "", 0, 0, fmt.Errorf("session Run is required")
	}
	queueSize := q.maxQueueSize
	if limit := run.Spec.Mode.Session.QueueSize; limit != nil {
		if *limit <= 0 {
			return "", 0, 0, fmt.Errorf("session queue size must be positive")
		}
		queueSize = min(queueSize, int(*limit))
	}
	timeout := q.maxOperationTimeout
	if limit := run.Spec.Mode.Session.OperationTimeout; limit != nil {
		if limit.Duration <= 0 {
			return "", 0, 0, fmt.Errorf("session operation timeout must be positive")
		}
		timeout = min(timeout, limit.Duration)
	}
	return string(run.UID), queueSize, timeout, nil
}

func (e *sessionOperationQueueEntry) run(timeout time.Duration) {
	for job := range e.jobs {
		if e.isClosed() {
			e.deliver(job, nil, status.Error(codes.Canceled, "session closed"))
			continue
		}
		select {
		case <-job.ctx.Done():
			e.deliver(job, nil, status.FromContextError(job.ctx.Err()).Err())
			continue
		default:
		}

		operationCtx, cancel := context.WithTimeout(job.ctx, timeout)
		if !e.setActive(cancel) {
			cancel()
			e.deliver(job, nil, status.Error(codes.Canceled, "session closed"))
			continue
		}
		response, err := job.execute(operationCtx)
		operationErr := operationCtx.Err()
		cancel()
		e.clearActive()
		if operationErr != nil && (err == nil || errors.Is(err, operationErr)) {
			if response == nil || response.GetCommand() == nil || !response.GetCommand().GetTimedOut() {
				err = status.FromContextError(operationErr).Err()
			}
		}
		e.deliver(job, response, err)
	}
}

func (e *sessionOperationQueueEntry) isClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

func (e *sessionOperationQueueEntry) markClosed() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
}

func (e *sessionOperationQueueEntry) setActive(cancel context.CancelFunc) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	e.activeCancel = cancel
	return true
}

func (e *sessionOperationQueueEntry) clearActive() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activeCancel = nil
}

func (e *sessionOperationQueueEntry) cancelActive() {
	e.mu.Lock()
	cancel := e.activeCancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (e *sessionOperationQueueEntry) deliver(job sessionOperationJob, response *pb.ExecuteSessionOperationResponse, err error) {
	job.result <- sessionOperationResult{response: response, err: err}
}
