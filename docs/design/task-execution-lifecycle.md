# Task Execution Lifecycle

Status: **Implemented**

## Purpose

Task-mode Runs execute one bounded command or script on an assigned warm Runtime
Pod. This document defines the boundary between local asynchronous work in
runtimed and Kubernetes control-plane state.

The invariant is simple: **only reconciliation updates `Run.status`**. A local
worker must never read a Run directly to bypass an informer cache, and must
never update Run status itself.

## Lifecycle

After scheduler assignment, the owning runtimed handles a Task Run as follows:

1. A cached `Scheduled` Run is claimed and updated to `Running`.
2. Source preparation happens before execution starts. A preparation error is
   applied by that same reconciliation as the normal Run failure path.
3. A later reconciliation of the cached `Running` Run calls Runtime Server
   `Status`.
4. When `Status` returns `NOT_FOUND` and no execution has been observed:
   - an asynchronous worker starts artifact input staging and Runtime Server
     `Execute` if another start is not already in flight;
   - otherwise runtimed requeues the Run and waits.
5. When Runtime Server reports `PENDING` or `RUNNING`, runtimed records that
   the execution exists and adds it to the Run Lifecycle Event Generator
   (RLEG).
6. `SUCCEEDED` and `FAILED` are applied by reconciliation using the ordinary
   output, artifact, retry, and terminal-state rules.

Once an execution has been observed, a later Runtime Server `NOT_FOUND` is an
`ExecutionLost` failure. This distinguishes an execution that has not yet been
created from one that disappeared after being accepted.

## Asynchronous Start Boundary

Artifact input staging and `Execute` may block on storage or the local Runtime
Server, so they run asynchronously. The worker only performs local work:

- On success, it records completion locally and enqueues the Run.
- On failure, it records the failure locally, emits an Event and a log entry,
  and enqueues the Run.
- If the Run is no longer locally active when `Execute` succeeds, runtimed
  forgets the created Runtime execution instead of reviving the Run.

The next reconciliation consumes a recorded start failure and applies it using
the normal retry policy. This includes failures while staging artifact inputs:
they are reported as Runtime execution-start failures, but no asynchronous
worker writes `Run.status`.

## Retry

When a Task attempt fails but remains retryable, runtimed first persists the
retry status and forgets that terminal Runtime execution. When retry backoff
expires, runtimed clears the previous attempt's local execution observation,
changes the `Running` condition back to true, and requeues. It
does not invoke `Execute` from that status-update call. The subsequent cached
`Running` reconciliation observes Runtime Server `NOT_FOUND` and starts the
next execution attempt. Forgetting the terminal result is necessary because a
Runtime Server keys executions by immutable Run UID; otherwise the next
reconciliation would mistake the previous attempt's `FAILED` result for the
new attempt.

This structure avoids racing an asynchronous local operation against the
controller cache immediately after a status update, while retaining bounded
reconciliation and the existing at-least-once execution semantics.
