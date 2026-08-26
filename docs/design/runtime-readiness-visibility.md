# Runtime Readiness Visibility

## Status

Implemented incrementally in v0.x. This document defines the observable
contract before the remaining integration and end-to-end coverage is added.

## Goal

Operators need one Runtime-level view of whether a warm pool has ready Pods.
They should not have to infer it by locating the controller-owned Deployment or
listing Pods. The same value must be available through Kubernetes, `krt runtime
list`, and `krt runtime get`.

## Contract

For a Runtime named `<name>`, the Runtime controller owns the Deployment named
`runtime-<name>` in the same namespace. It copies that Deployment's
`status.readyReplicas` into `Runtime.status.readyReplicas`.

- `readyReplicas` is the last observed Kubernetes Deployment ready-replica
  count. It is not the desired replica count, an independently probed health
  count, or a capacity value.
- A Deployment status change enqueues its owning Runtime. The controller reads
  the Deployment again and updates the Runtime status only when the value has
  changed. This makes a Pod becoming ready and a ready Pod becoming unavailable
  visible without relying on a periodic poll.
- The value is eventually consistent with Deployment status. Immediately after
  a spec update, rollout, or Pod event it can temporarily describe the last
  Deployment observation. It makes no availability or scheduling guarantee.
- The scheduler continues to evaluate individual Pod eligibility and capacity;
  it must not treat an aggregate `readyReplicas` value as permission to assign a
  Run.

When a Deployment reports no ready replicas, the Runtime reports `0`. Deleting
the Runtime also deletes its owned Deployment, so the Runtime status is not
used as a durable record after deletion.

## CLI and API visibility

`Runtime.status.readyReplicas` is part of the Runtime status API and is exposed
unchanged by structured `krt` output. Table output shows it as `READY` in
`krt runtime list` and as `Ready` in `krt runtime get`, beside the desired
`REPLICAS`/`Replicas` value. This keeps desired and observed counts distinct.

## Verification plan

1. Controller integration coverage verifies that Deployment status changes
   reconcile to the Runtime status for both an increase and a decrease.
2. CLI tests verify table and structured output preserve the observed value.
3. E2E creates a Runtime, waits for its ready count, then makes its Pod
   unavailable and verifies the count is updated before cleanup.

