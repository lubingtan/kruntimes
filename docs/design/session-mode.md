# Session Mode for Agent Sandboxes

Status: **Accepted; implementation in progress**

## Problem

`Run.mode.function` is a fixed-handler RPC model. It is appropriate for FaaS
and tool endpoints, but it is not an agent sandbox: an agent sandbox needs a
mutable workspace, arbitrary commands, file operations, ordered multi-step
work, and a session lifecycle.

Session mode uses the existing `Run` lifecycle object rather than a new
`Sandbox` CRD. A session Run reserves warm Runtime capacity and exposes one
stateful execution environment until it is closed, expires, fails, or is
deleted.

## Run API

`Run.spec.mode` is exactly one of `task`, `function`, or `session`:

```go
type RunMode struct {
    Task     *RunTaskMode     `json:"task,omitempty"`
    Function *RunFunctionMode `json:"function,omitempty"`
    Session  *RunSessionMode  `json:"session,omitempty"`
}

type RunSessionMode struct {
    IdleTimeoutSeconds *int32 `json:"idleTimeoutSeconds,omitempty"`
    QueueSize          *int32 `json:"queueSize,omitempty"`
    OperationTimeout   *metav1.Duration `json:"operationTimeout,omitempty"`
}
```

Session Runs use the existing `Pending -> Scheduled -> Running -> Ready`
lifecycle. `Ready` means the owning runtimed has registered a session with the
local Runtime Server and accepts session operations. It remains active and
holds Runtime capacity.

For v0, configure a Runtime used for sessions with effective `runs: 1`
capacity. The scheduler continues to apply only generic resource capacity
accounting. runtimed is the authoritative local gate: it atomically refuses to
claim a Session while any Run is active, and refuses to claim any Run while a
Session is active. This prevents mixed execution even during local races or a
misconfigured Runtime; a Run rejected by that gate remains `Scheduled` until
the Runtime Pod becomes available.

`source` and `artifactInputs` initialize the session before it becomes Ready.
They are not command definitions. Session Runs must not set `spec.workspace`:
v0 creates one Run-UID-scoped ephemeral workspace on the assigned Runtime Pod.
Files, installed dependencies, and process-visible state persist across session
operations on that Pod. Pod loss fails the session; v0 has no checkpoint,
resume, or transparent migration promise.

`Run.spec.timeout` bounds the entire reservation. `idleTimeoutSeconds` expires
the session after no accepted mutation or command activity. A Session Run uses
the normal Run cancellation, deletion, TTL, authorization, endpoint, and
assignment-UID fencing rules. Registration can retry before `Ready`, when no
usable session state exists. Once Ready, an assigned-Pod loss is terminal: the
client must create a new Session Run rather than silently continuing in an
empty workspace.

## Services and Request Flow

The following terms are distinct:

- **Runtime gateway** is one shared `runtime-gateway` Deployment installed by
  the Helm chart when `gateway.enabled` is true. Its Pods run the gateway server
  for all Runtimes in the cluster. It is stateless: it does not own a session,
  queue operations, or call a Runtime Server directly.
- **Runtime gateway Service** is the shared Kubernetes `ClusterIP` Service for
  the Runtime gateway Deployment. It is the stable HTTP address carried in
  `Run.status.endpoint`.
- **Runtime gateway server** is the HTTP server in each Runtime gateway Pod.
  Kubernetes Services only forward traffic; they cannot translate HTTP requests
  to gRPC. The gateway server implements the HTTP API and translates each
  request into an internal `SessionGateway` gRPC call. It maintains only a
  watch-backed list of ready runtimed endpoints for each Runtime; it does not
  cache Session ownership or operation state.
- **runtimed** runs in every Runtime Pod. It implements the versioned
  `SessionGateway` gRPC service and maintains a watch-backed ownership cache.
- **Ingress runtimed** is the ready runtimed for the Runtime named in the HTTP
  request that the gateway server selects for one request. It may or may not
  own the requested session.
- **Owner runtimed** is the runtimed in the Runtime Pod assigned to the target
  Session Run. It is the only component that owns that session's FIFO queue,
  operation state, idle timer, and local lifecycle.
- **Runtime Server** is the execution backend colocated with owner runtimed.
  It implements a separate local-only `SessionRuntime` gRPC service. It never
  accepts client traffic, authorizes Kubernetes users, routes across Pods, or
  owns the session queue.

The chart, rather than a controller, owns the gateway Deployment, Service, RBAC,
replicas, and TLS configuration. Values control whether the component is
installed and which serving certificate Secret it uses. The gateway watches
Runtime Pods to discover ready runtimed endpoints, but its Kubernetes resources
do not change when a Runtime is created, updated, or deleted.

The request path is therefore:

```text
External client / SDK
  -> Runtime gateway Service (HTTP)
     -> Runtime gateway server in the shared Runtime gateway Deployment
        -> ingress runtimed (SessionGateway gRPC)
           -> owner runtimed (only when the ingress runtimed is not the owner)
           -> local Runtime Server (SessionRuntime gRPC)
```

The Runtime gateway server exposes a versioned TLS HTTP API and receives the
client's Kubernetes bearer token. Every endpoint path identifies namespace,
Runtime, and immutable Run UID. The gateway server verifies the requested
Runtime, selects one ready runtimed from that Runtime's Pod set, and translates
the HTTP request to a `SessionGateway` gRPC request. The ingress runtimed
authorizes the caller and uses its ownership cache. If it is not the owner, it
forwards the same `SessionGateway` RPC once to owner runtimed using
authenticated Pod-to-Pod transport and a forwarding marker. The owner runtimed
performs queue admission and invokes its colocated Runtime Server over local
`SessionRuntime` gRPC.

The gateway server maps the following HTTP API operations to `SessionGateway`
gRPC methods:

| HTTP API | `SessionGateway` method | Behavior |
| --- | --- |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}` | `GetSession` | return readiness, identity, queue state, and bounded metadata |
| `POST /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations:execute` | `Execute` | enqueue one command operation |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations/{operationID}` | `GetOperation` | return operation state and bounded result |
| `DELETE /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations/{operationID}` | `CancelOperation` | cancel a queued or running operation |
| `/v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/files` | `ListFiles`, `ReadFile`, `WriteFile`, `DeleteFile`, `RenameFile` | workspace-relative file operations with bounded transfer sizes |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/logs` | `StreamLogs` | stream session and operation structured log events |

An exec request supplies exactly one of `argv` or `shell`. `argv` directly
executes a program. `shell` deliberately opts into the Runtime's shell. Both
support bounded stdin, a relative working directory, bounded environment
overrides, and an operation timeout.

Each mutating request returns an operation ID. All commands and file mutations
are ordered through one FIFO queue per session:

```text
Queued -> Running -> Succeeded | Failed | Cancelled | TimedOut
```

Only one mutation runs at a time. Read/list/status/log requests do not enter
the queue. The effective queue size is the minimum of `mode.session.queueSize`
and a runtimed-configured global maximum. The initial global maximum is 32;
the initial operation default and maximum are five minutes, and graceful
termination waits ten seconds. Administrators may configure lower or higher
global limits; a Run may only reduce its queue size or operation timeout.

When a session closes, runtimed rejects new operations, cancels queued work,
sends termination to the running process group, waits the grace period, then
force-kills it if necessary.

File paths are always relative to the session workspace. Absolute paths,
traversal, and symlink escapes are rejected. Direct upload and download are
limited to 32 MiB; larger durable results use ArtifactStore.

## Runtime Server Contract

Owner runtimed owns queue admission, operation lifecycle, Run status updates,
capacity release, and structured audit logs. Runtime Server owns local
workspace confinement, process groups, and local session state through an
internal gRPC `SessionRuntime` contract. runtimed alone owns the session queue
and operation state, so a Runtime Server never independently reorders work.

The contract is local to a Runtime Pod and is not exposed through the gateway:

```proto
service SessionRuntime {
  rpc RegisterSession(RegisterSessionRequest) returns (SessionStatus);
  rpc ExecuteSessionOperation(ExecuteSessionOperationRequest)
      returns (ExecuteSessionOperationResponse);
  rpc ReadSessionFile(ReadSessionFileRequest) returns (ReadSessionFileResponse);
  rpc ListSessionFiles(ListSessionFilesRequest) returns (ListSessionFilesResponse);
  rpc CloseSession(CloseSessionRequest) returns (CloseSessionResponse);
  rpc GetSessionStatus(GetSessionStatusRequest) returns (SessionStatus);
}
```

Every request includes the immutable Run UID and assignment identity. Only the
owning local runtimed may call this contract. `RegisterSession` receives the
prepared workspace path and immutable source inputs; it is idempotent for the
same identity. `ExecuteSessionOperation` contains exactly one `oneof` payload:
a command, file write, directory creation, delete, or rename. runtimed assigns
the operation ID and admits that mutation to its session queue before making
this local call. Its request context carries the command timeout; cancellation
terminates the matching process group. The read and list RPCs are synchronous,
bounded, and do not enter the mutation queue. Runtime Server methods do not
allocate operation IDs or queue requests. `CloseSession` is idempotent and
removes local session state after runtimed has rejected new gateway operations.

This preserves the current architecture: external clients invoke HTTP through
the shared Runtime gateway Service and never reach either internal gRPC service
directly. The gateway server translates HTTP to `SessionGateway`; owner
runtimed translates accepted requests to local `SessionRuntime` calls.

The public Session API does not expose a Runtime backend choice. The v0 backend
is one trusted container session per Runtime Pod. A future Runtime implementation
may multiplex multiple sessions in a worker Pod through gVisor or microVM
actors without changing the Run or `SessionGateway` API. This follows the useful
actor-versus-worker separation demonstrated by Agent Substrate, without making
its snapshotting or multiplexing model a v0 dependency.

## Security and Observability

v0 sessions are a **trusted-workload preview**. They are not safe isolation for
untrusted LLM-generated code. Runtime Pod templates remain responsible for
image pinning, ServiceAccount, resource limits, security context, and network
policy. v1.0 must provide at least one secure session backend, initially gVisor.

Every operation emits structured external logs keyed by Run UID, session ID,
operation ID, type, timestamps, result, exit code, and truncation metadata.
Command history, file contents, stdout, stderr, and high-frequency events are
not written to `Run.status`. ArtifactStore remains the durable path for large
outputs and exports.

## Non-Goals

v0 does not provide session checkpoint/resume, branching, persistent REPL
processes, durable audit storage, concurrent mutations, shared Runtime Pod
sessions, or secure execution of untrusted code.
