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
  to gRPC. The gateway server resolves the current Session Run assignment and
  sends each request through that Runtime's Kubernetes Service to
  `SessionRuntime` gRPC. It does not own a session, select Runtime Pods, or
  queue operations.
- **Runtime Service** is a ClusterIP Service created for every Runtime by the
  Runtime controller. It selects that Runtime's ready Pods and exposes the
  runtimed `session-runtime` port. It is the only Service the gateway uses to
  reach a Runtime Pod.
- **runtimed** runs in every Runtime Pod and implements `SessionRuntime` for
  gateway traffic. A runtimed may receive a request for a session assigned to
  another Runtime Pod.
- **Owner runtimed** is the runtimed in the Runtime Pod assigned to the target
  Session Run. It is the only component that owns that session's FIFO queue,
  operation state, idle timer, and local lifecycle.
- **Runtime Server** is the execution backend colocated with owner runtimed.
  It also implements `SessionRuntime`, but accepts calls only from its local
  runtimed. It never accepts client traffic, authorizes Kubernetes users,
  routes across Pods, or owns the session queue.

The chart, rather than a controller, owns the gateway Deployment, Service, RBAC,
and replicas. Values control whether the component is installed. TLS termination
and external exposure are configured outside this cluster-local Service. The Runtime controller
creates each Runtime Service; the gateway's Kubernetes resources do not change
when a Runtime is created, updated, or deleted.

The request path is therefore:

```text
External client / SDK
  -> Runtime gateway Service (HTTP)
     -> Runtime gateway server in the shared Runtime gateway Deployment
        -> Kubernetes Service for the Session's Runtime
           -> a ready Runtime Pod's runtimed (SessionRuntime gRPC)
              -> owner runtimed (only when the first runtimed is not the owner)
              -> local Runtime Server (SessionRuntime gRPC)
```

The Runtime gateway server exposes a versioned HTTP API and receives the
client's Kubernetes bearer token. Every endpoint path identifies namespace,
Runtime, and immutable Run UID. The gateway server verifies the requested Run,
derives its `SessionIdentity` from the current assignment, and calls that
Runtime's Service. Kubernetes routes the call to one ready Runtime Pod. The
receiving runtimed only lists Runs indexed by its own Runtime name, then verifies
that the requested Run is still a Session Run with the same assignment. If its
Pod UID is not the assigned Pod UID, it forwards the same call once to the owner
runtimed in the same Runtime. A forwarding marker prevents loops. The owner
runtimed performs queue admission and invokes its colocated Runtime Server over
local `SessionRuntime` gRPC.

The gateway server maps the following HTTP API operations to `SessionRuntime`
gRPC methods:

| HTTP API | `SessionRuntime` method | Behavior |
| --- | --- |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}` | `GetSessionStatus` | return readiness and bounded session metadata |
| `POST /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations:execute` | `ExecuteSessionOperation` | execute one command or file mutation |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/files` | `ReadSessionFile`, `ListSessionFiles` | bounded workspace-relative file access |

An exec request supplies exactly one of `argv` or `shell`. `argv` directly
executes a program. `shell` deliberately opts into the Runtime's shell. Both
support bounded stdin, a relative working directory, bounded environment
overrides, and an operation timeout.

Owner runtimed serializes commands and file mutations through one FIFO queue
per session:

```text
Queued -> Running -> Succeeded | Failed | Cancelled | TimedOut
```

Only one mutation runs at a time. Read/list/status requests do not enter the
queue. The effective queue size is the minimum of `mode.session.queueSize`
and the runtimed global maximum. The effective operation timeout is likewise
bounded by `mode.session.operationTimeout` and the global maximum. The v0
defaults are 32 queued mutations and a five-minute operation limit. A Run can
only reduce either limit. Administrators configure these platform-wide limits,
and the maximum time runtimed waits for the Runtime Server to finish closing a
session, through Helm `runtimed.session.maxQueueSize`,
`runtimed.session.maxOperationTimeout`, and
`runtimed.session.closeTimeout`.

When a session closes, owner runtimed rejects new operations, cancels queued work,
sends termination to the running process group, waits the grace period, then
force-kills it if necessary.

File paths are always relative to the session workspace. Absolute paths,
traversal, and symlink escapes are rejected. Direct upload and download are
limited to 32 MiB; larger durable results use ArtifactStore.

## Runtime Server Contract

Owner runtimed owns queue admission, operation lifecycle, Run status updates,
capacity release, and structured audit logs. Runtime Server owns local
workspace confinement, process groups, and local session state. Both hops use
the same gRPC `SessionRuntime` messages: runtimed implements the service for
gateway traffic and proxies an accepted owner request to its local Runtime
Server. runtimed alone owns the session queue and operation state, so a Runtime
Server never independently reorders work.

The `SessionRuntime` method set is:

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

Every request includes the immutable Run UID and assignment identity. The
gateway server derives that identity from the current Run assignment; it is not
client-controlled HTTP input. A receiving runtimed either forwards the request
to the owner or, when it is the owner, applies queue admission before calling
its local Runtime Server. `RegisterSession` receives the prepared workspace
path and immutable source inputs; it is idempotent for the same identity.
`ExecuteSessionOperation` contains exactly one `oneof` payload: a command,
file write, directory creation, delete, or rename. Its request context carries
the command timeout; cancellation terminates the matching process group. Read
and list RPCs are synchronous, bounded, and do not enter the mutation queue.
The local Runtime Server does not route requests or allocate operation state.
`CloseSession` is idempotent and removes local state after owner runtimed has
rejected new gateway operations.

External clients invoke HTTP through the shared Runtime gateway Service and
never reach a Runtime Server directly. The gateway server calls the target
Runtime Service's `SessionRuntime` endpoint; owner runtimed proxies accepted
requests to its local Runtime Server.

The public Session API does not expose a Runtime backend choice. The v0 backend
is one trusted container session per Runtime Pod. A future Runtime implementation
may multiplex multiple sessions in a worker Pod through gVisor or microVM
actors without changing the Run or `SessionRuntime` API. This follows the useful
actor-versus-worker separation demonstrated by Agent Substrate, without making
its snapshotting or multiplexing model a v0 dependency.

## Security and Observability

v0 sessions are a **trusted-workload preview**. They are not safe isolation for
untrusted LLM-generated code. Runtime Pod templates remain responsible for
image pinning, ServiceAccount, resource limits, security context, and network
policy. v1.0 must provide at least one secure session backend, initially gVisor.

Every operation emits structured external logs keyed by Run UID, session
identity, type, timestamps, result, exit code, and truncation metadata.
Command history, file contents, stdout, stderr, and high-frequency events are
not written to `Run.status`. ArtifactStore remains the durable path for large
outputs and exports.

## Non-Goals

v0 does not provide session checkpoint/resume, branching, persistent REPL
processes, durable audit storage, concurrent mutations, shared Runtime Pod
sessions, or secure execution of untrusted code.
