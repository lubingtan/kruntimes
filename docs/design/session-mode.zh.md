# Agent Sandbox 的 Session Mode

状态：**已接受；正在实现**

## 问题

`Run.mode.function` 是固定 handler 的 RPC 模型，适合 FaaS 和 tool endpoint，但不是
agent sandbox。sandbox 需要可变 workspace、任意命令、文件操作、有序多步工作和独立 session
lifecycle。

Session mode 复用现有 `Run` 作为 lifecycle object，不增加 `Sandbox` CRD。一个 session Run
占用预热 Runtime capacity，并在关闭、过期、失败或删除前提供同一个有状态执行环境。

## Run API

`Run.spec.mode` 必须且只能设置 `task`、`function` 或 `session` 之一：

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

Session 使用已有的 `Pending -> Scheduled -> Running -> Ready` lifecycle。`Ready` 表示 owning
runtimed 已在本地 Runtime Server 注册 session 并接受 operation；它是 active phase 并持续占用
Runtime capacity。

v0 中用于 session 的 Runtime 应配置为有效 `runs: 1` capacity。scheduler 仍只执行通用资源
capacity 计算；runtimed 是本地独占的权威门控：它原子地拒绝在其他 Run active 时 claim
Session，并在 Session active 时拒绝 claim 其他任何 Run。这在本地竞争或 Runtime 配置错误时也能
防止混合执行；被该门控拒绝的 Run 保持 `Scheduled`，直至 Runtime Pod 可用。

`source` 与 `artifactInputs` 在 session Ready 前初始化 workspace，不是 command 定义。session
Run 不得设置 `spec.workspace`：v0 为其在 assigned Runtime Pod 创建按 Run UID 隔离的 ephemeral
workspace。同一 Pod 上的操作之间保留文件、已安装依赖和 process-visible state；Pod 丢失会使
session 失败，v0 不承诺 checkpoint、resume 或透明迁移。

`Run.spec.timeout` 限制整个 reservation。`idleTimeoutSeconds` 在没有 accepted mutation 或 command
activity 后过期。Session 继续使用普通 Run 的 cancellation、deletion、TTL、authorization、endpoint
和 assignment-UID fencing。注册在 `Ready` 前可以 retry，因为此时尚不存在可用的 session state；
进入 Ready 后 assigned-Pod loss 是 terminal，client 必须创建新的 Session Run，而不能在空 workspace
中静默继续。

## 服务与请求路径

以下概念必须区分：

- **Runtime gateway**：当 Helm chart 的 `gateway.enabled` 为 true 时安装的共享
  `runtime-gateway` Deployment。其 Pod 为集群中的所有 Runtime 运行 gateway server；它是 stateless
  的，不拥有 session、不维护 operation queue，也不直接调用 Runtime Server。
- **Runtime gateway Service**：指向 Runtime gateway Deployment 的共享 Kubernetes `ClusterIP`
  Service。它是 `Run.status.endpoint` 中的稳定 HTTP 地址。
- **Runtime gateway server**：每个 Runtime gateway Pod 内的 HTTP server。Kubernetes Service 只能
  转发 traffic，不能把 HTTP request 转换为 gRPC；gateway server 实现 HTTP API，并将每个 request
  转换为内部 `SessionGateway` gRPC call。它只维护每个 Runtime 的 ready runtimed endpoint 的
  watch-backed list，不 cache Session ownership 或 operation state。
- **runtimed**：运行在每个 Runtime Pod 中。它实现 versioned 的 `SessionGateway` gRPC service，并维护
  watch-backed ownership cache。
- **ingress runtimed**：gateway server 针对 HTTP request 中指定的 Runtime，从该 Runtime 的 ready
  runtimed 中选择的实例；它可能拥有、也可能不拥有目标 session。
- **owner runtimed**：目标 Session Run 所 assigned Runtime Pod 中的 runtimed。只有它维护该 session
  的 FIFO queue、operation state、idle timer 和 local lifecycle。
- **Runtime Server**：与 owner runtimed colocated 的 execution backend。它实现独立且仅本地使用的
  `SessionRuntime` gRPC service；它不接收 client traffic、不 authorize Kubernetes user、不跨 Pod
  路由，也不拥有 session queue。

chart 而非 controller 拥有 gateway Deployment、Service、RBAC、replicas 和 TLS configuration。values
控制是否安装该组件以及使用哪个 serving certificate Secret。gateway watch Runtime Pod 以发现 ready
runtimed endpoint，但 Runtime 的创建、更新或删除不改变 gateway 的 Kubernetes resource。

请求路径如下：

```text
External client / SDK
  -> Runtime gateway Service (HTTP)
     -> shared Runtime gateway Deployment 中的 Runtime gateway server
        -> ingress runtimed (SessionGateway gRPC)
           -> owner runtimed（仅当 ingress runtimed 不是 owner 时）
           -> local Runtime Server (SessionRuntime gRPC)
```

Runtime gateway server 暴露 versioned TLS HTTP API，并接收 client 的 Kubernetes bearer token。
每个 endpoint path 标识 namespace、Runtime 和 immutable Run UID。gateway server 验证 request 中的
Runtime，从该 Runtime 的 ready Pod set 选择一个 runtimed，再将 HTTP request 转换为
`SessionGateway` gRPC request。ingress runtimed authorize caller 后查询 ownership cache；若它不是
owner，则通过 authenticated Pod-to-Pod transport 和 forwarding marker，将同一个 `SessionGateway`
RPC 单跳转发给 owner runtimed。owner runtimed 完成 queue admission，再通过 local
`SessionRuntime` gRPC 调用 colocated Runtime Server。

gateway server 将下列 HTTP API operation 映射到 `SessionGateway` gRPC method：

| HTTP API | `SessionGateway` method | 行为 |
| --- | --- |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}` | `GetSession` | 返回 readiness、identity、queue state 和 bounded metadata |
| `POST /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations:execute` | `Execute` | enqueue 一个 command operation |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations/{operationID}` | `GetOperation` | 返回 operation state 与 bounded result |
| `DELETE /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations/{operationID}` | `CancelOperation` | cancel queued 或 running operation |
| `/v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/files` | `ListFiles`、`ReadFile`、`WriteFile`、`DeleteFile`、`RenameFile` | workspace-relative file operation，transfer size 有界 |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/logs` | `StreamLogs` | stream session 与 operation structured log events |

Exec request 必须且只能提供 `argv` 或 `shell`。`argv` 直接执行程序；`shell` 显式选择 Runtime
shell。二者都支持 bounded stdin、relative working directory、bounded environment overrides 与
operation timeout。

每个 mutation request 返回 operation ID。所有 command 和 file mutation 按每个 session 一个 FIFO
queue 排序：

```text
Queued -> Running -> Succeeded | Failed | Cancelled | TimedOut
```

同一时刻只运行一个 mutation；read/list/status/log 不进入队列。effective queue size 是
`mode.session.queueSize` 和 runtimed global maximum 的最小值。初始 global maximum 为 32，默认和
最大 operation 时间为五分钟，graceful termination 为十秒。管理员可配置全局上限；Run 只能降低
queue size 或 operation timeout。

session 关闭时，runtimed 拒绝新 operation、取消 queued work、向 running process group 发送
termination，等待 grace period 后仍未退出则 force-kill。

file path 必须相对 session workspace；absolute path、traversal 和 symlink escape 都被拒绝。direct
upload/download 最大 32 MiB；更大的 durable result 使用 ArtifactStore。

## Runtime Server 协议

owner runtimed 负责 queue admission、operation lifecycle、Run status updates、capacity release 和
structured audit logs。Runtime Server 通过内部 gRPC `SessionRuntime` contract 负责本地 workspace
confinement、process groups 和 local session state。session queue 与 operation state 只由 runtimed
维护，因此 Runtime Server 不会独立重排工作。

该 contract 只在 Runtime Pod 内部使用，不通过 gateway 暴露：

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

每个请求携带 immutable Run UID 和 assignment identity，只有 owning local runtimed 可以调用。
`RegisterSession` 接收已准备的 workspace path 和 immutable source inputs；同一 identity 下调用是
幂等的。`ExecuteSessionOperation` 包含恰好一个 `oneof` payload：command、file write、directory creation、delete
或 rename。runtimed 在作出这个 local call 前分配 operation ID，并将该 mutation admission 到 session queue。它的 request context 携带 command timeout；
cancellation 终止匹配的 process group。read/list RPC 是 synchronous、有界的，不进入 mutation queue。Runtime Server method 不分配 operation ID，也不 queue request。
`CloseSession` 是幂等操作：runtimed 拒绝新的 gateway operation 后，它清理 local session state。

这保持当前 architecture：external client 通过共享的 Runtime gateway Service 调用 HTTP，绝不直接访问
任一内部 gRPC service。gateway server 将 HTTP 转换为 `SessionGateway`；owner runtimed 再将已接受的
request 转换为 local `SessionRuntime` call。

public Session API 不暴露 Runtime backend。v0 backend 是每个 Runtime Pod 一个 trusted container
session。未来 Runtime implementation 可以用 gVisor 或 microVM actor 在 worker Pod 中 multiplex 多个
session，而无需改变 Run 或 `SessionGateway` API。该模型借鉴 Agent Substrate 的 actor/worker 分层，但不把其
snapshot/multiplexing 实现引入 v0。

## 安全与可观测性

v0 session 是 **trusted-workload preview**，不是 untrusted LLM-generated code 的安全隔离。
Runtime Pod template 仍负责 image pinning、ServiceAccount、resource limits、security context 与
network policy。v1.0 必须至少提供一种 secure session backend，初始目标为 gVisor。

每个 operation 向外部日志输出带 Run UID、session ID、operation ID、type、timestamps、result、
exit code 与 truncation metadata 的 structured event。command history、file contents、stdout、stderr
和 high-frequency event 不写入 `Run.status`。ArtifactStore 是大输出与 export 的 durable 路径。

## 非目标

v0 不提供 session checkpoint/resume、branching、persistent REPL processes、durable audit storage、
concurrent mutations、shared Runtime Pod sessions 或 untrusted code 的 secure execution。
