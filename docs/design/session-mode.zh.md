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
中静默继续。idle expiry 同样是 terminal：它会关闭本地 session、清理 ephemeral workspace，并记录为
`RunTimeout`。重新打开或再次提交同一个 Run 不能恢复它；需要新 sandbox 的 client 必须创建新的
Session Run。显式 suspend/resume 是独立的 v1 design item，不能从 timeout recovery 隐式推导。

## 服务与请求路径

以下概念必须区分：

- **Runtime gateway**：当 Helm chart 的 `gateway.enabled` 为 true 时安装的共享
  `runtime-gateway` Deployment。其 Pod 为集群中的所有 Runtime 运行 gateway server；它是 stateless
  的，不拥有 session、不维护 operation queue，也不直接调用 Runtime Server。
- **Runtime gateway Service**：指向 Runtime gateway Deployment 的共享 Kubernetes `ClusterIP`
  Service。它是 `Run.status.endpoint` 中的稳定 HTTP 地址。
- **Runtime gateway server**：每个 Runtime gateway Pod 内的 HTTP server。Kubernetes Service 只能
  转发 traffic，不能把 HTTP request 转换为 gRPC；gateway server 解析当前 Session Run assignment，
  再经该 Runtime 的 Kubernetes Service 通过 `SessionRuntime` gRPC 发送每个 request。它不拥有
  session、不选择 Runtime Pod，也不维护 operation queue。
- **Runtime Service**：由 Runtime controller 为每个 Runtime 创建的 ClusterIP Service。它选择该
  Runtime 的 ready Pod 并暴露 runtimed 的 `session-runtime` port；gateway 只能通过此 Service
  到达 Runtime Pod。
- **runtimed**：运行在每个 Runtime Pod 中，并为 gateway traffic 实现 `SessionRuntime`。runtimed 可能收到
  实际 assigned 给另一个 Runtime Pod 的 session request。
- **owner runtimed**：目标 Session Run 所 assigned Runtime Pod 中的 runtimed。只有它维护该 session
  的 FIFO queue、operation state、idle timer 和 local lifecycle。
- **Runtime Server**：与 owner runtimed colocated 的 execution backend。它实现独立且仅本地使用的
  `SessionRuntime` gRPC service，但只接受本地 runtimed 的调用；它不接收 client traffic、不 authorize
  Kubernetes user、不跨 Pod 路由，也不拥有 session queue。

chart 而非 controller 拥有 gateway Deployment、Service、RBAC 和 replicas。values 控制是否安装该组件。
TLS termination 与 external exposure 在该 cluster-local Service 之外配置。Runtime controller 创建每个 Runtime
Service；Runtime 的创建、更新或删除不改变 gateway 自身的 Kubernetes resource。

请求路径如下：

```text
External client / SDK
  -> Runtime gateway Service (HTTP)
     -> shared Runtime gateway Deployment 中的 Runtime gateway server
        -> session 所属 Runtime 的 Kubernetes Service
           -> 一个 ready Runtime Pod 的 runtimed (SessionRuntime gRPC)
              -> owner runtimed（仅当第一个 runtimed 不是 owner 时）
              -> local Runtime Server (SessionRuntime gRPC)
```

Runtime gateway server 暴露 versioned HTTP API，并接收 client 的 Kubernetes bearer token。
每个 endpoint path 标识 namespace、Runtime 和 immutable Run UID。gateway server 验证目标 Run，从当前
assignment 推导 `SessionIdentity`，再调用该 Runtime 的 Service。Kubernetes 将 call 路由到一个 ready
Runtime Pod。收到 request 的 runtimed 只列出按自己 Runtime 名称索引的 Run，然后检查目标仍是具有相同
assignment 的 Session Run；若其 Pod UID 不是 assigned Pod UID，则将同一个 call 单跳转发给同一 Runtime 中的
owner runtimed。forwarding marker 防止 loop。owner runtimed 完成 queue admission，再通过 local
`SessionRuntime` gRPC 调用 colocated Runtime Server。

gateway server 将下列 HTTP API operation 映射到 `SessionRuntime` gRPC method：

| HTTP API | `SessionRuntime` method | 行为 |
| --- | --- |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}` | `GetSessionStatus` | 返回 readiness 与 bounded session metadata |
| `POST /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/operations:execute` | `ExecuteSessionOperation` | 执行一个 command 或 file mutation |
| `GET /v1/namespaces/{namespace}/runtimes/{runtime}/sessions/{runUID}/files` | `ReadSessionFile`、`ListSessionFiles` | 有界的 workspace-relative file access |

Exec request 必须且只能提供 `argv` 或 `shell`。`argv` 直接执行程序；`shell` 显式选择 Runtime
shell。二者都支持 bounded stdin、relative working directory、bounded environment overrides 与
operation timeout。

owner runtimed 将所有 command 和 file mutation 按每个 session 一个 FIFO queue 排序：

```text
Queued -> Running -> Succeeded | Failed | Cancelled | TimedOut
```

同一时刻只运行一个 mutation；read/list/status 不进入队列。effective queue size 是
`mode.session.queueSize` 和 runtimed global maximum 的最小值；effective operation timeout 也同时
受 `mode.session.operationTimeout` 和 global maximum 限制。v0 默认允许 32 个 queued mutation，
operation limit 为五分钟；Run 只能降低 queue size 或 operation timeout。管理员通过 Helm
`runtimed.session.maxQueueSize`、`runtimed.session.maxOperationTimeout` 和
`runtimed.session.closeTimeout` 配置这些平台级上限，以及 runtimed 等待 Runtime Server 完成
session close 的最长时间。

session 关闭时，owner runtimed 拒绝新 operation、取消 queued work、向 running process group 发送
termination，等待 grace period 后仍未退出则 force-kill。

file path 必须相对 session workspace；absolute path、traversal 和 symlink escape 都被拒绝。direct
upload/download 最大 32 MiB；更大的 durable result 使用 ArtifactStore。

## Runtime Server 协议

owner runtimed 负责 queue admission、operation lifecycle、Run status updates、capacity release 和
structured audit logs。Runtime Server 负责本地 workspace confinement、process groups 和 local session
state。两个 hop 使用同一套 gRPC `SessionRuntime` message：runtimed 为 gateway traffic 实现该 service，并将
owner 已接受的 request proxy 到本地 Runtime Server。session queue 与 operation state 只由 runtimed 维护，
因此 Runtime Server 不会独立重排工作。

`SessionRuntime` 的 method 集合为：

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

每个 request 携带 immutable Run UID 和 assignment identity。gateway server 从当前 Run assignment 推导该
identity；它不是 client 控制的 HTTP input。收到 request 的 runtimed 要么将其转发给 owner，要么在自己是
owner 时完成 queue admission 后调用本地 Runtime Server。`RegisterSession` 接收已准备的 workspace path 和
immutable source inputs；同一 identity 下调用是幂等的。`ExecuteSessionOperation` 包含恰好一个 `oneof`
payload：command、file write、directory creation、delete 或 rename。其 request context 携带 command timeout；
cancellation 终止对应 process group。read/list RPC 是 synchronous、有界的，不进入 mutation queue。本地
Runtime Server 不路由 request，也不分配 operation state。`CloseSession` 是幂等操作：owner runtimed 拒绝新的
gateway operation 后，它清理 local state。

external client 通过共享的 Runtime gateway Service 调用 HTTP，绝不直接访问 Runtime Server。gateway server
调用目标 Runtime Service 的 `SessionRuntime` endpoint；owner runtimed 将已接受的 request proxy 到本地
Runtime Server。

## SDK Contract

面向 agent 的 Go 和 Python SDK 将 Session Run 称为 **Sandbox**。这只是用户层语义：一个 Sandbox
由一个 `spec.mode.session` 的 `Run` 支撑，Kubernetes Run lifecycle 仍是权威来源。SDK 不创建另一种
Sandbox resource，也不会绕过 gateway。

两个 SDK 提供相同的 lifecycle 与操作：

| Helper | 行为 |
| --- | --- |
| `Create` | 使用请求的 Runtime、source、artifact inputs、environment 与 timeout settings 创建 Session Run |
| `Open` | 读取一个已有的、具名的 Session Run；绝不创建或重新注册它 |
| `Wait` | watch 或 poll 至 `Ready` 或 terminal Run phase；返回 typed terminal 或 readiness error |
| `Execute` | 通过 Run endpoint 发送恰好一个 command 或 file mutation；绝不隐式重试 mutation |
| `ReadFile`、`ListFiles`、`WriteFile`、`CreateDirectory`、`DeleteFile`、`RenameFile` | 使用有界且 workspace-relative 的 gateway operations |
| `Logs` | 读取 assigned runtimed container log，并按不可变 Run UID 过滤结构化日志行；不引入 gateway log store |
| `Close` | 设置 `spec.cancelRequested` 并等待 Run terminal lifecycle；runtimed 负责 queue fencing、Runtime Server close、workspace cleanup 与 capacity release |

`Open` 和每次 data-plane call 都从当前 Run status 推导 endpoint。SDK 会拒绝非 Session Run、未处于
`Ready` 的 Run，或 endpoint Run UID 与已打开 Run 不匹配的情况。HTTP failures 以保留 status code 和
有界 server message 的 typed errors 暴露。SDK 绝不会把 transport failure 当作 mutation 未执行的证明。

in-cluster caller 使用调用者的 Kubernetes REST credentials 创建/watch Runs、读取 runtimed logs，并向
Runtime gateway Service 认证。local caller 则对 shared Runtime gateway Pod 建立 scoped port-forward，保持
endpoint path 不变，并使用同一套 Kubernetes credentials。Runtime Servers 和 runtimed gRPC ports 在两种
模式下都仍是私有实现细节。

public Session API 不暴露 Runtime backend。v0 backend 是每个 Runtime Pod 一个 trusted container
session。未来 Runtime implementation 可以用 gVisor 或 microVM actor 在 worker Pod 中 multiplex 多个
session，而无需改变 Run 或 `SessionRuntime` API。该模型借鉴 Agent Substrate 的 actor/worker 分层，但不把其
snapshot/multiplexing 实现引入 v0。

## 安全与可观测性

v0 session 是 **trusted-workload preview**，不是 untrusted LLM-generated code 的安全隔离。
Runtime Pod template 仍负责 image pinning、ServiceAccount、resource limits、security context 与
network policy。v1.0 必须至少提供一种 secure session backend，初始目标为 gVisor。

owner runtimed 会为每个 session mutation 输出 structured JSONL container logs，包含 Run UID、assignment identity、
type、timestamps、result 和 exit code。bounded command output 使用已有的 `stdout` 与
`stderr` stream；独立的 `audit` line 不包含 command text、stdin 或 file contents。kruntimes 不持久化
这些日志，Kubernetes log collector（例如 Fluent Bit）负责持久化和导出。command history、file contents、
stdout、stderr 与 high-frequency event 不写入 `Run.status`。ArtifactStore 是大输出与 export 的 durable path。

## 非目标

v0 不提供 session checkpoint/resume、branching、persistent REPL processes、durable audit storage、
concurrent mutations、shared Runtime Pod sessions 或 untrusted code 的 secure execution。
