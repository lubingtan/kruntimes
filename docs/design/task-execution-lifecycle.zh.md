# Task 执行生命周期

状态：**已实现**

## 目标

Task mode 的 Run 会在已分配的预热 Runtime Pod 上执行一次有界的命令或脚本。本文定义 runtimed
中的本地异步工作与 Kubernetes control-plane 状态之间的边界。

核心约束是：**只有 reconcile 可以更新 `Run.status`**。本地 worker 不能为了绕过 informer cache
而直接读取 Run，也不能自行更新 Run status。

## 生命周期

scheduler 分配完成后，owning runtimed 按以下过程处理一个 Task Run：

1. 对缓存中的 `Scheduled` Run 执行 claim，并更新为 `Running`。
2. source preparation 在执行开始前完成。准备失败会在当前 reconcile 中走普通 Run failure path。
3. 随后对缓存中 `Running` Run 的 reconcile 调用 Runtime Server 的 `Status`。
4. 当 `Status` 返回 `NOT_FOUND` 且尚未观察到 execution 时：
   - 如果没有其他 start in flight，则异步执行 artifact input staging 和 Runtime Server `Execute`；
   - 否则仅 requeue 并等待。
5. 当 Runtime Server 返回 `PENDING` 或 `RUNNING`，runtimed 记录 execution 已存在，并将它加入
   Run Lifecycle Event Generator（RLEG）。
6. `SUCCEEDED` 和 `FAILED` 由 reconcile 使用既有的 output、artifact、retry 和 terminal-state
   规则处理。

一旦已经观察到 execution，后续 Runtime Server `NOT_FOUND` 就是 `ExecutionLost` failure。这能区分
尚未创建的 execution 与已被 Runtime 接受但之后丢失的 execution。

## 异步启动边界

artifact input staging 与 `Execute` 可能阻塞在存储或本地 Runtime Server 上，因此放在异步 worker 中。
worker 只做本地工作：

- 成功时，只记录本地完成结果并 enqueue Run；
- 失败时，只记录本地 failure、发送 Event 和 log，然后 enqueue Run；
- 如果 `Execute` 成功时 Run 已不再处于本地 active 状态，runtimed 会 forget 新建的 Runtime
  execution，而不会重新激活 Run。

下一次 reconcile 消费记录的 start failure，并通过普通 retry policy 处理。包括 artifact input staging
失败：它会作为 Runtime execution-start failure 报告，但异步 worker 不会写 `Run.status`。

## Retry

当 Task attempt 失败但仍可 retry 时，runtimed 会先持久化 retry status，再 forget 对应的 terminal
Runtime execution。retry backoff 到期后，runtimed 会清除前一 attempt 的本地 execution observation，再将
`Running` condition 恢复为 true 并 requeue。它不会在该 status update 的调用中直接调用 `Execute`。之后缓存中的
`Running` Run 被 reconcile 时会观察到 Runtime Server `NOT_FOUND`，再开始下一次 execution attempt。必须 forget
terminal result，因为 Runtime Server 用 immutable Run UID 作为 execution key；否则下一次 reconcile 会将前一
attempt 的 `FAILED` 误认为新 attempt 的结果。

这一结构避免了异步本地操作与 status update 后立即出现的 controller cache 竞争，同时保留 bounded
reconciliation 和既有的 at-least-once execution semantics。
