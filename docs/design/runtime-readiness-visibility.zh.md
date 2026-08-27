# Runtime 就绪状态可见性

## 状态

在 v0.x 中逐步实现。本文在补齐剩余 integration 和 E2E coverage 前定义可观察的 contract。

## 目标

Operator 需要在 Runtime 层面看到 warm pool 中有多少 ready Pod，而不必自行查找
controller 所拥有的 Deployment 或列出 Pod。同一个值必须能通过 Kubernetes、`krt runtime list`
和 `krt runtime get` 获取。

## Contract

对于名为 `<name>` 的 Runtime，Runtime controller 拥有同 namespace 中名为
`runtime-<name>` 的 Deployment。controller 将该 Deployment 的 `status.readyReplicas`
复制到 `Runtime.status.readyReplicas`。

- `readyReplicas` 是最后一次观察到的 Kubernetes Deployment ready-replica count；它不是
  desired replica count、独立 probe 的 health count，也不是 capacity。
- Deployment status 变化会将其 owning Runtime 入队。controller 会重新读取 Deployment，且仅在
  值变化时更新 Runtime status。因此 Pod 变为 ready 或已 ready 的 Pod 变为 unavailable 都能被看到，
  无需周期性 poll。
- 该值与 Deployment status 最终一致。spec update、rollout 或 Pod event 后，它可能暂时描述
  Deployment 上一次观察；它不提供 availability 或 scheduling guarantee。
- scheduler 仍须检查每个 Pod 的 eligibility 和 capacity，不能把 aggregate
  `readyReplicas` 当作分配 Run 的许可。

当 Deployment 报告没有 ready replica 时，Runtime 报告 `0`。删除 Runtime 会删除其拥有的
Deployment，因此 Runtime status 不会作为 deletion 后的持久记录。

## CLI 和 API 可见性

`Runtime.status.readyReplicas` 属于 Runtime status API，structured `krt` output 原样暴露它。
`krt runtime list` 的 table output 显示为 `READY`，`krt runtime get` 显示为 `Ready`，并与 desired
`REPLICAS`/`Replicas` 并列，从而区分 desired 和 observed count。

## 验证计划

1. Controller integration coverage 验证 Deployment status 增加和减少时都会 reconcile 到 Runtime status。
2. CLI tests 验证 table 和 structured output 保留 observed value。
3. E2E 创建 Runtime、等待 ready count，然后让 Pod unavailable，并在 cleanup 前验证 count 更新。

