# Runtime Gateway 传输安全与传输边界

状态：**Proposed**

## 问题

共享 Runtime gateway 当前提供已认证但明文的集群内 HTTP endpoint。`Run.status.endpoint`
支持 `HTTP`、`HTTPS` 和可选 PEM trust bundle，但 chart 尚未定义证书挂载、HTTPS 选择或
trust material 发布的约定。gateway 也依赖实现内固定的 JSON request body 和部分 response
大小限制，而非一个小而可由 operator 配置的策略。

本文定义渐进式 v0.x transport contract；它不会把 gateway 变为 Internet-facing ingress 或证书
颁发机构。

## 目标与非目标

- 保持 gateway Service 为 cluster-local，并保留 Kubernetes bearer-token authentication 和按 Run authorization。
- 使用已有 Kubernetes Secret 在 gateway Pod 终止 TLS；可选由 cert-manager 产生 `Certificate`。
- 以保守默认值显式配置 request/response bounds。
- 默认继续兼容当前 plain-HTTP v0.x deployment。

不创建 Ingress、Gateway、LoadBalancer 或外部 DNS；Runtime controller 不签发证书，也不向
Run status 暴露 private key；不自动把 HTTP 安装切换为 HTTPS，更不公开 Runtime Server gRPC。

## Helm API

```yaml
gateway:
  tls:
    enabled: false
    secretName: ""
    certificateKey: tls.crt
    privateKeyKey: tls.key
    caBundleKey: ca.crt
    certManager:
      enabled: false
      issuerRef:
        name: ""
        kind: Issuer
        group: cert-manager.io
```

`enabled=false` 保持 `http` Service port 和 `http://` endpoint。启用时，如未启用
cert-manager 则必须提供 `secretName`。chart 将 Secret 只读挂载，gateway 使用其中的
`tls.crt`、`tls.key` 启动 TLS listener；Service port 名称变为 `https`，controller 获得
`https://` gateway URL。

启用 cert-manager 时，chart 创建一个 namespaced `cert-manager.io/v1 Certificate`，其
`secretName` 为 `gateway.tls.secretName`，DNS names 包含 gateway Service 的 short、
namespace-qualified 和 cluster-local 名称。缺少 secret name 或 issuer reference 必须令 Helm
rendering 失败。此资源完全 opt-in，没有 cert-manager CRD 的安装不需要它。

chart 负责 Deployment、Service 与可选 Certificate；Runtime controller 不负责证书。修改 TLS
mode 会滚动 gateway Pod 并更新 controller flag；已经 Ready 的 Run 在 owner runtimed reconcile
前仍保留旧 endpoint，因此 operator 不应在依赖现存 endpoint URL 时切换模式。

## Endpoint 信任发布

controller 只接收 public gateway URL，不能读取 TLS Secret，否则会取得含 private key 资源的
权限。因此 v0.x 不自动把 `ca.crt` 复制到 `Run.status.endpoint.caBundle`。client 使用其
in-cluster 或显式配置的 trust store；`caBundle` 留给未来从非 Secret source 读取的
controller-managed trust distribution。

## 传输边界

| Value | 默认值 | 执行方式 |
| --- | ---: | --- |
| `gateway.maxRequestBodyBytes` | 1 MiB | JSON request 大于此值即以 `413 Payload Too Large` 拒绝，不进入 runtimed |
| `gateway.maxResponseBodyBytes` | 1 MiB | 先缓冲 JSON response；超限在写 header 前返回 `413`，绝不输出 partial JSON |
| `gateway.maxHeaderBytes` | 1 MiB | Go HTTP server 在路由前拒绝超大 request headers |

所有 value 必须为正数，并以显式 gateway flags 传入。response limit 仅作用于 gateway 生成的
JSON；Runtime Server 仍各自限制 direct gRPC output/file，session paging 仍负责目录列举边界。
较大的持久结果应进入 ArtifactStore。

## 安全、失败与交付

TLS 仅保护 client-to-gateway；内部 gRPC 仍是受 NetworkPolicy 和认证 gateway boundary 保护的
cluster-local hop。缺失、不可读、格式错误或不匹配的 TLS Secret 必须使 gateway Pod 不 Ready，
不能降级到 HTTP。key 仅只读挂载到 gateway Pod，绝不进入日志、ConfigMap、Run status 或 flags。

交付分为可独立 review 的三项：

1. Existing-Secret chart values/validation、TLS listener、HTTPS endpoint、Helm/unit coverage；
2. Opt-in cert-manager `Certificate` rendering 与 chart tests；
3. 可配置 HTTP body/header bounds、gateway unit tests，以及 focused HTTPS/rejection E2E。
