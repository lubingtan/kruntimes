# Dashboard

本文描述已接受的 v0.x 设计以及已实现的初始 Dashboard 功能。

kruntimes 应该提供一个小型只读 dashboard，帮助开发者和运维人员理解当前有哪些任务在运行、
哪些任务卡住了，以及如何找到 logs 和 artifacts，而不需要在多个 `kubectl` 和 `krt`
命令之间来回切换。

dashboard 不应该成为 workflow engine，也不应该成为新的主控制面。它应该展示已经存在于
CRD、Pod、conditions、logs 和 artifact references 中的 Kubernetes-native 状态。

## 目标

- 按 namespace 浏览 Runs。
- 查看 Run phase、conditions、runtime、assigned Runtime Pod、attempts、时间戳、
  有界 outputs 和 artifact references。
- 通过与 `krt logs` 相同的安全边界 stream 或 retrieve Run logs。
- 保持 Kubernetes RBAC 和 namespace 边界。
- 为 Pending、Scheduled、Running、Succeeded、Failed、Cancelled 和 TimedOut Runs
  提供面向运维的视图。
- 浏览 Runtime pool、其 Pod health/capacity 以及分配给它的 Runs。
- 浏览 WorkflowRun、其 job DAG 和 step 到 Run 的链接。

## 非目标

- 第一版不提供 create、cancel、delete、retry 或 edit 操作。
- 第一版不提供 workflow editor 或 visual DAG builder。
- 不允许浏览器直接访问 Runtime Pods、Runtime Servers 或 runtimed endpoints。
- 不引入绕过 Kubernetes authentication 和 authorization 的自定义身份系统。
- 不替代 Prometheus、log collection 或长期 audit storage。
- v0.x 不承诺稳定的公开 dashboard HTTP API。

## 用户

开发者通过 dashboard 回答：

- 我的 Run 是否启动了；
- 哪个 Runtime 处理了它；
- 为什么它 Pending 或 Failed；
- logs 和 bounded outputs 是什么；
- artifacts 存储在哪里。

运维人员通过 dashboard 回答：

- 哪些 namespace 里有卡住或失败的 Runs；
- capacity、readiness、RBAC 或 image/runtime 问题是否体现在 Run conditions 中；
- 哪些 Runtime Pods 正在接收任务；
- 用户是否需要额外 RBAC 才能读取 logs 或 artifacts。

## 架构

dashboard 应该包含两个组件：

| 组件 | 作用 |
| --- | --- |
| Dashboard backend | 访问 Kubernetes API，执行所选 auth/RBAC 模型，读取 kruntimes CRDs，并在允许时代理 logs/artifacts 访问。 |
| Dashboard frontend | 只读 Web UI，展示 namespace、Run list、Run detail、logs 和 artifact metadata。 |

### v0.x 决策

dashboard 是 `kruntimes` chart 的 opt-in 组件，默认 `dashboard.enabled: false`。其
Deployment、ServiceAccount、Service 和 TLS resources 与控制面一起由同一个 Helm release 安装。
这样在不增加不需要该组件的安装面时，仍保持统一的升级和 RBAC 边界。

生产环境的 dashboard 仅允许 HTTPS。Service 保持 ClusterIP，且 bearer-token login 页面绝不能
暴露在 plaintext HTTP 上。chart 允许 operator 选择一种 certificate source：

- 已存在的 TLS Secret，通常包含 dashboard 用户信任的 certificate；
- chart 生成的 self-signed certificate，适用于本地开发和已显式信任该 certificate 的私有部署；
  或
- 使用已有 Issuer 或 ClusterIssuer 的 cert-manager Certificate；所选 issuer 本身也可以是
  cert-manager self-signed issuer。

所选 source 都写入同一个挂载的 TLS Secret。chart 必须拒绝 ambiguous combination，而不能
静默选择 certificate source。

Helm values 将该选择明确化：默认 `dashboard.tls.selfSigned` 会让 chart 创建 TLS Secret；要
挂载 operator 已提供的 Secret，设置 `selfSigned: false`，保持
`certManager.enabled: false`，并设置 `secretName`；要使用 cert-manager，则设置
`selfSigned: false` 和 `certManager.enabled: true`，同时引用已经存在的 `issuerRef`。
cert-manager 可以写入默认 Dashboard TLS Secret，也可以写入 operator 设置的 `secretName`。
因此，使用已有 self-signed Issuer 时不需要额外的 dashboard 专用 mode。

backend 不拥有代表用户读取受保护资源的 ambient authority。它只复制 in-cluster transport
配置、清空挂载的 credential，并安装 caller bearer token。chart 默认启用极窄的 public-read：
Dashboard ServiceAccount 只能 get/list Namespaces、Runs、Runtimes 和 WorkflowRuns，且无 token
时 API 只暴露它们的 summary。operator 可以通过 `dashboard.publicRead.enabled=false` 禁用它。
资源详情仍由 caller 授权。

第一版应读取以下数据源：

- 通过 Kubernetes API 读取 `Run` objects；
- 读取 `Run.status.assignedPod` 引用的 Runtime Pod metadata；
- 在可用时读取与 Runs 和 Runtime Pods 相关的 Kubernetes Events；
- 通过 backend-controlled 路径访问 runtimed log/status endpoints；
- 读取 `Run.status.outputs` 和 `Run.status.artifactRefs`。

后续版本可以增加 PersistentWorkspace detail pages 以及基于 Prometheus 或其它 metrics backend
的 metrics panels。

## 日志访问

Dashboard backend 不能把 Runtime Pods 直接暴露给浏览器。

v0.x 预期路径是：

1. 用户打开某个 Run 的 logs。
2. 极窄的 public-read client 只读取 Run 来定位其 assigned Runtime Pod。
3. 用户 token 授权 Pod `log` subresource 请求。
4. Backend stream 或返回请求的 log tail。

具体 transport 可以演进。它可以使用 Kubernetes port-forwarding、internal service 或
专用 log proxy，但边界应该保持不变：用户需要有访问 runtime logs 的权限，而极窄的
ServiceAccount 只负责 Run 到 Pod 的定位。

结构化 runtimed logs 应继续以 Run UID 作为 key，这样即使 Runtime Pods 同时处理多个 Runs，
dashboard 也能展示正确的 logs。

v0.x 中，backend 用 public-read client 定位 assigned Pod，再通过 request-scoped caller client
读取该 Pod 的 runtimed container log subresource，并只返回 run_uid 与请求 immutable Run UID
匹配的 structured records。它不创建 browser-visible port-forward，也不把 Runtime Pods 直接暴露给
browser。因此 caller 需要 Pod `log` subresource 的 `get`，但仅读取日志时不需要 Run 或 Pod 的
读取权限。artifact references 作为 Run metadata 展示；artifact download 不属于第一阶段 Dashboard。

### 计划项：统一的 Run Log Authorization

当前的 `pods/log` authorization 是 Kubernetes 实现层权限，而不是期望暴露给用户的 Run-log
capability；它也和 `krt logs` 不一致，后者当前的主路径需要 Run access 与 Pod port-forward
access。后续工作将把共享 Runtime Gateway 作为 Dashboard 和 `krt` 唯一的 Run-log API。完整的
endpoint、authorization、bounds、error 和 migration contract 见 [Runtime Gateway Run Log API
设计](runtime-gateway-log-api.zh.md)。

在完成该迁移前，Dashboard log access 仍要求 caller 具有 `get pods/log` permission。

## 安全模型

dashboard 默认必须是只读的。

建议的 v0.x 生产模型是 Kubernetes bearer-token login：

- 用户通过 HTTPS 将 Kubernetes bearer token 输入 Dashboard。backend 以 host-only 的
  `HttpOnly`、`Secure`、`SameSite=Strict` session cookie 返回 token，时限八小时。JavaScript
  永远不读取或写入 token，且 token 不会写入 localStorage、sessionStorage 或 logs；
- backend 使用该 bearer token、in-cluster API server 地址和 cluster CA 创建 request-scoped
  Kubernetes client，并用它访问受保护页面和 Pod logs；
- chart 默认以权限极窄的 Dashboard ServiceAccount 提供免 token 的 namespace、Run、Runtime 和
  WorkflowRun summary。它只有这些资源的 `get`/`list` 权限，并可以显式禁用；
- Kubernetes API authorization 决定受保护页面的访问。没有 Run 权限的 token 只要具有
  `pods/log` 的 `get`，仍可读取 logs：ServiceAccount 会在不暴露 Run detail 的前提下定位 Pod；
- v0.x 只将 artifact references 作为 Run metadata 展示，不下载或代理 artifact content。
  将来的 artifact-download 设计必须单独定义 authorization 与 external-store 边界；
- 默认隐藏 secrets、service account tokens、environment variables 和 raw pod specs，
  除非未来明确增加 privileged operator view。

这与 Kubernetes Dashboard token login 的初始用户体验一致。集群 identity integration 可以在
dashboard 外部 mint 或 exchange bearer token，但 v0.x 不定义 external-auth header protocol、
impersonation model 或 custom identity provider。

本地开发中，`krt dashboard` 启动 loopback-only proxy 并 port-forward dashboard Service。
proxy 获取当前 kubeconfig credential，只将其注入被转发的请求；browser 永远不会得到该
credential。它只能绑定 127.0.0.1 或显式选择的 loopback address、必须拒绝 non-loopback
bind、不持久化或记录 credential，并在命令退出时关闭 port-forward。这不是生产 authentication
mode。

### 创建 Dashboard 登录 Token

operator 应在每个允许 dashboard 用户查看的 namespace 中，为最小权限的 *viewer*
ServiceAccount 创建短期 token。该 ServiceAccount 是登录 token 所代表的用户身份，与 dashboard
Deployment 自身使用的 ServiceAccount 不同。以下示例授予单个 namespace 的只读 Run、Runtime、
Workflow 和日志访问，不授予 Secrets、workload mutation verb、port-forwarding 或 artifact
download：

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kruntimes-dashboard-viewer
  namespace: team-a
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: kruntimes-dashboard-viewer
  namespace: team-a
rules:
  - apiGroups: ["kruntimes.io"]
    resources: ["runs", "runtimes", "workflowruns", "workflows", "actions", "persistentworkspaces"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: kruntimes-dashboard-viewer
  namespace: team-a
subjects:
  - kind: ServiceAccount
    name: kruntimes-dashboard-viewer
    namespace: team-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: kruntimes-dashboard-viewer
```

应用该 manifest 后，生成有时限的 token，并将其粘贴到 dashboard 登录页面。Dashboard 会将它保存
在八小时 HTTPS-only HttpOnly session cookie 中：

```bash
kubectl apply -f dashboard-viewer.yaml
kubectl -n team-a create token kruntimes-dashboard-viewer --duration=1h
```

`kubectl create token` 要求 Kubernetes 1.24 或更高版本。日常 dashboard 访问不要使用
cluster-admin credential。cluster identity system 也可以提供等价 user token；dashboard 会将两者
都视为标准 Kubernetes bearer token。若要浏览多个 namespaces，可以创建等价的 namespace-scoped
bindings，或者在审查其范围后显式授予额外的 cluster-level read access。

## 内部 API 形状

dashboard frontend 可以使用随二进制版本演进的内部 HTTP API。v0.x 不应把它文档化为稳定的
公开 API。

已实现 endpoints 为：

```text
GET /api/namespaces
GET /api/namespaces/{namespace}/runs
GET /api/namespaces/{namespace}/runs/{name}
GET /api/namespaces/{namespace}/runs/{name}/logs?tail=&follow=
GET /api/namespaces/{namespace}/runtimes
GET /api/namespaces/{namespace}/runtimes/{name}
GET /api/namespaces/{namespace}/workflowruns
GET /api/namespaces/{namespace}/workflowruns/{name}
POST /api/session
DELETE /api/session
```

日志 endpoint 只会通过 request-scoped Kubernetes client 读取 assigned Pod 的 `runtimed`
container，并丢弃 `run_uid` 不匹配该 Run immutable UID 的记录。`tail` 最多返回 500 条记录；
每个 request 最多读取 500 条最近 container lines 和 1 MiB。普通 tail response 为 JSON；当
`follow=true` 时，endpoint 会返回过滤后的 newline-delimited JSON records，直到 caller 断开或
Kubernetes log stream 关闭。它绝不会变成 browser-visible Pod proxy。

Run list endpoint 应尽量支持 server-side pagination 和过滤：

- phase；
- runtime；
- assigned pod；
- label selector；
- created-after 或 age window。

## 用户界面

第一版 UI 应保持聚焦、面向运维：

- namespace selector；
- Run table，包含 phase、runtime、assigned pod、age、attempts 和 last transition reason；
- phase 和 runtime filters；
- Run detail page 或 drawer；
- conditions timeline；
- bounded outputs 和 artifact references；
- logs panel，包含 tail 和 follow controls；
- 当用户有权限时，链接到相关 Runtime Pod metadata。

在只读授权模型被验证之前，不应加入 mutation buttons。

frontend 使用 React 和 TypeScript，构建为与 Dashboard backend 一同打包到镜像中的静态 assets，并与内部 API 从
同一 HTTPS origin 提供。source、backend、process entrypoint 和 image definition 都位于顶层
`dashboard/` 目录。它没有独立 frontend Service、没有 browser-to-Kubernetes connection，并使用
same-origin Content Security Policy。bearer token 只会保存在 HTTPS-only HttpOnly cookie；刷新可
恢复 session，Disconnect 会清除它。

## 实现顺序

1. 增加本文档，并在 roadmap 中保持 TODO 明确。
2. 增加 dashboard backend package，接入只读 Kubernetes client。
3. 实现已 review 的 bearer-token production mode，以及 local-only kubeconfig proxy mode。
4. 实现 Run list/detail APIs，并增加 unit tests。
5. 通过 backend-controlled 路径实现 log tail/follow。
6. 增加 frontend Run list/detail/log views。
7. 在 `kruntimes` chart 中增加可选的 `dashboard.enabled` resources。
8. 在标准 E2E environment 中部署 Dashboard。browser-specific E2E coverage 延后到具备稳定的
   browser test harness 时再增加。
9. 在相关 APIs 稳定后增加 WorkflowRun/Workflow/Action/PersistentWorkspace views。

## 剩余问题

- log access 是否继续使用 port-forward 语义，还是迁移到专用的 cluster-internal log proxy
  service？
- 当 artifact stores 位于集群外部时，artifact downloads 应如何授权和代理？
- 第一版 list/watch 实现应该支持怎样的规模目标？
