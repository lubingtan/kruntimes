# Dashboard

This document describes the accepted v0.x design and its implemented initial
Dashboard surface.

kruntimes should provide a small read-only dashboard for developers and
operators who need to understand what is running, what is stuck, and where to
find logs and artifacts without switching between multiple `kubectl` and `krt`
commands.

The dashboard is not intended to become the workflow engine or the primary
control plane. It should visualize Kubernetes-native state that already exists
in CRDs, pods, conditions, logs, and artifact references.

## Goals

- Browse Runs by namespace.
- Inspect Run phase, conditions, runtime, assigned Runtime Pod, attempts,
  timestamps, bounded outputs, and artifact references.
- Stream or retrieve Run logs through the same security boundary used by
  `krt logs`.
- Preserve Kubernetes RBAC and namespace boundaries.
- Provide an operator-friendly view for Pending, Scheduled, Running, Succeeded,
  Failed, Cancelled, and TimedOut Runs.
- Browse Runtime pools, their Pod health/capacity, and assigned Runs.
- Browse WorkflowRuns, their job DAG and step-to-Run links.

## Non-Goals

- No create, cancel, delete, retry, or edit operations in the first version.
- No workflow editor or visual DAG builder in the first version.
- No browser access directly to Runtime Pods, Runtime Servers, or runtimed
  endpoints.
- No custom identity system that bypasses Kubernetes authentication and
  authorization.
- No replacement for Prometheus, log collection, or long-term audit storage.
- No stable public dashboard HTTP API in v0.x.

## Users

Developers use the dashboard to answer:

- did my Run start;
- which Runtime handled it;
- why is it Pending or Failed;
- what did the logs and bounded outputs say;
- where are artifacts stored.

Operators use the dashboard to answer:

- which namespaces have stuck or failing Runs;
- whether capacity, readiness, RBAC, or image/runtime problems are visible in
  Run conditions;
- which Runtime Pods are receiving work;
- whether users are asking for logs or artifact access that require additional
  RBAC.

## Architecture

The dashboard should have two components:

| Component | Role |
| --- | --- |
| Dashboard backend | Talks to the Kubernetes API, enforces the selected auth/RBAC model, reads kruntimes CRDs, and proxies log/artifact access when allowed. |
| Dashboard frontend | Read-only web UI that renders namespace, Run list, Run detail, logs, and artifact metadata. |

### v0.x Decisions

The dashboard is an opt-in component of the `kruntimes` chart, with
`dashboard.enabled: false` by default. Its Deployment, ServiceAccount,
Service, and TLS resources are installed in the same Helm release as the
control plane. This preserves one upgrade and RBAC boundary without adding a
component to installations that do not need it.

Production dashboard traffic is HTTPS-only. The Service remains ClusterIP, and
a bearer-token login page is never exposed over plaintext HTTP. The chart lets
the operator choose one certificate source:

- an existing TLS Secret, normally for a certificate trusted by dashboard
  users;
- a chart-generated self-signed certificate, suitable for local development
  and explicitly trusted private deployments; or
- a cert-manager Certificate using an existing Issuer or ClusterIssuer. The
  selected issuer may itself be a cert-manager self-signed issuer.

The selected source writes the same mounted TLS Secret. The chart rejects
ambiguous combinations rather than silently choosing a certificate source.

The Helm values make that selection explicit: `dashboard.tls.selfSigned` is
the default and causes the chart to create the TLS Secret; to mount an
operator-provided Secret, set `selfSigned: false`, leave
`certManager.enabled: false`, and set `secretName`; to use cert-manager, set
`selfSigned: false` and `certManager.enabled: true` with an existing
`issuerRef`. cert-manager may write either the default Dashboard TLS Secret or
the `secretName` specified by the operator. An existing self-signed Issuer is
therefore supported without a separate dashboard-specific mode.

The backend has no ambient read authority for protected user requests. It
copies only the in-cluster transport configuration, clears the mounted
credential and installs the caller bearer token. The chart enables a
deliberately narrow public-read mode by default: the Dashboard ServiceAccount
may only get/list Namespaces, Runs, Runtimes, and WorkflowRuns, and the API
exposes only their summaries without a token. Operators can disable it with
`dashboard.publicRead.enabled=false`. Resource details and
Runtime/WorkflowRun pages remain caller-authorized.

The first version should read the following sources:

- `Run` objects through the Kubernetes API;
- Runtime Pod metadata referenced by `Run.status.assignedPod`;
- Kubernetes Events related to Runs and Runtime Pods when available;
- runtimed log/status endpoints through a backend-controlled path;
- `Run.status.outputs` and `Run.status.artifactRefs`.

Future versions can add PersistentWorkspace detail pages and metrics panels.

## Log Access

The dashboard backend must not expose Runtime Pods directly to browsers.

For v0.x, the expected path is:

1. The user opens logs for a Run.
2. The narrow public-read client reads the Run only to locate its assigned
   Runtime Pod.
3. The user token authorizes the Pod `log` subresource request.
4. The backend streams or returns the requested log tail.

The exact transport can evolve. It may use Kubernetes port-forwarding, an
internal service, or a dedicated log proxy, but the boundary should stay the
same: the user needs access to runtime logs, while the narrow service account
performs only the Run-to-Pod lookup.

Structured runtimed logs should remain keyed by Run UID so the dashboard can
show the correct logs even when Runtime Pods handle multiple Runs.

For v0.x, the backend uses the public-read client to resolve the assigned Pod,
then reads that Pod's runtimed container log subresource through the
request-scoped caller client. It returns only structured records whose
run_uid matches the requested immutable Run UID. It does not create a
browser-visible port-forward or expose Runtime Pods directly. The caller
therefore needs `get` on the Pod `log` subresource, but does not need Run or
Pod read permission merely to retrieve logs. Artifact references are shown as
Run metadata; artifact downloads are outside the first Dashboard slice.

### Planned: Unified Run Log Authorization

The current `pods/log` authorization is an implementation-level Kubernetes
permission, not the desired user-facing Run-log capability. It is also not
consistent with `krt logs`, whose current primary path requires Run access and
Pod port-forward access. A follow-up will make the shared Runtime Gateway the
single Run-log API for Dashboard and `krt`.

The Gateway already authenticates caller bearer tokens with `TokenReview` and
authorizes access to the exact Run with `SubjectAccessReview`. The planned log
endpoint will reuse that `get runs` decision, derive the assigned Pod and
immutable Run UID server-side, read only the `runtimed` container using the
Gateway ServiceAccount's `pods/log` permission, and return only bounded
structured records matching that UID. It will not accept caller-selected Pod
or container values. This is an ordinary Gateway HTTP endpoint, not a
Kubernetes aggregation API server.

Until that migration is implemented, Dashboard log access continues to require
the caller's `get pods/log` permission.

## Security Model

The dashboard must be read-only by default.

The proposed v0.x production model is Kubernetes bearer-token login:

- the user enters a Kubernetes bearer token into the Dashboard over HTTPS. The
  backend returns it in a host-only `HttpOnly`, `Secure`, `SameSite=Strict`
  session cookie with an eight-hour lifetime. JavaScript never reads or writes
  the token, and it is never written to localStorage, sessionStorage, or logs;
- the backend creates a request-scoped Kubernetes client with that bearer token,
  the in-cluster API server address, and the cluster CA. It uses this client
  for protected pages and Pod log access;
- the chart's narrowly privileged Dashboard ServiceAccount supplies tokenless
  namespace, Run, Runtime, and WorkflowRun summaries by default. It has only
  `get`/`list` on those resources and can be disabled explicitly;
- Kubernetes API authorization decides protected-page access. A token with no
  Run permission can still read logs if it has `get` on `pods/log`: the
  service account resolves the Run-to-Pod mapping without exposing Run detail;
- v0.x shows artifact references as Run metadata but does not download or proxy
  artifact content. A future artifact-download design must define its
  authorization and external-store boundary separately;
- secrets, service account tokens, environment variables, and raw pod specs are
  hidden unless a future privileged operator view explicitly exposes them.

This has the same initial user experience as Kubernetes Dashboard token login.
Cluster identity integrations may mint or exchange the bearer token outside the
dashboard, but v0.x does not define an external-auth header protocol,
impersonation model, or a custom identity provider.

For local development, `krt dashboard` starts a loopback-only proxy and
port-forwards the dashboard Service. The proxy obtains the current kubeconfig
credential and injects it only into forwarded requests; the browser never
receives the credential. It must bind only to 127.0.0.1 or another explicitly
chosen loopback address, reject non-loopback binds, not persist or log the
credential, and close the port-forward when the command exits. This is not a
production authentication mode.

### Creating a Dashboard Login Token

An operator should create a short-lived token for a least-privilege *viewer*
ServiceAccount in each namespace that a dashboard user may inspect. This is the
identity represented by the login token; it is distinct from the ServiceAccount
used by the dashboard Deployment itself. The following example grants one
namespace read-only Run, Runtime, Workflow, and log access; it does not grant
access to Secrets, workload mutation verbs, port-forwarding, or artifact
downloads:

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

Apply the manifest, then mint a bounded token and paste it into the dashboard
login page. The Dashboard keeps it in an eight-hour HTTPS-only HttpOnly session
cookie:

```bash
kubectl apply -f dashboard-viewer.yaml
kubectl -n team-a create token kruntimes-dashboard-viewer --duration=1h
```

`kubectl create token` requires Kubernetes 1.24 or later. Do not use a
cluster-admin credential for routine dashboard access. Cluster identity systems
may provide an equivalent user token instead; the dashboard treats both as a
standard Kubernetes bearer token. To browse multiple namespaces, create
equivalent namespace-scoped bindings or explicitly grant the additional
cluster-level read access after reviewing its scope.

## Internal API Shape

The dashboard frontend can use an internal, versioned-for-the-binary HTTP API.
It should not be documented as a stable public API in v0.x.

Implemented endpoints are:

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

The log endpoint reads only the assigned Pod's `runtimed` container through the
request-scoped Kubernetes client and discards records whose `run_uid` does not
match the Run's immutable UID. `tail` is bounded to 500 returned records; the
backend reads at most 500 recent container lines and 1 MiB per request. A
normal tail response is JSON. With `follow=true`, the endpoint returns filtered
newline-delimited JSON records until the caller disconnects or the Kubernetes
log stream closes. It never turns into a browser-visible Pod proxy.

The Run list endpoint should support server-side pagination and filter fields
where practical:

- phase;
- runtime;
- assigned pod;
- label selector;
- created-after or age window.

## User Interface

The first version should keep the UI narrow and operational:

- namespace selector;
- Run table with phase, runtime, assigned pod, age, attempts, and last
  transition reason;
- filters for phase and runtime;
- Run detail page or drawer;
- conditions timeline;
- bounded outputs and artifact references;
- logs panel with tail and follow controls;
- links to related Runtime Pod metadata when the user has permission.

It should not include mutation buttons until the read-only authorization model
is proven.

The frontend is React and TypeScript, built into static assets packaged beside
the Dashboard backend in its image and served from the same HTTPS origin as its internal API.
The source, backend, process entrypoint, and image definition live under the
top-level `dashboard/` directory. It has no separate frontend Service, no
browser-to-Kubernetes connection, and a same-origin Content Security Policy.
The bearer token is never readable by JavaScript: it is stored only in a
host-only HTTPS HttpOnly session cookie. Reload restores the session and
Disconnect removes it.

## Implementation Sequence

1. Add this design document and keep the roadmap explicit.
2. Add a dashboard backend package with read-only Kubernetes client wiring.
3. Implement the reviewed bearer-token production mode and the local-only
   kubeconfig proxy mode.
4. Implement Run list/detail APIs with unit tests.
5. Implement log tail/follow through a backend-controlled path.
6. Add the frontend Run list/detail/log views.
7. Add the optional `dashboard.enabled` resources to the `kruntimes` chart.
8. Deploy the Dashboard in the standard E2E environment. Browser-specific E2E
   coverage is deferred until there is a stable browser test harness.
9. Add PersistentWorkspace views after their API stabilizes.

## Remaining Questions

- Should log access continue to use port-forward semantics or move to a
  dedicated cluster-internal log proxy service?
- How should artifact downloads be authorized and proxied when artifact stores
  are outside the cluster?
- What scale target should the first list/watch implementation support?
