# Runtime Gateway Transport Security and Transfer Bounds

Status: **Proposed**

## Problem

The shared Runtime gateway currently serves an authenticated but plain HTTP
cluster-local endpoint. `Run.status.endpoint` already supports `HTTP`, `HTTPS`,
and an optional PEM trust bundle, but the chart has no contract for mounting a
certificate, selecting HTTPS, or publishing trust material. The gateway also
relies on fixed implementation limits for JSON request bodies and some response
shapes instead of exposing a small operator-controlled policy.

This document defines the incremental v0.x transport contract. It deliberately
does not make the gateway an Internet-facing ingress or certificate authority.

## Goals and Non-goals

- Keep the gateway Service cluster-local and retain Kubernetes bearer-token
  authentication and per-Run authorization.
- Permit operators to terminate TLS in gateway Pods using a pre-existing
  Kubernetes Secret, with an optional cert-manager `Certificate` producer.
- Make request and response bounds explicit with conservative defaults.
- Preserve the current plain-HTTP deployment as the default v0.x-compatible
  configuration.

This does not create an Ingress, Gateway, LoadBalancer, or external DNS record;
does not issue certificates in the Runtime controller or expose private key
material through Run status; and does not automatically change HTTP installs to
HTTPS or expose Runtime Server gRPC.

## Helm API

```yaml
gateway:
  tls:
    enabled: false
    # Required when enabled and certManager.enabled is false.
    secretName: ""
    # tls.crt and tls.key are mandatory. ca.crt is optional.
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

`gateway.tls.enabled=false` keeps the existing `http` Service port and
`http://` endpoint. When enabled, `secretName` is mandatory unless
`certManager.enabled=true`; the chart mounts that Secret read-only and starts
the gateway with its certificate and private-key paths. The Service port is
named `https`, targets the HTTPS listener, and the controller receives an
`https://` gateway URL.

With cert-manager enabled, the chart creates one namespaced
`cert-manager.io/v1 Certificate` whose `secretName` is `gateway.tls.secretName`.
Its DNS names are the gateway Service's short, namespace-qualified, and
cluster-local names. Rendering fails unless the secret name and issuer reference
are complete. This remains opt-in, so installations without cert-manager do not
require its CRDs.

The chart owns the Deployment, Service, and optional Certificate; the Runtime
controller does not own, create, or rotate certificate resources. Changing TLS
mode rolls gateway Pods and updates the controller flag; ready Runs retain their
old endpoint until owner runtimed reconciles them. Operators should not switch
mode while depending on issued endpoint URLs.

## Endpoint Trust Publication

The controller receives only the public gateway URL. It must not read TLS
Secrets, because that would grant it access to private-key-bearing resources.
Consequently, v0.x does not automatically copy `ca.crt` into
`Run.status.endpoint.caBundle`. Clients use their normal in-cluster or
explicitly configured trust store. `caBundle` remains reserved for a future
non-secret, controller-managed trust-distribution mechanism.

## Transfer Bounds

| Value | Default | Enforcement |
| --- | ---: | --- |
| `gateway.maxRequestBodyBytes` | 1 MiB | reject a larger JSON request with `413 Payload Too Large` before runtimed |
| `gateway.maxResponseBodyBytes` | 1 MiB | buffer JSON and return `413` before headers; never send partial JSON |
| `gateway.maxHeaderBytes` | 1 MiB | Go HTTP server rejects excessive headers before routing |

Values must be positive and are passed as explicit gateway flags. The response
limit applies to gateway-generated JSON only. Runtime Servers retain their own
direct-gRPC output/file bounds; session paging still bounds directory listings.
Larger durable results belong in ArtifactStore.

## Security, Failure, and Delivery

TLS protects only client-to-gateway traffic. Internal gRPC remains cluster-local
and protected by NetworkPolicy plus the authenticated gateway boundary. A
missing, unreadable, malformed, or mismatched TLS Secret must prevent readiness;
the gateway never falls back to HTTP. The key is mounted read-only only into
gateway Pods and is never copied to logs, ConfigMaps, status, or flags.

Delivery is split into independent reviewable slices:

1. Existing-Secret chart values and validation, TLS listener, HTTPS endpoint,
   Helm/unit coverage.
2. Opt-in cert-manager `Certificate` rendering and chart tests.
3. Configurable HTTP body/header bounds, gateway unit tests, and focused HTTPS
   and rejection E2E coverage.
