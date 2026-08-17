# kruntimes Python SDK

`kruntimes-sdk` creates and controls agent-facing Sandboxes backed by
Session-mode `Run` resources. A Sandbox is not another Kubernetes resource:
the `Run` lifecycle remains authoritative.

Install the Kubernetes adapters when the SDK manages Runs directly:

```bash
pip install 'kruntimes-sdk[kubernetes]'
```

In a Pod with a ServiceAccount that is authorized for the target `Run` and
Runtime gateway, construct the client directly:

```python
from kruntimes.kubernetes import from_incluster
from kruntimes.sandbox import Command, CreateOptions

client = from_incluster()
sandbox = client.create(CreateOptions(
    namespace="agents",
    name="diagnose-api",
    runtime="python-session",
))
sandbox.wait(timeout_seconds=60)
result = sandbox.execute(Command(argv=["sh", "-c", "kubectl get pods -A"]))
print(result.stdout.decode())
sandbox.close(timeout_seconds=30)
```

For local development, forward only the shared Runtime gateway Service. The
forward preserves each Run endpoint path and does not expose runtimed or
Runtime Server gRPC ports:

```python
from kruntimes.kubernetes import PortForwardGatewayTransport, from_kube_config

with PortForwardGatewayTransport.start(
    namespace="kruntimes-system",
    service="kruntimes-gateway",
    service_port=80,
) as gateway:
    client = from_kube_config(gateway=gateway)
    sandbox = client.open("agents", "diagnose-api")
    sandbox.wait(timeout_seconds=60)
```

`Execute`, file mutations, and `Close` are never retried automatically. A
transport failure has an unknown execution outcome; refresh the Run and use
the structured owner-runtimed logs to determine what happened.

The local caller needs `get` access to the target Run for gateway
authorization. Starting the port-forward also needs `get` on the shared gateway
Service and `get`, `list` on its Pods in the gateway namespace.
