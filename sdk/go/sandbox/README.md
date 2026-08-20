# Go Sandbox SDK

`sandbox` is the Go client for Session-mode Runs. A Sandbox is backed by the
existing Kubernetes `Run` resource; it is not a second resource type.

In a cluster, create the client from the caller's Kubernetes REST config:

```go
client, err := sandbox.NewFromRESTConfig(restConfig, sandbox.Config{})
if err != nil {
    return err
}
session, err := client.Create(ctx, sandbox.CreateOptions{
    Namespace: "agents",
    Name:      "diagnose-api",
    Runtime:   "python-session",
})
if err != nil {
    return err
}
if err := session.Wait(ctx); err != nil {
    return err
}
result, err := session.Execute(ctx, sandbox.Command{Argv: []string{"sh", "-c", "kubectl get pods -A"}})
```

For local development, forward only the shared Runtime gateway Service. Pass
the returned forward as `Config.HTTPClient`; it preserves the endpoint path and
does not expose runtimed or Runtime Server gRPC ports:

```go
forward, err := sandbox.StartGatewayPortForward(
    ctx, restConfig, "kruntimes-system", "kruntimes-gateway", 80,
)
if err != nil {
    return err
}
defer forward.Close()

client, err := sandbox.NewFromRESTConfig(restConfig, sandbox.Config{HTTPClient: forward})
```

The SDK never retries commands, file mutations, or `Close` after a transport
failure. Refresh the Run and read structured owner-runtimed logs to determine
the outcome.

`Close` returns successfully only after the Run reaches `Succeeded`; `Cancel`
returns successfully only after `Cancelled`. Any other terminal phase is a
typed `StateError` that retains the current Run.

The local caller needs `get` on the target Run for gateway authorization. A
port-forward client also needs `get` on the shared gateway Service and `get`,
`list` on its Pods in the gateway namespace.
