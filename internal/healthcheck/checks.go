package healthcheck

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
)

type runtimeHealthClient interface {
	Health(context.Context, *pb.HealthRequest, ...grpc.CallOption) (*pb.HealthResponse, error)
}

// KubernetesAPI checks that the component can read its primary API resource.
func KubernetesAPI(reader client.Reader, prototype client.ObjectList, opts ...client.ListOption) healthz.Checker {
	return func(req *http.Request) error {
		list, ok := prototype.DeepCopyObject().(client.ObjectList)
		if !ok {
			return fmt.Errorf("copy readiness list %T", prototype)
		}
		listOpts := append([]client.ListOption(nil), opts...)
		listOpts = append(listOpts, client.Limit(1))
		if err := reader.List(req.Context(), list, listOpts...); err != nil {
			return fmt.Errorf("query Kubernetes API: %w", err)
		}
		return nil
	}
}

// Runtime checks that the local Runtime Server answers its Health RPC.
func Runtime(runtimeClient runtimeHealthClient, timeout time.Duration) healthz.Checker {
	return func(req *http.Request) error {
		ctx := req.Context()
		cancel := func() {}
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()

		resp, err := runtimeClient.Health(ctx, &pb.HealthRequest{})
		if err != nil {
			return fmt.Errorf("runtime Health: %w", err)
		}
		if resp == nil {
			return fmt.Errorf("runtime Health returned nil response")
		}
		if !resp.GetHealthy() {
			return fmt.Errorf("runtime unhealthy: %s", resp.GetMessage())
		}
		return nil
	}
}

// TCPListener checks that a local TCP listener is accepting connections.
// Runtime Pod readiness uses this for the runtimed proxy that backs the
// Runtime Service, so Kubernetes does not publish an endpoint before Gateway
// traffic can be accepted.
func TCPListener(address string, timeout time.Duration) healthz.Checker {
	return func(req *http.Request) error {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("parse TCP listener address %s: %w", address, err)
		}
		if host == "" {
			address = net.JoinHostPort("127.0.0.1", port)
		}
		ctx := req.Context()
		cancel := func() {}
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()

		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err != nil {
			return fmt.Errorf("dial TCP listener %s: %w", address, err)
		}
		return conn.Close()
	}
}
