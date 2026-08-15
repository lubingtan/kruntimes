package runtimed

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/internal/artifact"
)

// StartRuntimeProxyServer starts the gRPC proxies exposed by runtimed on addr.
// Runtime status calls are proxied to the colocated Runtime Server.
// SessionRuntime calls route to the owner runtimed, which proxies to its local
// Runtime Server. Artifact downloads are served from this Runtime Pod's
// ArtifactStore.
func StartRuntimeProxyServer(
	ctx context.Context,
	runtimeEndpoint, addr string,
	apiReader, sessionReader client.Reader,
	store artifact.Store,
	runtimeNamespace,
	runtimeName,
	podName string,
) error {
	_, statusPort, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parse runtimed proxy address %q: %w", addr, err)
	}
	conn, err := grpc.NewClient(runtimeEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	srv := grpc.NewServer()
	pb.RegisterRuntimeServer(srv, &runtimeStatusProxy{runtimeCli: pb.NewRuntimeClient(conn)})
	pb.RegisterSessionRuntimeServer(srv, newSessionRuntimeProxy(
		sessionReader,
		pb.NewSessionRuntimeClient(conn),
		runtimeNamespace,
		runtimeName,
		podName,
		statusPort,
	))
	if store != nil {
		RegisterArtifactService(srv, apiReader, store, runtimeNamespace, runtimeName)
	}

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	klog.Infof("Runtime proxy server listening on %s", addr)
	return srv.Serve(lis)
}
