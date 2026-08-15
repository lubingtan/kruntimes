package runtimed

import (
	"context"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
)

// runtimeStatusProxy forwards Runtime status requests to the colocated Runtime
// Server. Logs use this proxy through kubectl port-forward.
type runtimeStatusProxy struct {
	pb.UnimplementedRuntimeServer
	runtimeCli pb.RuntimeClient
}

func (s *runtimeStatusProxy) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	return s.runtimeCli.Status(ctx, req)
}
