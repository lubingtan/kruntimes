package gateway

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
)

// GRPCDialer dials the SessionRuntime service exposed by a Runtime Service.
// The hop remains inside the Kubernetes cluster and is protected by the
// Runtime's NetworkPolicy and the gateway's authenticated HTTP boundary.
type GRPCDialer struct{}

func (GRPCDialer) Dial(_ context.Context, address string) (pb.SessionRuntimeClient, io.Closer, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("create SessionRuntime client: %w", err)
	}
	return pb.NewSessionRuntimeClient(connection), connection, nil
}

func (GRPCDialer) DialFunction(_ context.Context, address string) (pb.FunctionRuntimeClient, io.Closer, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("create FunctionRuntime client: %w", err)
	}
	return pb.NewFunctionRuntimeClient(connection), connection, nil
}
