package grpcapi

import (
	"context"

	pb "feishu-botd/gen/feishubotd/v1"
	"feishu-botd/internal/service"
)

// healthServer adapts BotdHealthService onto the shared core service.
type healthServer struct {
	pb.UnimplementedBotdHealthServiceServer
	svc *service.Service
}

func (h *healthServer) Health(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	info := h.svc.Health()
	return &pb.HealthResponse{Status: info.Status, Service: info.Service, Version: info.Version}, nil
}

func (h *healthServer) Ready(ctx context.Context, _ *pb.ReadyRequest) (*pb.ReadyResponse, error) {
	ready, checks := h.svc.Ready(ctx)
	return &pb.ReadyResponse{Ready: ready, Checks: checks}, nil
}
