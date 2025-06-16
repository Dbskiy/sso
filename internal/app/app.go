package app

import (
	"log/slog"
	grpcapp "sso/internal/app/grpc"
	"sso/internal/services/auth"
	postg "sso/internal/storage/postgres"
	"time"
)

type App struct {
	GRPCServer *grpcapp.App
}

func New(log *slog.Logger, grpcPort int, storagePath string, tokenTTL time.Duration) *App {
	storage, err := postg.New(storagePath)

	if err != nil {
		panic(err)
	}

	authService := auth.New(log, storage, storage, storage, tokenTTL)

	gRPCServer := grpcapp.New(log, authService, grpcPort)
	return &App{
		GRPCServer: gRPCServer,
	}
}
