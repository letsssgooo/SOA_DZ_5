package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/app"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("movie service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	return app.Run(ctx, cfg)
}
