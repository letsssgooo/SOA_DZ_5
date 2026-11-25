package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/client/kafka"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/client/schemaregistry"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/config"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/generator"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/service"
	httptransport "github.com/letsssgooo/SOA_DZ_5/movie-service/internal/transport/http"
)

const (
	movieEventSchemaPath  = "schemas/movie-event.proto"
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 10 * time.Second
	httpWriteTimeout      = 15 * time.Second
	httpIdleTimeout       = 30 * time.Second
	httpShutdownTimeout   = 5 * time.Second
)

func Run(ctx context.Context, cfg config.Config) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "movie-service")
	schema, err := os.ReadFile(movieEventSchemaPath)
	if err != nil {
		return fmt.Errorf("read movie event schema: %w", err)
	}
	schemaID, err := schemaregistry.LookupProtobufID(ctx, cfg.SchemaRegistryURL, kafka.MovieEventsTopic+"-value", string(schema))
	if err != nil {
		return err
	}
	publisher, err := kafka.NewPublisher(cfg.KafkaBrokers, schemaID, logger)
	if err != nil {
		return err
	}
	defer publisher.Close()

	events := service.NewEvents(publisher)
	server := &http.Server{
		Handler:           httptransport.NewHandler(events, logger),
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()
	logger.Info("service started", "http_addr", listener.Addr().String())

	if cfg.GeneratorEnabled {
		generatorCtx, stopGenerator := context.WithCancel(ctx)
		generatorDone := make(chan struct{})
		go func() {
			defer close(generatorDone)
			generator.Run(generatorCtx, events, logger)
		}()
		defer func() {
			stopGenerator()
			<-generatorDone
		}()
	}

	select {
	case err := <-serverDone:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shut down HTTP: %w", err)
		}
	}
	logger.Info("service stopped")
	return nil
}
