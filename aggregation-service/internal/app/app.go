package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsssgooo/SOA_DZ_5/aggregation-service/internal/config"
	"github.com/letsssgooo/SOA_DZ_5/aggregation-service/internal/service"
	httptransport "github.com/letsssgooo/SOA_DZ_5/aggregation-service/internal/transport/http"
)

const (
	databaseStartupTimeout = 5 * time.Second
	httpReadHeaderTimeout  = 5 * time.Second
	httpShutdownTimeout    = 5 * time.Second
)

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		DialTimeout: databaseStartupTimeout,
	})
	if err != nil {
		return fmt.Errorf("open ClickHouse connection: %w", err)
	}
	defer ch.Close()

	if err := checkClickHouse(ctx, ch); err != nil {
		return fmt.Errorf("check access to ClickHouse events: %w", err)
	}
	logger.Info("connected to ClickHouse")

	postgresURL := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.PostgresUser, cfg.PostgresPassword),
		Host:   net.JoinHostPort(cfg.PostgresHost, strconv.Itoa(int(cfg.PostgresPort))),
		Path:   "/" + cfg.PostgresDB,
		RawQuery: url.Values{
			"connect_timeout": {strconv.Itoa(int(databaseStartupTimeout / time.Second))},
			"sslmode":         {"disable"},
		}.Encode(),
	}
	pg, err := pgxpool.New(ctx, postgresURL.String())
	if err != nil {
		return fmt.Errorf("open PostgreSQL connection: %w", err)
	}
	defer pg.Close()

	recalculate := func(ctx context.Context, day time.Time) error {
		return service.Recalculate(ctx, ch, pg, logger, day)
	}
	checkCtx, cancel := context.WithTimeout(ctx, databaseStartupTimeout)
	err = pg.Ping(checkCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("check PostgreSQL connection: %w", err)
	}
	logger.Info("connected to PostgreSQL")
	return serve(ctx, cfg, logger, recalculate)
}

func serve(ctx context.Context, cfg config.Config, logger *slog.Logger, recalculate func(context.Context, time.Time) error) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	server := &http.Server{
		Handler:           httptransport.NewHandler(recalculate),
		ReadHeaderTimeout: httpReadHeaderTimeout,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		service.RunScheduler(ctx, cfg.Interval, recalculate)
	}()
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()
	logger.Info("service started", "http_addr", listener.Addr().String(), "interval", cfg.Interval.String())

	var serveErr error
	select {
	case serveErr = <-serverDone:
	case <-ctx.Done():
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	<-schedulerDone

	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", serveErr)
	}
	if shutdownErr != nil {
		return fmt.Errorf("shut down HTTP: %w", shutdownErr)
	}
	logger.Info("service stopped")
	return nil
}

func checkClickHouse(ctx context.Context, ch clickhouse.Conn) error {
	checkCtx, cancel := context.WithTimeout(ctx, databaseStartupTimeout)
	defer cancel()

	rows, err := ch.Query(checkCtx, "SELECT event_id FROM cinema.movie_events LIMIT 0")
	if err != nil {
		return err
	}
	return rows.Close()
}
