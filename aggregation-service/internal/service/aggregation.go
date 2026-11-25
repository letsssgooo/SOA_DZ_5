package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsssgooo/SOA_DZ_5/aggregation-service/internal/repository/clickhouse"
	"github.com/letsssgooo/SOA_DZ_5/aggregation-service/internal/repository/postgres"
)

const (
	writeAttempts = 3
	writeTimeout  = 45 * time.Second
	retryDelay    = 500 * time.Millisecond

	sqlStateSerializationFailure     = "40001"
	sqlStateDeadlockDetected         = "40P01"
	sqlStateLockNotAvailable         = "55P03"
	sqlStateAdminShutdown            = "57P01"
	sqlStateCrashShutdown            = "57P02"
	sqlStateCannotConnectNow         = "57P03"
	sqlStateConnectionExceptionClass = "08"
)

func Recalculate(ctx context.Context, conn ch.Conn, pool *pgxpool.Pool, logger *slog.Logger, day time.Time) (err error) {
	day = day.UTC().Truncate(24 * time.Hour)
	date := day.Format(time.DateOnly)
	startedAt := time.Now()
	today := startedAt.UTC().Truncate(24 * time.Hour)
	includeD1 := day.AddDate(0, 0, 1).Before(today)
	includeD7 := day.AddDate(0, 0, 7).Before(today)
	var processedEvents uint64
	logger.InfoContext(ctx, "daily aggregation started", "date", date)
	defer func() {
		status := "success"
		if err != nil {
			status = "failed"
		}
		logger.InfoContext(ctx, "daily aggregation finished", "date", date, "status", status,
			"processed_events", processedEvents, "duration_ms", time.Since(startedAt).Milliseconds())
	}()

	for attempt := 1; attempt <= writeAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = pgx.BeginFunc(attemptCtx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(attemptCtx, "SELECT pg_advisory_xact_lock(hashtext('analytics.daily_metrics')::bigint)"); err != nil {
				return fmt.Errorf("lock daily metrics: %w", err)
			}
			count, err := clickhouse.CountEvents(attemptCtx, conn, day)
			if err != nil {
				return err
			}
			processedEvents = count
			metrics, err := clickhouse.Materialize(attemptCtx, conn, day, includeD1, includeD7)
			if err != nil {
				return err
			}
			return postgres.Upsert(attemptCtx, tx, day, metrics)
		})
		cancel()
		if err == nil {
			return nil
		}

		retry := attempt < writeAttempts && ctx.Err() == nil && retryable(err)
		level := slog.LevelError
		if retry {
			level = slog.LevelWarn
		}
		logger.Log(ctx, level, "daily metrics write failed", "date", date, "attempt", attempt, "retry", retry, "error", err)
		if !retry {
			break
		}

		timer := time.NewTimer(retryDelay << (attempt - 1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("save daily metrics for %s: %w", date, err)
}

func retryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case sqlStateSerializationFailure, sqlStateDeadlockDetected, sqlStateLockNotAvailable,
			sqlStateAdminShutdown, sqlStateCrashShutdown, sqlStateCannotConnectNow:
			return true
		default:
			return strings.HasPrefix(pgErr.Code, sqlStateConnectionExceptionClass)
		}
	}
	var netErr *net.OpError
	return errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err)
}
