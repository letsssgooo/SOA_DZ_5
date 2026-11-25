package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/letsssgooo/SOA_DZ_5/aggregation-service/internal/entity"
)

const saveMetricsQuery = `
WITH incoming AS (
    SELECT *
    FROM unnest($2::text[], $3::text[], $4::double precision[], $5::timestamptz[])
        AS data(metric_name, movie_id, value, computed_at)
), saved AS (
    INSERT INTO analytics.daily_metrics (metric_date, metric_name, movie_id, value, computed_at)
    SELECT $1, metric_name, movie_id, value, computed_at
    FROM incoming
    ON CONFLICT (metric_date, metric_name, movie_id) DO UPDATE
    SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
)
DELETE FROM analytics.daily_metrics AS old
WHERE old.metric_date = $1
    AND NOT EXISTS (
        SELECT 1 FROM incoming
        WHERE incoming.metric_name = old.metric_name AND incoming.movie_id = old.movie_id
    )`

func Upsert(ctx context.Context, tx pgx.Tx, date time.Time, metrics []entity.Metric) error {
	names := make([]string, len(metrics))
	movieIDs := make([]string, len(metrics))
	values := make([]*float64, len(metrics))
	computedAt := make([]time.Time, len(metrics))
	for i, metric := range metrics {
		names[i] = metric.Name
		movieIDs[i] = metric.MovieID
		values[i] = metric.Value
		computedAt[i] = metric.ComputedAt
	}
	if _, err := tx.Exec(ctx, saveMetricsQuery, date, names, movieIDs, values, computedAt); err != nil {
		return fmt.Errorf("upsert PostgreSQL metrics: %w", err)
	}
	return nil
}
