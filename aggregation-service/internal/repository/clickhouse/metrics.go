package clickhouse

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/letsssgooo/SOA_DZ_5/aggregation-service/internal/entity"
)

//go:embed queries/daily.sql
var dailyQuery string

//go:embed queries/top_movies.sql
var moviesQuery string

//go:embed queries/retention.sql
var retentionQuery string

const dayStart = `toDateTime64(concat({date:String}, ' 00:00:00'), 3, 'UTC')`

var stageQuery = fmt.Sprintf(`
INSERT INTO analytics.daily_metrics_stage (metric_date, metric_name, movie_id, value, computed_at)
SELECT %s, metric_name, movie_id, value, now64(3, 'UTC')
FROM (
    SELECT metric.1 AS metric_name, '' AS movie_id, metric.2 AS value
    FROM (%s)
    ARRAY JOIN [('dau', toFloat64(dau)), ('avg_watch_seconds', avg_watch_seconds), ('conversion', conversion)] AS metric

    UNION ALL

    SELECT 'movie_views', movie_id, toFloat64(views)
    FROM (%s)
`, dayStart, queryBody(dailyQuery), queryBody(moviesQuery))

var retentionStageQuery = fmt.Sprintf(`
    UNION ALL

    SELECT metric.1, '', metric.2
    FROM (%s)
    ARRAY JOIN [('retention_d1', retention_d1), ('retention_d7', retention_d7)] AS metric
    WHERE metric.1 = 'retention_d1' OR {include_d7:Bool}
`, queryBody(retentionQuery))

func queryBody(query string) string {
	return strings.TrimSuffix(strings.TrimSpace(query), ";")
}

func CountEvents(ctx context.Context, conn ch.Conn, day time.Time) (uint64, error) {
	ctx = ch.Context(ctx, ch.WithParameters(ch.Parameters{"date": day.Format(time.DateOnly)}))
	var count uint64
	err := conn.QueryRow(ctx, `
WITH `+dayStart+` AS day_start
SELECT count() FROM cinema.movie_events
WHERE timestamp >= day_start AND timestamp < day_start + INTERVAL 24 HOUR`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count events in ClickHouse: %w", err)
	}
	return count, nil
}

func Materialize(ctx context.Context, conn ch.Conn, day time.Time, includeD1, includeD7 bool) ([]entity.Metric, error) {
	date := day.Format(time.DateOnly)
	ctx = ch.Context(ctx, ch.WithParameters(ch.Parameters{
		"date":       date,
		"partition":  day.Format("20060102"),
		"include_d7": strconv.FormatBool(includeD7),
	}))
	if err := conn.Exec(ctx, "TRUNCATE TABLE analytics.daily_metrics_stage"); err != nil {
		return nil, fmt.Errorf("clear ClickHouse staging table: %w", err)
	}
	query := stageQuery
	if includeD1 {
		query += retentionStageQuery
	}
	if err := conn.Exec(ctx, query+")"); err != nil {
		return nil, fmt.Errorf("calculate ClickHouse metrics: %w", err)
	}
	if err := conn.Exec(ctx, "ALTER TABLE analytics.daily_metrics REPLACE PARTITION {partition:UInt32} FROM analytics.daily_metrics_stage"); err != nil {
		return nil, fmt.Errorf("replace ClickHouse metrics for %s: %w", date, err)
	}

	rows, err := conn.Query(ctx, `
SELECT metric_name, movie_id, value, computed_at
FROM analytics.daily_metrics
WHERE metric_date = `+dayStart)
	if err != nil {
		return nil, fmt.Errorf("read saved ClickHouse metrics: %w", err)
	}
	defer rows.Close()

	var metrics []entity.Metric
	for rows.Next() {
		var metric entity.Metric
		if err := rows.Scan(&metric.Name, &metric.MovieID, &metric.Value, &metric.ComputedAt); err != nil {
			return nil, fmt.Errorf("scan saved ClickHouse metric: %w", err)
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read saved ClickHouse metrics: %w", err)
	}
	return metrics, nil
}
