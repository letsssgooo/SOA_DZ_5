CREATE TABLE IF NOT EXISTS analytics.daily_metrics
(
    metric_date DateTime64(3, 'UTC'),
    metric_name String,
    movie_id String,
    value Nullable(Float64),
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(metric_date)
ORDER BY (metric_date, metric_name, movie_id);

CREATE TABLE IF NOT EXISTS analytics.daily_metrics_stage AS analytics.daily_metrics;
