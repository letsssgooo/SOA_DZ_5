SET LOCAL ROLE aggregation;

CREATE TABLE IF NOT EXISTS analytics.daily_metrics
(
    metric_date date NOT NULL,
    metric_name text NOT NULL,
    movie_id text NOT NULL,
    value double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (metric_date, metric_name, movie_id)
);
