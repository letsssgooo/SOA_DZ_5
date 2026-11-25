WITH
    toDateTime64(concat({date:String}, ' 00:00:00'), 3, 'UTC') AS day_start,
    day_start + INTERVAL 24 HOUR AS day_end
SELECT
    uniqExact(user_id) AS dau,
    avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds,
    divide(countIf(event_type = 'VIEW_FINISHED'), nullIf(countIf(event_type = 'VIEW_STARTED'), 0)) AS conversion
FROM cinema.movie_events
WHERE timestamp >= day_start AND timestamp < day_end;
