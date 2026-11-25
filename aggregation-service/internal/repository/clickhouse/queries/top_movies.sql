WITH
    toDateTime64(concat({date:String}, ' 00:00:00'), 3, 'UTC') AS day_start,
    day_start + INTERVAL 24 HOUR AS day_end
SELECT
    movie_id,
    count() AS views
FROM cinema.movie_events
WHERE timestamp >= day_start AND timestamp < day_end
    AND event_type = 'VIEW_STARTED'
GROUP BY movie_id
ORDER BY views DESC;
