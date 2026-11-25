WITH
    toDateTime64(concat({date:String}, ' 00:00:00'), 3, 'UTC') AS cohort_start,
    cohort_start + INTERVAL 24 HOUR AS d1_start,
    cohort_start + INTERVAL 48 HOUR AS d1_end,
    cohort_start + INTERVAL 168 HOUR AS d7_start,
    cohort_start + INTERVAL 192 HOUR AS d7_end,
    cohort AS (
        SELECT user_id
        FROM cinema.movie_events
        WHERE event_type = 'VIEW_STARTED'
            AND timestamp < d1_start
        GROUP BY user_id
        HAVING min(timestamp) >= cohort_start
    ),
    activity AS (
        SELECT
            user_id,
            countIf(timestamp >= d1_start AND timestamp < d1_end) > 0 AS returned_d1,
            countIf(timestamp >= d7_start AND timestamp < d7_end) > 0 AS returned_d7
        FROM cinema.movie_events
        WHERE (timestamp >= d1_start AND timestamp < d1_end)
            OR (timestamp >= d7_start AND timestamp < d7_end)
        GROUP BY user_id
    )
SELECT
    divide(countIf(activity.returned_d1), nullIf(count(), 0)) AS retention_d1,
    divide(countIf(activity.returned_d7), nullIf(count(), 0)) AS retention_d7
FROM cohort
LEFT JOIN activity USING (user_id);
