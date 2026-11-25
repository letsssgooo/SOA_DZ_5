CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv
TO cinema.movie_events
AS SELECT
    toUUID(events.event_id) AS event_id,
    events.user_id,
    events.movie_id,
    events.event_type,
    fromUnixTimestamp64Milli(events.timestamp, 'UTC') AS timestamp,
    events.device_type,
    events.session_id,
    events.progress_seconds
FROM cinema.movie_events_kafka AS events;
