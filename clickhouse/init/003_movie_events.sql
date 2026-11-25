CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8(
        'VIEW_STARTED' = 0,
        'VIEW_FINISHED' = 1,
        'VIEW_PAUSED' = 2,
        'VIEW_RESUMED' = 3,
        'LIKED' = 4,
        'SEARCHED' = 5
    ),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 0, 'DESKTOP' = 1, 'TV' = 2, 'TABLET' = 3),
    session_id String,
    progress_seconds Int32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, user_id, event_id);
