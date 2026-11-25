CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
(
    event_id String,
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
    timestamp Int64,
    device_type Enum8('MOBILE' = 0, 'DESKTOP' = 1, 'TV' = 2, 'TABLET' = 3),
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'ProtobufSingle',
    kafka_schema = 'movie-event.proto:MovieEvent',
    kafka_schema_registry_skip_bytes = 6;
