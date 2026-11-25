# Online Cinema — Kafka и Protobuf

`movie-service` отправляет события из HTTP API и генератора в Kafka-топик `movie-events` по Protobuf-схеме из Schema Registry.

## Запуск

Требуются Docker, Docker Compose и Make.

```bash
make up
```

Compose запускает Kafka, Schema Registry, ClickHouse, PostgreSQL и Go-сервисы.

## Проверка

```bash
make test-e2e
```

```bash
make logs
```
