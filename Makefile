COMPOSE = docker compose

.PHONY: up test-e2e logs

up:
	$(COMPOSE) up -d --build --wait

test-e2e: export MOVIE_GENERATOR_ENABLED := true
test-e2e: export AGGREGATION_INTERVAL := 5s
test-e2e: up
	go test -count=1 -v -timeout=3m -tags=integration,e2e -run '^TestE2ECompletedParts$$' ./tests/integration

logs:
	$(COMPOSE) logs -f --tail=30 kafka-init schema-init postgres-migrate movie-service aggregation-service
