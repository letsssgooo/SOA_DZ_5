//go:build integration && e2e

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	e2eCommandTimeout = 20 * time.Second
	e2eWaitTimeout    = time.Minute
	e2ePollInterval   = time.Second
)

func TestE2ECompletedParts(t *testing.T) {
	t.Run("KafkaAndProtobuf", checkKafkaAndRegistry)
	if !t.Run("ProducerAndGenerator", checkProducer) {
		return
	}
	t.Run("KafkaToClickHouse", checkPipeline)
	t.Run("ManualAggregation", checkManualAggregation)
	t.Run("ScheduledAggregation", checkScheduledAggregation)
}

func checkKafkaAndRegistry(t *testing.T) {
	description := runCompose(t, "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092",
		"--describe", "--topic", "movie-events")
	fields := strings.Fields(description)
	index := slices.Index(fields, "PartitionCount:")
	if index < 0 || index+1 >= len(fields) || fields[index+1] != "3" {
		t.Fatalf("movie-events должен иметь три партиции:\n%s", description)
	}
	t.Logf("Kafka: %s", strings.SplitN(description, "\n", 2)[0])

	body := runCompose(t, "exec", "-T", "schema-registry", "curl", "-fsS",
		"http://localhost:8081/subjects/movie-events-value/versions/latest")
	var schema struct {
		Subject string `json:"subject"`
		Type    string `json:"schemaType"`
		ID      int    `json:"id"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal([]byte(body), &schema); err != nil {
		t.Fatalf("чтение ответа Schema Registry: %v", err)
	}
	if schema.Subject != "movie-events-value" || schema.Type != "PROTOBUF" || schema.ID < 1 || schema.Version < 1 {
		t.Fatalf("неожиданная схема: %+v", schema)
	}
	t.Logf("Schema Registry: %s, %s, id=%d, version=%d", schema.Subject, schema.Type, schema.ID, schema.Version)
}

func checkProducer(t *testing.T) {
	endpoint := postEndpoint(t, "movie-service", movieServiceContainerPort, "/events")
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: movieServicePublishTimeout}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /events без полей: HTTP %d, ожидался 400", response.StatusCode)
	}
	t.Log("POST /events отклонил событие без обязательных полей с HTTP 400")

	const query = `
SELECT session_id, arrayMap(event -> event.2, events) AS types,
    arrayMap(event -> event.3, events) AS progress
FROM (
    SELECT session_id,
        arraySort(event -> event.1, groupArray((timestamp, toString(event_type), progress_seconds))) AS events
    FROM cinema.movie_events
    WHERE startsWith(user_id, 'demo-user-') AND timestamp >= now('UTC') - INTERVAL 1 DAY
    GROUP BY session_id
    HAVING count() = 6 AND max(timestamp) >= fromUnixTimestamp64Milli({since:Int64}, 'UTC')
)
LIMIT 1
FORMAT JSONEachRow`
	since := strconv.FormatInt(time.Now().UnixMilli(), 10)
	var session struct {
		SessionID string   `json:"session_id"`
		Types     []string `json:"types"`
		Progress  []int    `json:"progress"`
	}
	waitE2E(t, "новая сессия генератора в ClickHouse", func() bool {
		row := runCompose(t, "exec", "-T", "clickhouse", "clickhouse-client", "--param_since="+since, "--query", query)
		if row == "" {
			return false
		}
		if err := json.Unmarshal([]byte(row), &session); err != nil {
			t.Fatalf("чтение сессии генератора: %v", err)
		}
		return true
	})
	want := []string{"SEARCHED", "VIEW_STARTED", "VIEW_PAUSED", "VIEW_RESUMED", "VIEW_FINISHED", "LIKED"}
	if !slices.Equal(session.Types, want) || len(session.Progress) != len(want) {
		t.Fatalf("сессия %s: события %v, прогресс %v", session.SessionID, session.Types, session.Progress)
	}
	if session.Progress[1] != 0 {
		t.Fatalf("сессия %s началась с прогресса %d", session.SessionID, session.Progress[1])
	}
	for i := 2; i <= 4; i++ {
		if session.Progress[i] <= session.Progress[i-1] {
			t.Fatalf("сессия %s: прогресс просмотра не растёт: %v", session.SessionID, session.Progress)
		}
	}
	t.Logf("Генератор → ClickHouse: сессия %s, события %v, прогресс %v", session.SessionID, session.Types, session.Progress)
}

func runCompose(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), e2eCommandTimeout)
	defer cancel()
	output, err := compose(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func postEndpoint(t *testing.T, service, port, path string) string {
	t.Helper()
	endpoint := "http://" + runCompose(t, "port", service, port) + path
	ctx, cancel := context.WithTimeout(t.Context(), httpReadyTimeout)
	defer cancel()
	waitForPostEndpoint(t, ctx, endpoint)
	return endpoint
}

func waitE2E(t *testing.T, description string, ready func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), e2eWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(e2ePollInterval)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatalf("не дождались результата за %s: %s", e2eWaitTimeout, description)
		case <-ticker.C:
		}
	}
}
