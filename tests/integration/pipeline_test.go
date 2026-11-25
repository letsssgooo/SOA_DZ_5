//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	composeStartupTimeout      = 5 * time.Minute
	composeWaitTimeout         = 3 * time.Minute
	httpReadyTimeout           = 30 * time.Second
	httpProbeTimeout           = 2 * time.Second
	httpProbeInterval          = 250 * time.Millisecond
	movieServiceContainerPort  = "8080"
	movieServicePublishTimeout = 10 * time.Second
	searchEventTimestampMillis = 1795622400123
	clickHouseEventTimeout     = 30 * time.Second
	clickHousePollInterval     = 500 * time.Millisecond
)

const findEventQuery = `
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    toUnixTimestamp64Milli(stored.timestamp) AS timestamp,
    device_type,
    session_id,
    progress_seconds
FROM cinema.movie_events AS stored
WHERE event_id = {event_id:UUID}
LIMIT 2
FORMAT JSONEachRow`

func TestPipeline(t *testing.T) {
	startupCtx, cancelStartup := context.WithTimeout(t.Context(), composeStartupTimeout)
	defer cancelStartup()
	if _, err := compose(startupCtx, "up", "--no-recreate", "--wait",
		"--wait-timeout", strconv.Itoa(int(composeWaitTimeout/time.Second))); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t)
}

func checkPipeline(t *testing.T) {
	t.Helper()
	readyCtx, cancelReady := context.WithTimeout(t.Context(), httpReadyTimeout)
	defer cancelReady()
	address, err := compose(readyCtx, "port", "movie-service", movieServiceContainerPort)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + address + "/events"
	waitForPostEndpoint(t, readyCtx, endpoint)
	t.Logf("movie-service is ready at http://%s", address)

	event := &eventsv1.MovieEvent{
		EventId:         proto.String(uuid.NewString()),
		UserId:          proto.String("integration-user"),
		MovieId:         proto.String(""),
		EventType:       eventsv1.MovieEvent_SEARCHED.Enum(),
		Timestamp:       proto.Int64(searchEventTimestampMillis),
		DeviceType:      eventsv1.MovieEvent_MOBILE.Enum(),
		SessionId:       proto.String("integration-session"),
		ProgressSeconds: proto.Int32(0),
	}
	publishEvent(t, endpoint, event)
	t.Logf("published SEARCHED event %s", event.GetEventId())

	if err := verifyStoredEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	t.Logf("event %s matched all eight fields in cinema.movie_events", event.GetEventId())
}

func compose(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", "docker-compose.yml"}, args...)...)
	command.Dir = filepath.Join("..", "..")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.Output()
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return "", fmt.Errorf("docker compose %s: %w\n%s\n%s", strings.Join(args, " "), err, stdout, stderr.String())
	}
	return strings.TrimSpace(string(stdout)), nil
}

func publishEvent(t *testing.T, endpoint string, event *eventsv1.MovieEvent) {
	t.Helper()

	payload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(event)
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("create POST /events request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: movieServicePublishTimeout}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send POST /events: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST /events returned HTTP %d, want %d", response.StatusCode, http.StatusOK)
	}

	var result struct {
		EventID string `json:"event_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode POST /events response: %v", err)
	}
	if result.EventID != event.GetEventId() {
		t.Fatalf("POST /events returned event_id %q, want %q", result.EventID, event.GetEventId())
	}
}

func verifyStoredEvent(ctx context.Context, expected *eventsv1.MovieEvent) error {
	ctx, cancel := context.WithTimeout(ctx, clickHouseEventTimeout)
	defer cancel()
	ticker := time.NewTicker(clickHousePollInterval)
	defer ticker.Stop()
	eventID := expected.GetEventId()

	for {
		rows, err := compose(ctx, "exec", "-T", "clickhouse", "clickhouse-client",
			"--param_event_id="+eventID, "--query", findEventQuery)
		if err != nil {
			return fmt.Errorf("query event %s in cinema.movie_events: %w", eventID, err)
		}
		if rows != "" {
			if strings.Contains(rows, "\n") {
				return fmt.Errorf("expected one row for event %s in cinema.movie_events, found at least two:\n%s", eventID, rows)
			}
			var actual eventsv1.MovieEvent
			if err := protojson.Unmarshal([]byte(rows), &actual); err != nil {
				return fmt.Errorf("decode stored event %s: %w", eventID, err)
			}
			if !proto.Equal(&actual, expected) {
				return fmt.Errorf("stored event %s differs from the published event\ngot:  %s\nwant: %s", eventID, &actual, expected)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for event %s in cinema.movie_events: %w", eventID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForPostEndpoint(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()

	request, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		t.Fatalf("create readiness request: %v", err)
	}
	client := &http.Client{Timeout: httpProbeTimeout}
	ticker := time.NewTicker(httpProbeInterval)
	defer ticker.Stop()

	for {
		response, probeErr := client.Do(request)
		if probeErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusMethodNotAllowed {
				return
			}
			probeErr = fmt.Errorf("received %s", response.Status)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("endpoint %s did not become ready: %v (last probe: %v)", endpoint, ctx.Err(), probeErr)
		case <-ticker.C:
		}
	}
}
