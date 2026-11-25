package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/service"
)

const (
	eventID    = "e9090966-ec50-4bb0-9968-a1a7d16081bb"
	searchJSON = `{"event_id":"` + eventID + `","user_id":"user-1","movie_id":"","event_type":"SEARCHED","timestamp":1795622400000,"device_type":"MOBILE","session_id":"session-1","progress_seconds":0}`
)

type publishFunc func(context.Context, *eventsv1.MovieEvent) error

func (f publishFunc) Publish(ctx context.Context, event *eventsv1.MovieEvent) error {
	return f(ctx, event)
}

func TestPostEventsReturnsIDAfterPublication(t *testing.T) {
	result := httptest.NewRecorder()
	calls := 0
	publisher := publishFunc(func(_ context.Context, event *eventsv1.MovieEvent) error {
		calls++
		if result.Body.Len() != 0 {
			t.Fatal("response was written before publication finished")
		}
		if event.GetEventId() != eventID {
			t.Fatalf("unexpected event: %v", event)
		}
		return nil
	})
	handler := NewHandler(service.NewEvents(publisher), slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler.ServeHTTP(result, httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(searchJSON)))

	var body map[string]string
	if err := json.NewDecoder(result.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if result.Code != http.StatusOK || body["event_id"] != eventID || calls != 1 {
		t.Fatalf("status=%d response=%v calls=%d", result.Code, body, calls)
	}
}

func TestPostEventsRejectsInvalidEventsBeforePublication(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		field string
	}{
		{"malformed JSON", "{", "JSON"},
		{"missing progress", strings.Replace(searchJSON, `,"progress_seconds":0`, "", 1), "progress_seconds"},
		{"invalid UUID", strings.Replace(searchJSON, eventID, "bad-id", 1), "event_id"},
		{"unknown enum number", strings.Replace(searchJSON, `"SEARCHED"`, `99`, 1), "event_type"},
		{"view without movie", strings.Replace(searchJSON, "SEARCHED", "VIEW_STARTED", 1), "movie_id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := publishFunc(func(context.Context, *eventsv1.MovieEvent) error {
				t.Fatal("invalid event reached the publisher")
				return nil
			})
			handler := NewHandler(service.NewEvents(publisher), slog.New(slog.NewTextHandler(io.Discard, nil)))
			result := httptest.NewRecorder()
			handler.ServeHTTP(result, httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(tt.body)))

			if result.Code != http.StatusBadRequest || !strings.Contains(result.Body.String(), tt.field) {
				t.Fatalf("status=%d response=%s", result.Code, result.Body.String())
			}
		})
	}
}

func TestPostEventsReturnsErrorWhenPublicationFails(t *testing.T) {
	publisher := publishFunc(func(context.Context, *eventsv1.MovieEvent) error {
		return errors.New("broker unavailable")
	})
	handler := NewHandler(service.NewEvents(publisher), slog.New(slog.NewTextHandler(io.Discard, nil)))
	result := httptest.NewRecorder()
	handler.ServeHTTP(result, httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(searchJSON)))

	var body map[string]string
	if err := json.NewDecoder(result.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if result.Code != http.StatusServiceUnavailable || body["error"] == "" || body["event_id"] != "" {
		t.Fatalf("status=%d response=%v", result.Code, body)
	}
}
