package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
)

func validateEvent(event *eventsv1.MovieEvent) error {
	requiredFields := []struct {
		name    string
		present bool
	}{
		{"event_id", event.EventId != nil},
		{"user_id", event.UserId != nil},
		{"movie_id", event.MovieId != nil},
		{"event_type", event.EventType != nil},
		{"timestamp", event.Timestamp != nil},
		{"device_type", event.DeviceType != nil},
		{"session_id", event.SessionId != nil},
		{"progress_seconds", event.ProgressSeconds != nil},
	}
	for _, field := range requiredFields {
		if !field.present {
			return fmt.Errorf("%w: %s is required", ErrInvalidEvent, field.name)
		}
	}

	if id := event.GetEventId(); len(id) != 36 || uuid.Validate(id) != nil {
		return fmt.Errorf("%w: event_id must be a UUID", ErrInvalidEvent)
	}
	if strings.TrimSpace(event.GetUserId()) == "" {
		return fmt.Errorf("%w: user_id must not be blank", ErrInvalidEvent)
	}
	if strings.TrimSpace(event.GetSessionId()) == "" {
		return fmt.Errorf("%w: session_id must not be blank", ErrInvalidEvent)
	}

	eventType := event.GetEventType()
	if _, ok := eventsv1.MovieEvent_EventType_name[int32(eventType)]; !ok {
		return fmt.Errorf("%w: unknown event_type", ErrInvalidEvent)
	}
	if _, ok := eventsv1.MovieEvent_DeviceType_name[int32(event.GetDeviceType())]; !ok {
		return fmt.Errorf("%w: unknown device_type", ErrInvalidEvent)
	}
	movieID := event.GetMovieId()
	searchWithoutMovie := eventType == eventsv1.MovieEvent_SEARCHED && movieID == ""
	if strings.TrimSpace(movieID) == "" && !searchWithoutMovie {
		return fmt.Errorf("%w: movie_id must identify a movie or be empty for SEARCHED", ErrInvalidEvent)
	}
	if year := time.UnixMilli(event.GetTimestamp()).UTC().Year(); year < 1 || year > 9999 {
		return fmt.Errorf("%w: timestamp must be Unix milliseconds in years 0001–9999", ErrInvalidEvent)
	}

	if event.GetProgressSeconds() < 0 {
		return fmt.Errorf("%w: progress_seconds must be non-negative", ErrInvalidEvent)
	}
	return nil
}
