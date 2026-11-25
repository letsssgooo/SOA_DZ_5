package generator

import (
	"testing"
	"time"

	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
)

func TestSessionEventsFollowPlayback(t *testing.T) {
	movie := movieProfile{movieID: "test-movie", duration: featureMovieDuration, device: eventsv1.MovieEvent_TV}
	likedAt := time.Date(2026, time.November, 25, 18, 0, 0, 0, time.UTC)
	events := sessionEvents(movie, likedAt)
	wantTypes := []eventsv1.MovieEvent_EventType{
		eventsv1.MovieEvent_SEARCHED,
		eventsv1.MovieEvent_VIEW_STARTED,
		eventsv1.MovieEvent_VIEW_PAUSED,
		eventsv1.MovieEvent_VIEW_RESUMED,
		eventsv1.MovieEvent_VIEW_FINISHED,
		eventsv1.MovieEvent_LIKED,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("got %d events, want %d", len(events), len(wantTypes))
	}

	for i, event := range events {
		if event.GetEventType() != wantTypes[i] {
			t.Fatalf("event %d type = %s, want %s", i, event.GetEventType(), wantTypes[i])
		}
		if event.GetSessionId() != events[0].GetSessionId() || event.GetUserId() != events[0].GetUserId() {
			t.Fatalf("event %d changed viewing session: %v", i, event)
		}
		if i > 0 && (event.GetTimestamp() <= events[i-1].GetTimestamp() || event.GetMovieId() != movie.movieID) {
			t.Fatalf("event %d changed film or timestamp order: %v", i, event)
		}
	}

	if events[0].GetMovieId() != "" || events[1].GetProgressSeconds() != 0 {
		t.Fatal("search must precede movie selection and playback must start from zero")
	}
	for i := 2; i <= 4; i++ {
		if events[i].GetProgressSeconds() <= events[i-1].GetProgressSeconds() {
			t.Fatalf("playback progress did not increase at %s", events[i].GetEventType())
		}
	}
}
