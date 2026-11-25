package generator

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/service"
	"google.golang.org/protobuf/proto"
)

const (
	eventInterval        = time.Second
	searchLead           = 30 * time.Second
	pauseDuration        = 2 * time.Minute
	resumeAdvance        = time.Second
	likeDelay            = 10 * time.Second
	shortMovieDuration   = 80 * time.Minute
	featureMovieDuration = 95 * time.Minute
	longMovieDuration    = 110 * time.Minute
)

type movieProfile struct {
	movieID  string
	duration time.Duration
	device   eventsv1.MovieEvent_DeviceType
}

func Run(ctx context.Context, events *service.Events, logger *slog.Logger) {
	profiles := [...]movieProfile{
		{movieID: "demo-movie-1", duration: shortMovieDuration, device: eventsv1.MovieEvent_MOBILE},
		{movieID: "demo-movie-2", duration: featureMovieDuration, device: eventsv1.MovieEvent_DESKTOP},
		{movieID: "demo-movie-3", duration: longMovieDuration, device: eventsv1.MovieEvent_TV},
		{movieID: "demo-movie-4", duration: shortMovieDuration, device: eventsv1.MovieEvent_TABLET},
	}
	ticker := time.NewTicker(eventInterval)
	defer ticker.Stop()
	logger.Info("event generator started")

	for i := 0; ctx.Err() == nil; i = (i + 1) % len(profiles) {
		for _, event := range sessionEvents(profiles[i], time.Now()) {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if err := events.Publish(ctx, event); err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.WarnContext(ctx, "generated session interrupted", "session_id", event.GetSessionId(), "event_id", event.GetEventId(), "error", err)
				break
			}
		}
	}
}

func sessionEvents(movie movieProfile, likedAt time.Time) []*eventsv1.MovieEvent {
	userID := "demo-user-" + uuid.NewString()
	sessionID := uuid.NewString()
	finishedAt := likedAt.Add(-likeDelay)
	startedAt := finishedAt.Add(-movie.duration - pauseDuration)
	pauseProgress := movie.duration / 3
	resumeProgress := pauseProgress + resumeAdvance
	pausedAt := startedAt.Add(pauseProgress)
	resumedAt := startedAt.Add(pauseDuration + resumeProgress)

	newEvent := func(eventType eventsv1.MovieEvent_EventType, movieID string, at time.Time, progress time.Duration) *eventsv1.MovieEvent {
		return &eventsv1.MovieEvent{
			EventId:         proto.String(uuid.NewString()),
			UserId:          proto.String(userID),
			MovieId:         proto.String(movieID),
			EventType:       eventType.Enum(),
			Timestamp:       proto.Int64(at.UnixMilli()),
			DeviceType:      movie.device.Enum(),
			SessionId:       proto.String(sessionID),
			ProgressSeconds: proto.Int32(int32(progress / time.Second)),
		}
	}

	return []*eventsv1.MovieEvent{
		newEvent(eventsv1.MovieEvent_SEARCHED, "", startedAt.Add(-searchLead), 0),
		newEvent(eventsv1.MovieEvent_VIEW_STARTED, movie.movieID, startedAt, 0),
		newEvent(eventsv1.MovieEvent_VIEW_PAUSED, movie.movieID, pausedAt, pauseProgress),
		newEvent(eventsv1.MovieEvent_VIEW_RESUMED, movie.movieID, resumedAt, resumeProgress),
		newEvent(eventsv1.MovieEvent_VIEW_FINISHED, movie.movieID, finishedAt, movie.duration),
		newEvent(eventsv1.MovieEvent_LIKED, movie.movieID, likedAt, 0),
	}
}
