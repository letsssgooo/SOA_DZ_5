package service

import (
	"context"
	"errors"
	"fmt"

	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
)

var ErrInvalidEvent = errors.New("invalid event")

type Publisher interface {
	Publish(ctx context.Context, event *eventsv1.MovieEvent) error
}

type Events struct {
	publisher Publisher
}

func NewEvents(publisher Publisher) *Events {
	return &Events{publisher: publisher}
}

func (s *Events) Publish(ctx context.Context, event *eventsv1.MovieEvent) error {
	if err := validateEvent(event); err != nil {
		return err
	}
	if err := s.publisher.Publish(ctx, event); err != nil {
		return fmt.Errorf("publish movie event: %w", err)
	}
	return nil
}
