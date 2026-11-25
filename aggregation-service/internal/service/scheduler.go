package service

import (
	"context"
	"time"
)

func RunScheduler(ctx context.Context, interval time.Duration, recalculate func(context.Context, time.Time) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
		for _, day := range []time.Time{yesterday, yesterday.AddDate(0, 0, -1), yesterday.AddDate(0, 0, -7)} {
			if ctx.Err() != nil {
				return
			}
			_ = recalculate(ctx, day)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
