package entity

import "time"

type Metric struct {
	Name       string
	MovieID    string
	Value      *float64
	ComputedAt time.Time
}
