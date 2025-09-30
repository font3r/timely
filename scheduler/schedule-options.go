package scheduler

import "time"

type ScheduleOption func(*Schedule)

func WithRetryPolicy(retryPolicy RetryPolicy) ScheduleOption {
	return func(s *Schedule) {
		s.RetryPolicy = retryPolicy
	}
}

func WithScheduleStart(time *time.Time) ScheduleOption {
	return func(s *Schedule) {
		s.ScheduleStart = time
	}
}

func WithJob(slug string, data *map[string]any) ScheduleOption {
	return func(s *Schedule) {
		s.Job = NewJob(slug, data)
	}
}
