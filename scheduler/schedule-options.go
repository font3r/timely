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

func WithConfiguration(transportType TransportType, url string) ScheduleOption {
	return func(s *Schedule) {
		s.Configuration = ScheduleConfiguration{
			TransportType: transportType,
			Url:           url,
		}
	}
}

func WithJob(slug string, data *map[string]any) ScheduleOption {
	return func(s *Schedule) {
		s.Job = NewJob(slug, data)
	}
}
