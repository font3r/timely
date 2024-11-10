package scheduler

import (
	"time"
	log "timely/logger"

	"github.com/google/uuid"
)

type ScheduleStatus string

const (
	// new schedule waiting for being scheduled
	Waiting ScheduleStatus = "waiting"

	// job scheduled, waiting for result
	Scheduled ScheduleStatus = "scheduled"

	// schedule finished, either with job completed or failed that cannot be retried further
	Finished ScheduleStatus = "finished"
)

type Schedule struct {
	Id                uuid.UUID
	GroupId           uuid.UUID
	Description       string
	Frequency         string
	Status            ScheduleStatus
	RetryPolicy       RetryPolicy
	Configuration     ScheduleConfiguration
	LastExecutionDate *time.Time
	NextExecutionDate *time.Time
	Job               *Job
}

type TransportType string

const (
	Http     TransportType = "http"
	Rabbitmq TransportType = "rabbitmq"
)

type ScheduleConfiguration struct {
	TransportType TransportType
	Url           string
}

func NewSchedule(description, frequency, slug string, data *map[string]any,
	policy RetryPolicy, configuration ScheduleConfiguration, scheduleStart *time.Time,
	time func() time.Time) Schedule {

	execution := getFirstExecutionTime(frequency, scheduleStart, time)

	return Schedule{
		Id:                uuid.New(),
		GroupId:           uuid.New(),
		Description:       description,
		Frequency:         frequency,
		Status:            Waiting,
		RetryPolicy:       policy,
		LastExecutionDate: nil,
		NextExecutionDate: &execution,
		Job:               NewJob(slug, data),
		Configuration:     configuration,
	}
}

func (s *Schedule) Start(now func() time.Time) {
	lastExecAt := now().Round(time.Second)
	s.LastExecutionDate = &lastExecAt
	s.NextExecutionDate = nil
	s.Status = Scheduled
}

func (s *Schedule) Succeed(now func() time.Time) {
	nextExecAt := getNextExecutionTime(s.Frequency, now)

	if nextExecAt == (time.Time{}) {
		s.NextExecutionDate = nil
		s.Status = Finished
	} else {
		s.NextExecutionDate = &nextExecAt
		s.Status = Waiting
	}
}

func (s *Schedule) Failed(attempt int, now func() time.Time) {
	if s.RetryPolicy != (RetryPolicy{}) {
		retryAt := s.RetryPolicy.GetNextExecutionTime(now(), attempt)

		if retryAt != (time.Time{}) {
			s.NextExecutionDate = &retryAt
			s.Status = Waiting
			log.Logger.Printf("schedule retrying at %v\n", retryAt)

			return
		}
	}

	nextExecAt := getNextExecutionTime(s.Frequency, now)
	if nextExecAt == (time.Time{}) {
		s.NextExecutionDate = nil
		s.Status = Finished
	} else {
		s.NextExecutionDate = &nextExecAt
		s.Status = Waiting
	}
}

func getFirstExecutionTime(frequency string, scheduleStart *time.Time, now func() time.Time) time.Time {
	if scheduleStart != nil {
		return *scheduleStart
	}

	if frequency == string(Once) {
		return now().Round(time.Second)
	}

	sch, _ := CronParser.Parse(frequency)

	return sch.Next(now().Round(time.Second))
}

func getNextExecutionTime(frequency string, now func() time.Time) time.Time {
	if frequency == string(Once) {
		return time.Time{}
	}

	sch, _ := CronParser.Parse(frequency)
	nextExec := sch.Next(now().Round(time.Second))

	return nextExec
}
