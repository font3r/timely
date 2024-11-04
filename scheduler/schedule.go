package scheduler

import (
	"time"
	log "timely/logger"

	"github.com/google/uuid"
)

type ScheduleStatus string

const (
	// new schedule waiting for start
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

func (s *Schedule) Start(timeFunc func() time.Time) {
	s.Status = Scheduled
	now := timeFunc().Round(time.Second)
	s.LastExecutionDate = &now
}

func (s *Schedule) Succeed(timeFunc func() time.Time) {
	nextExec := getNextExecutionTime(s.Frequency, timeFunc)

	if nextExec == (time.Time{}) {
		s.NextExecutionDate = nil
		s.Status = Finished
	} else {
		s.NextExecutionDate = &nextExec
		s.Status = Waiting
	}
}

func (s *Schedule) Failed(attempt int, timeFunc func() time.Time) error {
	if s.RetryPolicy == (RetryPolicy{}) {
		nextExec := getNextExecutionTime(s.Frequency, timeFunc)
		if nextExec == (time.Time{}) {
			s.NextExecutionDate = nil
			s.Status = Finished
		} else {
			s.NextExecutionDate = &nextExec
			s.Status = Waiting
		}
	}

	var next time.Time
	var err error

	if s.NextExecutionDate != nil {
		next, err = s.RetryPolicy.GetNextExecutionTime(*s.NextExecutionDate, attempt)
	} else {
		next, err = s.RetryPolicy.GetNextExecutionTime(timeFunc(), attempt)
	}

	if err != nil {
		return err
	}

	if next != (time.Time{}) {
		s.NextExecutionDate = &next
		s.Status = Waiting
		log.Logger.Printf("schedule retrying at %v\n", next)

		return nil
	}

	nextExec := getNextExecutionTime(s.Frequency, timeFunc)
	if nextExec == (time.Time{}) {
		s.NextExecutionDate = nil
		s.Status = Finished
	} else {
		s.NextExecutionDate = &nextExec
		s.Status = Waiting
	}

	return nil
}

func getFirstExecutionTime(frequency string, scheduleStart *time.Time, timeFunc func() time.Time) time.Time {
	if scheduleStart != nil {
		return *scheduleStart
	}

	if frequency == string(Once) {
		return timeFunc().Round(time.Second)
	}

	sch, _ := CronParser.Parse(frequency)

	return sch.Next(timeFunc().Round(time.Second))
}

func getNextExecutionTime(frequency string, timeFunc func() time.Time) time.Time {
	if frequency == string(Once) {
		return time.Time{}
	}

	sch, _ := CronParser.Parse(frequency)
	nextExec := sch.Next(timeFunc().Round(time.Second))

	return nextExec
}
