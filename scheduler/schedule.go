package scheduler

import (
	"time"
	log "timely/logger"

	"github.com/google/uuid"
)

type ScheduleStatus string

const (
	Waiting   ScheduleStatus = "waiting"   // waiting to schedule next job
	Scheduled ScheduleStatus = "scheduled" // job scheduled
	Succeed   ScheduleStatus = "succeed"   // successfully processed
	Failed    ScheduleStatus = "failed"    // error during processing
)

type Schedule struct {
	Id                uuid.UUID
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
	policy RetryPolicy, configuration ScheduleConfiguration, scheduleStart *time.Time) Schedule {

	execution := getFirstExecution(frequency, scheduleStart)

	return Schedule{
		Id:                uuid.New(),
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

func getFirstExecution(frequency string, scheduleStart *time.Time) time.Time {
	if scheduleStart != nil {
		return *scheduleStart
	}

	if frequency == string(Once) {
		return time.Now().Round(time.Second)
	}

	sch, _ := CronParser.Parse(frequency)

	return sch.Next(time.Now().Round(time.Second))
}

func (s *Schedule) Start() {
	s.Status = Scheduled
	now := time.Now().Round(time.Second)
	s.LastExecutionDate = &now
}

func (s *Schedule) Failed(attempt int) error {
	s.Status = Failed

	if s.RetryPolicy == (RetryPolicy{}) {
		s.NextExecutionDate = nil
		return nil
	}

	var next time.Time
	var err error

	if s.NextExecutionDate != nil {
		next, err = s.RetryPolicy.GetNextExecutionTime(*s.NextExecutionDate, attempt)
	} else {
		next, err = s.RetryPolicy.GetNextExecutionTime(time.Now(), attempt)
	}

	if err != nil {
		return err
	}

	if next == (time.Time{}) {
		s.NextExecutionDate = nil
		log.Logger.Println("schedule failed after retrying")
	} else {
		s.NextExecutionDate = &next
		log.Logger.Printf("schedule retrying at %v\n", next)
	}

	return nil
}

func (s *Schedule) Succeed() {
	if s.Frequency == string(Once) {
		s.NextExecutionDate = nil
		s.Status = Succeed
		return
	}

	sch, _ := CronParser.Parse(s.Frequency)
	nextExec := sch.Next(time.Now().Round(time.Second))

	s.NextExecutionDate = &nextExec
	s.Status = Waiting
}
