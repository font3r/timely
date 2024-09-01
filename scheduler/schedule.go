package scheduler

import (
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"time"
	log "timely/logger"
)

type ScheduleStatus string

const (
	New        ScheduleStatus = "new"        // created, waiting to schedule
	Scheduled  ScheduleStatus = "scheduled"  // scheduled, waiting for application status
	Processing ScheduleStatus = "processing" // during processing
	Finished   ScheduleStatus = "finished"   // successfully processed
	Failed     ScheduleStatus = "failed"     // error during processing
)

type Schedule struct {
	Id                uuid.UUID
	Description       string
	Frequency         string
	Status            ScheduleStatus
	Attempt           int
	RetryPolicy       RetryPolicy
	LastExecutionDate *time.Time
	NextExecutionDate *time.Time
	Job               *Job
}

type ScheduleJobEvent struct {
	ScheduleId uuid.UUID       `json:"schedule_id"`
	Job        string          `json:"job"`
	Data       *map[string]any `json:"data"`
}

func NewSchedule(description, frequency, slug string, data *map[string]any,
	policy RetryPolicy, scheduleStart *time.Time) Schedule {

	execution := getFirstExecution(frequency, scheduleStart)

	return Schedule{
		Id:                uuid.New(),
		Description:       description,
		Frequency:         frequency,
		Status:            New,
		Attempt:           0,
		RetryPolicy:       policy,
		LastExecutionDate: nil,
		NextExecutionDate: &execution,
		Job: &Job{
			Id:   uuid.New(),
			Slug: slug,
			Data: data,
		},
	}
}

func getFirstExecution(frequency string, scheduleStart *time.Time) time.Time {
	if scheduleStart != nil {
		return *scheduleStart
	}

	if frequency == string(Once) {
		return time.Now().Round(time.Second)
	}

	specParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sch, _ := specParser.Parse(frequency)

	return sch.Next(time.Now().Round(time.Second))
}

func (s *Schedule) Start(transport TransportDriver, result chan<- error) {
	err := transport.BindQueue(s.Job.Slug, string(ExchangeJobSchedule), s.Job.Slug)
	if err != nil {
		result <- err
		return
	}

	err = transport.Publish(string(ExchangeJobSchedule), s.Job.Slug,
		ScheduleJobEvent{
			ScheduleId: s.Id,
			Job:        s.Job.Slug,
			Data:       s.Job.Data,
		})

	if err != nil {
		log.Logger.Printf("failed to start job %v", err)

		result <- err
		return
	}

	s.Attempt++
	s.Status = Scheduled

	now := time.Now().Round(time.Second)
	s.LastExecutionDate = &now

	log.Logger.Printf("scheduled job %s/%s", s.Job.Id, s.Job.Slug)
	result <- nil
}

func (s *Schedule) Failed() error {
	s.Status = Failed

	if s.RetryPolicy == (RetryPolicy{}) {
		s.NextExecutionDate = nil
		return nil
	}

	var next time.Time
	var err error

	if s.NextExecutionDate != nil {
		next, err = s.RetryPolicy.GetNextExecutionTime(*s.NextExecutionDate, s.Attempt)
	} else {
		next, err = s.RetryPolicy.GetNextExecutionTime(time.Now(), s.Attempt)
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

func (s *Schedule) Finished() {
	s.Status = Finished // is job really finished if it's cyclic?

	if s.Frequency == string(Once) {
		s.NextExecutionDate = nil
		return
	}

	sch, _ := CronParser.Parse(s.Frequency)
	nextExec := sch.Next(time.Now().Round(time.Second))
	if nextExec != (time.Time{}) {
		s.NextExecutionDate = &nextExec
		return
	}

	s.NextExecutionDate = nil
}
