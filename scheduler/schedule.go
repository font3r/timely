package scheduler

import (
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
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
	JobName string `json:"jobName"`
}

func NewSchedule(description, frequency, slug string, policy RetryPolicy, nextExecutionDate time.Time) Schedule {
	return Schedule{
		Id:                uuid.New(),
		Description:       description,
		Frequency:         frequency,
		Status:            New,
		Attempt:           0,
		RetryPolicy:       policy,
		LastExecutionDate: nil,
		NextExecutionDate: &nextExecutionDate,
		Job: &Job{
			Id:   uuid.New(),
			Slug: slug,
		},
	}
}

func (s *Schedule) Start(t *Transport, result chan<- error) {
	err := t.BindQueue(s.Job.Slug, string(ExchangeJobSchedule), s.Job.Slug)
	if err != nil {
		result <- err
		return
	}

	err = t.Publish(string(ExchangeJobSchedule), s.Job.Slug,
		ScheduleJobEvent{JobName: s.Job.Slug})

	if err != nil {
		log.Printf("failed to start job %v", err)

		result <- err
		return
	}

	s.Attempt++
	s.Status = Scheduled

	now := time.Now().Round(time.Second)
	s.LastExecutionDate = &now

	log.Printf("scheduled job %s/%s", s.Job.Id, s.Job.Slug)
	result <- nil
}

func (s *Schedule) Failed() error {
	s.Status = Failed

	if s.RetryPolicy == (RetryPolicy{}) {
		return nil
	}

	var next time.Time
	var err error

	if s.NextExecutionDate != nil {
		next, err = s.RetryPolicy.GetNextExecutionTime(*s.NextExecutionDate, s.Attempt)
	} else {
		next, err = s.RetryPolicy.GetNextExecutionTime(*s.LastExecutionDate, s.Attempt)
	}

	if err != nil {
		return err
	}

	if next == (time.Time{}) {
		s.NextExecutionDate = nil
		log.Printf("schedule failed after retrying%v\n", next)
	} else {
		s.NextExecutionDate = &next
		log.Printf("schedule retrying at %v\n", next)
	}

	return nil
}

func (s *Schedule) Finished() {
	s.Status = Finished // is job really finished if it's cyclic?

	specParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sch, _ := specParser.Parse(s.Frequency)

	nextExec := sch.Next(time.Now())
	if nextExec != (time.Time{}) {
		s.NextExecutionDate = &nextExec
		return
	}

	s.NextExecutionDate = nil
}
