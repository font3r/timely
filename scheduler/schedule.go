package scheduler

import (
	"github.com/google/uuid"
	"log"
	"time"
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

func NewSchedule(description, frequency, slug string, policy RetryPolicy) Schedule {
	return Schedule{
		Id:                uuid.New(),
		Description:       description,
		Frequency:         frequency,
		Status:            New,
		Attempt:           0,
		RetryPolicy:       policy,
		LastExecutionDate: nil,
		NextExecutionDate: nil,
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

	log.Printf("scheduled job %s/%s", s.Job.Id, s.Job.Slug)
	result <- nil
	return
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
	s.Status = Finished
	s.NextExecutionDate = nil
}
