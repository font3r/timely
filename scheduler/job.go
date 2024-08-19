package scheduler

import (
	"log"
	"time"

	"github.com/google/uuid"
)

var (
	ErrJobCycleFinished = &Error{Code: "JOB_CYCLE_FINISHED", Message: "job cycle finished"}
	ErrJobInvalidStatus = &Error{Code: "INVALID_JOB_STATUS", Message: "invalid job status"}
)

type StartJobMessage struct {
	JobName string `json:"jobName"`
}

type JobStatus string

const (
	New        JobStatus = "new"        // created, waiting to schedule
	Scheduled  JobStatus = "scheduled"  // scheduled, waiting for application status
	Processing JobStatus = "processing" // during processing
	Finished   JobStatus = "finished"   // successfully processed
	Failed     JobStatus = "failed"     // error during processing
)

type JobRun struct {
	Id            uuid.UUID
	Status        JobStatus
	Reason        string
	Attempt       int
	ExecutionDate time.Time
}

type Schedule struct {
	Id                uuid.UUID
	Description       string
	Frequency         string
	Status            JobStatus
	Attempt           int
	RetryPolicy       RetryPolicy
	LastExecutionDate *time.Time
	NextExecutionDate *time.Time
	Job               *Job
}

type Job struct {
	Id   uuid.UUID
	Slug string
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

func (j *Job) Start(t *Transport, result chan<- error) {
	err := t.BindQueue(j.Slug, string(ExchangeJobSchedule), j.Slug)
	if err != nil {
		result <- err
		return
	}

	log.Printf("job %s/%s scheduled", j.Id, j.Slug)

	err = t.Publish(string(ExchangeJobSchedule), j.Slug,
		StartJobMessage{JobName: j.Slug})

	if err != nil {
		log.Printf("failed to start job %v", err)

		result <- err
		return
	}

	result <- nil
}
