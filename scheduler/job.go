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

type Schedule struct {
	Id                uuid.UUID
	Description       string
	Frequency         string
	LastExecutionDate *time.Time
	NextExecutionDate *time.Time
	Job               *Job
}

type Job struct {
	Id     uuid.UUID
	Slug   string
	Status JobStatus
	Reason string
}

func NewSchedule(description, frequency, slug string) Schedule {
	return Schedule{
		Id:                uuid.New(),
		Description:       description,
		Frequency:         frequency,
		LastExecutionDate: nil,
		NextExecutionDate: nil,
		Job: &Job{
			Id:     uuid.New(),
			Slug:   slug,
			Status: New,
			Reason: "",
		},
	}
}

func (j *Job) Start(t *Transport, result chan<- error) {
	err := t.BindQueue(j.Slug, string(ExchangeJobSchedule), j.Slug)
	if err != nil {
		result <- err
		return
	}

	j.Status = Scheduled

	log.Printf("job %s/%s scheduled", j.Id, j.Slug)

	err = t.Publish(string(ExchangeJobSchedule), j.Slug,
		StartJobMessage{JobName: j.Slug})

	if err != nil {
		log.Printf("failed to start job %v", err)
		j.Status = Failed

		result <- err
		return
	}

	result <- nil
}

func (j *Job) ProcessState(status, reason string) (JobStatus, error) {
	if j.Status == Finished || j.Status == Failed {
		return "", ErrJobCycleFinished
	}

	switch status {
	case string(Processing):
		j.Status = Processing
		return j.Status, nil
	case string(Finished):
		j.Status = Finished
		j.Reason = reason
		return j.Status, nil
	case string(Failed):
		j.Status = Failed // TODO: requirement - what if failure occurs
		j.Reason = reason
		return j.Status, nil
	default:
		return "", ErrJobInvalidStatus
	}
}
