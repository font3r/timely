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

type JobStatus string

type StartJobMessage struct {
	JobName string `json:"jobName"`
}

const (
	New        JobStatus = "new"        // created, waiting to schedule
	Scheduled  JobStatus = "scheduled"  // scheduled, waiting for result
	Processing JobStatus = "processing" // during processing
	Finished   JobStatus = "finished"   // successfuly processed
	Failed     JobStatus = "failed"     // error during processing
)

type Job struct {
	Id                uuid.UUID
	Slug              string
	Description       string
	Status            JobStatus
	Cron              string
	LastExecutionDate time.Time
	NextExecutionDate time.Time
}

func (j *Job) Start(t *Transport) {
	t.BindQueue(j.Slug, string(ExchangeJobSchedule), j.Slug)

	j.Status = Scheduled

	log.Printf("job %s/%s scheduled", j.Id, j.Slug)

	err := t.Publish(string(ExchangeJobSchedule), j.Slug,
		StartJobMessage{JobName: j.Slug})

	if err != nil {
		log.Printf("failed to start job %v", err)
		j.Status = Failed
		return
	}
}

func (j *Job) ProcessState(status string) error {
	if j.Status == Finished || j.Status == Failed {
		return ErrJobCycleFinished
	}

	switch status {
	case string(Processing):
		j.Status = Processing
		return nil
	case string(Finished):
		j.Status = Finished
		return nil
	case string(Failed):
		j.Status = Failed // TODO: requirement - what if failure occurs
		return nil
	default:
		return ErrJobInvalidStatus
	}
}
