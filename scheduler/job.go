package scheduler

import (
	"errors"
	"log"
	"time"
	"timely/scheduler/transport"

	"github.com/google/uuid"
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

func (j *Job) Start(t *transport.Transport) {
	t.BindQueue(j.Slug, string(transport.ExchangeJobSchedule), j.Slug)

	j.Status = Scheduled

	log.Printf("job %s/%s scheduled", j.Id, j.Slug)

	err := t.Publish(string(transport.ExchangeJobSchedule), j.Slug,
		StartJobMessage{JobName: j.Slug})

	if err != nil {
		log.Printf("failed to start job %v", err)
		j.Status = Failed
		return
	}
}

func (j *Job) ProcessState(status string) error {
	if j.Status == Finished || j.Status == Failed {
		return errors.New("job cycle finished")
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
		log.Printf("invalid job %s status %s", j.Slug, j.Status)
		return errors.New("invalid job status")
	}
}
