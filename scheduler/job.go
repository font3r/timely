package scheduler

import (
	"log"
	"task-scheduler/scheduler/transport"
	"time"

	"github.com/google/uuid"
)

type JobStatus string

type StartJobMessage struct {
	JobName string `json:"jobName"`
}

const (
	New        JobStatus = "new"
	Processing JobStatus = "processing"
	Finished   JobStatus = "finished"
	Failed     JobStatus = "failed"
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
	j.Status = Processing

	log.Printf("job %s started", j.Id)

	for i := 0; i < 10; i++ {
		err := t.Publish("test-exchange", "sample-key",
			StartJobMessage{JobName: "test-job"})

		if err != nil {
			log.Printf("failed to start job %v", err)
			j.Status = Failed
			return
		}
	}

	time.Sleep(time.Second)
	log.Printf("job %s completed", j.Id)

	j.Status = Finished
}
