package scheduler

import (
	"time"

	"github.com/google/uuid"
)

type Job struct {
	Id   uuid.UUID
	Slug string
	Data *map[string]any
}

type JobRunStatus string

const (
	JobWaiting JobRunStatus = "waiting" // waiting to receive first job status
	JobSucceed JobRunStatus = "succeed" // successfully processed
	JobFailed  JobRunStatus = "failed"  // error during processing
)

type JobRun struct {
	Id         uuid.UUID
	ScheduleId uuid.UUID
	Status     JobRunStatus
	Reason     *string
	StartDate  time.Time
	EndDate    *time.Time
}

func NewJobRun(scheduleId uuid.UUID) JobRun {
	return JobRun{
		Id:         uuid.New(),
		ScheduleId: scheduleId,
		Status:     JobWaiting,
		Reason:     nil,
		StartDate:  time.Now().Round(time.Second),
		EndDate:    nil,
	}
}

func (jr *JobRun) Succeed() {
	jr.Status = JobSucceed
	end := time.Now().Round(time.Second)
	jr.EndDate = &end
}

func (jr *JobRun) Failed(reason string) {
	jr.Status = JobFailed
	jr.Reason = &reason
	end := time.Now().Round(time.Second)
	jr.EndDate = &end
}
