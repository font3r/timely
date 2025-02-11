package scheduler

import (
	"time"

	"github.com/google/uuid"
)

type JobRunStatus string

const (
	// waiting to receive first job status
	JobWaiting JobRunStatus = "waiting"

	// successfully processed
	JobSucceed JobRunStatus = "succeed"

	// error during processing
	JobFailed JobRunStatus = "failed"
)

type JobRun struct {
	Id         uuid.UUID
	GroupId    uuid.UUID
	ScheduleId uuid.UUID
	Status     JobRunStatus
	Reason     *string
	StartDate  time.Time
	EndDate    *time.Time
}

type StaleJobRun struct {
	ScheduleId        uuid.UUID
	JobRunId          uuid.UUID
	LastExecutionDate *time.Time
	JobStartDate      time.Time
}

func NewJobRun(scheduleId uuid.UUID, groupId uuid.UUID, now func() time.Time) JobRun {
	return JobRun{
		Id:         uuid.New(),
		ScheduleId: scheduleId,
		GroupId:    groupId,
		Status:     JobWaiting,
		Reason:     nil,
		StartDate:  now().Round(time.Second),
		EndDate:    nil,
	}
}

func (jr *JobRun) Succeed(now func() time.Time) {
	jr.Status = JobSucceed
	end := now().Round(time.Second)
	jr.EndDate = &end
}

func (jr *JobRun) Failed(reason string, now func() time.Time) {
	jr.Status = JobFailed
	jr.Reason = &reason
	end := now().Round(time.Second)
	jr.EndDate = &end
}
