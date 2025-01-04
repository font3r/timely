package libs

import "github.com/google/uuid"

type JobStatusEvent struct {
	ScheduleId uuid.UUID `json:"scheduleId"`
	GroupId    uuid.UUID `json:"groupId"`
	JobRunId   uuid.UUID `json:"jobRunId"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason"`
}

type ScheduleJobEvent struct {
	ScheduleId uuid.UUID       `json:"scheduleId"`
	GroupId    uuid.UUID       `json:"groupId"`
	JobRunId   uuid.UUID       `json:"jobRunId"`
	Data       *map[string]any `json:"data"`
}

type JobRunStatus string

const (
	// successfully processed
	JobSucceed JobRunStatus = "succeed"

	// error during processing
	JobFailed JobRunStatus = "failed"
)
