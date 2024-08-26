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

type JobRun struct {
	Id            uuid.UUID
	Status        ScheduleStatus
	Reason        string
	Attempt       int
	ExecutionDate time.Time
}
