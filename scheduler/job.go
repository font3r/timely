package scheduler

import (
	"time"

	"github.com/google/uuid"
)

type Job struct {
	Id   uuid.UUID
	Slug string
}

type JobRun struct {
	Id            uuid.UUID
	Status        ScheduleStatus
	Reason        string
	Attempt       int
	ExecutionDate time.Time
}
