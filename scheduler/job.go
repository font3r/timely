package scheduler

import (
	"github.com/google/uuid"
)

type Job struct {
	Id   uuid.UUID
	Slug string
	Data *map[string]any
}

func NewJob(slug string, data *map[string]any) *Job {
	return &Job{
		Id:   uuid.New(),
		Slug: slug,
		Data: data,
	}
}
