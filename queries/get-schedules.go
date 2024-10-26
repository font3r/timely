package queries

import (
	"context"
	"time"
	"timely/scheduler"

	"github.com/google/uuid"
)

type GetSchedules struct{}

type GetSchedulesHandler struct {
	Storage scheduler.StorageDriver
}

type ScheduleDto struct {
	Id                uuid.UUID                `json:"id"`
	Description       string                   `json:"description"`
	Frequency         string                   `json:"frequency"`
	Status            scheduler.ScheduleStatus `json:"status"`
	Attempt           int                      `json:"attempt"`
	LastExecutionDate *time.Time               `json:"last_execution_date"`
	NextExecutionDate *time.Time               `json:"next_execution_date"`
	Job               JobDto                   `json:"job"`
}

type JobDto struct {
	Slug string          `json:"slug"`
	Data *map[string]any `json:"data"`
}

func (h GetSchedulesHandler) Handle(ctx context.Context) ([]ScheduleDto, error) {
	schedules, err := h.Storage.GetAll(ctx)

	if err != nil {
		return []ScheduleDto{}, err
	}

	schedulesDto := make([]ScheduleDto, 0, len(schedules))

	for _, schedule := range schedules {
		schedulesDto = append(schedulesDto, ScheduleDto{
			Id:                schedule.Id,
			Description:       schedule.Description,
			Frequency:         schedule.Frequency,
			Status:            schedule.Status,
			Attempt:           schedule.Attempt,
			LastExecutionDate: schedule.LastExecutionDate,
			NextExecutionDate: schedule.NextExecutionDate,
			Job: JobDto{
				Slug: schedule.Job.Slug,
				Data: schedule.Job.Data,
			},
		})
	}

	return schedulesDto, nil
}
