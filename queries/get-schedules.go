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
	LastExecutionDate *time.Time               `json:"lastExecutionDate"`
	NextExecutionDate *time.Time               `json:"nextExecutionDate"`
	JobSlug           string                   `json:"jobSlug"`
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
			LastExecutionDate: schedule.LastExecutionDate,
			NextExecutionDate: schedule.NextExecutionDate,
			JobSlug:           schedule.Job.Slug,
		})
	}

	return schedulesDto, nil
}
