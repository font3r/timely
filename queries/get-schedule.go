package queries

import (
	"context"
	"time"
	"timely/scheduler"

	"github.com/google/uuid"
)

type GetSchedule struct {
	ScheduleId uuid.UUID
}

type GetScheduleHandler struct {
	Storage scheduler.StorageDriver
}

type ScheduleDto struct {
	Id                uuid.UUID                `json:"id"`
	Description       string                   `json:"description"`
	Frequency         string                   `json:"frequency"`
	Status            scheduler.ScheduleStatus `json:"status"`
	Attempt           int                      `json:"attempt"`
	RetryPolicy       *RetryPolicyDto          `json:"retry_policy"`
	LastExecutionDate *time.Time               `json:"last_execution_date"`
	NextExecutionDate *time.Time               `json:"next_execution_date"`
	Job               JobDto                   `json:"job"`
	Configuration     ScheduleConfigurationDto `json:"configuration"`
}

type RetryPolicyDto struct {
	Strategy scheduler.StrategyType `json:"strategy"`
	Count    int                    `json:"count"`
	Interval string                 `json:"interval"`
}

type JobDto struct {
	Id   uuid.UUID       `json:"id"`
	Slug string          `json:"slug"`
	Data *map[string]any `json:"data"`
}

type ScheduleConfigurationDto struct {
	TransportType scheduler.TransportType `json:"transportType"`
	Url           string                  `json:"url"`
}

var (
	ErrScheduleNotFound = scheduler.Error{
		Code: "SCHEDULE_NOT_FOUND",
		Msg:  "schedule not found",
	}
)

func (h GetScheduleHandler) Handle(ctx context.Context, q GetSchedule) (ScheduleDto, error) {
	schedule, err := h.Storage.GetScheduleById(ctx, q.ScheduleId)
	if err != nil {
		return ScheduleDto{}, err
	}

	if schedule == nil {
		return ScheduleDto{}, ErrScheduleNotFound
	}

	var retry *RetryPolicyDto
	if schedule.RetryPolicy != (scheduler.RetryPolicy{}) {
		retry = &RetryPolicyDto{
			Strategy: schedule.RetryPolicy.Strategy,
			Count:    schedule.RetryPolicy.Count,
			Interval: schedule.RetryPolicy.Interval,
		}
	}

	return ScheduleDto{
		Id:                schedule.Id,
		Description:       schedule.Description,
		Frequency:         schedule.Frequency,
		Status:            schedule.Status,
		Attempt:           schedule.Attempt,
		RetryPolicy:       retry,
		LastExecutionDate: schedule.LastExecutionDate,
		NextExecutionDate: schedule.NextExecutionDate,
		Job: JobDto{
			Id:   schedule.Job.Id,
			Slug: schedule.Job.Slug,
			Data: schedule.Job.Data,
		},
		Configuration: ScheduleConfigurationDto{
			TransportType: schedule.Configuration.TransportType,
			Url:           schedule.Configuration.Url,
		},
	}, nil
}
