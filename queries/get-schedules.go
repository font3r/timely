package queries

import (
	"context"
	"timely/scheduler"
)

type GetSchedules struct{}

type GetSchedulesHandler struct {
	Storage scheduler.StorageDriver
}

func (h GetSchedulesHandler) Handle(ctx context.Context) ([]ScheduleDto, error) {
	schedules, err := h.Storage.GetAll(ctx)

	if err != nil {
		return []ScheduleDto{}, err
	}

	schedulesDto := make([]ScheduleDto, 0, len(schedules))

	for _, schedule := range schedules {
		var retry *RetryPolicyDto
		if schedule.RetryPolicy != (scheduler.RetryPolicy{}) {
			retry = &RetryPolicyDto{
				Strategy: schedule.RetryPolicy.Strategy,
				Count:    schedule.RetryPolicy.Count,
				Interval: schedule.RetryPolicy.Interval,
			}
		}

		schedulesDto = append(schedulesDto, ScheduleDto{
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
		})
	}

	return schedulesDto, nil
}
