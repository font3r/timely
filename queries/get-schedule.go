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

type ScheduleDetailsDto struct {
	Id                uuid.UUID                `json:"id"`
	Description       string                   `json:"description"`
	Frequency         string                   `json:"frequency"`
	Status            scheduler.ScheduleStatus `json:"status"`
	RetryPolicy       *RetryPolicyDto          `json:"retry_policy"`
	LastExecutionDate *time.Time               `json:"last_execution_date"`
	NextExecutionDate *time.Time               `json:"next_execution_date"`
	Job               ScheduleDetailsJobDto    `json:"job"`
	Configuration     ScheduleConfigurationDto `json:"configuration"`
	RecentJobRuns     []JobRunDto              `json:"recent_job_runs"`
}

type RetryPolicyDto struct {
	Strategy scheduler.StrategyType `json:"strategy"`
	Count    int                    `json:"count"`
	Interval string                 `json:"interval"`
}

type ScheduleDetailsJobDto struct {
	Id   uuid.UUID       `json:"id"`
	Slug string          `json:"slug"`
	Data *map[string]any `json:"data"`
}

type JobRunDto struct {
	Status    scheduler.JobRunStatus `json:"status"`
	Attempt   int                    `json:"attempt"`
	Reason    *string                `json:"reason"`
	StartDate time.Time              `json:"start_date"`
	EndDate   *time.Time             `json:"end_date"`
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

func (h GetScheduleHandler) Handle(ctx context.Context, q GetSchedule) (ScheduleDetailsDto, error) {
	schedule, err := h.Storage.GetScheduleById(ctx, q.ScheduleId)
	if err != nil {
		return ScheduleDetailsDto{}, err
	}

	if schedule == nil {
		return ScheduleDetailsDto{}, ErrScheduleNotFound
	}

	jobRuns, err := h.Storage.GetRecentJobRuns(ctx, schedule.Id)
	if err != nil {
		return ScheduleDetailsDto{}, err
	}

	var retry *RetryPolicyDto
	if schedule.RetryPolicy != (scheduler.RetryPolicy{}) {
		retry = &RetryPolicyDto{
			Strategy: schedule.RetryPolicy.Strategy,
			Count:    schedule.RetryPolicy.Count,
			Interval: schedule.RetryPolicy.Interval,
		}
	}

	recentJobRunsDto := make([]JobRunDto, 0)
	for _, jobRun := range jobRuns {
		recentJobRunsDto = append(recentJobRunsDto, JobRunDto{
			Status:    jobRun.Status,
			Attempt:   jobRun.Attempt,
			Reason:    jobRun.Reason,
			StartDate: jobRun.StartDate,
			EndDate:   jobRun.EndDate,
		})
	}

	return ScheduleDetailsDto{
		Id:                schedule.Id,
		Description:       schedule.Description,
		Frequency:         schedule.Frequency,
		Status:            schedule.Status,
		RetryPolicy:       retry,
		LastExecutionDate: schedule.LastExecutionDate,
		NextExecutionDate: schedule.NextExecutionDate,
		Job: ScheduleDetailsJobDto{
			Id:   schedule.Job.Id,
			Slug: schedule.Job.Slug,
			Data: schedule.Job.Data,
		},
		Configuration: ScheduleConfigurationDto{
			TransportType: schedule.Configuration.TransportType,
			Url:           schedule.Configuration.Url,
		},
		RecentJobRuns: recentJobRunsDto,
	}, nil
}
