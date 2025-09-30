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
	Id                uuid.UUID                 `json:"id"`
	GroupId           uuid.UUID                 `json:"groupId"`
	Description       string                    `json:"description"`
	Frequency         string                    `json:"frequency"`
	Status            scheduler.ScheduleStatus  `json:"status"`
	RetryPolicy       *RetryPolicyDto           `json:"retryPolicy"`
	LastExecutionDate *time.Time                `json:"lastExecutionDate"`
	NextExecutionDate *time.Time                `json:"nextExecutionDate"`
	Job               ScheduleDetailsJobDto     `json:"job"`
	RecentJobRuns     map[uuid.UUID][]JobRunDto `json:"recentJobRuns"`
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
	Id        uuid.UUID              `json:"id"`
	Status    scheduler.JobRunStatus `json:"status"`
	Reason    *string                `json:"reason"`
	StartDate time.Time              `json:"startDate"`
	EndDate   *time.Time             `json:"endDate"`
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

	recentJobRunsDto := make(map[uuid.UUID][]JobRunDto)
	for _, jobRun := range jobRuns {
		recentJobRunsDto[jobRun.GroupId] = append(recentJobRunsDto[jobRun.GroupId],
			JobRunDto{
				Id:        jobRun.Id,
				Status:    jobRun.Status,
				Reason:    jobRun.Reason,
				StartDate: jobRun.StartDate,
				EndDate:   jobRun.EndDate,
			})
	}

	return ScheduleDetailsDto{
		Id:                schedule.Id,
		GroupId:           schedule.GroupId,
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
		RecentJobRuns: recentJobRunsDto,
	}, nil
}
