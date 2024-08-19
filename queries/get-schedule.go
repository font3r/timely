package queries

import (
	"errors"
	"net/http"
	"time"
	"timely/scheduler"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type ScheduleDto struct {
	Id                uuid.UUID           `json:"id"`
	Description       string              `json:"description"`
	Frequency         string              `json:"frequency"`
	Status            scheduler.JobStatus `json:"status"`
	Attempt           int                 `json:"attempt"`
	RetryPolicy       *RetryPolicyDto     `json:"retry_policy"`
	LastExecutionDate *time.Time          `json:"last_execution_date"`
	NextExecutionDate *time.Time          `json:"next_execution_date"`
	Job               JobDto              `json:"job"`
}

type RetryPolicyDto struct {
	Strategy scheduler.StrategyType `json:"strategy"`
	Count    int                    `json:"count"`
	Interval string                 `json:"interval"`
}

type JobDto struct {
	Id   uuid.UUID `json:"id"`
	Slug string    `json:"slug"`
}

func GetSchedule(req *http.Request, str *scheduler.JobStorage) (ScheduleDto, error) {
	vars := mux.Vars(req)

	id, err := uuid.Parse(vars["id"])
	if err != nil {
		return ScheduleDto{}, errors.New("invalid schedule id")
	}

	schedule, err := str.GetScheduleById(id)
	if err != nil {
		return ScheduleDto{}, err
	}

	if schedule == nil {
		return ScheduleDto{}, nil
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
		RetryPolicy:       retry,
		LastExecutionDate: schedule.LastExecutionDate,
		NextExecutionDate: schedule.NextExecutionDate,
		Job: JobDto{
			Id:   schedule.Job.Id,
			Slug: schedule.Job.Slug,
		},
	}, nil
}
