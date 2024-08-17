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
	Id                uuid.UUID  `json:"id"`
	Description       string     `json:"description"`
	Frequency         string     `json:"frequency"`
	LastExecutionDate *time.Time `json:"lastExecutionDate"`
	NextExecutionDate *time.Time `json:"nextExecutionDate"`
	Job               JobDto     `json:"job"`
}

type JobDto struct {
	Id     uuid.UUID           `json:"id"`
	Slug   string              `json:"slug"`
	Status scheduler.JobStatus `json:"status"`
	Reason string              `json:"reason"`
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
		return ScheduleDto{}, errors.New("schedule not found")
	}

	return ScheduleDto{
		Id:                schedule.Id,
		Description:       schedule.Description,
		Frequency:         schedule.Frequency,
		LastExecutionDate: schedule.LastExecutionDate,
		NextExecutionDate: schedule.NextExecutionDate,
		Job: JobDto{
			Id:     schedule.Job.Id,
			Slug:   schedule.Job.Slug,
			Status: schedule.Job.Status,
			Reason: schedule.Job.Reason,
		},
	}, nil
}
