package queries

import (
	"errors"
	"net/http"
	"time"
	"timely/scheduler"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type JobDto struct {
	Id                uuid.UUID           `json:"id"`
	Slug              string              `json:"slug"`
	Description       string              `json:"description"`
	Status            scheduler.JobStatus `json:"status"`
	Reason            string              `json:"reason"`
	Schedule          ScheduleDto         `json:"schedule"`
	LastExecutionDate time.Time           `json:"lastExecutionDate"`
	NextExecutionDate time.Time           `json:"nextExecutionDate"`
}

type ScheduleDto struct {
	Frequency string `json:"frequency"`
}

func GetJob(req *http.Request, str *scheduler.JobStorage) (JobDto, error) {
	vars := mux.Vars(req)

	id, err := uuid.Parse(vars["id"])
	if err != nil {
		return JobDto{}, errors.New("invalid job id")
	}

	job, err := str.GetById(id)
	if err != nil {
		return JobDto{}, err
	}

	if job == nil {
		return JobDto{}, errors.New("job not found")
	}

	return JobDto{
		Id:                job.Id,
		Slug:              job.Slug,
		Description:       job.Description,
		Status:            job.Status,
		Reason:            job.Reason,
		Schedule:          ScheduleDto{Frequency: job.Schedule.Frequency},
		LastExecutionDate: job.LastExecutionDate,
		NextExecutionDate: job.NextExecutionDate,
	}, nil
}
