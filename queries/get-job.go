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
	Cron              string              `json:"cron"`
	LastExecutionDate time.Time           `json:"lastExecutionDate"`
	NextExecutionDate time.Time           `json:"nextExecutionDate"`
}

func GetJob(req *http.Request, str *scheduler.JobStorage) (JobDto, error) {
	vars := mux.Vars(req)

	id, err := uuid.Parse(vars["id"])
	if err != nil {
		return JobDto{}, errors.New("invlid job id")
	}

	job := str.Get(id)
	if job == nil {
		return JobDto{}, errors.New("job not found")
	}

	return JobDto{
		Id:                job.Id,
		Slug:              job.Slug,
		Description:       job.Description,
		Status:            job.Status,
		Cron:              job.Cron,
		LastExecutionDate: job.LastExecutionDate,
		NextExecutionDate: job.NextExecutionDate,
	}, nil
}
