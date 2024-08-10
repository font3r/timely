package commands

import (
	"encoding/json"
	"errors"
	"net/http"
	"task-scheduler/scheduler"
)

type CreateJobCommand struct {
	Slug        string `json:"slug"`
	Description string `json:"description"`
	Cron        string `json:"cron"`
}

func CreateJob(req *http.Request, str *scheduler.JobStorage) (any, error) {
	comm, err := validate(req)
	if err != nil {
		return nil, err
	}

	job := scheduler.Create(comm.Slug, comm.Description, comm.Cron)
	str.Add(job)

	return map[string]string{"id": job.Id.String()}, nil
}

func validate(req *http.Request) (*CreateJobCommand, error) {
	comm := &CreateJobCommand{}
	err := json.NewDecoder(req.Body).Decode(&comm)

	if err != nil {
		return nil, err
	}

	if comm.Slug == "" {
		return nil, errors.New("invalid slug")
	}

	if comm.Description == "" {
		return nil, errors.New("invalid description")
	}

	if comm.Cron == "" {
		return nil, errors.New("invalid cron")
	}

	return comm, nil
}
