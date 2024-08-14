package commands

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"timely/scheduler"
)

type CreateJobCommand struct {
	Slug        string `json:"slug"`
	Description string `json:"description"`
	Cron        string `json:"cron"`
}

type CreateJobCommandResponse struct {
	Id uuid.UUID `json:"id"`
}

func CreateJob(req *http.Request, str *scheduler.JobStorage) (*CreateJobCommandResponse, error) {
	comm, err := validate(req)
	if err != nil {
		return nil, err
	}

	job := scheduler.NewJob(comm.Slug, comm.Description, comm.Cron)
	err = str.Add(job)
	if err != nil {
		return nil, err
	}

	return &CreateJobCommandResponse{Id: job.Id}, nil
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
