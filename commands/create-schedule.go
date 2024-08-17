package commands

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"timely/scheduler"
)

type CreateScheduleCommand struct {
	Description string           `json:"description"`
	Frequency   string           `json:"frequency"`
	Job         JobConfiguration `json:"job"`
}

type JobConfiguration struct {
	Slug string `json:"slug"`
}

type CreateScheduleCommandResponse struct {
	Id uuid.UUID `json:"id"`
}

var ErrJobScheduleConflict = scheduler.Error{Code: "JOB_SCHEDULE_CONFLICT", Message: "Job has assigned schedule already"}

func CreateSchedule(req *http.Request, str *scheduler.JobStorage,
	tra *scheduler.Transport) (*CreateScheduleCommandResponse, error) {

	comm, err := validate(req)
	if err != nil {
		return nil, err
	}

	schedule := scheduler.NewSchedule(comm.Description, comm.Frequency, comm.Job.Slug)

	if err = str.Add(schedule); err != nil {
		if errors.Is(err, scheduler.ErrUniqueConstraintViolation) {
			return nil, ErrJobScheduleConflict
		}
		return nil, err
	}

	if err = tra.CreateQueue(schedule.Job.Slug); err != nil {
		// TODO: at this point we should delete job from db
		return nil, err
	}

	if err = tra.BindQueue(schedule.Job.Slug, string(scheduler.ExchangeJobSchedule),
		schedule.Job.Slug); err != nil {
		return nil, err
	}

	return &CreateScheduleCommandResponse{Id: schedule.Id}, nil
}

func validate(req *http.Request) (*CreateScheduleCommand, error) {
	comm := &CreateScheduleCommand{}

	if err := json.NewDecoder(req.Body).Decode(&comm); err != nil {
		return nil, err
	}

	if comm.Description == "" {
		return nil, errors.New("invalid description")
	}

	if comm.Frequency == "" {
		return nil, errors.New("missing frequency configuration")
	}

	if comm.Frequency != "once" {
		return nil, errors.New("invalid frequency configuration")
	}

	if comm.Job == (JobConfiguration{}) {
		return nil, errors.New("missing job configuration")
	}

	if comm.Job.Slug == "" {
		return nil, errors.New("invalid job slug")
	}

	return comm, nil
}
