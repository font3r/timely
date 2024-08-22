package commands

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"net/http"
	"time"
	"timely/scheduler"
)

type CreateScheduleCommand struct {
	Description string                   `json:"description"`
	Frequency   string                   `json:"frequency"`
	Job         JobConfiguration         `json:"job"`
	RetryPolicy RetryPolicyConfiguration `json:"retry_policy"`
}

type RetryPolicyConfiguration struct {
	Strategy scheduler.StrategyType `json:"strategy"`
	Count    int                    `json:"count"`
	Interval string                 `json:"interval"`
}

type JobConfiguration struct {
	Slug string `json:"slug"`
}

type CreateScheduleCommandResponse struct {
	Id uuid.UUID `json:"id"`
}

var ErrJobScheduleConflict = scheduler.Error{
	Code:    "JOB_SCHEDULE_CONFLICT",
	Message: "job has assigned schedule already"}

func CreateSchedule(req *http.Request, str *scheduler.JobStorage,
	tra *scheduler.Transport) (*CreateScheduleCommandResponse, error) {

	comm, err := validate(req)
	if err != nil {
		return nil, err
	}

	var retryPolicy scheduler.RetryPolicy
	if comm.RetryPolicy != (RetryPolicyConfiguration{}) {
		retryPolicy, err = scheduler.NewRetryPolicy(comm.RetryPolicy.Strategy, comm.RetryPolicy.Count,
			comm.RetryPolicy.Interval)
		if err != nil {
			return nil, err
		}
	} else {
		retryPolicy = scheduler.RetryPolicy{}
	}

	specParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sch, err := specParser.Parse(comm.Frequency)
	if err != nil {
		return nil, errors.New("invalid frequency configuration")
	}

	schedule := scheduler.NewSchedule(comm.Description, comm.Frequency, comm.Job.Slug,
		retryPolicy, sch.Next(time.Now()))

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

	if comm.Job == (JobConfiguration{}) {
		return nil, errors.New("missing job configuration")
	}

	if comm.Job.Slug == "" {
		return nil, errors.New("invalid job slug")
	}

	return comm, nil
}
