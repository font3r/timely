package commands

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"timely/scheduler"

	"github.com/google/uuid"
)

type CreateScheduleCommand struct {
	Description   string                   `json:"description"`
	Frequency     string                   `json:"frequency"`
	Job           JobConfiguration         `json:"job"`
	RetryPolicy   RetryPolicyConfiguration `json:"retry_policy"`
	ScheduleStart *time.Time               `json:"schedule_start"`
}

type RetryPolicyConfiguration struct {
	Strategy scheduler.StrategyType `json:"strategy"`
	Count    int                    `json:"count"`
	Interval string                 `json:"interval"`
}

type JobConfiguration struct {
	Slug string          `json:"slug"`
	Data *map[string]any `json:"data"`
}

type CreateScheduleResponse struct {
	Id uuid.UUID `json:"id"`
}

var ErrJobScheduleConflict = scheduler.Error{
	Code:    "JOB_SCHEDULE_CONFLICT",
	Message: "job has assigned schedule already"}

type CreateScheduleHandler struct {
	Storage   *scheduler.JobStorage
	Transport *scheduler.Transport
}

func (h CreateScheduleHandler) CreateSchedule(req *http.Request) (*CreateScheduleResponse, error) {
	comm, err := validate(req)
	if err != nil {
		return nil, err
	}

	retryPolicy, err := getRetryPolicy(comm.RetryPolicy)
	if err != nil {
		return nil, err
	}

	schedule := scheduler.NewSchedule(comm.Description, comm.Frequency, comm.Job.Slug,
		comm.Job.Data, retryPolicy, comm.ScheduleStart)

	if err = h.Storage.Add(schedule); err != nil {
		if errors.Is(err, scheduler.ErrUniqueConstraintViolation) {
			return nil, ErrJobScheduleConflict
		}
		return nil, err
	}

	if err = h.Transport.CreateQueue(schedule.Job.Slug); err != nil {
		// TODO: at this point we should delete job from db
		return nil, err
	}

	if err = h.Transport.BindQueue(schedule.Job.Slug, string(scheduler.ExchangeJobSchedule),
		schedule.Job.Slug); err != nil {
		return nil, err
	}

	return &CreateScheduleResponse{Id: schedule.Id}, nil
}

func validate(req *http.Request) (*CreateScheduleCommand, error) {
	comm := &CreateScheduleCommand{}

	if err := json.NewDecoder(req.Body).Decode(&comm); err != nil {
		return nil, err
	}

	var err error

	if comm.Description == "" {
		err = errors.Join(errors.New("invalid description"))
	}

	if comm.Frequency == "" {
		err = errors.Join(errors.New("missing frequency configuration"))
	}

	if comm.Frequency != string(scheduler.Once) {
		_, err = scheduler.CronParser.Parse(comm.Frequency)
		if err != nil {
			err = errors.Join(errors.New("invalid frequency configuration"))
		}
	}

	if comm.ScheduleStart != nil && time.Now().After(*comm.ScheduleStart) {
		err = errors.Join(errors.New("invalid schedule start"))
	}

	if comm.Job == (JobConfiguration{}) {
		err = errors.Join(errors.New("missing job configuration"))
	}

	if comm.Job.Slug == "" {
		err = errors.Join(errors.New("invalid job slug"))
	}

	if err != nil {
		return nil, err
	}

	return comm, nil
}

func getRetryPolicy(retryPolicyConf RetryPolicyConfiguration) (scheduler.RetryPolicy, error) {
	if retryPolicyConf == (RetryPolicyConfiguration{}) {
		return scheduler.RetryPolicy{}, nil
	}

	retryPolicy, err := scheduler.NewRetryPolicy(retryPolicyConf.Strategy, retryPolicyConf.Count,
		retryPolicyConf.Interval)
	if err != nil {
		return scheduler.RetryPolicy{}, err
	}

	return retryPolicy, nil
}
