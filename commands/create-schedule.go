package commands

import (
	"context"
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
	Configuration ScheduleConfiguration    `json:"configuration"`
}

type JobConfiguration struct {
	Slug string          `json:"slug"`
	Data *map[string]any `json:"data"`
}

type RetryPolicyConfiguration struct {
	Strategy scheduler.StrategyType `json:"strategy"`
	Count    int                    `json:"count"`
	Interval string                 `json:"interval"`
}

type ScheduleConfiguration struct {
	TransportType scheduler.TransportType `json:"transportType"`
	Url           string                  `json:"url"`
}

type CreateScheduleHandler struct {
	Storage   scheduler.StorageDriver
	Transport scheduler.AsyncTransportDriver
}

type CreateScheduleResponse struct {
	Id uuid.UUID `json:"id"`
}

var ErrJobScheduleConflict = scheduler.Error{
	Code: "JOB_SCHEDULE_CONFLICT",
	Msg:  "job has assigned schedule already"}

func (h CreateScheduleHandler) Handle(ctx context.Context, c CreateScheduleCommand) (*CreateScheduleResponse, error) {
	retryPolicy, err := getRetryPolicy(c.RetryPolicy)
	if err != nil {
		return nil, err
	}

	schedule := scheduler.NewSchedule(c.Description, c.Frequency, c.Job.Slug,
		c.Job.Data, retryPolicy, c.ScheduleStart)

	if err = h.Storage.Add(ctx, schedule); err != nil {
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
