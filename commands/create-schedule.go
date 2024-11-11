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
	RetryPolicy   RetryPolicyConfiguration `json:"retryPolicy"`
	ScheduleStart *time.Time               `json:"scheduleStart"`
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
	Storage        scheduler.StorageDriver
	AsyncTransport scheduler.AsyncTransportDriver
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
		c.Job.Data, retryPolicy, scheduler.ScheduleConfiguration{
			TransportType: c.Configuration.TransportType,
			Url:           c.Configuration.Url},
		c.ScheduleStart, time.Now)

	if err = h.Storage.Add(ctx, schedule); err != nil {
		return nil, err
	}

	// TODO: that part probably should not be a part of command handler
	if c.Configuration.TransportType == scheduler.Rabbitmq {
		if err = h.AsyncTransport.CreateQueue(schedule.Job.Slug); err != nil {
			// TODO: at this point we should delete job from db
			return nil, err
		}

		if err = h.AsyncTransport.BindQueue(schedule.Job.Slug, string(scheduler.ExchangeJobSchedule),
			schedule.Job.Slug); err != nil {
			return nil, err
		}
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
