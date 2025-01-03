package commands

import (
	"context"
	"slices"
	"time"
	"timely/scheduler"

	"github.com/google/uuid"
	"go.uber.org/zap"
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
	Logger         *zap.SugaredLogger
}

type CreateScheduleResponse struct {
	Id uuid.UUID `json:"id"`
}

var ErrUnsupportedTransportType = scheduler.Error{
	Code: "UNSUPPORTED_TRANSPORT_TYPE",
	Msg:  "unsupported transport type"}
var ErrTransportError = scheduler.Error{
	Code: "TRANSPORT_ERROR",
	Msg:  "transport error"}

func (h CreateScheduleHandler) Handle(ctx context.Context, c CreateScheduleCommand) (*CreateScheduleResponse, error) {
	if !slices.Contains(scheduler.Supports, string(c.Configuration.TransportType)) {
		return nil, ErrUnsupportedTransportType
	}

	retryPolicy, err := getRetryPolicy(c.RetryPolicy)
	if err != nil {
		return nil, err
	}

	schedule := scheduler.NewSchedule(c.Description, c.Frequency, time.Now,
		scheduler.WithScheduleStart(c.ScheduleStart),
		scheduler.WithRetryPolicy(retryPolicy),
		scheduler.WithJob(c.Job.Slug, c.Job.Data),
		scheduler.WithConfiguration(c.Configuration.TransportType, c.Configuration.Url))

	if err = h.Storage.Add(ctx, schedule); err != nil {
		return nil, err
	}

	// TODO: that part probably should not be a part of command handler
	if c.Configuration.TransportType == scheduler.Rabbitmq {
		err := h.createTransportDependencies(schedule)
		if err != nil {
			h.Logger.Errorf("error during creating schedule %+v", err)
			h.Storage.DeleteScheduleById(ctx, schedule.Id)
			return nil, ErrTransportError
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

func (h CreateScheduleHandler) createTransportDependencies(schedule scheduler.Schedule) error {
	if err := h.AsyncTransport.CreateQueue(schedule.Job.Slug); err != nil {
		return err
	}

	if err := h.AsyncTransport.BindQueue(schedule.Job.Slug, string(scheduler.ExchangeJobSchedule),
		schedule.Job.Slug); err != nil {
		return err
	}

	return nil
}
