package scheduler

import (
	"fmt"
	"github.com/robfig/cron/v3"
)

type ExchangeName string
type QueueName string
type RoutingKey string
type PredefinedFrequency string

const (
	ExchangeJobSchedule ExchangeName = "timely-schedule-job"
	ExchangeJobStatus   ExchangeName = "timely-job-status"
	QueueJobStatus      QueueName    = "timely-job-status"
	RoutingKeyJobStatus RoutingKey   = "timely-job-status"
)

const Once PredefinedFrequency = "once"

var CronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

const (
	ContentTypeHeader        = "Content-Type"
	ApplicationJson   string = "application/json"
)

type Error struct {
	Code string
	Msg  string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s - %s", e.Code, e.Msg)
}
