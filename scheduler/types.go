package scheduler

import "fmt"

type ExchangeName string
type QueueName string
type RoutingKey string

const (
	ExchangeJobSchedule ExchangeName = "timely-schedule-job"
	ExchangeJobStatus   ExchangeName = "timely-job-status"
	QueueJobStatus      QueueName    = "timely-job-status"
	RoutingKeyJobStatus RoutingKey   = "timely-job-status"
)

const (
	ContentTypeHeader        = "Content-Type"
	ApplicationJson   string = "application/json"
)

type Error struct {
	Code    string
	Message string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s - %s", e.Code, e.Message)
}
