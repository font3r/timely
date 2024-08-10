package transport

type ExchangeName string
type QueueName string
type RoutingKey string

const (
	Timely string = "timely"

	ExchangeJobSchedule ExchangeName = "timely-schedule-job"
	ExchangeJobStatus   ExchangeName = "timely-job-status"

	QueueJobStatus QueueName = "timely-job-status"

	RoutingKeyJobStatus RoutingKey = "timely-job-status"
)
