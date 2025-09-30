package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"
	"timely/libs"
	"timely/scheduler"

	"go.uber.org/zap"
)

const jobsAmount = 1

func main() {
	ctx := context.Background()

	baseLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("can't initialize zap logger: %v", err))
	}

	logger := baseLogger.Sugar()
	defer logger.Sync()

	StartRabbitMq(context.Background(), logger)

	<-ctx.Done()
}

func StartRabbitMq(ctx context.Context, logger *zap.SugaredLogger) {
	logger.Infoln("starting ...")

	tra, err := scheduler.NewRabbitMqTransport("amqp://guest:guest@localhost:5672", logger)
	if err != nil {
		panic(fmt.Sprintf("test-app: create transport error - %s", err))
	}

	go func(jobSlug string) {
		err = tra.Subscribe(ctx, jobSlug, func(message []byte) error {
			logger.Infof("requested job start with slug %s", jobSlug)

			var event libs.ScheduleJobEvent
			err = json.Unmarshal(message, &event)
			if err != nil {
				return err
			}

			err = processFakeJob(ctx, tra, event, logger)
			if err != nil {
				logger.Errorf("error during job processing - %s", err)
			}

			return nil
		})

		if err != nil {
			logger.Errorf("error during subscribe for job %s - %s", jobSlug, err)
			panic(err)
		}
	}("process-user-notifications")

	logger.Infof("test app listening for 'process-user-notifications'")
}

func processFakeJob(ctx context.Context, tra *scheduler.RabbitMqTransport, event libs.ScheduleJobEvent,
	logger *zap.SugaredLogger) error {
	for i := 0; i < 5; i++ {
		if jitterFail() {
			err := tra.Publish(ctx, string(scheduler.JobStatusExchange),
				string(scheduler.JobStatusRoutingKey), libs.JobStatusEvent{
					ScheduleId: event.ScheduleId,
					GroupId:    event.GroupId,
					JobRunId:   event.JobRunId,
					Status:     string(libs.JobFailed),
					Reason:     fmt.Sprintf("failed due to jitter error at %d", i),
				})

			if err != nil {
				logger.Errorf("error during publishing job status %v", err)
			}

			return errors.New("jitter job failure")
		}

		logger.Infoln("job processing")
		time.Sleep(time.Second)
	}

	err := tra.Publish(ctx, string(scheduler.JobStatusExchange),
		string(scheduler.JobStatusRoutingKey), libs.JobStatusEvent{
			ScheduleId: event.ScheduleId,
			GroupId:    event.GroupId,
			JobRunId:   event.JobRunId,
			Status:     string(libs.JobSucceed),
			Reason:     "success",
		})

	if err != nil {
		logger.Errorf("error publishing during job status %v", err)
		return err
	}

	logger.Infoln("sent success event")

	return nil
}

func jitterFail() bool {
	failPercentage := 0

	return rand.Intn(100) <= failPercentage-1
}
