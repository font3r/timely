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

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const jobCycles = 1
const testJobSlug = "process-user-notifications"

func main() {
	ctx := context.Background()

	baseLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("can't initialize zap logger: %v", err))
	}

	logger := baseLogger.Sugar()
	defer logger.Sync()

	StartRabbitMq(testJobSlug, context.Background(), logger)

	<-ctx.Done()
}

func StartRabbitMq(queue string, ctx context.Context, logger *zap.SugaredLogger) error {
	conn, err := amqp.DialConfig("amqp://timely:timely@localhost:5672", amqp.Config{
		Properties: amqp.Table{
			"product":  "async-job-handler",
			"version":  "v0.1.0",
			"platform": "golang",
		}})

	if err != nil {
		return err
	}

	chann, err := conn.Channel()
	if err != nil {
		return err
	}

	_, err = chann.QueueDeclare(queue, true, false, false,
		false, amqp.Table{})

	go func(jobSlug string) {
		deliveryChan, err := chann.ConsumeWithContext(ctx, queue, "", false,
			false, false, false, amqp.Table{})
		if err != nil {
			logger.Errorf("error during rabbitmq consume - %v", err)
			panic(err)
		}

		logger.Infof("subscribed to queue %s", testJobSlug)

		for {
			rawMessage, open := <-deliveryChan
			if !open {
				fmt.Printf("channel closed")
				return
			}

			var event libs.ScheduleJobEvent
			err = json.Unmarshal(rawMessage.Body, &event)
			if err != nil {
				panic(err)
			}

			err = processFakeJob(ctx, chann, event, logger)
			if err != nil {
				panic(err)
			}
		}
	}(queue)

	return err
}

func processFakeJob(ctx context.Context, chann *amqp.Channel, event libs.ScheduleJobEvent,
	logger *zap.SugaredLogger) error {
	for i := 0; i < jobCycles; i++ {
		if jitterFail() {
			data, err := json.Marshal(libs.JobStatusEvent{
				ScheduleId: event.ScheduleId,
				GroupId:    event.GroupId,
				JobRunId:   event.JobRunId,
				Status:     string(libs.JobFailed),
				Reason:     fmt.Sprintf("failed due to jitter error at %d", i),
			})

			if err != nil {
				return errors.New("invalid message format")
			}

			msg := amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				ContentType:  "application/json",
				Body:         data,
			}

			err = chann.PublishWithContext(ctx, string(scheduler.JobStatusExchange),
				string(scheduler.JobStatusRoutingKey), false, false, msg)
			if err != nil {
				logger.Errorf("error during publishing job status %v", err)
			}

			return errors.New("jitter job failure")
		}

		logger.Infoln("job processing")
		time.Sleep(time.Second)
	}

	data, err := json.Marshal(libs.JobStatusEvent{
		ScheduleId: event.ScheduleId,
		GroupId:    event.GroupId,
		JobRunId:   event.JobRunId,
		Status:     string(libs.JobSucceed),
		Reason:     "success",
	})

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         data,
	}

	chann.PublishWithContext(ctx, string(scheduler.JobStatusExchange),
		string(scheduler.JobStatusRoutingKey), false, false, msg)

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
