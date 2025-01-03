package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
	"timely/scheduler"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type JobStatusEvent struct {
	ScheduleId uuid.UUID `json:"scheduleId"`
	GroupId    uuid.UUID `json:"groupId"`
	JobRunId   uuid.UUID `json:"jobRunId"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason"`
}

type ScheduleJobEvent struct {
	ScheduleId uuid.UUID       `json:"scheduleId"`
	GroupId    uuid.UUID       `json:"groupId"`
	JobRunId   uuid.UUID       `json:"jobRunId"`
	Data       *map[string]any `json:"data"`
}

type JobRunStatus string

const (
	// successfully processed
	JobSucceed JobRunStatus = "succeed"

	// error during processing
	JobFailed JobRunStatus = "failed"
)

const statusAddress = "http://localhost:7468/api/v1/schedules/status"

func main() {
	r := mux.NewRouter()
	baseLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("can't initialize zap logger: %v", err))
	}

	logger := baseLogger.Sugar()
	defer logger.Sync()

	srv := &http.Server{
		Addr:    ":5001",
		Handler: r,
	}

	v1 := r.PathPrefix("/api/v1").Subrouter()
	v1.HandleFunc("/jobs/process-user-notifications", func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var event ScheduleJobEvent
		err := json.Unmarshal(data, &event)

		if err != nil {
			w.WriteHeader(http.StatusAccepted)
			fmt.Printf("error during request processing %s", err)
			return
		}

		go processSyncJob(event, logger)
		w.WriteHeader(http.StatusAccepted)
	}).Methods("POST")

	StartRabbitMq(context.Background(), logger)

	logger.Infof("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		logger.Errorln(err)
	}
}

func StartRabbitMq(ctx context.Context, logger *zap.SugaredLogger) {
	logger.Infoln("starting ...")

	tra, err := scheduler.NewRabbitMqConnection("amqp://guest:guest@localhost:5672", logger)
	if err != nil {
		panic(fmt.Sprintf("test-app: create transport error - %s", err))
	}

	for _, jobSlug := range []string{"process-user-notifications"} {
		logger.Infof("test app registered %s", jobSlug)
		go func(jobSlug string) {
			err = tra.Subscribe(ctx, jobSlug, func(message []byte) error {
				logger.Infof("requested job start with slug %s", jobSlug)

				var event ScheduleJobEvent
				err = json.Unmarshal(message, &event)
				if err != nil {
					return err
				}

				err = processAsyncJob(ctx, tra, event, logger)
				if err != nil {
					logger.Errorf("error during job processing - %s", err)
				}

				return nil
			})

			if err != nil {
				logger.Errorf("error during subscribe for job %s - %s", jobSlug, err)
				return
			}
		}(jobSlug)
	}
}

func processSyncJob(event ScheduleJobEvent, logger *zap.SugaredLogger) {
	for i := 0; i < 5; i++ {
		if jitterFail() {
			jobFailed := JobStatusEvent{
				ScheduleId: event.ScheduleId,
				GroupId:    event.GroupId,
				JobRunId:   event.JobRunId,
				Status:     string(JobFailed),
				Reason:     "failed due to jitter error",
			}

			json, _ := json.Marshal(jobFailed)
			_, err := http.Post(statusAddress, "application/json", bytes.NewBuffer(json))
			if err != nil {
				logger.Errorf("received error on send job processing event %v", err)
				break
			}

			logger.Infoln("sent job failed event")
			return
		}

		logger.Infoln("job processing")
		time.Sleep(time.Second)
	}

	jobSuccess := JobStatusEvent{
		ScheduleId: event.ScheduleId,
		GroupId:    event.GroupId,
		JobRunId:   event.JobRunId,
		Status:     string(JobSucceed),
		Reason:     "success",
	}

	json, _ := json.Marshal(jobSuccess)
	_, err := http.Post(statusAddress, "application/json", bytes.NewBuffer(json))
	if err != nil {
		logger.Errorf("received error on send job processing event %v", err)
	}

	logger.Infoln("sent success event")
}

func processAsyncJob(ctx context.Context, tra *scheduler.RabbitMqTransport, event ScheduleJobEvent,
	logger *zap.SugaredLogger) error {
	for i := 0; i < 5; i++ {
		if jitterFail() {
			err := tra.Publish(ctx, string(scheduler.ExchangeJobStatus),
				string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
					ScheduleId: event.ScheduleId,
					GroupId:    event.GroupId,
					JobRunId:   event.JobRunId,
					Status:     string(JobFailed),
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

	err := tra.Publish(ctx, string(scheduler.ExchangeJobStatus),
		string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
			ScheduleId: event.ScheduleId,
			GroupId:    event.GroupId,
			JobRunId:   event.JobRunId,
			Status:     string(JobSucceed),
			Reason:     "success",
		})

	if err != nil {
		logger.Errorf("error publishing during job status %v", err)
	}

	return nil
}

func jitterFail() bool {
	failPercentage := 15

	return rand.Intn(100) <= failPercentage-1
}
