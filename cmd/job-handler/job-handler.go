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
	log "timely/logger"
	"timely/scheduler"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
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

		go processSyncJob(event)
		w.WriteHeader(http.StatusAccepted)
	}).Methods("POST")

	StartRabbitMq()

	log.Logger.Printf("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Logger.Println(err)
	}
}

func StartRabbitMq() {
	log.Logger.Println("starting ...")

	tra, err := scheduler.NewRabbitMqConnection("amqp://guest:guest@localhost:5672")
	if err != nil {
		panic(fmt.Sprintf("test-app: create transport error - %s", err))
	}

	for _, jobSlug := range []string{"process-user-notifications"} {
		log.Logger.Printf("test app registered %s", jobSlug)
		go func(jobSlug string) {
			err = tra.Subscribe(context.Background(), jobSlug, func(message []byte) error {
				log.Logger.Printf("requested job start with slug %s", jobSlug)

				var event ScheduleJobEvent
				err = json.Unmarshal(message, &event)
				if err != nil {
					return err
				}

				err = processAsyncJob(tra, event)
				if err != nil {
					log.Logger.Printf("error during job processing - %s", err)
				}

				return nil
			})

			if err != nil {
				log.Logger.Printf("error during subscribe for job %s - %s", jobSlug, err)
				return
			}
		}(jobSlug)
	}
}

func processSyncJob(event ScheduleJobEvent) {
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
				log.Logger.Printf("received error on send job processing event %v\n", err)
				break
			}

			log.Logger.Println("sent job failed event")
			return
		}

		log.Logger.Println("job processing")
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
		log.Logger.Printf("received error on send job processing event %v\n", err)
	}

	log.Logger.Println("sent success event")
}

func processAsyncJob(tra *scheduler.RabbitMqTransport, event ScheduleJobEvent) error {
	for i := 0; i < 5; i++ {
		if jitterFail() {
			err := tra.Publish(context.Background(), string(scheduler.ExchangeJobStatus),
				string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
					ScheduleId: event.ScheduleId,
					GroupId:    event.GroupId,
					JobRunId:   event.JobRunId,
					Status:     string(JobFailed),
					Reason:     "failed due to jitter error",
				})

			if err != nil {
				log.Logger.Printf("error during publishing job status %v", err)
			}

			return errors.New("jitter job failure")
		}

		log.Logger.Println("job processing")
		time.Sleep(time.Second)
	}

	err := tra.Publish(context.Background(), string(scheduler.ExchangeJobStatus),
		string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
			ScheduleId: event.ScheduleId,
			GroupId:    event.GroupId,
			JobRunId:   event.JobRunId,
			Status:     string(JobSucceed),
			Reason:     "success",
		})

	if err != nil {
		log.Logger.Printf("error publishing during job status %v", err)
	}

	return nil
}

func jitterFail() bool {
	failPercentage := 100

	return rand.Intn(100) <= failPercentage-1
}
