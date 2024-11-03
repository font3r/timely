package main

import (
	"bytes"
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

// TODO: clean this mess to avoid unnecessary bugs, even if it's test handler

func main() {
	r := mux.NewRouter()
	srv := &http.Server{
		Addr:    ":5001",
		Handler: r,
	}

	v1 := r.PathPrefix("/api/v1").Subrouter()
	v1.HandleFunc("/jobs/process-user-notifications", func(w http.ResponseWriter, r *http.Request) {
		log.Logger.Println("received job start request")

		var event ScheduleJobEvent
		data, _ := io.ReadAll(r.Body)
		err := json.Unmarshal(data, &event)
		if err != nil {
			w.WriteHeader(http.StatusAccepted)
			fmt.Printf("error during request processing %s", err)
			return
		}

		go processSyncJob(event)
		w.WriteHeader(http.StatusAccepted)
	}).Methods("POST")

	Start()

	log.Logger.Printf("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Logger.Println(err)
	}
}

type JobStatusEvent struct {
	ScheduleId uuid.UUID `json:"schedule_id"`
	GroupId    uuid.UUID `json:"group_id"`
	JobRunId   uuid.UUID `json:"job_run_id"`
	JobSlug    string    `json:"job_slug"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason"`
}

type ScheduleJobEvent struct {
	ScheduleId uuid.UUID       `json:"schedule_id"`
	GroupId    uuid.UUID       `json:"group_id"`
	JobRunId   uuid.UUID       `json:"job_run_id"`
	Job        string          `json:"job"`
	Data       *map[string]any `json:"data"`
}

var statusAddress = "http://localhost:5000/api/v1/schedules/status"

func processSyncJob(event ScheduleJobEvent) {
	for i := 0; i < 5; i++ {
		if jitterFail() { // random 10% failure rate for testing
			jobFailed, _ := json.Marshal(JobStatusEvent{
				ScheduleId: event.ScheduleId,
				GroupId:    event.GroupId,
				JobRunId:   event.JobRunId,
				JobSlug:    event.Job,
				Status:     "failed",
				Reason:     "failed due to jitter error",
			})

			json, _ := json.Marshal(jobFailed)
			_, err := http.Post(statusAddress, "application/json", bytes.NewBuffer(json))
			if err != nil {
				log.Logger.Println("received error on job processing event")
				break
			}
		}

		jobProcessing, _ := json.Marshal(JobStatusEvent{
			ScheduleId: event.ScheduleId,
			GroupId:    event.GroupId,
			JobRunId:   event.JobRunId,
			JobSlug:    event.Job,
			Status:     "processing",
		})

		json, _ := json.Marshal(jobProcessing)
		_, err := http.Post(statusAddress, "application/json", bytes.NewBuffer(json))
		if err != nil {
			log.Logger.Println("received error on job processing event")
			break
		}

		log.Logger.Println("sent processing event")
		time.Sleep(time.Second)
	}

	jobSuccess := JobStatusEvent{
		ScheduleId: event.ScheduleId,
		GroupId:    event.GroupId,
		JobRunId:   event.JobRunId,
		JobSlug:    event.Job,
		Status:     "succeed",
		Reason:     "success",
	}

	json, _ := json.Marshal(jobSuccess)
	http.Post(statusAddress, "application/json", bytes.NewBuffer(json))

	log.Logger.Println("sent success event")
}

func Start() {
	log.Logger.Println("starting ...")

	tra, err := scheduler.NewRabbitMqTransportConnection("amqp://guest:guest@localhost:5672")
	if err != nil {
		panic(fmt.Sprintf("test-app: create transport error - %s", err))
	}

	for _, jobSlug := range []string{"process-user-notifications"} {
		log.Logger.Printf("test app registered %s", jobSlug)
		go func(jobSlug string) {
			err = tra.Subscribe(jobSlug, func(message []byte) error {
				log.Logger.Printf("requested job start with slug %s", jobSlug)

				var event ScheduleJobEvent
				err = json.Unmarshal(message, &event)
				if err != nil {
					return err
				}

				if event.Data == nil {
					log.Logger.Printf("job slug %s", jobSlug)
				} else {
					log.Logger.Printf("job data slug %s with data %+v", jobSlug, *event.Data)
				}

				err = processMockJob(tra, event.ScheduleId, event.GroupId, event.JobRunId, jobSlug)
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

func processMockJob(tra *scheduler.RabbitMqTransport, scheduleId, groupId, jobRunId uuid.UUID, jobSlug string) error {
	for i := 0; i < 5; i++ {
		if jitterFail() { // random 10% failure rate for testing
			err := tra.Publish(string(scheduler.ExchangeJobStatus),
				string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
					ScheduleId: scheduleId,
					GroupId:    groupId,
					JobRunId:   jobRunId,
					JobSlug:    jobSlug,
					Status:     "failed",
					Reason:     "failed due to jitter error",
				})

			if err != nil {
				log.Logger.Printf("error during publishing job status %s", err)
			}

			return errors.New("jitter job failure")
		}

		err := tra.Publish(string(scheduler.ExchangeJobStatus),
			string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
				ScheduleId: scheduleId,
				GroupId:    groupId,
				JobRunId:   jobRunId,
				JobSlug:    jobSlug,
				Status:     "processing",
			})

		if err != nil {
			log.Logger.Printf("error during publishing job status %s", err)
		}

		time.Sleep(time.Second)
	}

	err := tra.Publish(string(scheduler.ExchangeJobStatus),
		string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
			ScheduleId: scheduleId,
			GroupId:    groupId,
			JobRunId:   jobRunId,
			JobSlug:    jobSlug,
			Status:     "succeed",
			Reason:     "success",
		})

	if err != nil {
		log.Logger.Printf("error publishing during job status %s", err)
	}

	return nil
}

func jitterFail() bool {
	return rand.Intn(10) <= 0 // random 10% failure rate for testing
}
