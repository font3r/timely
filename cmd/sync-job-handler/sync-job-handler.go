package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
	"timely/libs"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
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
	v1.HandleFunc("/jobs/process-test-job", func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var event libs.ScheduleJobEvent
		err := json.Unmarshal(data, &event)

		if err != nil {
			w.WriteHeader(http.StatusAccepted)
			fmt.Printf("error during request processing %s", err)
			return
		}

		go processSyncJob(event, logger)
		w.WriteHeader(http.StatusAccepted)
	}).Methods("POST")

	logger.Infof("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		logger.Errorln(err)
	}
}

func processSyncJob(event libs.ScheduleJobEvent, logger *zap.SugaredLogger) {
	for i := 0; i < 5; i++ {
		if jitterFail() {
			jobFailed := libs.JobStatusEvent{
				ScheduleId: event.ScheduleId,
				GroupId:    event.GroupId,
				JobRunId:   event.JobRunId,
				Status:     string(libs.JobFailed),
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

	jobSuccess := libs.JobStatusEvent{
		ScheduleId: event.ScheduleId,
		GroupId:    event.GroupId,
		JobRunId:   event.JobRunId,
		Status:     string(libs.JobSucceed),
		Reason:     "success",
	}

	json, _ := json.Marshal(jobSuccess)
	_, err := http.Post(statusAddress, "application/json", bytes.NewBuffer(json))
	if err != nil {
		logger.Errorf("received error on send job processing event %v", err)
	}

	logger.Infoln("sent success event")
}

func jitterFail() bool {
	failPercentage := 15

	return rand.Intn(100) <= failPercentage-1
}
