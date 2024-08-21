package scheduler

import (
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
)

type Scheduler struct {
	Id        uuid.UUID
	Storage   *JobStorage
	Transport *Transport
}

type JobStatusEvent struct {
	JobSlug string `json:"jobSlug"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Seq     int16  `json:"seq"`
}

var (
	ErrReceivedStatusForUnknownSchedule = &Error{
		Code:    "UNKNOWN_SCHEDULE",
		Message: "received status for unknown schedule"}
	ErrFetchNewSchedules = &Error{
		Code:    "FETCH_NEW_SCHEDULES_ERROR",
		Message: "fetch new schedules failed"}
	ErrFetchFailedSchedules = &Error{
		Code:    "FETCH_FAILED_SCHEDULES_ERROR",
		Message: "fetch failed schedules failed"}
)

func Start(str *JobStorage, tra *Transport) *Scheduler {
	schedulerId := uuid.New()
	scheduler := Scheduler{
		Id:        schedulerId,
		Storage:   str,
		Transport: tra,
	}

	log.Printf("starting scheduler with id %s\n", schedulerId)

	err := createTransportDependencies(tra)
	if err != nil {
		log.Printf("creating internal exchanges/queues error - %v", err)
		return nil
	}

	go func(str *JobStorage, tra *Transport) {
		tickResult := make(chan error)

		for {
			go processTick(str, tra, tickResult)

			err = <-tickResult
			if err != nil {
				log.Printf("processing scheduler tick error - %v", err)
			}

			time.Sleep(time.Second)
		}
	}(str, tra)

	go processJobEvents(tra, str)

	return &scheduler
}

func createTransportDependencies(tran *Transport) error {
	err := tran.CreateExchange(string(ExchangeJobSchedule))
	if err != nil {
		return nil
	}

	err = tran.CreateExchange(string(ExchangeJobStatus))
	if err != nil {
		return err
	}

	err = tran.CreateQueue(string(QueueJobStatus))
	if err != nil {
		return err
	}

	err = tran.BindQueue(string(QueueJobStatus), string(ExchangeJobStatus),
		string(RoutingKeyJobStatus))
	if err != nil {
		return err
	}

	return nil
}

func processTick(str *JobStorage, tr *Transport, tickResult chan<- error) {
	schedules, err := getSchedulesReadyToStart(str)
	if err != nil {
		tickResult <- err
		return
	}

	for _, schedule := range schedules {
		scheduleResult := make(chan error)
		go schedule.Start(tr, scheduleResult)

		scheduleStartError := <-scheduleResult
		if scheduleStartError != nil {
			tickResult <- scheduleStartError
			return
		}

		err = str.UpdateSchedule(schedule)
		if err != nil {
			log.Printf("error updating schedule status - %v\n", err)
			tickResult <- err
			return
		}
	}

	tickResult <- nil
}

func processJobEvents(tra *Transport, storage *JobStorage) {
	err := tra.Subscribe(string(QueueJobStatus), func(message []byte) error {
		return handleJobEvent(message, storage)
	})

	if err != nil {
		log.Printf("error during processing job events - %v\n", err)
		return
	}
}

func handleJobEvent(message []byte, storage *JobStorage) error {
	jobStatus := JobStatusEvent{}
	err := json.Unmarshal(message, &jobStatus)
	if err != nil {
		return err
	}

	schedule, err := storage.GetScheduleByJobSlug(jobStatus.JobSlug)
	if err != nil {
		return err
	}

	if schedule == nil {
		return ErrReceivedStatusForUnknownSchedule
	}

	if schedule.LastExecutionDate == nil {
		now := time.Now()
		schedule.LastExecutionDate = &now
	}

	log.Printf("received job status %v\n", jobStatus)
	switch jobStatus.Status {
	case string(Processing):
		schedule.Status = Processing
	case string(Failed):
		if err = schedule.Failed(); err != nil {
			return err
		}
	case string(Finished):
		schedule.Finished() // TODO: after one time schedules we have to clean transport
	}

	err = storage.UpdateSchedule(schedule)
	if err != nil {
		return err
	}

	return nil
}

func getSchedulesReadyToStart(str *JobStorage) ([]*Schedule, error) {
	readySchedules, err := str.GetSchedulesWithStatus(New)
	if err != nil {
		log.Printf("fetch new schedules error - %v\n", err)
		return nil, ErrFetchNewSchedules
	}

	rescheduleReady, err := str.GetSchedulesReadyToReschedule()
	if err != nil {
		log.Printf("fetch failed schedules error - %v\n", err)
		return nil, ErrFetchFailedSchedules
	}

	readySchedules = append(readySchedules, rescheduleReady...)

	return readySchedules, nil
}
