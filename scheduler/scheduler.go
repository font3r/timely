package scheduler

import (
	"encoding/json"
	"time"
	log "timely/logger"

	"github.com/google/uuid"
)

type Scheduler struct {
	Id        uuid.UUID
	Storage   StorageDriver
	Transport TransportDriver
}

type JobStatusEvent struct {
	JobSlug string `json:"job_slug"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
}

var (
	ErrReceivedStatusForUnknownSchedule = &Error{
		Code: "UNKNOWN_SCHEDULE",
		Msg:  "received status for unknown schedule"}
	ErrFetchNewSchedules = &Error{
		Code: "FETCH_NEW_SCHEDULES_ERROR",
		Msg:  "fetch new schedules failed"}
	ErrFetchFailedSchedules = &Error{
		Code: "FETCH_FAILED_SCHEDULES_ERROR",
		Msg:  "fetch failed schedules failed"}
)

func Start(storage StorageDriver, transport TransportDriver) *Scheduler {
	schedulerId := uuid.New()
	scheduler := Scheduler{
		Id:        schedulerId,
		Storage:   storage,
		Transport: transport,
	}

	log.Logger.Printf("starting scheduler with id %s\n", schedulerId)

	err := createTransportDependencies(transport)
	if err != nil {
		log.Logger.Printf("creating internal exchanges/queues error - %v", err)
		return nil
	}

	go func(storage StorageDriver, transport TransportDriver) {
		tickResult := make(chan error)

		for {
			go processTick(storage, transport, tickResult)

			err = <-tickResult
			if err != nil {
				log.Logger.Printf("processing scheduler tick error - %v", err)
			}

			time.Sleep(time.Second)
		}
	}(storage, transport)

	go processJobEvents(storage, transport)

	return &scheduler
}

func createTransportDependencies(transport TransportDriver) error {
	err := transport.CreateExchange(string(ExchangeJobSchedule))
	if err != nil {
		return nil
	}

	err = transport.CreateExchange(string(ExchangeJobStatus))
	if err != nil {
		return err
	}

	err = transport.CreateQueue(string(QueueJobStatus))
	if err != nil {
		return err
	}

	err = transport.BindQueue(string(QueueJobStatus), string(ExchangeJobStatus),
		string(RoutingKeyJobStatus))
	if err != nil {
		return err
	}

	return nil
}

func processTick(storage StorageDriver, transport TransportDriver, tickResult chan<- error) {
	schedules, err := getSchedulesReadyToStart(storage)
	if err != nil {
		tickResult <- err
		return
	}

	for _, schedule := range schedules {
		scheduleResult := make(chan error)
		go schedule.Start(transport, scheduleResult)

		scheduleStartError := <-scheduleResult
		if scheduleStartError != nil {
			tickResult <- scheduleStartError
			return
		}

		err = storage.UpdateSchedule(schedule)
		if err != nil {
			log.Logger.Printf("error updating schedule status - %v\n", err)
			tickResult <- err
			return
		}
	}

	tickResult <- nil
}

func processJobEvents(storage StorageDriver, transport TransportDriver) {
	err := transport.Subscribe(string(QueueJobStatus), func(message []byte) error {
		return handleJobEvent(message, storage)
	})

	if err != nil {
		log.Logger.Printf("error during processing job events - %v\n", err)
		return
	}
}

func handleJobEvent(message []byte, storage StorageDriver) error {
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
		now := time.Now().Round(time.Second)
		schedule.LastExecutionDate = &now
	}

	log.Logger.Printf("received job status %+v\n", jobStatus)
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

func getSchedulesReadyToStart(storage StorageDriver) ([]*Schedule, error) {
	readySchedules, err := storage.GetSchedulesWithStatus(New)
	if err != nil {
		log.Logger.Printf("fetch new schedules error - %v\n", err)
		return nil, ErrFetchNewSchedules
	}

	rescheduleReady, err := storage.GetSchedulesReadyToReschedule()
	if err != nil {
		log.Logger.Printf("fetch failed schedules error - %v\n", err)
		return nil, ErrFetchFailedSchedules
	}

	readySchedules = append(readySchedules, rescheduleReady...)

	return readySchedules, nil
}
