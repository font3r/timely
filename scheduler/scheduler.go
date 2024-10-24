package scheduler

import (
	"context"
	"encoding/json"
	"time"
	log "timely/logger"

	"github.com/google/uuid"
)

type Scheduler struct {
	Id        uuid.UUID
	Storage   StorageDriver
	Transport AsyncTransportDriver
}

type JobStatusEvent struct {
	ScheduleId uuid.UUID `json:"schedule_id"`
	JobRunId   uuid.UUID `json:"job_run_id"`
	JobSlug    string    `json:"job_slug"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason"`
}

var (
	ErrReceivedStatusForUnknownSchedule = &Error{
		Code: "UNKNOWN_SCHEDULE",
		Msg:  "received status for unknown schedule"}
	ErrReceivedStatusForUnknownJobRun = &Error{
		Code: "UNKNOWN_JOB_RUN",
		Msg:  "received status for unknown job run"}
	ErrFetchNewSchedules = &Error{
		Code: "FETCH_NEW_SCHEDULES_ERROR",
		Msg:  "fetch new schedules failed"}
	ErrFetchFailedSchedules = &Error{
		Code: "FETCH_FAILED_SCHEDULES_ERROR",
		Msg:  "fetch failed schedules failed"}
)

func Start(ctx context.Context, storage StorageDriver, transport AsyncTransportDriver) *Scheduler {
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

	go func(storage StorageDriver, transport AsyncTransportDriver) {
		tickResult := make(chan error)

		for {
			go processTick(ctx, storage, transport, tickResult)

			err = <-tickResult
			if err != nil {
				log.Logger.Printf("processing scheduler tick error - %v", err)
			}

			time.Sleep(time.Second)
		}
	}(storage, transport)

	go processJobEvents(ctx, storage, transport)

	return &scheduler
}

func createTransportDependencies(transport AsyncTransportDriver) error {
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

func processTick(ctx context.Context, storage StorageDriver, transport AsyncTransportDriver, tickResult chan<- error) {
	schedules, err := getSchedulesReadyToStart(ctx, storage)
	if err != nil {
		tickResult <- err
		return
	}

	for _, schedule := range schedules {
		scheduleResult := make(chan error)
		jobRun := NewJobRun(schedule.Id)

		// TODO: this should be transactional so outbox is most likely needed
		// Job run has to be created before starting job because we can hit race condition with job statuses
		err = storage.AddJobRun(ctx, jobRun)
		if err != nil {
			log.Logger.Printf("error adding job run - %v\n", err)
			tickResult <- err
			return
		}

		go schedule.Start(jobRun.Id, transport, scheduleResult)

		scheduleStartError := <-scheduleResult
		if scheduleStartError != nil {
			tickResult <- scheduleStartError
			return
		}

		err = storage.UpdateSchedule(ctx, *schedule)
		if err != nil {
			log.Logger.Printf("error updating schedule status - %v\n", err)
			tickResult <- err
			return
		}
	}

	tickResult <- nil
}

func processJobEvents(ctx context.Context, storage StorageDriver, transport AsyncTransportDriver) {
	err := transport.Subscribe(string(QueueJobStatus), func(message []byte) error {
		return handleJobEvent(ctx, message, storage)
	})

	if err != nil {
		log.Logger.Printf("error during processing job events - %v\n", err)
		return
	}
}

func handleJobEvent(ctx context.Context, message []byte, storage StorageDriver) error {
	jobStatus := JobStatusEvent{}
	err := json.Unmarshal(message, &jobStatus)
	if err != nil {
		return err
	}

	log.Logger.Printf("received status %+v", jobStatus)

	schedule, err := storage.GetScheduleById(ctx, jobStatus.ScheduleId)
	if err != nil {
		return err
	}

	if schedule == nil {
		return ErrReceivedStatusForUnknownSchedule
	}

	// TODO: at this point we can detect if we received status for stale job
	jobRun, err := storage.GetJobRun(ctx, jobStatus.JobRunId)
	if err != nil {
		return err
	}

	if jobRun == nil {
		return ErrReceivedStatusForUnknownJobRun
	}

	switch jobStatus.Status {
	// TODO: handle job not started within X time
	case string(JobProcessing):
		jobRun.Status = JobProcessing
	case string(JobFailed):
		{
			jobRun.Failed(jobStatus.Reason)
			if err = schedule.Failed(); err != nil {
				return err
			}
		}
	case string(JobSucceed):
		jobRun.Succeed()
		schedule.Succeed() // TODO: after one time schedules we have to clean transport
	}

	err = storage.UpdateJobRun(ctx, *jobRun)
	if err != nil {
		return err
	}

	err = storage.UpdateSchedule(ctx, *schedule)
	if err != nil {
		return err
	}

	return nil
}

func getSchedulesReadyToStart(ctx context.Context, storage StorageDriver) ([]*Schedule, error) {
	readySchedules, err := storage.GetSchedulesWithStatus(ctx, Waiting)
	if err != nil {
		return nil, ErrFetchNewSchedules
	}

	rescheduleReady, err := storage.GetSchedulesReadyToReschedule(ctx)
	if err != nil {
		return nil, ErrFetchFailedSchedules
	}

	readySchedules = append(readySchedules, rescheduleReady...)

	return readySchedules, nil
}
