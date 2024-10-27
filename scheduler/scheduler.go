package scheduler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
	log "timely/logger"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type Scheduler struct {
	Id             uuid.UUID
	Storage        StorageDriver
	AsyncTransport AsyncTransportDriver
	SyncTransport  SyncTransportDriver
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

var (
	ErrReceivedStatusForUnknownSchedule = &Error{
		Code: "UNKNOWN_SCHEDULE",
		Msg:  "received status for unknown schedule"}
	ErrReceivedStatusForUnknownJobRun = &Error{
		Code: "UNKNOWN_JOB_RUN",
		Msg:  "received status for unknown job run"}
	ErrFetchAwaitingSchedules = &Error{
		Code: "FETCH_AWAITING_SCHEDULES_ERROR",
		Msg:  "fetch awaiting schedules failed"}
)

const SchedulerTickDelay = time.Second

func Start(ctx context.Context, storage StorageDriver, asyncTransport AsyncTransportDriver,
	syncTransport SyncTransportDriver) *Scheduler {

	schedulerId := uuid.New()
	scheduler := Scheduler{
		Id:             schedulerId,
		Storage:        storage,
		AsyncTransport: asyncTransport,
		SyncTransport:  syncTransport,
	}

	log.Logger.Printf("starting scheduler with id %s\n", schedulerId)

	err := createAsyncTransportDependencies(asyncTransport)
	if err != nil {
		log.Logger.Printf("creating internal exchanges/queues error - %v", err)
		return nil
	}

	go processJobEvents(ctx, storage, asyncTransport)
	go func(storage StorageDriver, asyncTransport AsyncTransportDriver) {
		tickResult := make(chan error)

		for {
			go processTick(ctx, storage, asyncTransport, syncTransport, tickResult)

			err = <-tickResult
			if err != nil {
				log.Logger.Printf("processing scheduler tick error - %v", err)
			}

			time.Sleep(SchedulerTickDelay)
		}
	}(storage, asyncTransport)

	return &scheduler
}

func (s *Scheduler) ListenForJobEvents(ctx context.Context, r *mux.Router) {
	r.HandleFunc("/schedules/status", func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = handleJobEvent(ctx, payload, s.Storage)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})
}

func createAsyncTransportDependencies(transport AsyncTransportDriver) error {
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

func processTick(ctx context.Context, storage StorageDriver, asyncTransport AsyncTransportDriver,
	syncTransport SyncTransportDriver, tickResult chan<- error) {

	schedules, err := getSchedulesReadyToStart(ctx, storage)
	if err != nil {
		tickResult <- err
		return
	}

	for _, schedule := range schedules {
		jobRun := NewJobRun(schedule.Id, schedule.GroupId)

		// TODO: starting schedule should be transactional so outbox is most likely needed for async transport
		// Job run has to be created before starting job because we can hit race condition with job statuses
		err = storage.AddJobRun(ctx, jobRun)
		if err != nil {
			log.Logger.Printf("error adding job run - %v\n", err)
			tickResult <- err
			return
		}

		schedule.Start()

		switch schedule.Configuration.TransportType {
		case Http:
			{
				// TODO: such task have to be non-blocking to do not block tick but at the same time we have to wait to set schedule corectly
				syncTransport.Start(ctx, schedule.Configuration.Url,
					ScheduleJobRequest{
						ScheduleId: schedule.Id,
						GroupId:    jobRun.GroupId,
						JobRunId:   jobRun.Id,
						Job:        schedule.Job.Slug,
						Data:       schedule.Job.Data,
					})
			}
		case Rabbitmq:
			{
				err = asyncTransport.BindQueue(schedule.Job.Slug, string(ExchangeJobSchedule), schedule.Job.Slug)
				if err != nil {
					tickResult <- err
					return
				}

				err = asyncTransport.Publish(string(ExchangeJobSchedule), schedule.Job.Slug,
					ScheduleJobEvent{
						ScheduleId: schedule.Id,
						GroupId:    jobRun.GroupId,
						JobRunId:   jobRun.Id,
						Job:        schedule.Job.Slug,
						Data:       schedule.Job.Data,
					})
			}
		}

		if err != nil {
			log.Logger.Printf("failed to start job %v", err)
			tickResult <- err
			return
		}

		err = storage.UpdateSchedule(ctx, *schedule)
		if err != nil {
			log.Logger.Printf("error updating schedule status - %v\n", err)
			tickResult <- err
			return
		}

		log.Logger.Printf("scheduled job %s/%s, run %s", schedule.Job.Id, schedule.Job.Slug, jobRun.Id)
	}

	tickResult <- nil
}

func getSchedulesReadyToStart(ctx context.Context, storage StorageDriver) ([]*Schedule, error) {
	awaitingSchedules, err := storage.GetAwaitingSchedules(ctx)
	if err != nil {
		return nil, ErrFetchAwaitingSchedules
	}

	return awaitingSchedules, nil
}

func processJobEvents(ctx context.Context, storage StorageDriver, asyncTransport AsyncTransportDriver) {
	err := asyncTransport.Subscribe(string(QueueJobStatus), func(message []byte) error {
		return handleJobEvent(ctx, message, storage)
	})

	if err != nil {
		log.Logger.Printf("error during init process job events - %v\n", err)
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

	groupRuns, err := storage.GetJobRunGroup(ctx, jobStatus.ScheduleId, jobStatus.GroupId)
	if err != nil {
		return err
	}

	var jobRun *JobRun
	for _, jr := range groupRuns {
		if jr.Id == jobStatus.JobRunId {
			jobRun = jr
			break
		}
	}

	if jobRun == nil {
		return ErrReceivedStatusForUnknownJobRun
	}

	switch jobStatus.Status {
	case string(JobFailed):
		onJobFailed(schedule, jobRun, jobStatus, len(groupRuns))
	case string(JobSucceed):
		jobRun.Succeed()
		schedule.Succeed() // TODO: after one time schedules we have to clean some transport methods eg. rabbit
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

func onJobFailed(schedule *Schedule, jobRun *JobRun, jobStatus JobStatusEvent, attempt int) error {
	jobRun.Failed(jobStatus.Reason)
	if err := schedule.Failed(attempt); err != nil {
		return err
	}

	return nil
}
