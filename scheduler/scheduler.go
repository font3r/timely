package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
	log "timely/logger"

	"github.com/google/uuid"
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
	Status     string    `json:"status"`
	Reason     string    `json:"reason"`
}

type ScheduleJobEvent struct {
	ScheduleId uuid.UUID       `json:"schedule_id"`
	GroupId    uuid.UUID       `json:"group_id"`
	JobRunId   uuid.UUID       `json:"job_run_id"`
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

	scheduler := Scheduler{
		Id:             uuid.New(),
		Storage:        storage,
		AsyncTransport: asyncTransport,
		SyncTransport:  syncTransport,
	}

	log.Logger.Printf("starting scheduler with id %s\n", scheduler.Id)

	err := scheduler.createAsyncTransportDependencies()
	if err != nil {
		log.Logger.Printf("creating internal exchanges/queues error - %v", err)
		return nil
	}

	go scheduler.processJobEvents(ctx)
	go func(storage StorageDriver, asyncTransport AsyncTransportDriver) {
		for {
			err := scheduler.processTick(ctx)
			if err != nil {
				log.Logger.Printf("processing scheduler tick error - %v", err)
			}

			time.Sleep(SchedulerTickDelay)
		}
	}(storage, asyncTransport)

	return &scheduler
}

func (s *Scheduler) createAsyncTransportDependencies() error {
	err := s.AsyncTransport.CreateExchange(string(ExchangeJobSchedule))
	if err != nil {
		return nil
	}

	err = s.AsyncTransport.CreateExchange(string(ExchangeJobStatus))
	if err != nil {
		return err
	}

	err = s.AsyncTransport.CreateQueue(string(QueueJobStatus))
	if err != nil {
		return err
	}

	err = s.AsyncTransport.BindQueue(string(QueueJobStatus), string(ExchangeJobStatus),
		string(RoutingKeyJobStatus))
	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) processTick(ctx context.Context) error {
	schedules, err := s.Storage.GetAwaitingSchedules(ctx)
	if err != nil {
		return errors.Join(ErrFetchAwaitingSchedules, err)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(schedules)) // TODO: probably this should be limited
	defer wg.Wait()

	for _, schedule := range schedules {
		go s.processSchedule(ctx, schedule, &wg)
	}

	return nil
}

func (s *Scheduler) processSchedule(ctx context.Context, schedule *Schedule, wg *sync.WaitGroup) {
	defer wg.Done()
	schedule.Start(time.Now)
	jobRun := NewJobRun(schedule.Id, schedule.GroupId, time.Now)

	var schueduleStartErr error
	switch schedule.Configuration.TransportType {
	case Http:
		schueduleStartErr = s.handleHttp(ctx, schedule, &jobRun)
	case Rabbitmq:
		schueduleStartErr = s.handleRabbitMq(ctx, schedule, &jobRun)
	}

	if schueduleStartErr != nil {
		log.Logger.Printf("failed to start job - %v", schueduleStartErr)
		groupRuns, innerErr := s.Storage.GetJobRunGroup(ctx, schedule.Id, jobRun.GroupId)
		if innerErr != nil {
			log.Logger.Printf("error getting job run group - %v\n", innerErr)
			jobRun.Failed(errors.Join(schueduleStartErr, innerErr).Error(), time.Now)
			schedule.Failed(1, time.Now) // TODO: probably infinite loop
		} else {
			jobRun.Failed(schueduleStartErr.Error(), time.Now)
			// len + 1 because current job run is not yet stored in persistent storage
			schedule.Failed(len(groupRuns)+1, time.Now)
		}

	} else {
		log.Logger.Printf("scheduled job %s/%s, run %s", schedule.Job.Id, schedule.Job.Slug, jobRun.Id)
	}

	// TODO: starting schedule should be transactional so outbox is most likely needed for async transport
	// Job run has to be created before starting job because we can hit race condition with job statuses
	err := s.Storage.AddJobRun(ctx, jobRun)
	if err != nil {
		log.Logger.Printf("error adding job run - %v\n", err)
		return
	}

	err = s.Storage.UpdateSchedule(ctx, *schedule)
	if err != nil {
		log.Logger.Printf("error updating schedule status - %v\n", err)
		return
	}
}

func (s *Scheduler) handleHttp(ctx context.Context, schedule *Schedule, jobRun *JobRun) error {
	err := s.SyncTransport.Start(ctx, schedule.Configuration.Url,
		ScheduleJobRequest{
			ScheduleId: schedule.Id,
			GroupId:    jobRun.GroupId,
			JobRunId:   jobRun.Id,
			Job:        schedule.Job.Slug,
			Data:       schedule.Job.Data,
		})

	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) handleRabbitMq(ctx context.Context, schedule *Schedule, jobRun *JobRun) error {
	err := s.AsyncTransport.BindQueue(schedule.Job.Slug, string(ExchangeJobSchedule), schedule.Job.Slug)
	if err != nil {
		return err
	}

	err = s.AsyncTransport.Publish(ctx, string(ExchangeJobSchedule), schedule.Job.Slug,
		ScheduleJobEvent{
			ScheduleId: schedule.Id,
			GroupId:    jobRun.GroupId,
			JobRunId:   jobRun.Id,
			Data:       schedule.Job.Data,
		})

	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) processJobEvents(ctx context.Context) {
	err := s.AsyncTransport.Subscribe(ctx, string(QueueJobStatus), func(message []byte) error {
		err := HandleJobEvent(ctx, message, s.Storage)
		if err != nil {
			log.Logger.Printf("error during job event processing - %v\n", err)
			return err
		}

		return nil
	})

	if err != nil {
		log.Logger.Printf("error during subscribing to process job events - %v\n", err)
		return
	}
}

func HandleJobEvent(ctx context.Context, message []byte, storage StorageDriver) error {
	jobStatus := JobStatusEvent{}
	err := json.Unmarshal(message, &jobStatus)
	if err != nil {
		log.Logger.Printf("received invalid job event  %+v", jobStatus)
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
		{
			jobRun.Failed(jobStatus.Reason, time.Now)
			schedule.Failed(len(groupRuns), time.Now)
		}
	case string(JobSucceed):
		{
			jobRun.Succeed(time.Now)
			schedule.Succeed(time.Now) // TODO: after one time schedules we have to clean some transport methods eg. rabbit
		}
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
