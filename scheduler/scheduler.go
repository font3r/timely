package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Scheduler struct {
	Id             uuid.UUID
	Storage        StorageDriver
	AsyncTransport AsyncTransportDriver
	SyncTransport  SyncTransportDriver
	logger         *zap.SugaredLogger
}

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

const schedulerTickDelay = time.Second
const getStaleJobsDelay = time.Second * 5
const MAX_SCHEDULES_CONCURRENCY = 2

var Supports []string

func Start(ctx context.Context, storage StorageDriver, asyncTransport AsyncTransportDriver,
	syncTransport SyncTransportDriver, supports []string, logger *zap.SugaredLogger) *Scheduler {

	scheduler := Scheduler{
		Id:             uuid.New(),
		Storage:        storage,
		AsyncTransport: asyncTransport,
		SyncTransport:  syncTransport,
		logger:         logger,
	}

	Supports = supports

	logger.Infof("starting scheduler with id %s", scheduler.Id)

	if slices.Contains(Supports, "rabbitmq") {
		go scheduler.processJobEvents(ctx)
	}

	go scheduler.staleJobSearch(ctx)

	go func() {
		for {
			err := scheduler.processTick(ctx)
			if err != nil {
				logger.Infof("processing scheduler tick error - %v", err)
			}

			time.Sleep(schedulerTickDelay)
		}
	}()

	return &scheduler
}

func (s *Scheduler) staleJobSearch(ctx context.Context) {
	s.logger.Info("starting stale jobs searching")

	// TODO: how should we handle stale jobs
	for {
		staleJobs, err := s.Storage.GetStaleJobs(ctx)
		if err != nil {
			s.logger.Error("error during getting stale jobs %v", err)
			continue
		}

		if len(staleJobs) > 0 {
			s.logger.Warnf("found %d stale schedules", len(staleJobs))
		}

		time.Sleep(getStaleJobsDelay)
	}
}

func (s *Scheduler) processTick(ctx context.Context) error {
	schedules, err := s.Storage.GetAwaitingSchedules(ctx)
	if err != nil {
		return errors.Join(ErrFetchAwaitingSchedules, err)
	}

	sem := make(chan struct{}, MAX_SCHEDULES_CONCURRENCY)

	for _, schedule := range schedules {
		sem <- struct{}{}
		go s.processSchedule(ctx, schedule, sem)
	}

	return nil
}

func (s *Scheduler) processSchedule(ctx context.Context, schedule *Schedule, sem chan struct{}) {
	defer func() { <-sem }()
	schedule.Start(time.Now)
	jobRun := NewJobRun(schedule.Id, schedule.GroupId, time.Now)

	var schueduleStartErr error
	switch schedule.Configuration.TransportType {
	case Http:
		schueduleStartErr = s.handleHttp(ctx, schedule, &jobRun)
	case Rabbitmq:
		schueduleStartErr = s.handleRabbitMq(ctx, schedule, &jobRun)
	default:
		schueduleStartErr = fmt.Errorf("unsupported transport type - %s",
			schedule.Configuration.TransportType)
	}

	if schueduleStartErr != nil {
		s.logger.Errorf("failed to start job for schedule %s - %v", schedule.Id, schueduleStartErr)
		groupRuns, innerErr := s.Storage.GetJobRunGroup(ctx, schedule.Id, jobRun.GroupId)
		if innerErr != nil {
			s.logger.Errorf("error getting job run group for schedule %s - %v", schedule.Id, innerErr)
			jobRun.Failed(errors.Join(schueduleStartErr, innerErr).Error(), time.Now)
			schedule.Failed(1, time.Now) // TODO: probably infinite loop
		} else {
			jobRun.Failed(schueduleStartErr.Error(), time.Now)
			// len + 1 because current job run is not yet stored in persistent storage
			schedule.Failed(len(groupRuns)+1, time.Now)
		}

	} else {
		s.logger.Infof("scheduled job %s/%s, run %s", schedule.Job.Id, schedule.Job.Slug, jobRun.Id)
	}

	// TODO: starting schedule should be transactional so outbox is most likely needed for async transport
	// Job run has to be created before starting job because we can hit race condition with job statuses
	err := s.Storage.AddJobRun(ctx, jobRun)
	if err != nil {
		s.logger.Errorf("error adding job run - %v", err)
		return
	}

	err = s.Storage.UpdateSchedule(ctx, *schedule)
	if err != nil {
		s.logger.Errorf("error updating schedule status - %v", err)
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

// TODO: this probably should be external dependency that signals scheduler about event
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
		err := HandleJobEvent(ctx, message, s.Storage, s.logger)
		if err != nil {
			s.logger.Errorf("error during job event processing - %v", err)
			return err
		}

		return nil
	})

	if err != nil {
		s.logger.Errorf("error during subscribing to process job events - %v", err)
		return
	}
}

func HandleJobEvent(ctx context.Context, message []byte, storage StorageDriver, logger *zap.SugaredLogger) error {
	jobStatus := JobStatusEvent{}
	err := json.Unmarshal(message, &jobStatus)
	if err != nil {
		logger.Infof("received invalid job event  %+v", err)
		return err
	}

	logger.Infof("received status %+v", jobStatus)

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
			schedule.Succeed(time.Now)
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
