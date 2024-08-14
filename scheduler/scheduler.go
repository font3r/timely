package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type SchedulerState string

const (
	Running SchedulerState = "running"
	Stopped SchedulerState = "stopped"
)

type Scheduler struct {
	Id     uuid.UUID
	state  SchedulerState
	ctx    context.Context
	cancel context.CancelFunc
}

type JobStatusEvent struct {
	JobSlug string `json:"jobSlug"`
	Status  string `json:"status"`
	Seq     int16  `json:"seq"`
}

func Start(str *JobStorage) *Scheduler {
	log.Println("starting scheduler")

	ctx, cancel := context.WithCancel(context.Background())
	schedulerId := uuid.New()
	scheduler := Scheduler{
		Id:     schedulerId,
		ctx:    ctx,
		state:  Running,
		cancel: cancel,
	}

	tra, err := NewConnection(1)
	if err != nil {
		panic(fmt.Sprintf("create transport error %s", err))
	}

	go func(str *JobStorage, schedulerId uuid.UUID, tra *Transport, ctx context.Context) {
		tickResult := make(chan error)

		for {
			select {
			case <-ctx.Done():
				log.Println("scheduler stopped on demand")
				return
			default:
				go processTick(str, tra, tickResult)
				<-tickResult
				time.Sleep(time.Second)
			}
		}
	}(str, schedulerId, tra, scheduler.ctx)

	err = createInternalExchanges(tra)
	if err != nil {
		log.Printf("error during creating internal exchanges/queues - %v", err)
		return nil
	}

	go processJobStatus(tra, str)

	return &scheduler
}

func (s *Scheduler) Stop() error {
	if s.state != Running {
		return fmt.Errorf("invalid scheduler state, expected %s, got %s",
			Running, s.state)
	}

	s.state = Stopped
	s.cancel()

	return nil
}

func processTick(str *JobStorage, tr *Transport, tickResult chan<- error) {
	pendingJobs, err := str.GetNew()

	if err != nil {
		log.Printf("error getting new jobs - %v\n", err)
		tickResult <- err
		return
	}

	for _, j := range pendingJobs {
		jobResult := make(chan bool)
		go j.Start(tr, jobResult)

		<-jobResult // error check

		err = str.UpdateStatus(j)
		if err != nil {
			log.Printf("error updating status - %v\n", err)
			tickResult <- err
			return
		}
	}

	tickResult <- nil
}

func processJobStatus(tra *Transport, storage *JobStorage) {
	err := tra.Subscribe(string(QueueJobStatus),
		func(_ string, message []byte) error {
			jobStatus := JobStatusEvent{}
			err := json.Unmarshal(message, &jobStatus)
			if err != nil {
				return err
			}

			job, err := storage.GetBySlug(jobStatus.JobSlug)
			if err != nil {
				return err
			}

			if job == nil {
				log.Printf("received status for unregistered job %s", jobStatus.JobSlug)
				return errors.New("received status for unregistered job")
			}

			log.Printf("received job status %v\n", jobStatus)
			status, err := job.ProcessState(jobStatus.Status)

			if err != nil {
				return err
			}

			switch status {
			case Finished, Failed:
				{
					err = storage.UpdateStatus(job)
					if err != nil {
						return err
					}
				}
			}

			return nil
		})

	if err != nil {
		log.Printf("error during statusing jobs - %v\n", err)
		return
	}
}

func createInternalExchanges(tran *Transport) error {
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
