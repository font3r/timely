package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"task-scheduler/scheduler/transport"
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
	JobName string `json:"jobName"`
	Status  string `json:"status"`
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

	tra, err := transport.Create(10)
	if err != nil {
		panic(fmt.Sprintf("create transport error %s", err))
	}

	go tra.Subscribe("job-status", func(message []byte) error {
		jobStatus := JobStatusEvent{}
		err := json.Unmarshal([]byte(message), &jobStatus)
		if err != nil {
			return err
		}

		log.Printf("received %v\n", jobStatus)

		return nil
	})

	go func(str *JobStorage, schedulerId uuid.UUID, tra *transport.Transport, ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				log.Println("scheduler stopped on demand")
				return
			default:
				go processTick(str, tra)
				time.Sleep(time.Second)
			}

		}

	}(str, schedulerId, tra, scheduler.ctx)

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

func processTick(str *JobStorage, tr *transport.Transport) {
	for _, j := range str.GetPending() {
		go j.Start(tr)
	}
}
