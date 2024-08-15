package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type Scheduler struct {
	Id      uuid.UUID
	Storage *JobStorage
}

type JobStatusEvent struct {
	JobSlug string `json:"jobSlug"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Seq     int16  `json:"seq"`
}

func Start(str *JobStorage) *Scheduler {
	schedulerId := uuid.New()
	scheduler := Scheduler{
		Id:      schedulerId,
		Storage: str,
	}

	log.Printf("starting scheduler with id %s\n", schedulerId)

	tra, err := NewConnection()
	if err != nil {
		panic(fmt.Sprintf("create transport error %s", err))
	}

	err = createInternalExchanges(tra)
	if err != nil {
		log.Printf("error during creating internal exchanges/queues - %v", err)
		return nil
	}

	go func(str *JobStorage, tra *Transport) {
		tickResult := make(chan error)

		for {
			select {
			default:
				go processTick(str, tra, tickResult)

				err = <-tickResult
				if err != nil {
					log.Printf("error during processing tick - %v", err)
				}

				time.Sleep(time.Second)
			}
		}
	}(str, tra)

	go processJobStatus(tra, str)

	return &scheduler
}

func processTick(str *JobStorage, tr *Transport, tickResult chan<- error) {
	pendingJobs, err := getJobsReadyToSchedule(str)
	if err != nil {
		tickResult <- err
	}

	for _, j := range pendingJobs {
		jobResult := make(chan error)
		go j.Start(tr, jobResult)

		jobStartResult := <-jobResult
		if jobStartResult != nil {
			tickResult <- jobStartResult
			return
		}

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
			status, err := job.ProcessState(jobStatus.Status, jobStatus.Reason)

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

func getJobsReadyToSchedule(str *JobStorage) ([]*Job, error) {
	pendingJobs, err := str.GetNew()
	if err != nil {
		log.Printf("error getting new jobs - %v\n", err)
		return nil, err
	}

	return pendingJobs, nil
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
