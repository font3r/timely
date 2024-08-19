package scheduler

import (
	"encoding/json"
	"errors"
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
		log.Printf("error during creating internal exchanges/queues - %v", err)
		return nil
	}

	go func(str *JobStorage, tra *Transport) {
		tickResult := make(chan error)

		for {
			go processTick(str, tra, tickResult)

			err = <-tickResult
			if err != nil {
				log.Printf("error during processing tick - %v", err)
			}

			time.Sleep(time.Second)
		}
	}(str, tra)

	go processJobStatus(tra, str)

	return &scheduler
}

func processTick(str *JobStorage, tr *Transport, tickResult chan<- error) {
	schedules, err := getSchedulesReadyToStart(str)
	if err != nil {
		tickResult <- err
	}

	for _, schedule := range schedules {
		jobResult := make(chan error)
		go schedule.Job.Start(tr, jobResult)

		jobStartResult := <-jobResult
		if jobStartResult != nil {
			tickResult <- jobStartResult
			return
		}

		schedule.Attempt++
		schedule.Status = Scheduled

		err = str.UpdateSchedule(schedule)
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

			schedule, err := storage.GetScheduleByJobSlug(jobStatus.JobSlug)
			if err != nil {
				return err
			}

			if schedule == nil {
				log.Printf("received status for unregistered job %s", jobStatus.JobSlug)
				return errors.New("received status for unregistered job")
			}

			if schedule.LastExecutionDate == nil {
				now := time.Now()
				schedule.LastExecutionDate = &now
			}

			log.Printf("received job status %v\n", jobStatus)
			switch jobStatus.Status {
			case string(Processing):
				{
					schedule.Status = Processing
				}
			case string(Failed):
				{
					schedule.Status = Failed
					if schedule.RetryPolicy != (RetryPolicy{}) {

						var next time.Time
						if schedule.NextExecutionDate != nil {
							next, err = schedule.RetryPolicy.GetNextExecutionTime(*schedule.NextExecutionDate, schedule.Attempt)
							if err != nil {
								return err
							}
						} else {
							next, err = schedule.RetryPolicy.GetNextExecutionTime(*schedule.LastExecutionDate, schedule.Attempt)
							if err != nil {
								return err
							}
						}

						if next == (time.Time{}) {
							schedule.NextExecutionDate = nil
							log.Printf("schedule failed after retrying%v\n", next)
						} else {
							schedule.NextExecutionDate = &next
							log.Printf("schedule retrying at %v\n", next)
						}
					}
				}
			case string(Finished):
				{
					schedule.Status = Finished
				}
			}

			err = storage.UpdateSchedule(schedule)
			if err != nil {
				return err
			}

			return nil
		})

	if err != nil {
		log.Printf("error during statusing jobs - %v\n", err)
		return
	}
}

func getSchedulesReadyToStart(str *JobStorage) ([]*Schedule, error) {
	readySchedules, err := str.GetSchedulesWithStatus(New)
	if err != nil {
		log.Printf("error getting new jobs - %v\n", err)
		return nil, err
	}

	rescheduleReady, err := str.GetSchedulesReadyToReschedule()
	if err != nil {
		log.Printf("error getting new jobs - %v\n", err)
		return nil, err
	}

	readySchedules = append(readySchedules, rescheduleReady...)

	return readySchedules, nil
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
