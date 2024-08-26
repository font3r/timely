package test_job_handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"
	log "timely/logger"
	"timely/scheduler"

	"github.com/spf13/viper"
)

type JobStatusEvent struct {
	JobSlug string `json:"job_slug"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
}

type ScheduleJobEvent struct {
	Job  string          `json:"job"`
	Data *map[string]any `json:"data"`
}

func Start() {
	log.Logger.Println("starting ...")

	tra, err := scheduler.NewConnection(viper.GetString("transport.rabbitmq.connectionString"))
	if err != nil {
		panic(fmt.Sprintf("test-app: create transport error - %s", err))
	}

	for _, jobSlug := range []string{"process-user-notifications"} {
		log.Logger.Printf("test app registered %s", jobSlug)
		go func(jobSlug string) {
			err = tra.Subscribe(jobSlug, func(message []byte) error {
				log.Logger.Printf("requested job start with slug %s", jobSlug)

				var event ScheduleJobEvent
				err = json.Unmarshal(message, &event)
				if err != nil {
					return err
				}

				if event.Data == nil {
					log.Logger.Printf("job slug %s", jobSlug)
				} else {
					log.Logger.Printf("job data slug %s with data %+v", jobSlug, *event.Data)
				}

				err = processMockJob(tra, jobSlug)
				if err != nil {
					log.Logger.Printf("error during job processing - %s", err)
				}

				return nil
			})

			if err != nil {
				log.Logger.Printf("error during subscribe for job %s - %s", jobSlug, err)
				return
			}
		}(jobSlug)
	}
}

func processMockJob(tra *scheduler.Transport, jobSlug string) error {
	var seq int16 = 0

	for i := 0; i < 10; i++ {
		if rand.Intn(10) < 1 { // random 10% failure rate for testing
			err := tra.Publish(string(scheduler.ExchangeJobStatus),
				string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
					JobSlug: jobSlug,
					Status:  "failed",
					Reason:  "failed due to jitter error",
				})

			if err != nil {
				log.Logger.Printf("error during publishing job status %s", err)
			}

			return errors.New("jitter job failure")
		}

		err := tra.Publish(string(scheduler.ExchangeJobStatus),
			string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
				JobSlug: jobSlug,
				Status:  "processing",
			})

		if err != nil {
			log.Logger.Printf("error during publishing job status %s", err)
		}

		seq++
		time.Sleep(time.Second)
	}

	err := tra.Publish(string(scheduler.ExchangeJobStatus),
		string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
			JobSlug: jobSlug,
			Status:  "finished",
			Reason:  "success",
		})

	if err != nil {
		log.Logger.Printf("error publishing during job status %s", err)
	}

	return nil
}
