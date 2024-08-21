package test_job_handler

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"math/rand"
	"time"
	"timely/scheduler"
)

type JobStatusEvent struct {
	JobSlug string `json:"jobSlug"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Seq     int16  `json:"seq"`
}

func Start() {
	log.Println("test-app: starting ...")

	tra, err := scheduler.NewConnection(viper.GetString("transport.rabbitmq.connectionString"))
	if err != nil {
		panic(fmt.Sprintf("test-app: create transport error %s", err))
	}

	for _, jobSlug := range []string{"test-example-job-1", "test-example-job-2"} {
		log.Printf("test-app: test app registered %s", jobSlug)
		go func(jobSlug string) {
			err = tra.Subscribe(jobSlug, func(message []byte) error {
				log.Printf("test-app: requested job start with slug %s", jobSlug)

				err = processMockJob(tra, jobSlug)
				if err != nil {
					log.Printf("test-app: error during job processing %s", err)
				}

				return nil
			})

			if err != nil {
				log.Printf("test-app: error during subscribe for job %s", jobSlug)
				return
			}
		}(jobSlug)
	}
}

func processMockJob(tra *scheduler.Transport, jobSlug string) error {
	var seq int16 = 0

	for i := 0; i < 10; i++ {
		if rand.Intn(10) <= -1 { // random failure rate for testing
			err := tra.Publish(string(scheduler.ExchangeJobStatus),
				string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
					JobSlug: jobSlug,
					Status:  "failed",
					Reason:  "failed due to jitter error",
					Seq:     seq,
				})

			if err != nil {
				log.Printf("test-app: error during publishing job status %s", err)
			}

			return errors.New("jitter job failure")
		}

		err := tra.Publish(string(scheduler.ExchangeJobStatus),
			string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
				JobSlug: jobSlug,
				Status:  "processing",
				Seq:     seq,
			})

		if err != nil {
			log.Printf("test-app: error during publishing job status %s", err)
		}

		seq++
		time.Sleep(time.Second)
	}

	err := tra.Publish(string(scheduler.ExchangeJobStatus),
		string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
			JobSlug: jobSlug,
			Status:  "finished",
			Reason:  "success",
			Seq:     seq,
		})

	if err != nil {
		log.Printf("test-app: error publishing during job status %s", err)
	}

	return nil
}
