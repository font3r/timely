package test_job_handler

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"
	"timely/scheduler"
)

type JobStatusEvent struct {
	JobSlug string `json:"jobSlug"`
	Status  string `json:"status"`
	Seq     int16  `json:"seq"`
}

func Start() {
	log.Println("starting example-service ...")

	tra, err := scheduler.NewConnection(1)
	if err != nil {
		panic(fmt.Sprintf("create transport error %s", err))
	}

	for _, jobSlug := range []string{"test-example-job-1", "test-example-job-2"} {
		log.Printf("registered %s", jobSlug)
		go tra.Subscribe(jobSlug, func(jobSlug string, message []byte) error {
			log.Printf("requested job start with slug %s", jobSlug)

			err = processMockJob(tra, jobSlug)
			if err != nil {
				log.Printf("error during job processing %s", err)
			}

			return nil
		})
	}
}

func processMockJob(tra *scheduler.Transport, jobSlug string) error {
	var seq int16 = 0

	for i := 0; i < 10; i++ {

		if rand.Intn(4) == 1 { // just random 25% failure rate for testing
			err := tra.Publish(string(scheduler.ExchangeJobStatus),
				string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
					JobSlug: jobSlug,
					Status:  "failed",
					Seq:     int16(seq),
				})

			if err != nil {
				log.Printf("error during job status %s", err)
			}

			return errors.New("job failure")
		}

		err := tra.Publish(string(scheduler.ExchangeJobStatus),
			string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
				JobSlug: jobSlug,
				Status:  "processing",
				Seq:     int16(seq),
			})

		if err != nil {
			log.Printf("error during job status %s", err)
		}

		seq++
		time.Sleep(time.Second)
	}

	err := tra.Publish(string(scheduler.ExchangeJobStatus),
		string(scheduler.RoutingKeyJobStatus), JobStatusEvent{
			JobSlug: jobSlug,
			Status:  "finished",
			Seq:     seq,
		})

	if err != nil {
		log.Printf("error during job status %s", err)
	}

	return nil
}
