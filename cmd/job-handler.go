package test_job_handler

import (
	"fmt"
	"log"
	"time"
	"timely/scheduler/transport"
)

type JobStatusEvent struct {
	JobName string `json:"jobName"`
	Status  string `json:"status"`
}

func Start() {
	tra, err := transport.Create(1)
	if err != nil {
		panic(fmt.Sprintf("create transport error %s", err))
	}

	err = processJob(tra)
	if err != nil {
		log.Printf("error during job processing %s", err)
	}
}

func processJob(tra *transport.Transport) error {
	for i := 0; i < 10; i++ {
		err := tra.Publish("job-status", "job-status", JobStatusEvent{
			JobName: "test-job",
			Status:  "working",
		})

		if err != nil {
			log.Printf("error during job status %s", err)
		}

		log.Println("sent job status event")

		time.Sleep(time.Second * 2)
	}

	err := tra.Publish("job-status", "job-status", JobStatusEvent{
		JobName: "test-job",
		Status:  "finished",
	})

	if err != nil {
		log.Printf("error during job status %s", err)
	}

	return nil
}
