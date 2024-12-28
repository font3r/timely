package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"timely/scheduler"
)

type CreateScheduleCommand struct {
	Description   string                   `json:"description"`
	Frequency     string                   `json:"frequency"`
	Job           JobConfiguration         `json:"job"`
	RetryPolicy   RetryPolicyConfiguration `json:"retryPolicy"`
	ScheduleStart *time.Time               `json:"scheduleStart"`
	Configuration ScheduleConfiguration    `json:"configuration"`
}

type JobConfiguration struct {
	Slug string          `json:"slug"`
	Data *map[string]any `json:"data"`
}

type RetryPolicyConfiguration struct {
	Strategy scheduler.StrategyType `json:"strategy"`
	Count    int                    `json:"count"`
	Interval string                 `json:"interval"`
}

type ScheduleConfiguration struct {
	TransportType scheduler.TransportType `json:"transportType"`
	Url           string                  `json:"url"`
}

func main() {
	// TODO: Parallel
	jobsAmount := 10000
	timelyAddress, _ := url.Parse("http://localhost:7468/api/v1/schedules")
	jobHandlerAddress, _ := url.Parse("http://localhost:5001/api/v1/jobs/process-user-notifications")
	jobName := "test-stress-job-%i"

	schedule := CreateScheduleCommand{
		Description: "replace-me",
		Frequency:   "once",
		Job: JobConfiguration{
			Slug: "replace-me",
		},
		RetryPolicy: RetryPolicyConfiguration{
			Strategy: "constant",
			Count:    5,
			Interval: "10s",
		},
		Configuration: ScheduleConfiguration{
			TransportType: "http",
			Url:           jobHandlerAddress.String(),
		},
	}

	for i := range make([]int, jobsAmount) {
		schedule.Job.Slug = strings.Replace(jobName, "%i", strconv.Itoa(i), 1)
		schedule.Description = strings.Replace(jobName, "%i", strconv.Itoa(i), 1)

		json, _ := json.Marshal(schedule)
		_, err := http.Post(timelyAddress.String(), "application/json", bytes.NewBuffer(json))

		if err != nil {
			fmt.Printf("fail during creating job %s - %v\n", schedule.Job.Slug, err)
		}

		fmt.Printf("created job %s\n", schedule.Job.Slug)
	}
}
