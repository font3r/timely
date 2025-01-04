package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
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
	args := os.Args[1:]

	// TODO: Parallel
	jobsAmount := 1000
	timelyAddress, _ := url.Parse("http://localhost:7468/api/v1/schedules")

	if slices.Contains(args, "--sync") {
		createSyncSchedule(jobsAmount, timelyAddress)
	}

	if slices.Contains(args, "--async") {
		createAsyncSchedules(jobsAmount, timelyAddress)
	}
}

func createSyncSchedule(n int, timelyAddress *url.URL) {
	jobHandlerAddress, _ := url.Parse("http://localhost:5001/api/v1/jobs/process-test-job")
	jobName := "test-stress-sync-job-%i"

	syncSchedule := CreateScheduleCommand{
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

	for i := range make([]int, n) {
		syncSchedule.Job.Slug = strings.Replace(jobName, "%i", strconv.Itoa(i), 1)
		syncSchedule.Description = strings.Replace(jobName, "%i", strconv.Itoa(i), 1)

		json, _ := json.Marshal(syncSchedule)
		_, err := http.Post(timelyAddress.String(), "application/json", bytes.NewBuffer(json))

		if err != nil {
			fmt.Printf("fail during creating job %s - %v\n", syncSchedule.Job.Slug, err)
		}

		fmt.Printf("created job %s\n", syncSchedule.Job.Slug)
	}
}

func createAsyncSchedules(n int, timelyAddress *url.URL) {
	jobName := "qtest-stress-async-job-%i"

	asyncSchedule := CreateScheduleCommand{
		Description: "replace-me",
		Frequency:   "*/15 * * * * *",
		Job: JobConfiguration{
			Slug: "replace-me",
		},
		Configuration: ScheduleConfiguration{
			TransportType: "rabbitmq",
		},
	}

	for i := range make([]int, n) {
		asyncSchedule.Job.Slug = strings.Replace(jobName, "%i", strconv.Itoa(i), 1)
		asyncSchedule.Description = strings.Replace(jobName, "%i", strconv.Itoa(i), 1)

		json, _ := json.Marshal(asyncSchedule)
		_, err := http.Post(timelyAddress.String(), "application/json", bytes.NewBuffer(json))

		if err != nil {
			fmt.Printf("fail during creating job %s - %v\n", asyncSchedule.Job.Slug, err)
		}

		fmt.Printf("created job %s\n", asyncSchedule.Job.Slug)
	}
}
