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

func main() {
	// TODO: Parallel
	jobsAmount := 1000
	timelyAddress, _ := url.Parse("http://localhost:7468/api/v1/schedules")

	createTestSchedules(jobsAmount, timelyAddress)
}

func createTestSchedules(n int, timelyAddress *url.URL) {
	jobName := "qtest-stress-async-job-%i"

	asyncSchedule := CreateScheduleCommand{
		Description: "replace-me",
		Frequency:   "*/15 * * * * *",
		Job: JobConfiguration{
			Slug: "replace-me",
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
