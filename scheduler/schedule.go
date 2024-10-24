package scheduler

import (
	"time"
	log "timely/logger"

	"github.com/google/uuid"
)

type ScheduleStatus string

const (
	Waiting   ScheduleStatus = "waiting"   // waiting to schedule next job
	Scheduled ScheduleStatus = "scheduled" // job scheduled
	Succeed   ScheduleStatus = "succeed"   // successfully processed
	Failed    ScheduleStatus = "failed"    // error during processing
)

type Schedule struct {
	Id                uuid.UUID
	Description       string
	Frequency         string
	Status            ScheduleStatus
	Attempt           int
	RetryPolicy       RetryPolicy
	LastExecutionDate *time.Time
	NextExecutionDate *time.Time
	Job               *Job
}

type ScheduleJobEvent struct {
	ScheduleId uuid.UUID       `json:"schedule_id"`
	JobRunId   uuid.UUID       `json:"job_run_id"`
	Job        string          `json:"job"`
	Data       *map[string]any `json:"data"`
}

func NewSchedule(description, frequency, slug string, data *map[string]any,
	policy RetryPolicy, scheduleStart *time.Time) Schedule {

	execution := getFirstExecution(frequency, scheduleStart)

	return Schedule{
		Id:                uuid.New(),
		Description:       description,
		Frequency:         frequency,
		Status:            Waiting,
		Attempt:           0,
		RetryPolicy:       policy,
		LastExecutionDate: nil,
		NextExecutionDate: &execution,
		Job: &Job{
			Id:   uuid.New(),
			Slug: slug,
			Data: data,
		},
	}
}

func getFirstExecution(frequency string, scheduleStart *time.Time) time.Time {
	if scheduleStart != nil {
		return *scheduleStart
	}

	if frequency == string(Once) {
		return time.Now().Round(time.Second)
	}

	sch, _ := CronParser.Parse(frequency)

	return sch.Next(time.Now().Round(time.Second))
}

func (s *Schedule) Start(runId uuid.UUID, transport AsyncTransportDriver, result chan<- error) {
	err := transport.BindQueue(s.Job.Slug, string(ExchangeJobSchedule), s.Job.Slug)
	if err != nil {
		result <- err
		return
	}

	err = transport.Publish(string(ExchangeJobSchedule), s.Job.Slug,
		ScheduleJobEvent{
			ScheduleId: s.Id,
			JobRunId:   runId,
			Job:        s.Job.Slug,
			Data:       s.Job.Data,
		})

	if err != nil {
		log.Logger.Printf("failed to start job %v", err)

		result <- err
		return
	}

	s.Attempt++
	s.Status = Scheduled

	now := time.Now().Round(time.Second)
	s.LastExecutionDate = &now

	log.Logger.Printf("scheduled job %s/%s, run %s", s.Job.Id, s.Job.Slug, runId)
	result <- nil
}

func (s *Schedule) Failed() error {
	s.Status = Failed

	if s.RetryPolicy == (RetryPolicy{}) {
		s.NextExecutionDate = nil
		return nil
	}

	var next time.Time
	var err error

	if s.NextExecutionDate != nil {
		next, err = s.RetryPolicy.GetNextExecutionTime(*s.NextExecutionDate, s.Attempt)
	} else {
		next, err = s.RetryPolicy.GetNextExecutionTime(time.Now(), s.Attempt)
	}

	if err != nil {
		return err
	}

	if next == (time.Time{}) {
		s.NextExecutionDate = nil
		log.Logger.Println("schedule failed after retrying")
	} else {
		s.NextExecutionDate = &next
		log.Logger.Printf("schedule retrying at %v\n", next)
	}

	return nil
}

func (s *Schedule) Succeed() {
	if s.Frequency == string(Once) {
		s.NextExecutionDate = nil
		s.Status = Succeed
		return
	}

	sch, _ := CronParser.Parse(s.Frequency)
	nextExec := sch.Next(time.Now().Round(time.Second))

	s.NextExecutionDate = &nextExec
	s.Status = Waiting
}
