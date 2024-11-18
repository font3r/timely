package scheduler

import (
	"testing"
	"time"
)

func TestNewSchedule(t *testing.T) {
	net := getStubDate().Round(time.Second)
	expected := Schedule{
		Id:          [16]byte{},
		GroupId:     [16]byte{},
		Description: "description",
		Frequency:   "once",
		Status:      Waiting,
		RetryPolicy: RetryPolicy{
			Strategy: Constant,
			Interval: "2s",
			Count:    5,
		},
		Configuration: ScheduleConfiguration{
			TransportType: "http",
			Url:           "http://example.com",
		},
		LastExecutionDate: nil,
		NextExecutionDate: &net,
		Job: &Job{
			Slug: "slug",
			Data: nil,
		},
	}

	rp, _ := NewRetryPolicy(Constant, 5, "2s")
	sc := ScheduleConfiguration{
		TransportType: "http",
		Url:           "http://example.com",
	}

	s := NewSchedule("description", "once", "slug", nil, rp, sc, nil, getStubDate)

	expected.Id = s.Id
	expected.GroupId = s.GroupId
	expected.Job.Id = s.Job.Id

	if *s.Job != *expected.Job {
		t.Errorf("expect result %+v, got %+v", expected.Job, s.Job)
	}

	if *expected.NextExecutionDate != *s.NextExecutionDate {
		t.Errorf("expect result %+v, got %+v", *expected.NextExecutionDate, *s.NextExecutionDate)
	}

	expected.NextExecutionDate = s.NextExecutionDate
	expected.Job = s.Job
	if s != expected {
		t.Errorf("expect result %+v, got %+v", expected, s)
	}
}

func TestStart(t *testing.T) {
	s := NewSchedule("", "once", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getStubDate)

	s.Start(getStubDate)

	expected := getStubDate().Round(time.Second)
	if *s.LastExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *s.LastExecutionDate)
	}

	if s.Status != Scheduled {
		t.Errorf("expect result %+v, got %+v", Scheduled, s.Status)
	}
}

func TestNewScheduleWithSpecifiedScheduleStart(t *testing.T) {
	scheduleStart := getStubDate()
	s := NewSchedule("", "once", "", nil, RetryPolicy{}, ScheduleConfiguration{}, &scheduleStart, getStubDate)

	if *s.NextExecutionDate != scheduleStart {
		t.Errorf("expect result %+v, got %+v", scheduleStart, *s.NextExecutionDate)
	}
}

func TestNewScheduleWithOnceFrequency(t *testing.T) {
	s := NewSchedule("", "once", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getStubDate)

	expected := getStubDate().Round(time.Second)

	if *s.NextExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *s.NextExecutionDate)
	}
}

func TestNewScheduleWithCronFrequency(t *testing.T) {
	s := NewSchedule("", "*/10 * * * * *", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getStubDate)

	expected := getStubDate().Add(time.Second * 10).Round(time.Second)

	if *s.NextExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *s.NextExecutionDate)
	}
}

func TestSucceedWithoutNextExecution(t *testing.T) {
	s := NewSchedule("", "once", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getStubDate)

	s.Succeed(getStubDate)

	if s.NextExecutionDate != nil {
		t.Errorf("expect result %+v, got %+v", nil, *s.NextExecutionDate)
	}

	if s.Status != Finished {
		t.Errorf("expect result %+v, got %+v", Finished, s.Status)
	}
}

func TestSucceedWithValidNextExecution(t *testing.T) {
	s := NewSchedule("", "*/10 * * * * *", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getStubDate)

	s.Succeed(getStubDate)

	expected := getStubDate().Add(time.Second * 10).Round(time.Second)

	if *s.NextExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *s.NextExecutionDate)
	}

	if s.Status != Waiting {
		t.Errorf("expect result %+v, got %+v", Waiting, s.Status)
	}
}

func TestFailedWithRetryPolicyWithPossibleRetryDate(t *testing.T) {
	rp, _ := NewRetryPolicy(Constant, 3, "15s")
	s := NewSchedule("", "once", "", nil, rp, ScheduleConfiguration{}, nil, getStubDate)

	s.Failed(2, getStubDate)

	expected := getStubDate().Add(time.Second * 15).Round(time.Second)

	if *s.NextExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *s.NextExecutionDate)
	}

	if s.Status != Waiting {
		t.Errorf("expect result %+v, got %+v", Waiting, s.Status)
	}
}

func TestFailedWithRetryPolicyWithoutPossibleRetryDate(t *testing.T) {
	rp, _ := NewRetryPolicy(Constant, 3, "1s")
	s := NewSchedule("", "once", "", nil, rp, ScheduleConfiguration{}, nil, getStubDate)

	s.Failed(100, getStubDate)

	if s.NextExecutionDate != nil {
		t.Errorf("expect result %+v, got %+v", nil, *s.NextExecutionDate)
	}

	if s.Status != Finished {
		t.Errorf("expect result %+v, got %+v", Finished, s.Status)
	}
}

func TestFailedWithoutRetryPolicyWithoutNextExecutionTime(t *testing.T) {
	s := NewSchedule("", "once", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getStubDate)

	s.Failed(1, getStubDate)

	if s.NextExecutionDate != nil {
		t.Errorf("expect result %+v, got %+v", nil, *s.NextExecutionDate)
	}

	if s.Status != Finished {
		t.Errorf("expect result %+v, got %+v", Finished, s.Status)
	}
}

func TestFailedWithoutRetryPolicyWithNextExecutionTime(t *testing.T) {
	s := NewSchedule("", "*/10 * * * * *", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getStubDate)

	s.Failed(1, getStubDate)

	expected := getStubDate().Add(time.Second * 10).Round(time.Second)

	if *s.NextExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *s.NextExecutionDate)
	}

	if s.Status != Waiting {
		t.Errorf("expect result %+v, got %+v", Waiting, s.Status)
	}
}

func getStubDate() time.Time {
	d, err := time.Parse(time.RFC3339, "2000-01-01T10:30:00+01:00")
	if err != nil {
		panic(err)
	}

	return d
}
