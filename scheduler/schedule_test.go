package scheduler

import (
	"testing"
	"time"
)

func TestNewSchedule(t *testing.T) {
	net := getFakeDate().Round(time.Second)
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

	s := NewSchedule("description", "once", "slug", nil, rp, sc, nil, getFakeDate)

	// fill test
	expected.Id = s.Id
	expected.GroupId = s.GroupId
	expected.Job.Id = s.Job.Id

	if *s.Job != *expected.Job {
		t.Errorf("expect result %+v, got %+v", expected.Job, s.Job)
	}

	expected.Job = s.Job
	if s != expected {
		t.Errorf("expect result %+v, got %+v", expected, s)
	}
}

func TestNewScheduleWithSpecifiedScheduleStart(t *testing.T) {
	scheduleStart := getFakeDate()
	s := NewSchedule("", "", "", nil, RetryPolicy{}, ScheduleConfiguration{}, &scheduleStart, getFakeDate)

	if *s.NextExecutionDate != scheduleStart {
		t.Errorf("expect result %+v, got %+v", scheduleStart, s.NextExecutionDate)
	}
}

func TestNewScheduleWithOnceFrequency(t *testing.T) {
	s := NewSchedule("", "once", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getFakeDate)

	expected := getFakeDate().Round(time.Second)

	if *s.NextExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, s.NextExecutionDate)
	}
}

func TestNewScheduleWithCronFrequency(t *testing.T) {
	s := NewSchedule("", "*/10 * * * * *", "", nil, RetryPolicy{}, ScheduleConfiguration{}, nil, getFakeDate)

	expected := getFakeDate().Add(time.Second * 10).Round(time.Second)

	if *s.NextExecutionDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, s.NextExecutionDate)
	}
}

func getFakeDate() time.Time {
	d, err := time.Parse(time.RFC3339, "2000-01-01T10:30:00+01:00")
	if err != nil {
		panic(err)
	}

	return d
}
