package scheduler

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewRun(t *testing.T) {
	groupId, scheduleId := uuid.New(), uuid.New()

	expected := JobRun{
		Id:         [16]byte{},
		GroupId:    groupId,
		ScheduleId: scheduleId,
		Status:     JobWaiting,
		Reason:     nil,
		StartDate:  getFakeDate().Round(time.Second),
		EndDate:    nil,
	}

	jr := NewJobRun(scheduleId, groupId, getFakeDate)

	expected.Id = jr.Id

	if jr != expected {
		t.Errorf("expect result %+v, got %+v", expected, jr)
	}
}

func TestSucceed(t *testing.T) {
	jr := NewJobRun(uuid.New(), uuid.New(), getFakeDate)

	jr.Succeed(getFakeDate)

	expected := getFakeDate().Round(time.Second)
	if *jr.EndDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *jr.EndDate)
	}

	if jr.Status != JobSucceed {
		t.Errorf("expect result %+v, got %+v", JobSucceed, jr.Status)
	}
}

func TestFailed(t *testing.T) {
	jr := NewJobRun(uuid.New(), uuid.New(), getFakeDate)

	jr.Failed("test fail reason", getFakeDate)

	expected := getFakeDate().Round(time.Second)
	if *jr.EndDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *jr.EndDate)
	}

	if jr.Status != JobFailed {
		t.Errorf("expect result %+v, got %+v", JobFailed, jr.Status)
	}

	if *jr.Reason != "test fail reason" {
		t.Errorf("expect result %+v, got %+v", "test fail reason", jr.Status)
	}
}
