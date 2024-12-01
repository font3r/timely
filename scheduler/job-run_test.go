package scheduler

import (
	"testing"

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
		StartDate:  getStubDate(),
		EndDate:    nil,
	}

	jr := NewJobRun(scheduleId, groupId, getStubDate)

	expected.Id = jr.Id

	if jr != expected {
		t.Errorf("expect result %+v, got %+v", expected, jr)
	}
}

func TestSucceed(t *testing.T) {
	jr := NewJobRun(uuid.New(), uuid.New(), getStubDate)

	jr.Succeed(getStubDate)

	expected := getStubDate()
	if *jr.EndDate != expected {
		t.Errorf("expect result %+v, got %+v", expected, *jr.EndDate)
	}

	if jr.Status != JobSucceed {
		t.Errorf("expect result %+v, got %+v", JobSucceed, jr.Status)
	}
}

func TestFailed(t *testing.T) {
	jr := NewJobRun(uuid.New(), uuid.New(), getStubDate)

	jr.Failed("test fail reason", getStubDate)

	expected := getStubDate()
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
