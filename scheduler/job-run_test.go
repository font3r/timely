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
