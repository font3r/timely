package queries

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"testing"
	"timely/scheduler"
)

func TestGetSchedule(t *testing.T) {
	tests := map[string]struct {
		id uuid.UUID

		expected  ScheduleDto
		expectErr string
	}{
		"storage_returns_error": {
			expectErr: "storage error",
		},
		"schedule_with_id_does_not_exist": {
			expected: ScheduleDto{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			deps := getDeps()
			sch, err := deps.handler.Handle(context.Background(), GetSchedule{ScheduleId: test.id})

			if test.expectErr != "" {
				if test.expectErr != err.Error() {
					t.Errorf("expect error %s, got %s", test.expectErr, err.Error())
				}
			} else {
				if sch != test.expected {
					t.Errorf("expect result %s, got %s", test.expectErr, err.Error())
				}
			}
		})
	}
}

type storageDriverMock struct {
}

func (s storageDriverMock) GetScheduleById(ctx context.Context, id uuid.UUID) (*scheduler.Schedule, error) {
	return &scheduler.Schedule{}, errors.New("storage error")
}

func (s storageDriverMock) GetScheduleByJobSlug(slug string) (*scheduler.Schedule, error) {
	//TODO implement me
	panic("implement me")
}

func (s storageDriverMock) GetSchedulesWithStatus(status scheduler.ScheduleStatus) ([]*scheduler.Schedule, error) {
	//TODO implement me
	panic("implement me")
}

func (s storageDriverMock) GetSchedulesReadyToReschedule() ([]*scheduler.Schedule, error) {
	//TODO implement me
	panic("implement me")
}

func (s storageDriverMock) GetAll() ([]*scheduler.Schedule, error) {
	//TODO implement me
	panic("implement me")
}

func (s storageDriverMock) Add(schedule scheduler.Schedule) error {
	//TODO implement me
	panic("implement me")
}

func (s storageDriverMock) DeleteScheduleById(id uuid.UUID) error {
	//TODO implement me
	panic("implement me")
}

func (s storageDriverMock) UpdateSchedule(schedule *scheduler.Schedule) error {
	//TODO implement me
	panic("implement me")
}

type dependencies struct {
	storageDriver *storageDriverMock
	handler       GetScheduleHandler
}

func getDeps() dependencies {
	storageDriver := &storageDriverMock{}

	return dependencies{
		storageDriver: storageDriver,
		handler:       GetScheduleHandler{Storage: storageDriver},
	}
}
