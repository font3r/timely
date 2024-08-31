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
			id:        uuid.Nil,
			expectErr: "storage error",
		},
		"schedule_with_id_does_not_exist": {
			id:        uuid.New(),
			expectErr: ErrScheduleNotFound.Error(),
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

type dependencies struct {
	storageDriver *storageDriverMock
	handler       GetScheduleHandler
}

func getDeps() dependencies {
	storageDriver := &storageDriverMock{
		Schedules: []scheduler.Schedule{
			{
				Id:                uuid.UUID{},
				Description:       "",
				Frequency:         "",
				Status:            "",
				Attempt:           0,
				RetryPolicy:       scheduler.RetryPolicy{},
				LastExecutionDate: nil,
				NextExecutionDate: nil,
				Job:               nil,
			},
		},
	}

	return dependencies{
		storageDriver: storageDriver,
		handler:       GetScheduleHandler{Storage: storageDriver},
	}
}

type storageDriverMock struct {
	Schedules []scheduler.Schedule
}

func (s storageDriverMock) GetScheduleById(ctx context.Context, id uuid.UUID) (*scheduler.Schedule, error) {
	if id == uuid.Nil {
		return nil, errors.New("storage error")
	}

	return &scheduler.Schedule{}, ErrScheduleNotFound
}

func (s storageDriverMock) GetScheduleByJobSlug(slug string) (*scheduler.Schedule, error) {
	panic("implement me")
}

func (s storageDriverMock) GetSchedulesWithStatus(status scheduler.ScheduleStatus) ([]*scheduler.Schedule, error) {
	panic("implement me")
}

func (s storageDriverMock) GetSchedulesReadyToReschedule() ([]*scheduler.Schedule, error) {
	panic("implement me")
}

func (s storageDriverMock) GetAll() ([]*scheduler.Schedule, error) {
	panic("implement me")
}

func (s storageDriverMock) Add(schedule scheduler.Schedule) error {
	panic("implement me")
}

func (s storageDriverMock) DeleteScheduleById(id uuid.UUID) error {
	panic("implement me")
}

func (s storageDriverMock) UpdateSchedule(schedule *scheduler.Schedule) error {
	panic("implement me")
}
