package queries

import (
	"context"
	"errors"
	"testing"
	"timely/scheduler"

	"github.com/google/uuid"
)

func TestGetSchedule(t *testing.T) {
	tests := map[string]struct {
		id uuid.UUID

		expected  ScheduleDetailsDto
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
			_, err := deps.handler.Handle(context.Background(), GetSchedule{ScheduleId: test.id})

			if test.expectErr != "" {
				if test.expectErr != err.Error() {
					t.Errorf("expect error %s, got %s", test.expectErr, err.Error())
				}
			} else {
				//if sch != test.expected {
				//	t.Errorf("expect result %s, got %s", test.expectErr, err.Error())
				//}
			}
		})
	}
}

type dependencies struct {
	storageDriver *storageDriverFake
	handler       GetScheduleHandler
}

func getDeps() dependencies {
	storageDriver := &storageDriverFake{
		Schedules: []scheduler.Schedule{
			{
				Id:                uuid.UUID{},
				Description:       "",
				Frequency:         "",
				Status:            "",
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

type storageDriverFake struct {
	Schedules []scheduler.Schedule
}

func (s storageDriverFake) GetScheduleById(ctx context.Context, id uuid.UUID) (*scheduler.Schedule, error) {
	if id == uuid.Nil {
		return nil, errors.New("storage error")
	}

	return &scheduler.Schedule{}, ErrScheduleNotFound
}

func (s storageDriverFake) GetScheduleByJobSlug(ctx context.Context, slug string) (*scheduler.Schedule, error) {
	panic("implement me")
}

func (s storageDriverFake) GetAwaitingSchedules(ctx context.Context) ([]*scheduler.Schedule, error) {
	panic("implement me")
}

func (s storageDriverFake) GetAll(ctx context.Context) ([]*scheduler.Schedule, error) {
	panic("implement me")
}

func (s storageDriverFake) Add(ctx context.Context, schedule scheduler.Schedule) error {
	panic("implement me")
}

func (s storageDriverFake) DeleteScheduleById(ctx context.Context, id uuid.UUID) error {
	panic("implement me")
}

func (s storageDriverFake) UpdateSchedule(ctx context.Context, schedule scheduler.Schedule) error {
	panic("implement me")
}

func (s storageDriverFake) AddJobRun(ctx context.Context, jobRun scheduler.JobRun) error {
	panic("implement me")
}

func (s storageDriverFake) UpdateJobRun(ctx context.Context, jobRun scheduler.JobRun) error {
	panic("implement me")
}

func (s storageDriverFake) GetJobRun(ctx context.Context, id uuid.UUID) (*scheduler.JobRun, error) {
	panic("implement me")
}

func (s storageDriverFake) GetJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*scheduler.JobRun, error) {
	panic("implement me")
}

func (s storageDriverFake) GetRecentJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*scheduler.JobRun, error) {
	panic("implement me")
}
