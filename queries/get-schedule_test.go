package queries

import (
	"context"
	"errors"
	"reflect"
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
			id:        uuid.MustParse("0f54d8c5-6690-4fa4-9489-f7ec575140bd"),
			expectErr: ErrScheduleNotFound.Error(),
		},
		"schedule_with_id_exists_should_be_returned": {
			id: uuid.MustParse("ad39c83f-59b1-4f01-8c6d-0196ce59127f"),
			expected: ScheduleDetailsDto{
				Id: uuid.MustParse("ad39c83f-59b1-4f01-8c6d-0196ce59127f"),
			},
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
				if reflect.DeepEqual(sch, test.expected) {
					t.Errorf("expect result %s, got %s", test.expectErr, err.Error())
				}
			}
		})
	}
}

type dependencies struct {
	storageDriver *storageDriverFake
	handler       GetScheduleHandler
}

type storageDriverFake struct {
	schedules map[string]*scheduler.Schedule
	jobRuns   map[string][]*scheduler.JobRun
}

func getDeps() dependencies {
	storageDriver := &storageDriverFake{
		schedules: map[string]*scheduler.Schedule{
			"ad39c83f-59b1-4f01-8c6d-0196ce59127f": {
				Id: uuid.MustParse("ad39c83f-59b1-4f01-8c6d-0196ce59127f"),
				Job: &scheduler.Job{
					Id:   uuid.New(),
					Slug: "test-slug",
					Data: nil,
				},
			},
		},
		jobRuns: map[string][]*scheduler.JobRun{
			"ad39c83f-59b1-4f01-8c6d-0196ce59127f": {
				{
					Id:         uuid.New(),
					ScheduleId: uuid.MustParse("ad39c83f-59b1-4f01-8c6d-0196ce59127f"),
				},
			},
		},
	}

	return dependencies{
		storageDriver: storageDriver,
		handler:       GetScheduleHandler{Storage: storageDriver},
	}
}

func (s storageDriverFake) GetScheduleById(ctx context.Context, id uuid.UUID) (*scheduler.Schedule, error) {
	if id == uuid.Nil {
		return nil, errors.New("storage error")
	}

	v, exists := s.schedules[id.String()]
	if !exists {
		return nil, ErrScheduleNotFound
	}

	return v, nil
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

func (s storageDriverFake) GetJobRunGroup(ctx context.Context, scheduleId uuid.UUID, groupId uuid.UUID) ([]*scheduler.JobRun, error) {
	panic("implement me")
}

func (s storageDriverFake) GetJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*scheduler.JobRun, error) {
	panic("implement me")
}

func (s storageDriverFake) GetRecentJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*scheduler.JobRun, error) {
	v, exists := s.jobRuns[scheduleId.String()]
	if !exists {
		return nil, ErrScheduleNotFound
	}

	return v, nil
}
