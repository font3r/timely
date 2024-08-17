package queries

import (
	"timely/scheduler"
)

func GetSchedules(str *scheduler.JobStorage) ([]ScheduleDto, error) {
	schedules, err := str.GetAll()

	if err != nil {
		return []ScheduleDto{}, err
	}

	schedulesDto := make([]ScheduleDto, 0, len(schedules))

	for _, schedule := range schedules {
		schedulesDto = append(schedulesDto, ScheduleDto{
			Id:                schedule.Id,
			Description:       schedule.Description,
			Frequency:         schedule.Frequency,
			LastExecutionDate: schedule.LastExecutionDate,
			NextExecutionDate: schedule.NextExecutionDate,
			Job: JobDto{
				Id:     schedule.Job.Id,
				Slug:   schedule.Job.Slug,
				Status: schedule.Job.Status,
				Reason: schedule.Job.Reason,
			},
		})
	}

	return schedulesDto, nil
}
