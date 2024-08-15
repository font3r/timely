package queries

import (
	"timely/scheduler"
)

func GetJobs(str *scheduler.JobStorage) ([]JobDto, error) {
	jobs, err := str.GetAll()

	if err != nil {
		return []JobDto{}, err
	}

	jobDtos := make([]JobDto, 0, len(jobs))

	for _, job := range jobs {
		jobDtos = append(jobDtos, JobDto{
			Id:                job.Id,
			Slug:              job.Slug,
			Description:       job.Description,
			Status:            job.Status,
			Reason:            job.Reason,
			Schedule:          ScheduleDto{Frequency: job.Schedule.Frequency},
			LastExecutionDate: job.LastExecutionDate,
			NextExecutionDate: job.NextExecutionDate,
		})
	}

	return jobDtos, nil
}
