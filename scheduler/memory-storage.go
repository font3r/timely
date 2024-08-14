package scheduler

//
//import (
//	"time"
//
//	"github.com/google/uuid"
//)
//
//func Create(slug string, description string, cron string) Job {
//	return Job{
//		Id:                uuid.New(),
//		Slug:              slug,
//		Description:       description,
//		Cron:              cron,
//		Status:            New,
//		LastExecutionDate: time.Time{},
//		NextExecutionDate: time.Time{},
//	}
//}
//
//type JobStorage struct {
//	jobs []*Job
//}
//
//func (s *JobStorage) GetById(id uuid.UUID) *Job {
//	for _, j := range s.jobs {
//		if j.Id == id {
//			return j
//		}
//	}
//
//	return nil
//}
//
//func (s *JobStorage) GetBySlug(slug string) *Job {
//	for _, j := range s.jobs {
//		if j.Slug == slug {
//			return j
//		}
//	}
//
//	return nil
//}
//
//func (s *JobStorage) GetPending() (jobs []*Job) {
//	for _, j := range s.jobs {
//		if j.Status == New {
//			jobs = append(jobs, j)
//		}
//	}
//
//	return jobs
//}
//
//func (s *JobStorage) GetAll() []*Job {
//	return s.jobs
//}
//
//func (s *JobStorage) Add(job Job) {
//	s.jobs = append(s.jobs, &job)
//}
