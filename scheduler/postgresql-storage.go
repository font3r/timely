package scheduler

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	UniqueConstraintViolation = "23505"
)

var (
	ErrUniqueConstraintViolation = &Error{Code: "UNIQUE_CONSTRAINT_VIOLATION", Message: "Unique constraint violation"}
)

type JobStorage struct {
	pool *pgxpool.Pool
}

func NewJobStorage(connectionString string) (*JobStorage, error) {
	dbPool, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		return nil, err
	}

	return &JobStorage{pool: dbPool}, nil
}

func (s *JobStorage) GetScheduleById(id uuid.UUID) (*Schedule, error) {
	var schedule = Schedule{
		Job: &Job{},
	}

	sql := `SELECT js.id, js.description, js.frequency, js.last_execution_date, js.next_execution_date, 
				j.id, j.slug, j.status, j.reason
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id 
			WHERE js.id = $1`

	err := s.pool.QueryRow(context.Background(), sql, id).
		Scan(&schedule.Id, &schedule.Description, &schedule.Frequency, &schedule.LastExecutionDate, &schedule.NextExecutionDate,
			&schedule.Job.Id, &schedule.Job.Slug, &schedule.Job.Status, &schedule.Job.Reason)

	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

func (s *JobStorage) GetBySlug(slug string) (*Job, error) {
	var job Job

	sql := `SELECT id, slug, status, reason FROM jobs WHERE slug = $1`

	err := s.pool.QueryRow(context.Background(), sql, slug).
		Scan(&job.Id, &job.Slug, &job.Status, &job.Reason)

	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *JobStorage) GetNew() ([]*Job, error) {
	rows, err := s.pool.Query(context.Background(), `SELECT id, slug, status FROM jobs WHERE status = $1`, New)

	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0)
	for rows.Next() {
		var job Job
		err = rows.Scan(&job.Id, &job.Slug, &job.Status)

		if err != nil {
			return nil, err
		}

		jobs = append(jobs, &job)
	}

	return jobs, nil
}

func (s *JobStorage) GetAll() ([]*Schedule, error) {
	sql := `SELECT js.id, js.description, js.frequency, js.last_execution_date, js.next_execution_date, 
				j.id, j.slug, j.status, j.reason
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id`

	rows, err := s.pool.Query(context.Background(), sql)

	if err != nil {
		return nil, err
	}

	schedules := make([]*Schedule, 0)
	for rows.Next() {
		var schedule = Schedule{
			Job: &Job{},
		}
		err = rows.Scan(&schedule.Id, &schedule.Description, &schedule.Frequency, &schedule.LastExecutionDate,
			&schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &schedule.Job.Status, &schedule.Job.Reason)

		if err != nil {
			return nil, err
		}

		schedules = append(schedules, &schedule)
	}

	return schedules, nil
}

func (s *JobStorage) Add(schedule Schedule) error {
	// TX, batches etc
	conn, err := s.pool.Acquire(context.Background())
	if err != nil {
		return err
	}

	defer conn.Release()

	_, err = conn.Exec(context.Background(),
		"INSERT INTO job_schedule VALUES ($1, $2, $3, $4, $5)",
		schedule.Id, schedule.Description, schedule.Frequency, schedule.LastExecutionDate, schedule.NextExecutionDate)

	if err != nil {
		return err
	}

	_, err = conn.Exec(context.Background(),
		"INSERT INTO jobs VALUES ($1, $2, $3, $4, $5)", schedule.Job.Id, schedule.Id, schedule.Job.Slug,
		schedule.Job.Status, schedule.Job.Reason)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == UniqueConstraintViolation {
			return ErrUniqueConstraintViolation
		}

		return err
	}

	return nil
}

func (s *JobStorage) UpdateStatus(job *Job) error {
	_, err := s.pool.Exec(context.Background(), `UPDATE jobs SET status = $1, reason = $2 WHERE id = $3`,
		job.Status, job.Reason, job.Id)

	if err != nil {
		return err
	}

	return nil
}
