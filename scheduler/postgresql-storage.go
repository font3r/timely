package scheduler

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobStorage struct {
	pool *pgxpool.Pool
}

const (
	UniqueConstrainViolation string = "23505"
)

var (
	ErrJobAlreadyExists = &Error{Code: "JOB_ALREADY_EXISTS", Message: "Specified job already exists"}
)

func NewJobStorage(connectionString string) (*JobStorage, error) {
	dbPool, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		return nil, err
	}

	return &JobStorage{pool: dbPool}, nil
}

func (s *JobStorage) GetById(id uuid.UUID) (*Job, error) {
	var job Job

	err := s.pool.QueryRow(context.Background(), `SELECT * FROM jobs WHERE id = $1`, id).
		Scan(&job.Id, &job.Slug, &job.Description, &job.Cron, &job.Status, &job.LastExecutionDate, &job.NextExecutionDate)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *JobStorage) GetBySlug(slug string) (*Job, error) {
	var job Job

	err := s.pool.QueryRow(context.Background(), `SELECT * FROM jobs WHERE slug = $1`, slug).
		Scan(&job.Id, &job.Slug, &job.Description, &job.Cron, &job.Status, &job.LastExecutionDate, &job.NextExecutionDate)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *JobStorage) GetNew() ([]*Job, error) {
	rows, err := s.pool.Query(context.Background(), `SELECT * FROM jobs WHERE status = $1`, New)

	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0)
	for rows.Next() {
		var job Job
		err = rows.Scan(&job.Id, &job.Slug, &job.Description, &job.Cron, &job.Status,
			&job.LastExecutionDate, &job.NextExecutionDate)

		if err != nil {
			return nil, err
		}

		jobs = append(jobs, &job)
	}

	return jobs, nil
}

func (s *JobStorage) GetAll() ([]*Job, error) {
	rows, err := s.pool.Query(context.Background(), `SELECT * FROM jobs`)

	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0)
	for rows.Next() {
		var job Job
		err = rows.Scan(&job.Id, &job.Slug, &job.Description, &job.Cron, &job.Status,
			&job.LastExecutionDate, &job.NextExecutionDate)

		if err != nil {
			return nil, err
		}
	}

	return jobs, nil
}

func (s *JobStorage) Add(job Job) error {
	_, err := s.pool.Exec(context.Background(),
		"INSERT INTO jobs VALUES ($1, $2, $3, $4, $5, $6, $7)",
		job.Id, job.Slug, job.Description, job.Cron, job.Status, job.LastExecutionDate, job.NextExecutionDate)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == UniqueConstrainViolation {
			return ErrJobAlreadyExists
		}

		return err
	}

	return nil
}

func (s *JobStorage) UpdateStatus(job *Job) error {
	_, err := s.pool.Exec(context.Background(), `UPDATE jobs SET status = $1 WHERE id = $2`, job.Status, job.Id)

	if err != nil {
		return err
	}

	return nil
}
