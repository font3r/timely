package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	UniqueConstraintViolation = "23505"
)

var (
	ErrUniqueConstraintViolation = &Error{
		Code: "UNIQUE_CONSTRAINT_VIOLATION",
		Msg:  "unique constraint violation"}
)

type StorageDriver interface {
	GetScheduleById(ctx context.Context, id uuid.UUID) (*Schedule, error)
	GetScheduleByJobSlug(slug string) (*Schedule, error)
	GetSchedulesWithStatus(status ScheduleStatus) ([]*Schedule, error)
	GetSchedulesReadyToReschedule() ([]*Schedule, error)
	GetAll() ([]*Schedule, error)
	Add(schedule Schedule) error
	DeleteScheduleById(id uuid.UUID) error
	UpdateSchedule(schedule *Schedule) error
}

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

func (js JobStorage) GetScheduleById(ctx context.Context, id uuid.UUID) (*Schedule, error) {
	var schedule = Schedule{
		RetryPolicy: RetryPolicy{},
		Job:         &Job{},
	}

	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id
			WHERE js.id = $1`

	var jobData string

	err := js.pool.QueryRow(ctx, sql, id).
		Scan(&schedule.Id, &schedule.Description, &schedule.Status, &schedule.Attempt, &schedule.Frequency, &schedule.RetryPolicy.Strategy,
			&schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval, &schedule.LastExecutionDate,
			&schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &jobData)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	err = json.Unmarshal([]byte(jobData), &schedule.Job.Data)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

func (js JobStorage) GetScheduleByJobSlug(slug string) (*Schedule, error) {
	var schedule = Schedule{
		RetryPolicy: RetryPolicy{},
		Job:         &Job{},
	}

	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id
			WHERE j.slug = $1`

	err := js.pool.QueryRow(context.Background(), sql, slug).
		Scan(&schedule.Id, &schedule.Description, &schedule.Status, &schedule.Attempt, &schedule.Frequency, &schedule.RetryPolicy.Strategy,
			&schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval, &schedule.LastExecutionDate,
			&schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	return &schedule, nil
}

func (js JobStorage) GetSchedulesWithStatus(status ScheduleStatus) ([]*Schedule, error) {
	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id
			WHERE status = $1 AND next_execution_date <= $2`

	rows, err := js.pool.Query(context.Background(), sql, status, time.Now())

	if err != nil {
		return nil, err
	}

	var jobData string
	schedules := make([]*Schedule, 0)
	for rows.Next() {
		var schedule = Schedule{
			RetryPolicy: RetryPolicy{},
			Job:         &Job{},
		}
		err = rows.Scan(&schedule.Id, &schedule.Description, &schedule.Status, &schedule.Attempt, &schedule.Frequency,
			&schedule.RetryPolicy.Strategy, &schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval,
			&schedule.LastExecutionDate, &schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &jobData)

		if err != nil {
			return nil, err
		}

		err = json.Unmarshal([]byte(jobData), &schedule.Job.Data)
		if err != nil {
			return nil, err
		}

		schedules = append(schedules, &schedule)
	}

	return schedules, nil
}

func (js JobStorage) GetSchedulesReadyToReschedule() ([]*Schedule, error) {
	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id
			WHERE (status = $1 OR status = $2) AND next_execution_date <= $3`

	rows, err := js.pool.Query(context.Background(), sql, Failed, Finished, time.Now())

	if err != nil {
		return nil, err
	}

	var jobData string
	schedules := make([]*Schedule, 0)
	for rows.Next() {
		var schedule = Schedule{
			RetryPolicy: RetryPolicy{},
			Job:         &Job{},
		}
		err = rows.Scan(&schedule.Id, &schedule.Description, &schedule.Status, &schedule.Attempt, &schedule.Frequency,
			&schedule.RetryPolicy.Strategy, &schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval,
			&schedule.LastExecutionDate, &schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &jobData)

		if err != nil {
			return nil, err
		}

		err = json.Unmarshal([]byte(jobData), &schedule.Job.Data)
		if err != nil {
			return nil, err
		}

		schedules = append(schedules, &schedule)
	}

	return schedules, nil
}

func (js JobStorage) GetAll() ([]*Schedule, error) {
	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id`

	rows, err := js.pool.Query(context.Background(), sql)

	if err != nil {
		return nil, err
	}

	schedules := make([]*Schedule, 0)
	for rows.Next() {
		var schedule = Schedule{
			RetryPolicy: RetryPolicy{},
			Job:         &Job{},
		}

		var jobData string

		err = rows.Scan(&schedule.Id, &schedule.Description, &schedule.Status, &schedule.Attempt, &schedule.Frequency,
			&schedule.RetryPolicy.Strategy, &schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval,
			&schedule.LastExecutionDate, &schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &jobData)

		if err != nil {
			return nil, err
		}

		err = json.Unmarshal([]byte(jobData), &schedule.Job.Data)
		if err != nil {
			return nil, err
		}

		schedules = append(schedules, &schedule)
	}

	return schedules, nil
}

func (js JobStorage) Add(schedule Schedule) error {
	tx, err := js.pool.Begin(context.Background())
	if err != nil {
		return err
	}

	_, err = tx.Exec(context.Background(),
		"INSERT INTO job_schedule VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		schedule.Id, schedule.Description, schedule.Status, schedule.Frequency, schedule.Attempt, schedule.RetryPolicy.Strategy,
		schedule.RetryPolicy.Count, schedule.RetryPolicy.Interval, schedule.LastExecutionDate, schedule.NextExecutionDate)

	if err != nil {
		if txErr := tx.Rollback(context.Background()); txErr != nil {
			return txErr
		}

		return err
	}

	jobData, err := json.Marshal(schedule.Job.Data)
	if err != nil {
		return err
	}

	_, err = tx.Exec(context.Background(),
		"INSERT INTO jobs VALUES ($1, $2, $3, $4)", schedule.Job.Id, schedule.Id, schedule.Job.Slug, jobData)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == UniqueConstraintViolation {
			if txErr := tx.Rollback(context.Background()); txErr != nil {
				return txErr
			}

			return ErrUniqueConstraintViolation
		}

		if txErr := tx.Rollback(context.Background()); txErr != nil {
			return txErr
		}

		return err
	}

	if err = tx.Commit(context.Background()); err != nil {
		return err
	}

	return nil
}

func (js JobStorage) DeleteScheduleById(id uuid.UUID) error {
	tx, err := js.pool.Begin(context.Background())
	if err != nil {
		return err
	}

	_, err = tx.Exec(context.Background(), `DELETE FROM jobs WHERE schedule_id = $1`, id)
	if err != nil {
		if txErr := tx.Rollback(context.Background()); txErr != nil {
			return txErr
		}

		return err
	}

	_, err = tx.Exec(context.Background(), `DELETE FROM job_schedule WHERE id = $1`, id)
	if err != nil {
		if txErr := tx.Rollback(context.Background()); txErr != nil {
			return txErr
		}

		return err
	}

	if err = tx.Commit(context.Background()); err != nil {
		return err
	}

	return nil
}

func (js JobStorage) UpdateSchedule(schedule *Schedule) error {
	sql := `UPDATE job_schedule SET last_execution_date = $1, next_execution_date = $2, attempt = $3, status = $4 WHERE id = $5`

	_, err := js.pool.Exec(context.Background(), sql,
		schedule.LastExecutionDate, schedule.NextExecutionDate, schedule.Attempt, schedule.Status, schedule.Id)

	if err != nil {
		return err
	}

	return nil
}
