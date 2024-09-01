package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type StorageDriver interface {
	GetScheduleById(ctx context.Context, id uuid.UUID) (*Schedule, error)
	GetSchedulesWithStatus(ctx context.Context, status ScheduleStatus) ([]*Schedule, error)
	GetSchedulesReadyToReschedule(ctx context.Context) ([]*Schedule, error)
	GetAll(ctx context.Context) ([]*Schedule, error)
	Add(ctx context.Context, schedule Schedule) error
	DeleteScheduleById(ctx context.Context, id uuid.UUID) error
	UpdateSchedule(ctx context.Context, schedule *Schedule) error
}

type Pgsql struct {
	pool *pgxpool.Pool
}

func NewPgsqlConnection(ctx context.Context, connectionString string) (*Pgsql, error) {
	dbPool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, err
	}

	return &Pgsql{pool: dbPool}, nil
}

func (pg Pgsql) GetScheduleById(ctx context.Context, id uuid.UUID) (*Schedule, error) {
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

	err := pg.pool.QueryRow(ctx, sql, id).
		Scan(&schedule.Id, &schedule.Description, &schedule.Status, &schedule.Attempt, &schedule.Frequency,
			&schedule.RetryPolicy.Strategy, &schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval,
			&schedule.LastExecutionDate, &schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &jobData)

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

func (pg Pgsql) GetSchedulesWithStatus(ctx context.Context, status ScheduleStatus) ([]*Schedule, error) {
	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id
			WHERE status = $1 AND next_execution_date <= $2`

	rows, err := pg.pool.Query(ctx, sql, status, time.Now())

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

func (pg Pgsql) GetSchedulesReadyToReschedule(ctx context.Context) ([]*Schedule, error) {
	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id
			WHERE (status = $1 OR status = $2) AND next_execution_date <= $3`

	rows, err := pg.pool.Query(ctx, sql, Failed, Finished, time.Now())

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

func (pg Pgsql) GetAll(ctx context.Context) ([]*Schedule, error) {
	sql := `SELECT js.id, js.description, js.status, js.attempt, js.frequency, js.retry_policy_strategy, js.retry_policy_count, 
				js.retry_policy_interval, js.last_execution_date, js.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN job_schedule AS js ON js.id = j.schedule_id`

	rows, err := pg.pool.Query(ctx, sql)

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

func (pg Pgsql) Add(ctx context.Context, schedule Schedule) error {
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx,
		"INSERT INTO job_schedule VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		schedule.Id, schedule.Description, schedule.Status, schedule.Frequency, schedule.Attempt, schedule.RetryPolicy.Strategy,
		schedule.RetryPolicy.Count, schedule.RetryPolicy.Interval, schedule.LastExecutionDate, schedule.NextExecutionDate)

	if err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			return txErr
		}

		return err
	}

	jobData, err := json.Marshal(schedule.Job.Data)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx,
		"INSERT INTO jobs VALUES ($1, $2, $3, $4)", schedule.Job.Id, schedule.Id, schedule.Job.Slug, jobData)

	if err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			return txErr
		}

		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (pg Pgsql) DeleteScheduleById(ctx context.Context, id uuid.UUID) error {
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM jobs WHERE schedule_id = $1`, id)
	if err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			return txErr
		}

		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM job_schedule WHERE id = $1`, id)
	if err != nil {
		if txErr := tx.Rollback(ctx); txErr != nil {
			return txErr
		}

		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (pg Pgsql) UpdateSchedule(ctx context.Context, schedule *Schedule) error {
	sql := `UPDATE job_schedule SET last_execution_date = $1, next_execution_date = $2, attempt = $3, status = $4 WHERE id = $5`

	_, err := pg.pool.Exec(ctx, sql,
		schedule.LastExecutionDate, schedule.NextExecutionDate, schedule.Attempt, schedule.Status, schedule.Id)

	if err != nil {
		return err
	}

	return nil
}
