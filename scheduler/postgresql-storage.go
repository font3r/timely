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
	GetAwaitingSchedules(ctx context.Context) ([]*Schedule, error)
	GetSchedulesPaged(ctx context.Context, page int, pageSize int) ([]*Schedule, error)
	Add(ctx context.Context, schedule Schedule) error
	DeleteScheduleById(ctx context.Context, id uuid.UUID) error
	UpdateSchedule(ctx context.Context, schedule Schedule) error
	AddJobRun(ctx context.Context, jobRun JobRun) error
	GetJobRun(ctx context.Context, id uuid.UUID) (*JobRun, error)
	GetJobRunGroup(ctx context.Context, scheduleId uuid.UUID, groupId uuid.UUID) ([]*JobRun, error)
	GetJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*JobRun, error)
	GetRecentJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*JobRun, error)
	UpdateJobRun(ctx context.Context, jobRun JobRun) error
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

	sql := `SELECT s.id, s.group_id, s.description, s.status, s.frequency, s.schedule_start, 
				s.retry_policy_strategy, s.retry_policy_count, s.retry_policy_interval, 
				s.last_execution_date, s.next_execution_date, j.id, 
				j.slug, j.data
			FROM jobs AS j 
			JOIN schedules AS s ON s.id = j.schedule_id
			WHERE s.id = $1`

	var jobData string

	err := pg.pool.QueryRow(ctx, sql, id).
		Scan(&schedule.Id, &schedule.GroupId, &schedule.Description, &schedule.Status, &schedule.Frequency,
			&schedule.ScheduleStart, &schedule.RetryPolicy.Strategy, &schedule.RetryPolicy.Count,
			&schedule.RetryPolicy.Interval, &schedule.LastExecutionDate, &schedule.NextExecutionDate,
			&schedule.Job.Id, &schedule.Job.Slug, &jobData)

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

func (pg Pgsql) GetAwaitingSchedules(ctx context.Context) ([]*Schedule, error) {
	sql := `SELECT s.id, s.group_id, s.description, s.status, s.frequency, s.schedule_start,
				s.retry_policy_strategy, s.retry_policy_count, s.retry_policy_interval, 
				s.last_execution_date, s.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN schedules AS s ON s.id = j.schedule_id
			WHERE status IN ($1) AND next_execution_date <= $2
			ORDER BY next_execution_date ASC`

	rows, err := pg.pool.Query(ctx, sql, Waiting, time.Now())

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
		err = rows.Scan(&schedule.Id, &schedule.GroupId, &schedule.Description, &schedule.Status,
			&schedule.Frequency, &schedule.ScheduleStart, &schedule.RetryPolicy.Strategy,
			&schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval, &schedule.LastExecutionDate,
			&schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &jobData)

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

func (pg Pgsql) GetSchedulesPaged(ctx context.Context, page int, pageSize int) ([]*Schedule, error) {
	sql := `SELECT s.id, s.group_id, s.description, s.status, s.frequency, s.schedule_start, 
				s.retry_policy_strategy, s.retry_policy_count, s.retry_policy_interval, 
				s.last_execution_date, s.next_execution_date, j.id, j.slug, j.data
			FROM jobs AS j 
			JOIN schedules AS s ON s.id = j.schedule_id
			ORDER BY last_execution_date DESC
			OFFSET $1
			LIMIT $2`

	rows, err := pg.pool.Query(ctx, sql, (page-1)*pageSize, pageSize)

	if err != nil {
		return nil, err
	}

	schedules := make([]*Schedule, 0, pageSize)
	for rows.Next() {
		var schedule = Schedule{
			RetryPolicy: RetryPolicy{},
			Job:         &Job{},
		}

		var jobData string

		err = rows.Scan(&schedule.Id, &schedule.GroupId, &schedule.Description, &schedule.Status,
			&schedule.Frequency, &schedule.ScheduleStart, &schedule.RetryPolicy.Strategy,
			&schedule.RetryPolicy.Count, &schedule.RetryPolicy.Interval, &schedule.LastExecutionDate,
			&schedule.NextExecutionDate, &schedule.Job.Id, &schedule.Job.Slug, &jobData)

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
		"INSERT INTO schedules VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		schedule.Id, schedule.GroupId, schedule.Description, schedule.Status, schedule.Frequency,
		schedule.ScheduleStart, schedule.RetryPolicy.Strategy, schedule.RetryPolicy.Count,
		schedule.RetryPolicy.Interval, schedule.LastExecutionDate, schedule.NextExecutionDate)

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

	_, err = tx.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, id)
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

func (pg Pgsql) UpdateSchedule(ctx context.Context, schedule Schedule) error {
	sql := `UPDATE schedules SET last_execution_date = $1, next_execution_date = $2, status = $3, group_id = $4 WHERE id = $5`

	_, err := pg.pool.Exec(ctx, sql,
		schedule.LastExecutionDate, schedule.NextExecutionDate, schedule.Status, schedule.GroupId, schedule.Id)

	if err != nil {
		return err
	}

	return nil
}

func (pg Pgsql) AddJobRun(ctx context.Context, jobRun JobRun) error {
	sql := `INSERT INTO job_runs VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := pg.pool.Exec(ctx, sql, jobRun.Id, jobRun.GroupId, jobRun.ScheduleId, jobRun.Status, jobRun.Reason,
		jobRun.StartDate, jobRun.EndDate)
	if err != nil {
		return err
	}

	return nil
}

func (pg Pgsql) GetJobRun(ctx context.Context, id uuid.UUID) (*JobRun, error) {
	var jobRun = JobRun{}

	sql := `SELECT id, group_id, schedule_id, status, attempt, reason, start_date, end_date FROM job_runs WHERE id = $1`

	err := pg.pool.QueryRow(ctx, sql, id).
		Scan(&jobRun.Id, &jobRun.GroupId, &jobRun.ScheduleId, &jobRun.Status, &jobRun.Reason,
			&jobRun.StartDate, &jobRun.EndDate)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	return &jobRun, nil
}

func (pg Pgsql) GetJobRunGroup(ctx context.Context, scheduleId uuid.UUID, groupId uuid.UUID) ([]*JobRun, error) {
	sql := `SELECT id, group_id, schedule_id, status, reason, start_date, end_date FROM job_runs 
			WHERE schedule_id = $1 AND group_id = $2`

	rows, err := pg.pool.Query(ctx, sql, scheduleId, groupId)
	if err != nil {
		return nil, err
	}

	jobRuns := make([]*JobRun, 0)
	for rows.Next() {
		var jobRun = JobRun{}

		err = rows.Scan(&jobRun.Id, &jobRun.GroupId, &jobRun.ScheduleId, &jobRun.Status,
			&jobRun.Reason, &jobRun.StartDate, &jobRun.EndDate)
		if err != nil {
			return nil, err
		}

		jobRuns = append(jobRuns, &jobRun)
	}

	return jobRuns, nil
}

func (pg Pgsql) GetJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*JobRun, error) {
	sql := `SELECT id, group_id, schedule_id, status, reason, start_date, end_date FROM job_runs 
			WHERE schedule_id = $1`

	rows, err := pg.pool.Query(ctx, sql, scheduleId)
	if err != nil {
		return nil, err
	}

	jobRuns := make([]*JobRun, 0)
	for rows.Next() {
		var jobRun = JobRun{}

		err = rows.Scan(&jobRun.Id, &jobRun.GroupId, &jobRun.ScheduleId, &jobRun.Status,
			&jobRun.Reason, &jobRun.StartDate, &jobRun.EndDate)
		if err != nil {
			return nil, err
		}

		jobRuns = append(jobRuns, &jobRun)
	}

	return jobRuns, nil
}

func (pg Pgsql) GetRecentJobRuns(ctx context.Context, scheduleId uuid.UUID) ([]*JobRun, error) {
	sql := `SELECT * FROM (
				SELECT id, group_id, schedule_id, status, reason, start_date, end_date FROM job_runs 
				WHERE schedule_id = $1 ORDER BY end_date DESC LIMIT 5
			) ORDER BY end_date ASC`

	rows, err := pg.pool.Query(ctx, sql, scheduleId)
	if err != nil {
		return nil, err
	}

	jobRuns := make([]*JobRun, 0)
	for rows.Next() {
		var jobRun = JobRun{}

		err = rows.Scan(&jobRun.Id, &jobRun.GroupId, &jobRun.ScheduleId, &jobRun.Status,
			&jobRun.Reason, &jobRun.StartDate, &jobRun.EndDate)
		if err != nil {
			return nil, err
		}

		jobRuns = append(jobRuns, &jobRun)
	}
	return jobRuns, nil
}

func (pg Pgsql) UpdateJobRun(ctx context.Context, jobRun JobRun) error {
	sql := `UPDATE job_runs SET status = $1, reason = $2, end_date = $3 WHERE id = $4`

	_, err := pg.pool.Exec(ctx, sql, jobRun.Status, jobRun.Reason, jobRun.EndDate, jobRun.Id)
	if err != nil {
		return err
	}

	return nil
}
