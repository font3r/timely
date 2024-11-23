package integration

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"timely/scheduler"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestCreateSchedule(t *testing.T) {
	ctx := context.Background()
	pgContainer, err := startPostgres(ctx)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	pgStorage, err := scheduler.NewPgsqlConnection(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}

	rp, _ := scheduler.NewRetryPolicy("constant", 5, "10s")
	date := getStubDate().Add(time.Minute * 10)
	newSchedule := scheduler.NewSchedule("test-description", "*/10 * * * * *", getStubDate,
		scheduler.WithJob("test-slug", nil),
		scheduler.WithRetryPolicy(rp),
		scheduler.WithConfiguration("http", "http://example.com"),
		scheduler.WithScheduleStart(&date))

	err = pgStorage.Add(ctx, newSchedule)
	if err != nil {
		t.Fatal(err)
	}

	schedule, err := pgStorage.GetScheduleById(ctx, newSchedule.Id)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(*schedule, newSchedule) {
		t.Fatalf("expected %+v, got %+v", newSchedule, *schedule)
	}
}

func getStubDate() time.Time {
	d, err := time.Parse(time.RFC3339, "2000-01-01T10:30:00+01:00")
	if err != nil {
		panic(err)
	}

	return d
}

func startPostgres(ctx context.Context) (*postgres.PostgresContainer, error) {
	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithInitScripts(filepath.Join("../..", "database_schema.sql")),
		postgres.WithDatabase("timely-integration-test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)))

	return pgContainer, err
}
