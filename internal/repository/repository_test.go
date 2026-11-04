//go:build integration

package repository_test

import (
	"NotificationService/internal/repository"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("notification_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.WithInitScripts(filepath.Join("..", "..", "migrations", "000001_create_processed_events.up.sql")),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(time.Minute),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = testcontainers.TerminateContainer(container)
	}()

	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}
	pool, err = pgxpool.New(ctx, connString)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	return m.Run()
}

func newRepo(t *testing.T) *repository.Repository {
	t.Helper()
	_, err := pool.Exec(context.Background(), "truncate table processed_events")
	require.NoError(t, err)
	return repository.NewRepository(pool)
}

func isProcessed(t *testing.T, eventID uuid.UUID) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(context.Background(),
		"select exists(select 1 from processed_events where event_id = $1)", eventID).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func TestProcessOnce_NewEvent(t *testing.T) {
	repo := newRepo(t)
	eventID := uuid.New()
	calls := 0

	err := repo.ProcessOnce(context.Background(), eventID, func(ctx context.Context) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.True(t, isProcessed(t, eventID))
}

func TestProcessOnce_DuplicateEventSkipsFn(t *testing.T) {
	repo := newRepo(t)
	eventID := uuid.New()
	calls := 0
	fn := func(ctx context.Context) error {
		calls++
		return nil
	}

	require.NoError(t, repo.ProcessOnce(context.Background(), eventID, fn))
	require.NoError(t, repo.ProcessOnce(context.Background(), eventID, fn))
	assert.Equal(t, 1, calls)
}

func TestProcessOnce_FnErrorRollsBack(t *testing.T) {
	repo := newRepo(t)
	eventID := uuid.New()
	errSMTPDown := errors.New("smtp down")

	err := repo.ProcessOnce(context.Background(), eventID, func(ctx context.Context) error {
		return errSMTPDown
	})
	assert.ErrorIs(t, err, errSMTPDown)
	assert.False(t, isProcessed(t, eventID))

	calls := 0
	err = repo.ProcessOnce(context.Background(), eventID, func(ctx context.Context) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.True(t, isProcessed(t, eventID))
}

func TestProcessOnce_ConcurrentDuplicatesRunFnOnce(t *testing.T) {
	repo := newRepo(t)
	eventID := uuid.New()
	var calls atomic.Int32
	var wg sync.WaitGroup

	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := repo.ProcessOnce(context.Background(), eventID, func(ctx context.Context) error {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond)
				return nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), calls.Load())
}
