package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Consumer interface {
	Run(ctx context.Context) error
	Close() error
}

type App struct {
	pool     *pgxpool.Pool
	logger   *slog.Logger
	consumer Consumer
}

func (a *App) Run() error {
	defer a.pool.Close()
	defer a.consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a.logger.Info("consumer started")
	if err := a.consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("consumer failed: %w", err)
	}
	a.logger.Info("consumer stopped")
	return nil
}
