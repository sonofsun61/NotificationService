package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) ProcessOnce(ctx context.Context, eventID uuid.UUID, fn func(ctx context.Context) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	query := `INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING`
	tag, err := tx.Exec(ctx, query, eventID)
	if err != nil {
		return fmt.Errorf("failed to insert new event info in processed_events table: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if err := fn(ctx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}
