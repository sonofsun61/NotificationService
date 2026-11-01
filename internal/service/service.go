package service

import (
	"NotificationService/internal/events"
	"context"
	"fmt"

	"github.com/google/uuid"
)

type Mailer interface {
	Send(ctx context.Context, to, subject, body string) error
}

type Repository interface {
	ProcessOnce(ctx context.Context, eventID uuid.UUID, fn func(ctx context.Context) error) error
}

type Service struct {
	mailer Mailer
	repo   Repository
}

func NewService(mailer Mailer, repo Repository) *Service {
	return &Service{
		mailer: mailer,
		repo:   repo,
	}
}

func (s *Service) HandleUserRegistered(ctx context.Context, event events.UserRegistered) error {
	err := s.repo.ProcessOnce(ctx, event.EventID, func(ctx context.Context) error {
		subject, body := "Welcome!", "You registered on our site. Welcome!"
		if err := s.mailer.Send(ctx, event.Email, subject, body); err != nil {
			return fmt.Errorf("failed to process event: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to process user registered event: %w", err)
	}
	return nil
}
