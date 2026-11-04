package events

import (
	"time"

	"github.com/google/uuid"
)

type UserRegistered struct {
	EventID    uuid.UUID `json:"event_id"`
	UserID     uuid.UUID `json:"user_id"`
	Email      string    `json:"email"`
	Name       string    `json:"name"`
	OccurredAt time.Time `json:"occurred_at"`
}
