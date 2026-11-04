package consumer

import (
	"NotificationService/internal/events"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type EventHandler interface {
	HandleUserRegistered(ctx context.Context, event events.UserRegistered) error
}

type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader        messageReader
	handler       EventHandler
	logger        *slog.Logger
	handleTimeout time.Duration
	commitTimeout time.Duration
	minBackoff    time.Duration
	maxBackoff    time.Duration
}

var errInvalidEvent = errors.New("invalid event")

func NewConsumer(brokers []string, topic, groupID string, handler EventHandler, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})
	return newConsumer(reader, handler, logger)
}

func newConsumer(reader messageReader, handler EventHandler, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader:        reader,
		handler:       handler,
		logger:        logger,
		handleTimeout: 10 * time.Second,
		commitTimeout: 5 * time.Second,
		minBackoff:    time.Second,
		maxBackoff:    30 * time.Second,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("failed to fetch message: %w", err)
		}
		if err := c.process(ctx, msg); err != nil {
			return nil
		}
		if err := c.commit(ctx, msg); err != nil {
			return fmt.Errorf("failed to commit message: %w", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	event, err := decodeUserRegistered(msg.Value)
	if err != nil {
		c.logger.Error("skipping invalid message", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		return nil
	}
	backoff := c.minBackoff
	for {
		err := c.handle(ctx, event)
		if err == nil {
			c.logger.Info("event processed", "event_id", event.EventID, "partition", msg.Partition, "offset", msg.Offset)
			return nil
		}
		c.logger.Warn("failed to handle event, retrying", "event_id", event.EventID, "retry_in", backoff, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.maxBackoff)
	}
}

func (c *Consumer) handle(ctx context.Context, event events.UserRegistered) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.handleTimeout)
	defer cancel()
	return c.handler.HandleUserRegistered(ctx, event)
}

func (c *Consumer) commit(ctx context.Context, msg kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.commitTimeout)
	defer cancel()
	return c.reader.CommitMessages(ctx, msg)
}

func decodeUserRegistered(value []byte) (events.UserRegistered, error) {
	var event events.UserRegistered
	if err := json.Unmarshal(value, &event); err != nil {
		return events.UserRegistered{}, fmt.Errorf("%w: %w", errInvalidEvent, err)
	}
	if event.EventID == uuid.Nil {
		return events.UserRegistered{}, fmt.Errorf("%w: missing event_id", errInvalidEvent)
	}
	addr, err := mail.ParseAddress(event.Email)
	if err != nil || addr.Address != event.Email {
		return events.UserRegistered{}, fmt.Errorf("%w: bad email %q", errInvalidEvent, event.Email)
	}
	return event, nil
}
