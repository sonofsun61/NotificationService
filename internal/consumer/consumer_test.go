package consumer

import (
	"NotificationService/internal/events"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	fetchErr  error
	commitErr error
	committed []kafka.Message
	closed    bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if r.fetchErr != nil {
		err := r.fetchErr
		r.mu.Unlock()
		return kafka.Message{}, err
	}
	if len(r.msgs) > 0 {
		msg := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.commitErr != nil {
		return r.commitErr
	}
	r.committed = append(r.committed, msgs...)
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *fakeReader) committedOffsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	offsets := make([]int64, 0, len(r.committed))
	for _, m := range r.committed {
		offsets = append(offsets, m.Offset)
	}
	return offsets
}

type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) HandleUserRegistered(ctx context.Context, event events.UserRegistered) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

var discardLogger = slog.New(slog.DiscardHandler)

func newTestConsumer(reader messageReader, handler EventHandler) *Consumer {
	c := newConsumer(reader, handler, discardLogger)
	c.minBackoff = time.Millisecond
	c.maxBackoff = 5 * time.Millisecond
	return c
}

func validEvent() events.UserRegistered {
	return events.UserRegistered{
		EventID:    uuid.New(),
		UserID:     uuid.New(),
		Email:      "user@example.com",
		Name:       "Ivan",
		OccurredAt: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
	}
}

func message(t *testing.T, offset int64, value any) kafka.Message {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return kafka.Message{Offset: offset, Value: data}
}

func runConsumer(c *Consumer) (cancel context.CancelFunc, done <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- c.Run(ctx) }()
	return cancel, ch
}

func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop")
		return nil
	}
}

func TestRun_Success(t *testing.T) {
	event := validEvent()
	reader := &fakeReader{msgs: []kafka.Message{message(t, 7, event)}}
	handler := new(mockHandler)
	handler.On("HandleUserRegistered", mock.MatchedBy(func(ctx context.Context) bool {
		_, hasDeadline := ctx.Deadline()
		return hasDeadline
	}), event).Return(nil).Once()
	cancel, done := runConsumer(newTestConsumer(reader, handler))

	assert.Eventually(t, func() bool { return len(reader.committedOffsets()) == 1 }, time.Second, time.Millisecond)
	cancel()
	assert.NoError(t, waitDone(t, done))
	assert.Equal(t, []int64{7}, reader.committedOffsets())
	handler.AssertExpectations(t)
}

func TestRun_InvalidMessageIsCommittedWithoutHandling(t *testing.T) {
	noEventID := validEvent()
	noEventID.EventID = uuid.Nil
	badEmail := validEvent()
	badEmail.Email = "not-an-email"
	tests := []struct {
		name string
		msg  kafka.Message
	}{
		{"broken json", kafka.Message{Offset: 1, Value: []byte(`{"event_id":`)}},
		{"missing event id", message(t, 1, noEventID)},
		{"bad email", message(t, 1, badEmail)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &fakeReader{msgs: []kafka.Message{tt.msg}}
			handler := new(mockHandler)
			cancel, done := runConsumer(newTestConsumer(reader, handler))

			assert.Eventually(t, func() bool { return len(reader.committedOffsets()) == 1 }, time.Second, time.Millisecond)
			cancel()
			assert.NoError(t, waitDone(t, done))
			handler.AssertNotCalled(t, "HandleUserRegistered", mock.Anything, mock.Anything)
		})
	}
}

func TestRun_RetriesSameMessageUntilSuccess(t *testing.T) {
	event := validEvent()
	reader := &fakeReader{msgs: []kafka.Message{message(t, 3, event), message(t, 4, validEvent())}}
	handler := new(mockHandler)
	handler.On("HandleUserRegistered", mock.Anything, event).Return(errors.New("smtp down")).Twice()
	handler.On("HandleUserRegistered", mock.Anything, event).Return(nil).Once()
	handler.On("HandleUserRegistered", mock.Anything, mock.Anything).Return(nil).Once()
	cancel, done := runConsumer(newTestConsumer(reader, handler))

	assert.Eventually(t, func() bool { return len(reader.committedOffsets()) == 2 }, time.Second, time.Millisecond)
	cancel()
	assert.NoError(t, waitDone(t, done))
	assert.Equal(t, []int64{3, 4}, reader.committedOffsets())
	handler.AssertExpectations(t)
}

func TestRun_ShutdownDuringRetryDoesNotCommit(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{message(t, 5, validEvent())}}
	handler := new(mockHandler)
	var calls atomic.Int32
	handler.On("HandleUserRegistered", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { calls.Add(1) }).
		Return(errors.New("smtp down"))
	cancel, done := runConsumer(newTestConsumer(reader, handler))

	assert.Eventually(t, func() bool { return calls.Load() > 1 }, time.Second, time.Millisecond)
	cancel()
	assert.NoError(t, waitDone(t, done))
	assert.Empty(t, reader.committedOffsets())
}

func TestRun_FetchError(t *testing.T) {
	reader := &fakeReader{fetchErr: errors.New("broker unavailable")}
	handler := new(mockHandler)

	err := newTestConsumer(reader, handler).Run(context.Background())
	assert.ErrorContains(t, err, "failed to fetch message")
}

func TestRun_CommitError(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{message(t, 1, validEvent())}, commitErr: errors.New("rebalance")}
	handler := new(mockHandler)
	handler.On("HandleUserRegistered", mock.Anything, mock.Anything).Return(nil)

	err := newTestConsumer(reader, handler).Run(context.Background())
	assert.ErrorContains(t, err, "failed to commit message")
}

func TestClose(t *testing.T) {
	reader := &fakeReader{}

	require.NoError(t, newTestConsumer(reader, new(mockHandler)).Close())
	assert.True(t, reader.closed)
}
