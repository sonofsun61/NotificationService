package service_test

import (
	"NotificationService/internal/events"
	"NotificationService/internal/service"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockMailer struct {
	mock.Mock
}

var _ service.Mailer = (*mockMailer)(nil)

func (m *mockMailer) Send(ctx context.Context, to, subject, body string) error {
	args := m.Called(ctx, to, subject, body)
	return args.Error(0)
}

type mockRepo struct {
	mock.Mock
}

var _ service.Repository = (*mockRepo)(nil)

func (m *mockRepo) ProcessOnce(ctx context.Context, eventID uuid.UUID, fn func(ctx context.Context) error) error {
	args := m.Called(ctx, eventID, fn)
	return args.Error(0)
}

func runFn(args mock.Arguments) error {
	fn := args.Get(2).(func(ctx context.Context) error)
	return fn(args.Get(0).(context.Context))
}

var errSMTPDown = errors.New("smtp down")

func validEvent() events.UserRegistered {
	return events.UserRegistered{
		EventID:    uuid.New(),
		UserID:     uuid.New(),
		Email:      "user@example.com",
		Name:       "Ivan",
		OccurredAt: time.Now().UTC(),
	}
}

func TestHandleUserRegistered_SendsWelcomeEmail(t *testing.T) {
	event := validEvent()
	mailer := new(mockMailer)
	repo := new(mockRepo)
	var fnErr error
	repo.On("ProcessOnce", mock.Anything, event.EventID, mock.Anything).
		Run(func(args mock.Arguments) { fnErr = runFn(args) }).
		Return(nil)
	mailer.On("Send", mock.Anything, event.Email, "Welcome!", mock.MatchedBy(func(body string) bool {
		return strings.Contains(body, "Hello, Ivan!")
	})).Return(nil).Once()
	svc := service.NewService(mailer, repo)

	err := svc.HandleUserRegistered(context.Background(), event)
	assert.NoError(t, err)
	assert.NoError(t, fnErr)
	repo.AssertExpectations(t)
	mailer.AssertExpectations(t)
}

func TestHandleUserRegistered_MailerErrorIsReturnedToRepo(t *testing.T) {
	event := validEvent()
	mailer := new(mockMailer)
	repo := new(mockRepo)
	var fnErr error
	repo.On("ProcessOnce", mock.Anything, event.EventID, mock.Anything).
		Run(func(args mock.Arguments) { fnErr = runFn(args) }).
		Return(errSMTPDown)
	mailer.On("Send", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errSMTPDown)
	svc := service.NewService(mailer, repo)

	err := svc.HandleUserRegistered(context.Background(), event)
	assert.ErrorIs(t, fnErr, errSMTPDown)
	assert.ErrorIs(t, err, errSMTPDown)
	mailer.AssertExpectations(t)
}

func TestHandleUserRegistered_AlreadyProcessed(t *testing.T) {
	event := validEvent()
	mailer := new(mockMailer)
	repo := new(mockRepo)
	repo.On("ProcessOnce", mock.Anything, event.EventID, mock.Anything).Return(nil)
	svc := service.NewService(mailer, repo)

	err := svc.HandleUserRegistered(context.Background(), event)
	assert.NoError(t, err)
	mailer.AssertNotCalled(t, "Send", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleUserRegistered_RepoError(t *testing.T) {
	event := validEvent()
	mailer := new(mockMailer)
	repo := new(mockRepo)
	errDBDown := errors.New("db down")
	repo.On("ProcessOnce", mock.Anything, event.EventID, mock.Anything).Return(errDBDown)
	svc := service.NewService(mailer, repo)

	err := svc.HandleUserRegistered(context.Background(), event)
	assert.ErrorIs(t, err, errDBDown)
	mailer.AssertNotCalled(t, "Send", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
