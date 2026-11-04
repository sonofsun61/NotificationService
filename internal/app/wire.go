//go:build wireinject
// +build wireinject

package app

import (
	"NotificationService/internal/config"
	"NotificationService/internal/consumer"
	"NotificationService/internal/database"
	"NotificationService/internal/logger"
	"NotificationService/internal/mailer"
	"NotificationService/internal/repository"
	"NotificationService/internal/service"
	"context"
	"log/slog"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

func InitializeApp() *App {
	wire.Build(
		config.MustLoadConfig,
		ProvideConnString,
		ProvideContext,
		logger.NewLogger,
		database.MustConnectToDatabase,
		repository.NewRepository,
		wire.Bind(new(service.Repository), new(*repository.Repository)),
		ProvideMailer,
		wire.Bind(new(service.Mailer), new(*mailer.Mailer)),
		service.NewService,
		wire.Bind(new(consumer.EventHandler), new(*service.Service)),
		ProvideConsumer,
		wire.Bind(new(Consumer), new(*consumer.Consumer)),
		ProvideApp,
	)
	return nil
}

func ProvideConnString(cfg *config.Config) database.ConnString {
	return database.ConnString(cfg.PostgresURL)
}

func ProvideContext() context.Context {
	return context.Background()
}

func ProvideMailer(cfg *config.Config) *mailer.Mailer {
	return mailer.NewMailer(cfg.SMTPAddr, cfg.MailFrom)
}

func ProvideConsumer(cfg *config.Config, handler consumer.EventHandler, logger *slog.Logger) *consumer.Consumer {
	return consumer.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopicUserEvent, cfg.KafkaGroupID, handler, logger)
}

func ProvideApp(pool *pgxpool.Pool, logger *slog.Logger, consumer Consumer) *App {
	return &App{
		pool:     pool,
		logger:   logger,
		consumer: consumer,
	}
}
