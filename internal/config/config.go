package config

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	KafkaBrokers        []string
	KafkaTopicUserEvent string
	KafkaGroupID        string
	PostgresURL         string
	SMTPAddr            string
	MailFrom            string
}

func MustLoadConfig() *Config {
	_ = godotenv.Load()
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		panic("KAFKA_BROKERS is not set")
	}
	kafkaTopicUserEvent := os.Getenv("KAFKA_TOPIC_USER_EVENTS")
	if kafkaTopicUserEvent == "" {
		panic("KAFKA_TOPIC_USER_EVENTS is not set")
	}
	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
	if kafkaGroupID == "" {
		panic("KAFKA_GROUP_ID is not set")
	}
	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		panic("POSTGRES_URL is not set")
	}
	smtpAddr := os.Getenv("SMTP_ADDR")
	if smtpAddr == "" {
		panic("SMTP_ADDR is not set")
	}
	mailFrom := os.Getenv("MAIL_FROM")
	if mailFrom == "" {
		panic("MAIL_FROM is not set")
	}
	return &Config{
		KafkaBrokers:        strings.Split(kafkaBrokers, ","),
		KafkaTopicUserEvent: kafkaTopicUserEvent,
		KafkaGroupID:        kafkaGroupID,
		PostgresURL:         postgresURL,
		SMTPAddr:            smtpAddr,
		MailFrom:            mailFrom,
	}
}
