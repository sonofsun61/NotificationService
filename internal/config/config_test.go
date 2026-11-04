package config_test

import (
	"NotificationService/internal/config"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var validEnv = map[string]string{
	"KAFKA_BROKERS":           "k1:9092,k2:9092",
	"KAFKA_TOPIC_USER_EVENTS": "user.events",
	"KAFKA_GROUP_ID":          "notification-service",
	"POSTGRES_URL":            "postgres://user:pass@localhost:5434/db",
	"SMTP_ADDR":               "localhost:1025",
	"MAIL_FROM":               "noreply@shop.local",
}

func setEnv(t *testing.T) {
	t.Helper()
	for k, v := range validEnv {
		t.Setenv(k, v)
	}
}

func TestMustLoadConfig_Success(t *testing.T) {
	setEnv(t)

	cfg := config.MustLoadConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, []string{"k1:9092", "k2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "user.events", cfg.KafkaTopicUserEvent)
	assert.Equal(t, "notification-service", cfg.KafkaGroupID)
	assert.Equal(t, "postgres://user:pass@localhost:5434/db", cfg.PostgresURL)
	assert.Equal(t, "localhost:1025", cfg.SMTPAddr)
	assert.Equal(t, "noreply@shop.local", cfg.MailFrom)
}

func TestMustLoadConfig_MissingEnv(t *testing.T) {
	tests := []struct {
		name  string
		unset string
	}{
		{"no kafka brokers", "KAFKA_BROKERS"},
		{"no user events topic", "KAFKA_TOPIC_USER_EVENTS"},
		{"no group id", "KAFKA_GROUP_ID"},
		{"no postgres url", "POSTGRES_URL"},
		{"no smtp addr", "SMTP_ADDR"},
		{"no mail from", "MAIL_FROM"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t)
			t.Setenv(tt.unset, "")

			assert.PanicsWithValue(t, tt.unset+" is not set", func() {
				config.MustLoadConfig()
			})
		})
	}
}
