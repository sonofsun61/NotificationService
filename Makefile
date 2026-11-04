-include .env

export

BIN_DIR := $(CURDIR)/bin
MIGRATE_VERSION ?= v4.20.1
MIGRATE := $(BIN_DIR)/migrate-$(MIGRATE_VERSION)

.PHONY: test test-integration lint db-up db-down wire migrate-up migrate-down run docker-build

test:
	@go test -race -cover -count=1 ./...

test-integration:
	@go test -tags=integration -race -cover -count=1 ./...

lint:
	@go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "не отформатированы"; gofmt -l .; exit 1)

db-up:
	@docker compose up -d --wait

db-down:
	@docker compose down

wire:
	@cd internal/app && go tool wire

$(MIGRATE):
	GOBIN=$(BIN_DIR) go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)
	mv $(BIN_DIR)/migrate $(MIGRATE)

migrate-up: $(MIGRATE)
	$(MIGRATE) -path migrations -database "$$POSTGRES_URL" up

migrate-down: $(MIGRATE)
	$(MIGRATE) -path migrations -database "$$POSTGRES_URL" down 1

run: db-up migrate-up
	go run ./cmd/notification

docker-build:
	@docker build -t notification-service:dev .
