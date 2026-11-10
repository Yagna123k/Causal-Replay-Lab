DATABASE_URL ?= postgres://replay:replay@localhost:5432/replay?sslmode=disable
export DATABASE_URL

.PHONY: run build test test-integration vet migrate-up migrate-down up down

up:
	docker compose up -d --wait

down:
	docker compose down

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

run:
	go run ./cmd/server

build:
	go build -o bin/ ./cmd/...

vet:
	go vet ./...

test:
	go test ./...

# Requires a running PostgreSQL (make up). Uses TEST_DATABASE_URL, defaulting to DATABASE_URL.
test-integration:
	TEST_DATABASE_URL=$(DATABASE_URL) go test -race -count=1 ./...
