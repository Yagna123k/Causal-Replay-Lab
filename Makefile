DATABASE_URL ?= postgres://replay:replay@localhost:5432/replay?sslmode=disable
export DATABASE_URL

.PHONY: run build test test-integration bench vet migrate-up migrate-down up up-postgres down

up:
	docker compose up -d --build

up-postgres:
	docker compose up -d --wait postgres

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

bench:
	go test -run=NONE -bench=Benchmark -benchmem -count=3 ./internal/ingestion ./internal/replay

# Requires a running PostgreSQL (make up-postgres). Uses TEST_DATABASE_URL, defaulting to DATABASE_URL.
test-integration:
	TEST_DATABASE_URL=$(DATABASE_URL) go test -race -count=1 ./...
