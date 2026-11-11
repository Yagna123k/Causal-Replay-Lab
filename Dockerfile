# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS builder
WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 appuser
WORKDIR /app

COPY --from=builder /out/server /app/server
COPY --from=builder /out/migrate /app/migrate

USER appuser
EXPOSE 8080

ENTRYPOINT ["/app/server"]
