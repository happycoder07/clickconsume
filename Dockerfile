# ---------- Stage 1: Build ----------
FROM golang:1.23 AS builder

WORKDIR /app

# Copy and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o kafka-clickhouse .

# ---------- Stage 2: Runtime ----------
FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/kafka-clickhouse .

# Add minimal dependencies
RUN apk add --no-cache ca-certificates tzdata

ENV KAFKA_BROKER=localhost:9092
ENV KAFKA_TOPIC=ipfix-topic
ENV CLICKHOUSE_DSN=clickhouse:9000

EXPOSE 9092
CMD ["./kafka-clickhouse"]
