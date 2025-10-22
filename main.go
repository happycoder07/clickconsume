package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/segmentio/kafka-go"
)

// =================== CONFIG ===================

var (
	kafkaBroker   = getEnv("KAFKA_BROKER", "kafka:9092")
	kafkaTopic    = getEnv("KAFKA_TOPIC", "ipfix-topic")
	clickhouseDSN = getEnv("CLICKHOUSE_DSN", "localhost:9000")

	batchSize     = 1000
	flushInterval = 10 * time.Second
	workerCount   = 5
)

// =================== DATA MODEL ===================

// Event defines the parsed IPFIX JSON message.
type Event struct {
	Type                    string    `json:"@type"`
	FlowStart               time.Time `json:"iana:flowStartMilliseconds"`
	FlowEnd                 time.Time `json:"iana:flowEndMilliseconds"`
	SourceIPv4Address       string    `json:"iana:sourceIPv4Address"`
	DestinationIPv4Address  string    `json:"iana:destinationIPv4Address"`
	SourceTransportPort     int       `json:"iana:sourceTransportPort"`
	DestinationTransportPort int      `json:"iana:destinationTransportPort"`
	ProtocolIdentifier      string    `json:"iana:protocolIdentifier"`
	OctetTotalCount         int       `json:"iana:octetTotalCount"`
	PacketTotalCount        int       `json:"iana:packetTotalCount"`
	SSLServerName           string    `json:"nccc:sslServerName"`
	JA3Hash                 string    `json:"nccc:JA3_Hash"`
}

// BatchJob holds parsed events for batch insert.
type BatchJob struct {
	Events []Event
}

// =================== MAIN ===================

func main() {
	ctx := context.Background()
	conn := initClickHouse(ctx)
	createTable(conn)

	jobChan := make(chan BatchJob, 20)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(ctx, conn, jobChan, i, &wg)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaTopic,
		GroupID: "clickhouse-writer-group",
	})
	defer r.Close()

	buffer := make([]Event, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	log.Println("🚀 Kafka → ClickHouse container started...")

	for {
		select {
		case <-ticker.C:
			if len(buffer) > 0 {
				jobChan <- BatchJob{Events: buffer}
				buffer = make([]Event, 0, batchSize)
			}
		default:
			msg, err := r.FetchMessage(ctx)
			if err != nil {
				log.Printf("⚠️ Kafka read error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			var e Event
			if err := json.Unmarshal(msg.Value, &e); err != nil {
				log.Printf("⚠️ JSON parse error: %v", err)
				continue
			}

			buffer = append(buffer, e)
			if len(buffer) >= batchSize {
				jobChan <- BatchJob{Events: buffer}
				buffer = make([]Event, 0, batchSize)
			}

			_ = r.CommitMessages(ctx, msg)
		}
	}
}

// =================== CLICKHOUSE ===================

func initClickHouse(ctx context.Context) clickhouse.Conn {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{clickhouseDSN},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		log.Fatalf("❌ ClickHouse connect error: %v", err)
	}
	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("❌ ClickHouse ping failed: %v", err)
	}
	log.Println("✅ Connected to ClickHouse")
	return conn
}

func createTable(conn clickhouse.Conn) {
	query := `
	CREATE TABLE IF NOT EXISTS ipfix_events (
		type String,
		flow_start DateTime,
		flow_end DateTime,
		source_ip String,
		destination_ip String,
		source_port UInt16,
		destination_port UInt16,
		protocol String,
		octet_total UInt64,
		packet_total UInt64,
		ssl_server_name String,
		ja3_hash String
	) ENGINE = MergeTree()
	ORDER BY flow_start;
	`
	ctx := context.Background()
	if err := conn.Exec(ctx, query); err != nil {
		log.Fatalf("❌ Table creation failed: %v", err)
	}
	log.Println("✅ Table ready: ipfix_events")
}

// =================== WORKER POOL ===================

func worker(ctx context.Context, conn clickhouse.Conn, jobs <-chan BatchJob, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		if len(job.Events) == 0 {
			continue
		}
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO ipfix_events")
		if err != nil {
			log.Printf("[Worker-%d] PrepareBatch error: %v", id, err)
			continue
		}
		for _, e := range job.Events {
			err := batch.Append(
				e.Type,
				e.FlowStart,
				e.FlowEnd,
				e.SourceIPv4Address,
				e.DestinationIPv4Address,
				uint16(e.SourceTransportPort),
				uint16(e.DestinationTransportPort),
				e.ProtocolIdentifier,
				uint64(e.OctetTotalCount),
				uint64(e.PacketTotalCount),
				e.SSLServerName,
				e.JA3Hash,
			)
			if err != nil {
				log.Printf("[Worker-%d] Append error: %v", id, err)
			}
		}
		if err := batch.Send(); err != nil {
			log.Printf("[Worker-%d] Insert failed: %v", id, err)
		} else {
			log.Printf("[Worker-%d] ✅ Inserted %d records", id, len(job.Events))
		}
	}
}

// =================== UTIL ===================

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}
