package pkg

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func Consumer(brokerAddress, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  "messageConsumer",
		Topic:    topic,
		MinBytes: 1,    // 1B
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
