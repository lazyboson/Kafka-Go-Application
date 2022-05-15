package pkg

import (
	"context"
	"log"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func Producer(brokerAddress, topic string, partition int, data []byte) {
	// make a writer that produces to topic, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	uuid := uuid.New().String()
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(uuid),
			Value: data,
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
