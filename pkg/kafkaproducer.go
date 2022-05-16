package pkg

import (
	"context"
	"log"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func Producer(w *kafka.Writer, data []byte) {
	// make a writer that produces to topic, using the least-bytes distribution

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

}
