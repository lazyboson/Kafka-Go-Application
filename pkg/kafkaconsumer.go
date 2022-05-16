package pkg

import (
	"context"
	"fmt"
	"kafka-test/gen/pb"
	"log"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func Consumer(brokerAddress, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  "consumemessage3",
		Topic:    topic,
		MinBytes: 1,    // 1B
		MaxBytes: 10e6, // 10MB
	})

	for {
		prod := &pb.Product{}
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			break
		}
		if err := proto.Unmarshal(m.Value, prod); err != nil {
			fmt.Println(err)
		}

		fmt.Printf("message at offset %d: %s = %v\n", m.Offset, string(m.Key), prod)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
