package main

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"kafka-test/gen/pb"
	"log"
	"net"
	"strconv"

	"kafka-test/pkg"

	"github.com/segmentio/kafka-go"
)

const (
	topic             = "product-topic3"
	brokerAddress     = "localhost:9093"
	partition         = 1
	replicationFactor = 1
)

func newKafkaTopic(topic, brokerUrl string, partitionCount, replicationFactor int) {
	//if multiple brokers are running, you can give a single broker address and zookeeper will take care of distribution of message among partitions
	conn, err := kafka.Dial("tcp", brokerUrl)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitionCount,
			ReplicationFactor: replicationFactor,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	newKafkaTopic(topic, brokerAddress, partition, replicationFactor)
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	for i := 1; i < 100; i++ {
		product := pb.Product{
			Id:    "1234-uuuti",
			Name:  "CKl",
			Price: float32(i),
		}
		out, err := proto.Marshal(&product)
		if err != nil {
			fmt.Println(err)
		}
		pkg.Producer(w, out)
	}
	pkg.Consumer(brokerAddress, topic)
	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
