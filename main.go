package main

import (
	"fmt"
	"net"
	"strconv"

	pb "kafka-test/gen/pb"
	"kafka-test/pkg"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	topic             = "my-topic-1"
	brokerAddress     = "localhost:9092"
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
	product := pb.Product {
		Id: "1234-uuuti",
		Name: "CKl",
		Price: 3457.9,
	}
	out, err := proto.Marshal(&product)
	if err != nil {
		fmt.Println(err)	
	}
	pkg.Producer(brokerAddress, topic, partition, out)
	pkg.Consumer(brokerAddress, topic)

}
