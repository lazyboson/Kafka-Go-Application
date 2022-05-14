package main

import (
	"net"
	"strconv"

	"kafka-test/pkg"

	"github.com/segmentio/kafka-go"
)

const (
	topic             = "my-topic"
	brokerAddress     = "localhost:9093"
	partition         = 3
	replicationFactor = 2
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
	data := make(map[string]interface{})
	pkg.Producer(brokerAddress, topic, partition, data)
	pkg.Consumer(brokerAddress, topic)

}
