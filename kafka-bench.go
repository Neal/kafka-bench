package main

import (
	"flag"
	"os"
	"strings"

	"github.com/neal/kafka-bench/consumer"
	"github.com/neal/kafka-bench/producer"
)

type Driver string

const (
	Sarama Driver = "sarama"
	Kafka  Driver = "kafka"
)

var (
	topic   = flag.String("topic", "", "topic")
	produce = flag.Int("produce", 0, "produce msgs")
	driver  = flag.String("driver", "", "sarama or kafka")
)

func main() {
	flag.Parse()

	brokers := strings.Split(os.Getenv("KAFKA_ADDR"), ",")

	if *produce > 0 {
		var p producer.Producer
		switch Driver(*driver) {
		case Kafka:
			p = producer.NewKafkaProducer(brokers, *topic)
		case Sarama:
			p = producer.NewSaramaProducer(brokers, *topic)
		default:
			panic("unknown driver")
		}
		producer.Produce(p, *produce)
		p.Close()
		return
	}

	var c consumer.Consumer
	switch Driver(*driver) {
	case Kafka:
		c = consumer.NewKafkaConsumer(brokers, *topic)
	case Sarama:
		c = consumer.NewSaramaConsumer(brokers, *topic)
	default:
		panic("unknown driver")
	}
	consumer.Consume(c)
	c.Close()
}
