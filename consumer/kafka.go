package consumer

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader *kafka.Reader
}

func NewKafkaConsumer(addr []string, topic string) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:         addr,
			Topic:           topic,
			QueueCapacity:   1000000,
			MinBytes:        1,
			MaxBytes:        100e6,
			MaxWait:         250 * time.Millisecond,
			ReadLagInterval: -1,
			// Logger:          kafka.LoggerFunc(log.Printf),
			ErrorLogger: kafka.LoggerFunc(log.Printf),
		}),
	}
}

func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}

func (c *KafkaConsumer) Read(ch chan Message, timeout time.Duration) error {
	for {
		m, err := c.fetchMessage(context.TODO(), timeout)
		if err != nil {
			return err
		}
		ch <- Message{
			Key:   m.Key,
			Value: m.Value,
		}
	}
	return nil
}

func (c *KafkaConsumer) fetchMessage(parent context.Context, timeout time.Duration) (kafka.Message, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return c.reader.ReadMessage(ctx)
}
