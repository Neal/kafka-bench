package consumer

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

type SaramaConsumer struct {
	consumer sarama.Consumer
	topic    string
}

func NewSaramaConsumer(addr []string, topic string) *SaramaConsumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(addr, config)
	if err != nil {
		log.Fatalf("failed to start sarama consumer: %v", err)
	}

	return &SaramaConsumer{
		consumer: consumer,
		topic:    topic,
	}
}

func (c *SaramaConsumer) Close() error {
	return c.consumer.Close()
}

func (c *SaramaConsumer) Read(ch chan Message, timeout time.Duration) error {
	pc, err := c.consumer.ConsumePartition(c.topic, 0, 1)
	if err != nil {
		return err
	}
	defer pc.Close()

	for {
		select {
		case err := <-pc.Errors():
			return err

		case msg := <-pc.Messages():
			ch <- Message{
				Key:   msg.Key,
				Value: msg.Value,
			}

		case <-time.After(timeout):
			return nil
		}
	}
}
