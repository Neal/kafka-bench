package producer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(addr []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(addr...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
			// BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,
			RequiredAcks: kafka.RequireOne,
			// Logger:       kafka.LoggerFunc(log.Printf),
			ErrorLogger: kafka.LoggerFunc(log.Printf),
		},
	}
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

func (p *KafkaProducer) WriteMessages(msgs ...Message) error {
	// group messages per account to send them in batches per account's goroutine
	pMsgs := make(map[string][]kafka.Message)

	for _, msg := range msgs {
		if pMsgs[msg.Key] == nil {
			pMsgs[msg.Key] = make([]kafka.Message, 0)
		}
		pMsgs[msg.Key] = append(pMsgs[msg.Key], kafka.Message{
			Key:   []byte(msg.Key),
			Value: msg.Value,
		})
	}

	var wg sync.WaitGroup

	for _, msgs := range pMsgs {
		wg.Add(1)
		go func(msgs []kafka.Message) {
			defer wg.Done()
			for _, msg := range msgs {
				if err := p.writer.WriteMessages(context.TODO(), msg); err != nil {
					log.Printf("error writing msg: %v", err)
				}
			}
		}(msgs)
	}

	wg.Wait()

	return nil
}
