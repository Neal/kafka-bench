package producer

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type SaramaProducer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewSaramaProducer(addr []string, topic string) *SaramaProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		log.Fatalf("failed to start sarama consumer: %v", err)
	}

	return &SaramaProducer{
		producer: producer,
		topic:    topic,
	}
}

func (p *SaramaProducer) Close() error {
	return p.producer.Close()
}

func (p *SaramaProducer) WriteMessages(msgs ...Message) error {
	// group messages per account to send them in batches per account's goroutine
	pMsgs := make(map[string][]*sarama.ProducerMessage)

	for _, msg := range msgs {
		if pMsgs[msg.Key] == nil {
			pMsgs[msg.Key] = make([]*sarama.ProducerMessage, 0)
		}
		pMsgs[msg.Key] = append(pMsgs[msg.Key], &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.StringEncoder(msg.Key),
			Value: sarama.ByteEncoder(msg.Value),
		})
	}

	var wg sync.WaitGroup

	for _, msgs := range pMsgs {
		wg.Add(1)
		go func(msgs []*sarama.ProducerMessage) {
			defer wg.Done()
			p.producer.SendMessages(msgs)
		}(msgs)
	}

	wg.Wait()

	return nil
}
