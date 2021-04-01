package producer

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type Message struct {
	Key   string
	Value []byte
}

type Producer interface {
	Close() error
	WriteMessages(...Message) error
}

func Produce(producer Producer, count int) {
	start := time.Now()

	log.Printf("generating %d msgs", count)

	numKeys := 1
	if count > 100 {
		numKeys = count / 100
	}
	keys := make([]string, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	var msgs []Message

	for i := 0; i < count; i++ {
		msgs = append(msgs, Message{
			Key:   keys[rand.Intn(numKeys)],
			Value: []byte(uuid.New().String()),
		})
	}

	log.Printf("producing %d msgs after %v", count, time.Since(start).Seconds())

	if err := producer.WriteMessages(msgs...); err != nil {
		log.Printf("error producing: %v", err)
	}

	elapsed := time.Since(start)
	log.Printf("done producing %d msgs in %v @ %.2f/sec", count, elapsed, float64(count)/elapsed.Seconds())
}
