package consumer

import (
	"context"
	"errors"
	"log"
	"time"
)

type Message struct {
	Key   []byte
	Value []byte
}

type Consumer interface {
	Close() error
	Read(ch chan Message, timeout time.Duration) error
}

func Consume(consumer Consumer) {
	start := time.Now()
	timeout := 1100 * time.Millisecond
	statFreq := 20000

	log.Printf("consuming msgs")

	var total int64

	ch := make(chan Message, 1)

	go func() {
		lap := time.Now()
		var count int64
		for range ch {
			total++
			count++
			if count == int64(statFreq) {
				elapsed := time.Since(lap)
				log.Printf("consumed %v msgs in %v @ %.2f/s", statFreq, elapsed, float64(statFreq)/elapsed.Seconds())
				lap = time.Now()
				count = 0
			}
		}
	}()

	if err := consumer.Read(ch, timeout); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("error consuming msgs: %v", err)
		}
	}
	close(ch)

	elapsed := time.Since(start) - timeout

	log.Printf("done consuming %v msgs in %v @ %.2f/s", total, elapsed, float64(total)/elapsed.Seconds())
}
