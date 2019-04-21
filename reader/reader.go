package main

import (
	"context"
	"log"
	"time"
	"os"
	kafka "github.com/segmentio/kafka-go"
)

const topic = "Events"

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15 * time.Second)
		defer cancel()

		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Println("Kafka::ReadMessage error: ", err)
			r.Close()
			os.Exit(0)
		}
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}
