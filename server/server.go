package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"log"
	"encoding/json"
	"context"
	kafka "github.com/segmentio/kafka-go"
)

const topic = "Events"
const partition = 0

func main() {
	conn := setupKafka()
	setupRouter(conn)
}

func setupKafka() *kafka.Writer {
	conn := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
		Balancer: &kafka.LeastBytes{},
		Async: true,
	})
	return conn
}

func setupRouter(conn *kafka.Writer) {
	r := gin.Default()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	r.POST("/event", eventHandler(conn))

	r.Run()
}

func eventHandler(conn *kafka.Writer) func(*gin.Context) {
	return func (c *gin.Context) {
		var body interface{}
			c.BindJSON(&body)
			value, _ := json.Marshal(body)
			
			err := conn.WriteMessages(context.Background(), kafka.Message{Value: value})
			if err != nil {
				log.Println("Kafka::WriteMessages Error: ", err)
				c.AbortWithStatus(http.StatusInternalServerError)
			} else {
				log.Println("Written to topic:", body)
				c.Status(http.StatusOK)
			}
	}
}
