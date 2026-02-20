package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Notification struct {
	To        string `json:"to"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
	EventType string `json:"eventType,omitempty"`
}

var notificationWriter *kafkago.Writer
var notificationTopic string

func StartupKafkaProducer() {
	broker := os.Getenv("KAFKA_BROKER")
	notificationTopic := os.Getenv("KAFKA_TOPIC")
	if broker == "" || notificationTopic == "" {
		log.Println("MISSING KAFKA_BROKER OR KAFKA_TOPIC")
		return
	}

	notificationWriter = &kafkago.Writer{
		Addr:         kafkago.TCP(broker),
		Topic:        notificationTopic,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
	}

	log.Println("Connected to Kafka!:", broker, "topic:", notificationTopic)
}

func PushNotification(message Notification) error {
	if notificationWriter == nil {
		return nil
	}

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	context, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return notificationWriter.WriteMessages(context, kafkago.Message{
		Value: data,
	})
}
