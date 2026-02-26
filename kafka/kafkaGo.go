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

func StartupKafkaProducer() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		log.Println("MISSING KAFKA_BROKER")
		return
	}

	notificationWriter = &kafkago.Writer{
		Addr:         kafkago.TCP(broker),
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
	}

	log.Println("Connected to Kafka!:", broker)
}

func PushNotification(message Notification) error {
	if notificationWriter == nil {
		return nil
	}

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	topic := message.EventType

	cxt, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return notificationWriter.WriteMessages(cxt, kafkago.Message{
		Topic: topic,
		Value: data,
	})

}
