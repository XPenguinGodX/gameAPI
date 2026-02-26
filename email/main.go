package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Notification struct {
	To        string `json:"to"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
	EventType string `json:"eventType,omitempty"`
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env var: %s", k)
	}
	return v
}

func sendSMTP(n Notification) error {
	host := mustEnv("SMTP_HOST")
	port := mustEnv("SMTP_PORT")
	user := mustEnv("SMTP_USER")
	pass := mustEnv("SMTP_PASS")
	from := mustEnv("SMTP_FROM")

	addr := net.JoinHostPort(host, port)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}
	defer c.Quit()

	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(&tls.Config{ServerName: host}); err != nil {
			return err
		}
	}

	// AUTH
	if ok, _ := c.Extension("AUTH"); ok {
		if err := c.Auth(smtp.PlainAuth("", user, pass, host)); err != nil {
			return err
		}
	}

	if err := c.Mail(from); err != nil {
		return err
	}
	if err := c.Rcpt(n.To); err != nil {
		return err
	}

	w, err := c.Data()
	if err != nil {
		return err
	}
	defer w.Close()

	msg := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s\r\n",
		from, n.To, n.Subject, n.Body,
	)

	_, err = w.Write([]byte(msg))
	return err
}

func main() {
	broker := mustEnv("KAFKA_BROKER")
	group := os.Getenv("KAFKA_GROUP")
	if group == "" {
		group = "email-service"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		GroupTopics: []string{"offers", "users"},
		GroupID:     group,
		MinBytes:    1e3,
		MaxBytes:    10e6,
	})
	defer r.Close()

	log.Println("email-service consuming:", "offers", "from:", broker)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("kafka read error:", err)
			continue
		}

		var n Notification
		if err := json.Unmarshal(m.Value, &n); err != nil {
			log.Println("bad json:", err, "value:", string(m.Value))
			continue
		}

		if n.To == "" || n.Subject == "" {
			log.Println("missing fields:", string(m.Value))
			continue
		}

		if err := sendSMTP(n); err != nil {
			log.Println("smtp send failed:", err, "to:", n.To)
			continue
		}

		log.Println("email sent:", n.To, "event:", n.EventType)
	}

}
