package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/smtp"
	"os"
	"time"

	ProMetrics "github.com/prometheus/client_golang/prometheus"
	PromHttp "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type emailsFailed struct {
	FailedEmails *ProMetrics.CounterVec
}

type emailsSuccessful struct {
	SuccessfulEmails *ProMetrics.CounterVec
}

type totalEmails struct {
	TotalEmails ProMetrics.Counter
}

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

func failedMail(register ProMetrics.Registerer) *emailsFailed {
	ef := &emailsFailed{
		FailedEmails: ProMetrics.NewCounterVec(
			ProMetrics.CounterOpts{
				Name: "total_failed_emails",
				Help: "This is the number of emails that failed",
			},
			[]string{"subject", "type"},
		),
	}
	register.MustRegister(ef.FailedEmails)
	return ef
}

func successfulMail(register ProMetrics.Registerer) *emailsSuccessful {
	es := &emailsSuccessful{
		SuccessfulEmails: ProMetrics.NewCounterVec(
			ProMetrics.CounterOpts{
				Name: "total_successful_emails",
				Help: "The Number of successful emails",
			},
			[]string{"subject", "type"},
		),
	}
	register.MustRegister(es.SuccessfulEmails)
	return es
}

func totalMail(register ProMetrics.Registerer) *totalEmails {
	te := &totalEmails{
		TotalEmails: ProMetrics.NewCounter(ProMetrics.CounterOpts{
			Name: "total_emails",
			Help: "The number of total emails",
		}),
	}
	register.MustRegister(te.TotalEmails)
	return te
}

var register = ProMetrics.NewRegistry()
var ef = failedMail(register)
var es = successfulMail(register)
var et = totalMail(register)

func setStartingValues() {
	ef.FailedEmails.With(ProMetrics.Labels{"subject": "Game Offer Created", "type": "offers"}).Add(0)
	es.SuccessfulEmails.With(ProMetrics.Labels{"subject": "Game Offer Created", "type": "offers"}).Add(0)
	ef.FailedEmails.With(ProMetrics.Labels{"subject": "Password Changed", "type": "users"}).Add(0)
	es.SuccessfulEmails.With(ProMetrics.Labels{"subject": "Password Changed", "type": "users"}).Add(0)
	ef.FailedEmails.With(ProMetrics.Labels{"subject": "Game Offer Rejected", "type": "offers"}).Add(0)
	es.SuccessfulEmails.With(ProMetrics.Labels{"subject": "Game Offer Rejected", "type": "offers"}).Add(0)
	ef.FailedEmails.With(ProMetrics.Labels{"subject": "Game Offer Accepted", "type": "offers"}).Add(0)
	es.SuccessfulEmails.With(ProMetrics.Labels{"subject": "Game Offer Accepted", "type": "offers"}).Add(0)
}

func main() {
	setStartingValues()
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

	//THIS was given to me by grok I was having trouble figuring out WHERE and WHEN to have the Listen and serve set up
	go func() {
		http.Handle("/metrics", PromHttp.HandlerFor(register, PromHttp.HandlerOpts{
			Registry: register,
		}))
		log.Println("Metrics server listening on :2112")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

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
			ef.FailedEmails.With(ProMetrics.Labels{"subject": n.Subject, "type": n.EventType}).Inc()
			continue
		}

		if n.To == "" || n.Subject == "" {
			log.Println("missing fields:", string(m.Value))
			ef.FailedEmails.With(ProMetrics.Labels{"subject": n.Subject, "type": n.EventType}).Inc()
			continue
		}

		if err := sendSMTP(n); err != nil {
			log.Println("smtp send failed:", err, "to:", n.To)
			ef.FailedEmails.With(ProMetrics.Labels{"subject": n.Subject, "type": n.EventType}).Inc()
			continue
		}
		log.Println("email sent:", n.To, "event:", n.EventType)
		es.SuccessfulEmails.With(ProMetrics.Labels{"subject": n.Subject, "type": n.EventType}).Inc()
		et.TotalEmails.Inc()

	}

}
