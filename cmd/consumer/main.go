package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/par1ram/kafka/internal/handler"
	"github.com/par1ram/kafka/internal/kafka"
	"github.com/sirupsen/logrus"
)

const (
	topic        string = "my-topic"
	cosumerGroup string = "my-consumer-group"
)

var adress = []string{"localhost:9091", "localhost:9092", "localhost:9093"}

func main() {
	h := handler.NewHandler()

	c1, err := kafka.NewConsumer(h, adress, topic, cosumerGroup, 1)
	if err != nil {
		logrus.Fatal(err)
	}
	c2, err := kafka.NewConsumer(h, adress, topic, cosumerGroup, 2)
	if err != nil {
		logrus.Fatal(err)
	}
	c3, err := kafka.NewConsumer(h, adress, topic, cosumerGroup, 3)
	if err != nil {
		logrus.Fatal(err)
	}

	// Записк консумера
	go func() {
		go c1.Start()
	}()
	go func() {
		go c2.Start()
	}()
	go func() {
		go c3.Start()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logrus.Error(c1.Stop())
	logrus.Error(c2.Stop())
	logrus.Error(c3.Stop())
}
