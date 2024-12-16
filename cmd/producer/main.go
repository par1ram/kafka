package main

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/par1ram/kafka/internal/kafka"
	"github.com/sirupsen/logrus"
)

const (
	topic        string = "my-topic"
	numberOfKeys int    = 30
)

var adress = []string{"localhost:9091", "localhost:9092", "localhost:9093"}

func main() {
	p, err := kafka.NewProducer(adress)
	if err != nil {
		logrus.Fatal(err)
	}

	keys := generateUUID()
	for i := 0; i < 100; i++ {
		key := keys[i%numberOfKeys]

		msg := fmt.Sprintln("Kafka message:", i)
		if err := p.Produce(msg, topic, key); err != nil {
			logrus.Error(err)
		}
	}
}

func generateUUID() [numberOfKeys]string {
	var uuids [numberOfKeys]string

	for i := range uuids {
		uuids[i] = uuid.NewString()
	}

	return uuids
}
