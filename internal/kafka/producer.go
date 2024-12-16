package kafka

import (
	"errors"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	flushTimeout int = 5000 //ms
)

var errUnknownType error = errors.New("unknown event type")

type Producer struct {
	producer *kafka.Producer
}

func NewProducer(adress []string) (*Producer, error) {
	conf := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(adress, ","),
	}

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, err
	}

	return &Producer{producer: p}, nil
}

func (p *Producer) Produce(message, topic, key string) error {
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
		Key:   []byte(key),
	}

	kafkaChan := make(chan kafka.Event)

	// Отпрвили сообщение брокеру
	if err := p.producer.Produce(kafkaMsg, kafkaChan); err != nil {
		return err
	}

	// Ждем ответ
	event := <-kafkaChan

	switch ev := event.(type) {
	case *kafka.Message:
		return nil
	case kafka.Error:
		return ev
	default:
		return errUnknownType
	}
}

func (p *Producer) Close() {
	// Ждем пока сообщения дойдут,
	// либо по истечению таймаута закрываем
	p.producer.Flush(flushTimeout)
	p.producer.Close()
}
