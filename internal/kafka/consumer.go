package kafka

import (
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	sessionTimeout int           = 6000 //ms
	noTimeout      time.Duration = -1
)

type Handler interface {
	HandleMessage(message []byte, topic kafka.TopicPartition, cn int) error
}

type Consumer struct {
	consumer       *kafka.Consumer
	handler        Handler
	stop           bool
	consumerNumber int
}

func NewConsumer(h Handler, adress []string, topic, consumerGroup string, consumerNumber int) (*Consumer, error) {
	cfg := kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(adress, ","),
		"group.id":                 consumerGroup,
		"session.timeout.ms":       sessionTimeout,
		"enable.auto.offset.store": false,      // Сохраняем оффсет вручную
		"enable.auto.commit":       true,       // by default
		"auto.commit.interval.ms":  5000,       //ms
		"auto.offset.reset":        "earliest", // Прочитаем все сообщения в партиции
	}

	c, err := kafka.NewConsumer(&cfg)
	if err != nil {
		return nil, err
	}

	if err := c.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	return &Consumer{consumer: c, handler: h, consumerNumber: consumerNumber}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			break
		}

		// Читаем сообщение
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			logrus.Error(err)
		}

		if kafkaMsg == nil {
			continue
		}

		// Обрабатываем сообщение
		if err := c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition, c.consumerNumber); err != nil {
			logrus.Error(err)
			continue
		}

		// Сохраняем сообщение вручную для последующего коммита
		if _, err := c.consumer.StoreMessage(kafkaMsg); err != nil {
			logrus.Error(err)
			continue
		}
	}
}

func (c *Consumer) Stop() error {
	c.stop = true

	// Коммитим сообщение на случай, если интревал не истек
	if _, err := c.consumer.Commit(); err != nil {
		return err
	}
	logrus.Infof("Commited offset")

	return c.consumer.Close()
}
