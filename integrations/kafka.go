package integrations

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	Writer *kafka.Writer
}

type KafkaReader struct {
	Reader *kafka.Reader
}

func NewKafkaWriter(broker, topic string) *KafkaWriter {
	return &KafkaWriter{
		Writer: &kafka.Writer{
			Addr:     kafka.TCP(broker),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (k *KafkaWriter) SendMessage(id int, content string) error {
	return k.Writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", id)),
		Value: []byte(content),
	})
}

func NewKafkaReader(broker, topic string) *KafkaReader {
	return &KafkaReader{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{broker},
			Topic:     topic,
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		}),
	}
}

func (k *KafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return k.Reader.ReadMessage(ctx)
}
