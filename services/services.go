package services

import (
	"ServiceV2/db"
	"ServiceV2/integrations"
	"ServiceV2/models"
	"context"
	"fmt"
)

type Service struct {
	DB          *db.Connection
	KafkaWriter *integrations.KafkaWriter
	KafkaReader *integrations.KafkaReader
}

func NewService(dbConn *db.Connection, kafkaWriter *integrations.KafkaWriter, kafkaReader *integrations.KafkaReader) *Service {
	return &Service{
		DB:          dbConn,
		KafkaWriter: kafkaWriter,
		KafkaReader: kafkaReader,
	}
}

func (s *Service) ProcessRequest(content []models.Messages) error {
	for _, msg := range content {
		id, err := s.DB.SaveMessage(msg.Content)
		err = s.KafkaWriter.SendMessage(id, msg.Content)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) GetStats() (int, error) {
	count, err := s.DB.GetProcessedCount()
	if err != nil {
		return -1, err
	}
	return count, nil
}

func (s *Service) ConsumeKafkaMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping Kafka message consumption")
			return
		default:
			msg, err := s.KafkaReader.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					fmt.Println("Context canceled")
					return
				}
				fmt.Printf("Failed reading: %v\n", err)
				continue
			}
			var id int
			_, err = fmt.Sscanf(string(msg.Key), "%d", &id)
			if err != nil {
				fmt.Printf("Parsing error: %v\n", err)
			}

			err = s.DB.UpdateMessageStatus(id, db.PROCESSED)
			if err != nil {
				fmt.Printf("Failed to update message status in database: %v\n", err)
			}
		}
	}
}
