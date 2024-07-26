package main

import (
	"ServiceV2/config"
	"ServiceV2/db"
	"ServiceV2/handlers"
	"ServiceV2/integrations"
	"ServiceV2/services"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	conf := config.LoadConfig()

	dbConn, err := db.GetDBConnection(conf.PostgresUser, conf.PostgresPassword, conf.PostgresDB, conf.PostgresHost, conf.PostgresPort)
	if err != nil {
		log.Fatalf("Connection error: %v", err)
	}

	kafkaWriter := integrations.NewKafkaWriter(conf.KafkaBroker, conf.KafkaTopic)
	kafkaReader := integrations.NewKafkaReader(conf.KafkaBroker, conf.KafkaTopic)

	svc := services.NewService(dbConn, kafkaWriter, kafkaReader)
	handler := handlers.NewHandler(svc)

	http.HandleFunc("/messages", handler.MessagesHandler)
	http.HandleFunc("/statistics", handler.StatisticsHandler)

	ctx, cancel := context.WithCancel(context.Background())
	go svc.ConsumeKafkaMessage(ctx)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		cancel()
	}()

	log.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
