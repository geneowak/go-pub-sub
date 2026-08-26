package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/geneowak/go-pub-sub/internal/pubsub"
	"github.com/geneowak/go-pub-sub/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	const connString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal("Unable to connnect to rabbitmq server", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connnected to rabbitmq server.")

	publishChan, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to create channel: ", err)
	}
	err = pubsub.PublishJSON(
		publishChan,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: true},
	)

	if err != nil {
		log.Println("Failed to publish: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	fmt.Println("Shutting down...")
	time.Sleep(time.Second)
	fmt.Println("Program exited.")
}
