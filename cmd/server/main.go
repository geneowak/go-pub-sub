package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	const connString = "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal("Unable to connnect to rabbitmq server", err)
	}
	defer connection.Close()
	fmt.Println("Successfully connnected to rabbitmq server.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	fmt.Println("Shutting down...")
	time.Sleep(time.Second)
	fmt.Println("Program exited.")
}
