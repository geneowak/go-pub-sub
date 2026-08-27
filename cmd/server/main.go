package main

import (
	"fmt"
	"log"
	"time"

	"github.com/geneowak/go-pub-sub/internal/gamelogic"
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

	gamelogic.PrintServerHelp()

	for {
		inputs := gamelogic.GetInput()
		if inputs == nil {
			continue
		}
		if inputs[0] == "pause" {
			fmt.Println("Game has been paused...")
			err = pubsub.PublishJSON(
				publishChan,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)

			if err != nil {
				fmt.Println("Failed to publish: %w", err)
			}
			continue
		}
		if inputs[0] == "resume" {
			fmt.Println("Game is being resumed...")
			err = pubsub.PublishJSON(
				publishChan,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)

			if err != nil {
				fmt.Println("Failed to publish: %w", err)
			}
			continue
		}
		if inputs[0] == "quit" {
			break
		}
	}

	fmt.Println("Shutting down...")
	time.Sleep(time.Second)
	fmt.Println("Program exited.")
}
