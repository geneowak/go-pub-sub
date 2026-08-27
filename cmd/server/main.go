package main

import (
	"fmt"
	"log"

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
		if len(inputs) == 0 {
			continue
		}
		switch inputs[0] {
		case "pause":
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
		case "resume":
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
		case "quit":
			log.Println("See you later...")
			return
		default:
			fmt.Println("unknown command")
		}
	}
}
