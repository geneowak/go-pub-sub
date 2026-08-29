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
	fmt.Println("Starting Peril client...")
	const connString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Unable to connect to rabbitmq server: %v", err)
	}
	defer conn.Close()

	publishChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	gs := gamelogic.NewGameState(username)

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, gs.GetUsername())
	// subscribe to pauses
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(publishChan, gs),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to pauses: %v", err)
	}
	// subscribe to moves
	queueName = fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gs.GetUsername())
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		queueName,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(publishChan, gs),
	)
	if err != nil {
		log.Fatalf("Failed to subscibe to moves channel: %v", err)
	}
	// subscribe to war
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(publishChan, gs),
	)
	if err != nil {
		log.Fatalf("Failed to subscibe to war channel: %v", err)
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		switch inputs[0] {
		case "spawn":
			err := gs.CommandSpawn(inputs)
			if err != nil {
				log.Println("Error spawning: ", err)
				continue
			}
		case "move":
			mv, err := gs.CommandMove(inputs)
			if err != nil {
				log.Println("Error moving: ", err)
				continue
			}
			err = pubsub.PublishJSON(
				publishChan,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+mv.Player.Username,
				mv,
			)
			if err != nil {
				log.Println("Failed to publish message", err)
			}
			log.Println("Move published successfully")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
