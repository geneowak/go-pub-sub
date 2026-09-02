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

func handlerPause(
	ch *amqp.Channel,
	gs *gamelogic.GameState,
) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(
	ch *amqp.Channel,
	gs *gamelogic.GameState,
) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(am gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			key := routing.WarRecognitionsPrefix + "." + gs.GetUsername()
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				key,
				gamelogic.RecognitionOfWar{
					Attacker: am.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				log.Println("Failed to publish message", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(
	ch *amqp.Channel,
	gs *gamelogic.GameState,
) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			log := fmt.Sprintf("%s won against %s", winner, loser)
			gamelog := routing.GameLog{
				CurrentTime: time.Now().UTC(),
				Message:     log,
				Username:    gs.GetUsername(),
			}
			return publisGameLog(ch, rw, gamelog)
		case gamelogic.WarOutcomeYouWon:
			log := fmt.Sprintf("%s won against %s", winner, loser)
			gamelog := routing.GameLog{
				CurrentTime: time.Now().UTC(),
				Message:     log,
				Username:    gs.GetUsername(),
			}
			return publisGameLog(ch, rw, gamelog)
		case gamelogic.WarOutcomeDraw:
			log := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			gamelog := routing.GameLog{
				CurrentTime: time.Now().UTC(),
				Message:     log,
				Username:    gs.GetUsername(),
			}
			return publisGameLog(ch, rw, gamelog)
		default:
			fmt.Println("Error: war outcome not determinable.")
			return pubsub.NackDiscard
		}
	}
}

func publisGameLog(
	ch *amqp.Channel,
	rw gamelogic.RecognitionOfWar,
	gl routing.GameLog,
) pubsub.Acktype {
	key := routing.GameLogSlug + "." + rw.Attacker.Username
	err := pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		key,
		gl,
	)
	if err != nil {
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
