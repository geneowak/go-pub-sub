package main

import (
	"fmt"

	"github.com/geneowak/go-pub-sub/internal/gamelogic"
	"github.com/geneowak/go-pub-sub/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) string {
	return func(ps routing.PlayingState) string {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return "Ack"
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) string {
	return func(am gamelogic.ArmyMove) string {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeMakeWar:
			return "Ack"
		default:
			return "NackDiscard"
		}
	}
}
