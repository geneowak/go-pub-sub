package main

import (
	"fmt"

	"github.com/geneowak/go-pub-sub/internal/gamelogic"
	"github.com/geneowak/go-pub-sub/internal/pubsub"
	"github.com/geneowak/go-pub-sub/internal/routing"
)

func handlerLog() func(routing.GameLog) pubsub.Acktype {
	return func(gl routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		gamelogic.WriteLog(gl)
		return pubsub.Ack
	}
}
