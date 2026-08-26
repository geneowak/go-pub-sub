package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	result, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Failed to marshal val: %w", err)
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        result,
	})
}

type SimpleQueueType string

const (
	Transient SimpleQueueType = "transient"
	Durable   SimpleQueueType = "durable"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		queueType == Durable,
		queueType == Transient,
		queueType == Transient,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	channel.QueueBind(queueName, key, exchange, false, nil)

	return channel, queue, nil
}
