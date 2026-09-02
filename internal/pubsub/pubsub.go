package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int
type SimpleQueueType int

const (
	SimpleQueueTransient SimpleQueueType = iota
	SimpleQueueDurable
)

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

func PublishJSON[T any](
	ch *amqp.Channel,
	exchange, key string,
	val T,
) error {
	result, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Failed to marshal val: %w", err)
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        result,
		},
	)
}

func PublishGob[T any](
	ch *amqp.Channel,
	exchange, key string,
	val T,
) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("Failed to encode val: %w", err)
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buffer.Bytes(),
		},
	)
}

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

	table := make(amqp.Table)
	// tells RabbitMQ to send failed messages to the dead letter exchange
	table["x-dead-letter-exchange"] = "peril_dlx"
	queue, err := channel.QueueDeclare(
		queueName,                       // name
		queueType == SimpleQueueDurable, // durable
		queueType != SimpleQueueDurable, // delete when unused
		queueType != SimpleQueueDurable, // exclusive
		false,                           // no-wait
		table,                           // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	channel.QueueBind(queueName, key, exchange, false, nil)

	return channel, queue, nil
}
