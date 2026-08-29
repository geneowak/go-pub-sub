package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

type SimpleQueueType int

const (
	SimpleQueueTransient SimpleQueueType = iota
	SimpleQueueDurable
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) string,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("Failed to declare and bind: %w", err)
	}

	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to setup consumer: %w", err)
	}
	go func() {
		defer ch.Close()
		for msg := range msgs {
			var body T
			err := json.Unmarshal(msg.Body, &body)
			if err != nil {
				log.Println("Failed to Unmarshal: ", err)
				continue
			}
			ack := handler(body)
			switch ack {
			case "Ack":
				msg.Ack(false)
				log.Println("Message acknowledged")
			case "NackRequeue":
				msg.Nack(false, true)
				log.Println("Message requeued")
			case "NackDiscard":
				msg.Nack(false, false)
				log.Println("Message discarded")
			}
		}
	}()

	return nil
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

	queue, err := channel.QueueDeclare(
		queueName,                       // name
		queueType == SimpleQueueDurable, // durable
		queueType != SimpleQueueDurable, // delete when unused
		queueType != SimpleQueueDurable, // exclusive
		false,                           // no-wait
		nil,                             // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	channel.QueueBind(queueName, key, exchange, false, nil)

	return channel, queue, nil
}
