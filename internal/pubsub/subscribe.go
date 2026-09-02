package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
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
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
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
	defer fmt.Println("> ")
	go func() {
		defer ch.Close()
		for msg := range msgs {
			var body T
			buffer := bytes.NewBuffer(msg.Body)
			decoder := gob.NewDecoder(buffer)
			err := decoder.Decode(&body)
			if err != nil {
				log.Println("Failed to Unmarshal: ", err)
				continue
			}
			ack := handler(body)
			switch ack {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}
