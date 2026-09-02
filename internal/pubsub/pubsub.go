package pubsub

import (
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
