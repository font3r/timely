package transport

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"slices"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Transport struct {
	connection        *amqp.Connection
	channel           *amqp.Channel // TODO: performace - support for multiple channels
	declaredQueues    []string      // TODO: should we go for explicit/implicit queue/exchange declaration?
	declaredExchanges []string
}

// TODO: requirement - autocrete exchange/queue, some of the queues are temporary (one-time job),
// other are durable for cyclic jobs (both types should be created at api call)

func Create(channels int) (*Transport, error) {
	log.Printf("creating new connection")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Printf("error during opening connection - %v", err)
		return nil, err
	}

	log.Printf("creating new channel")
	channel, err := connection.Channel()

	if err != nil {
		log.Printf("error during opening channel - %v", err)
		return nil, err
	}

	transport := &Transport{
		connection: connection,
		channel:    channel,
	}

	return transport, nil
}

func (transport *Transport) Publish(exchange, routingKey string, message any) error {
	err := transport.CreateExchange(exchange)
	if err != nil {
		return err
	}

	data, err := json.Marshal(message)
	if err != nil {
		return errors.New("invalid message format")
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         data,
	}

	err = transport.channel.PublishWithContext(context.Background(), exchange,
		routingKey, false, false, msg)

	if err != nil {
		log.Printf("error during publish %v", err)
		return err
	}

	return nil
}

func (transport *Transport) Subscribe(queue string, handle func(jobSlug string, message []byte) error) error {
	err := transport.CreateQueue(queue)
	if err != nil {
		return err
	}

	delivery, err := transport.channel.ConsumeWithContext(context.Background(), queue, "", false,
		false, false, false, amqp.Table{})
	if err != nil {
		log.Printf("error during consumer %v", err)
		return err
	}

	for {
		rawMessage := <-delivery
		err = handle(rawMessage.RoutingKey, rawMessage.Body)
		if err != nil {
			rawMessage.Nack(false, false) // TODO: requirement - handle DLQ, do not requeue for now
		} else {
			rawMessage.Ack(false)
		}
	}
}

func (transport *Transport) CreateQueue(queue string) error {
	queue = strings.Trim(queue, " ")

	if slices.Contains(transport.declaredQueues, queue) {
		return nil
	}

	createdQueue, err := transport.channel.QueueDeclare(queue, true, false, false,
		false, amqp.Table{})

	if err != nil {
		log.Printf("error during creating queue %s - %s", string(ExchangeJobStatus), err)
		return err
	}

	log.Printf("declared queue %s", createdQueue.Name)
	transport.declaredQueues = append(transport.declaredQueues, createdQueue.Name)

	return nil
}

func (transport *Transport) CreateExchange(exchange string) error {
	exchange = strings.Trim(exchange, " ")

	if slices.Contains(transport.declaredExchanges, exchange) {
		return nil
	}

	err := transport.channel.ExchangeDeclare(exchange, "direct", true, false,
		false, false, amqp.Table{})
	if err != nil {
		log.Printf("error during creating exchange %s - %s", string(ExchangeJobSchedule), err)
		return err
	}

	log.Printf("declared exchange %s", exchange)
	transport.declaredExchanges = append(transport.declaredExchanges, exchange)

	return nil
}

func (transport *Transport) BindQueue(queue, exchange, routingKey string) error {
	err := transport.channel.QueueBind(queue, routingKey, exchange, false, amqp.Table{})

	if err != nil {
		log.Printf("error during queue %s - exchange %s binding, routing key - %s, err - %s",
			exchange, queue, routingKey, err)
		return err
	}

	log.Printf("bound queue %s to exchange %s with routing key %s",
		queue, exchange, routingKey)

	return nil
}
