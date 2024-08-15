package scheduler

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

func NewConnection() (*Transport, error) {
	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Printf("error during opening rabbitmq connection - %v", err)
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		log.Printf("error during opening rabbitmq channel - %v", err)
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
			log.Printf("error during consumer processing - %v\n", err)

			err = rawMessage.Nack(false, false)
			if err != nil {
				return err
			}
		} else {
			err = rawMessage.Ack(false)
			if err != nil {
				return err
			}
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

	return nil
}
