package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	log "timely/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AsyncTransportDriver interface {
	Publish(exchange, routingKey string, message any) error
	Subscribe(queue string, handle func(message []byte) error) error
	CreateQueue(queue string) error
	CreateExchange(exchange string) error
	BindQueue(queue, exchange, routingKey string) error
}

type Transport struct {
	connection        *amqp.Connection
	channel           *amqp.Channel // TODO: performace - support for multiple channels - channel is very fragile, closed on error
	declaredQueues    []string
	declaredExchanges []string
}

func NewTransportConnection(url string) (*Transport, error) {
	connection, err := amqp.Dial(url)
	if err != nil {
		log.Logger.Printf("error during opening rabbitmq connection - %v", err)
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		log.Logger.Printf("error during opening rabbitmq channel - %v", err)
		return nil, err
	}

	transport := &Transport{
		connection: connection,
		channel:    channel,
	}

	return transport, nil
}

func (t *Transport) Publish(exchange, routingKey string, message any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return errors.New("invalid message format")
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  ApplicationJson,
		Body:         data,
	}

	err = t.channel.PublishWithContext(context.Background(), exchange,
		routingKey, false, false, msg)

	if err != nil {
		log.Logger.Printf("publish error - %v", err)
		return err
	}

	return nil
}

func (t *Transport) Subscribe(queue string, handle func(message []byte) error) error {
	err := t.CreateQueue(queue)
	if err != nil {
		return err
	}

	delivery, err := t.channel.ConsumeWithContext(context.Background(), queue, "", false,
		false, false, false, amqp.Table{})
	if err != nil {
		log.Logger.Printf("error during consumer - %v\n", err)
		return err
	}

	for {
		// TODO: probably it would be safer to limit number of goroutines
		go func(delivery amqp.Delivery, handle func(message []byte) error) {
			err := handle(delivery.Body)

			if err != nil {
				log.Logger.Printf("error during consumer processing - %v\n", err)

				err = delivery.Nack(false, false)
				if err != nil {
					log.Logger.Printf("error during nack - %v\n", err)
				}
			} else {
				err = delivery.Ack(false)
				if err != nil {
					log.Logger.Printf("error during ack - %v\n", err)
				}
			}
		}(<-delivery, handle)
	}
}

func (t *Transport) CreateQueue(queue string) error {
	queue = strings.Trim(queue, " ")

	if slices.Contains(t.declaredQueues, queue) {
		return nil
	}

	createdQueue, err := t.channel.QueueDeclare(queue, true, false, false,
		false, amqp.Table{})

	if err != nil {
		log.Logger.Printf("creating queue error %s - %v\n", string(ExchangeJobStatus), err)
		return err
	}

	t.declaredQueues = append(t.declaredQueues, createdQueue.Name)

	return nil
}

func (t *Transport) CreateExchange(exchange string) error {
	exchange = strings.Trim(exchange, " ")

	if slices.Contains(t.declaredExchanges, exchange) {
		return nil
	}

	err := t.channel.ExchangeDeclare(exchange, "direct", true, false,
		false, false, amqp.Table{})
	if err != nil {
		log.Logger.Printf("creating exchange error %s - %v\n", string(ExchangeJobSchedule), err)
		return err
	}

	t.declaredExchanges = append(t.declaredExchanges, exchange)

	return nil
}

func (t *Transport) BindQueue(queue, exchange, routingKey string) error {
	err := t.channel.QueueBind(queue, routingKey, exchange, false, amqp.Table{})

	if err != nil {
		log.Logger.Printf("exchange %s queue %s with routing key %s binding error - %v\n", exchange, queue, routingKey, err)
		return err
	}

	return nil
}
