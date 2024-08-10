package transport

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Transport struct {
	connection *amqp.Connection
	channels   []*amqp.Channel
}

func Create(channels int) (*Transport, error) {
	log.Printf("creating new connection")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Printf("error during opening connection - %v", err)
		return nil, err
	}

	transport := &Transport{
		connection: connection,
	}

	for i := 0; i < channels; i++ {
		log.Printf("creating new channel %d", i)
		channel, err := connection.Channel()
		if err != nil {
			log.Printf("error during opening channel - %v", err)
		}

		transport.channels = append(transport.channels, channel)
	}

	if len(transport.channels) <= 0 {
		return nil, errors.New("failed to open any channel")
	}

	return transport, nil
}

func (transport *Transport) Publish(exchange string, routingKey string, message any) error {
	if len(transport.channels) <= 0 {
		return errors.New("cannot get free channel")
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

	err = transport.channels[rand.Intn(len(transport.channels))].
		PublishWithContext(context.Background(), exchange, routingKey, false, false, msg)

	if err != nil {
		log.Printf("error during publish %v", err)
		return err
	}

	log.Println("send message to exchange")

	return nil
}

func (transport *Transport) Subscribe(queue string, handle func(message []byte) error) error {
	delivery, err := transport.channels[0].ConsumeWithContext(context.Background(), queue, "", false,
		false, false, false, amqp.Table{})
	if err != nil {
		log.Printf("error during consumer %v", err)
		return err
	}

	for {
		rawMessage := <-delivery
		err = handle(rawMessage.Body)
		if err != nil {
			rawMessage.Nack(false, true)
		} else {
			rawMessage.Ack(false)
		}
	}
}
