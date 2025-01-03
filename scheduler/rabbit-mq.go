package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const WORKER_COUNT = 20

type AsyncTransportDriver interface {
	Publish(ctx context.Context, exchange, routingKey string, message any) error
	Subscribe(ctx context.Context, queue string, handle func(message []byte) error) error
	CreateQueue(queue string) error
	CreateExchange(exchange string) error
	BindQueue(queue, exchange, routingKey string) error
}

type RabbitMqTransport struct {
	connection        *amqp.Connection
	channel           *amqp.Channel
	declaredQueues    []string
	declaredExchanges []string
	logger            *zap.SugaredLogger
}

func NewRabbitMqConnection(url string, logger *zap.SugaredLogger) (*RabbitMqTransport, error) {
	connection, err := amqp.Dial(url)
	if err != nil {
		logger.Errorf("error during opening rabbitmq connection - %v", err)
		return nil, err
	}

	channel, err := createChannel(connection, logger)
	if err != nil {
		return nil, err
	}

	transport := &RabbitMqTransport{
		connection: connection,
		channel:    channel,
		logger:     logger,
	}

	return transport, nil
}

func createChannel(conn *amqp.Connection, logger *zap.SugaredLogger) (*amqp.Channel, error) {
	channel, err := conn.Channel()
	if err != nil {
		logger.Errorf("error during opening rabbitmq channel - %v", err)
		return nil, err
	}

	return channel, nil
}

func (t *RabbitMqTransport) Publish(ctx context.Context, exchange, routingKey string, message any) error {
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

	err = t.channel.PublishWithContext(ctx, exchange,
		routingKey, false, false, msg)

	if err != nil {
		t.logger.Errorf("publish error - %v", err)
		return err
	}

	return nil
}

func (t *RabbitMqTransport) Subscribe(ctx context.Context, queue string, handle func(message []byte) error) error {
	err := t.CreateQueue(queue)
	if err != nil {
		return err
	}

	delivery, err := t.channel.ConsumeWithContext(ctx, queue, "", false,
		false, false, false, amqp.Table{})
	if err != nil {
		t.logger.Errorf("error during consume - %v", err)
		t.logger.Infoln("recreating channel")
		channel, err := createChannel(t.connection, t.logger)
		if err != nil {
			return err
		}

		t.channel = channel
	}

	sem := make(chan struct{}, WORKER_COUNT)

	for {
		sem <- struct{}{}
		go func(delivery amqp.Delivery, handle func(message []byte) error) {
			err := handle(delivery.Body)

			if err != nil {
				t.logger.Errorf("error during consumer processing - %v", err)

				err = delivery.Nack(true, false)
				if err != nil {
					t.logger.Errorf("error during nack - %v", err)
				}
			} else {
				err = delivery.Ack(true)
				if err != nil {
					t.logger.Errorf("error during ack - %v", err)
				}
			}

			<-sem
		}(<-delivery, handle)
	}
}

func (t *RabbitMqTransport) CreateQueue(queue string) error {
	queue = strings.Trim(queue, " ")

	if slices.Contains(t.declaredQueues, queue) {
		return nil
	}

	createdQueue, err := t.channel.QueueDeclare(queue, true, false, false,
		false, amqp.Table{})

	if err != nil {
		t.logger.Infof("creating queue error %s - %v", string(ExchangeJobStatus), err)
		return err
	}

	t.declaredQueues = append(t.declaredQueues, createdQueue.Name)

	return nil
}

func (t *RabbitMqTransport) CreateExchange(exchange string) error {
	exchange = strings.Trim(exchange, " ")

	if slices.Contains(t.declaredExchanges, exchange) {
		return nil
	}

	err := t.channel.ExchangeDeclare(exchange, "direct", true, false,
		false, false, amqp.Table{})
	if err != nil {
		t.logger.Infof("creating exchange error %s - %v", string(ExchangeJobSchedule), err)
		return err
	}

	t.declaredExchanges = append(t.declaredExchanges, exchange)

	return nil
}

func (t *RabbitMqTransport) BindQueue(queue, exchange, routingKey string) error {
	err := t.channel.QueueBind(queue, routingKey, exchange, false, amqp.Table{})

	if err != nil {
		t.logger.Infof("exchange %s queue %s with routing key %s binding error - %v", exchange, queue, routingKey, err)
		return err
	}

	return nil
}
