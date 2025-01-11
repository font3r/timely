package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	TimelyAdminChannel string = "timely-admin-channel"
	WorkerCount        int    = 20
)

type AsyncTransportDriver interface {
	Publish(ctx context.Context, exchange, routingKey string, message any) error
	Subscribe(ctx context.Context, queue string, handle func(message []byte) error) error
	CreateQueue(queue string) error
	DeleteQueue(queue string) error
	CreateExchange(exchange string) error
	BindQueue(queue, exchange, routingKey string) error
}

type RabbitMqTransport struct {
	rcvConnection *recoverableConnection
	channels      *sync.Map

	declaredQueues    []string
	declaredExchanges []string

	logger *zap.SugaredLogger
}

type recoverableConnection struct {
	connection *amqp.Connection
	attempt    int
}

func NewRabbitMqTransport(url string, logger *zap.SugaredLogger) (*RabbitMqTransport, error) {
	conn, err := establishRcvConn(url, logger)
	if err != nil {
		return nil, err
	}

	channelStore := &sync.Map{}
	transport := &RabbitMqTransport{
		rcvConnection: conn,
		channels:      channelStore,
		logger:        logger,
	}

	return transport, nil
}

func establishRcvConn(url string, logger *zap.SugaredLogger) (*recoverableConnection, error) {
	conn, err := connect(url)

	if err != nil {
		logger.Error(err)
		return nil, err
	}
	logger.Info("connected to rabbitmq")

	rcvConn := &recoverableConnection{connection: conn}
	go func(rConn *recoverableConnection, logger *zap.SugaredLogger) {
		closeErr := <-rConn.connection.NotifyClose(make(chan *amqp.Error))
		if closeErr != nil {
			logger.Errorf("rabbitmq non graceful connection close - %s", closeErr.Error())
		} else {
			logger.Warnln("rabbitmq graceful connection close")
		}

		for {
			rConn.attempt++
			conn, err := connect(url)
			if err != nil {
				logger.Error(err)
				time.Sleep(time.Second)
				continue
			}
			rConn.connection = conn
			break
		}
		logger.Infof("reconnected to rabbitmq after %d attempt", rConn.attempt)
	}(rcvConn, logger)

	return rcvConn, nil
}

func connect(url string) (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Properties: amqp.Table{
			"product":  "timely",
			"version":  "v0.1.0",
			"platform": "golang",
		}})

	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (t *RabbitMqTransport) getChannel(key string) (*amqp.Channel, error) {
	if channel, exists := t.channels.Load(key); exists {
		return channel.(*amqp.Channel), nil
	}

	channel, err := t.createChannel(key)
	if err != nil {
		return nil, err
	}

	t.channels.Store(key, channel)
	return channel, nil
}

func (t *RabbitMqTransport) createChannel(key string) (*amqp.Channel, error) {
	//t.logger.Infof("creating new channel for %s", key)
	channel, err := t.rcvConnection.connection.Channel()
	if err != nil {
		t.logger.Errorf("unable to open connection channel %s", err.Error())
		return nil, err
	}

	go func(channel *amqp.Channel, key string) {
		closeErr := <-channel.NotifyClose(make(chan *amqp.Error))
		if closeErr != nil {
			t.logger.Errorf("rabbitmq non graceful channel close - %s", closeErr.Error())
		} else {
			t.logger.Warnln("rabbitmq graceful channel close")
		}

		for {
			// wait for reconnection
			if t.rcvConnection.connection.IsClosed() {
				continue
			}

			channel, err := t.rcvConnection.connection.Channel()
			if err != nil {
				t.logger.Error(err)
				time.Sleep(time.Second)
				continue
			}
			t.channels.Store(key, channel)
			break
		}
		t.logger.Infof("created a new %s channel", key)
	}(channel, key)

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
		//CorrelationId   string // correlation identifier, might be usefull
	}

	chann, err := t.getChannel(exchange)
	if err != nil {
		t.logger.Errorf("get channel error - %v", err)
		return err
	}

	err = chann.PublishWithContext(ctx, exchange,
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

	chann, err := t.getChannel(queue)
	if err != nil {
		return err
	}

	delivery, err := chann.ConsumeWithContext(ctx, queue, "", false,
		false, false, false, amqp.Table{})
	if err != nil {
		t.logger.Errorf("error during rabbitmq consume - %v", err)
		return err
	}

	sem := make(chan struct{}, WorkerCount)

	for {
		sem <- struct{}{}
		go func(delivery amqp.Delivery, handle func(message []byte) error) {
			err := handle(delivery.Body)

			if err != nil {
				t.logger.Errorf("error during consumer action processing - %v", err)

				err = delivery.Nack(false, false)
				if err != nil {
					t.logger.Errorf("error during nack - %v", err)
				}
			} else {
				err = delivery.Ack(false)
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

	chann, err := t.getChannel(TimelyAdminChannel)
	if err != nil {
		return err
	}

	createdQueue, err := chann.QueueDeclare(queue, true, false, false,
		false, amqp.Table{})

	if err != nil {
		t.logger.Infof("creating queue error %s - %v", string(JobStatusExchange), err)
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

	chann, err := t.getChannel(TimelyAdminChannel)
	if err != nil {
		return err
	}

	err = chann.ExchangeDeclare(exchange, "direct", true, false,
		false, false, amqp.Table{})
	if err != nil {
		t.logger.Errorf("creating exchange error %s - %v", string(JobScheduleExchange), err)
		return err
	}

	t.declaredExchanges = append(t.declaredExchanges, exchange)

	return nil
}

func (t *RabbitMqTransport) BindQueue(queue, exchange, routingKey string) error {
	if !slices.Contains(t.declaredExchanges, exchange) {
		return errors.New("exchange has not beed declared")
	}

	if !slices.Contains(t.declaredQueues, queue) {
		return errors.New("queue has not beed declared")
	}

	chann, err := t.getChannel(TimelyAdminChannel)
	if err != nil {
		return err
	}

	err = chann.QueueBind(queue, routingKey, exchange, false, amqp.Table{})

	if err != nil {
		t.logger.Errorf("exchange %s queue %s with routing key %s binding error - %v", exchange, queue, routingKey, err)
		return err
	}

	return nil
}

func (t *RabbitMqTransport) DeleteQueue(queue string) error {
	channel, err := t.getChannel(TimelyAdminChannel)
	if err != nil {
		return err
	}

	_, err = channel.QueueDelete(queue, true, true, false)
	if err != nil {
		return err
	}

	return nil
}
