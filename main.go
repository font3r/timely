package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	log "timely/logger"
	"timely/scheduler"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

var supported []string

type Application struct {
	Scheduler *scheduler.Scheduler
}

func main() {
	ctx := context.Background()

	loadConfig() // TODO: config validation

	r := mux.NewRouter()
	srv := &http.Server{
		Addr:    ":7468",
		Handler: handlers.CORS()(r),
	}

	app := buildDependencies(ctx)
	registerApiRoutes(r, app)

	log.Logger.Printf("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Logger.Println(err)
	}
}

func loadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			log.Logger.Panicf("no config file found - %s", err)
		} else {
			log.Logger.Panicf("config file error - %s", err)
		}
	}
}

func buildDependencies(ctx context.Context) Application {
	var pgStorage *scheduler.Pgsql
	if viper.IsSet("database.postgres") {
		pg, err := scheduler.NewPgsqlConnection(ctx,
			viper.GetString("database.postgres.connectionString"))
		if err != nil {
			log.Logger.Fatal(err)
		}

		pgStorage = pg
	}

	var rabbitMqTransport *scheduler.RabbitMqTransport
	if viper.IsSet("transport.rabbitmq") && viper.GetBool("transport.rabbitmq.enabled") {
		rabbitMq, err := scheduler.NewRabbitMqTransportConnection(
			viper.GetString("transport.rabbitmq.connectionString"))
		if err != nil {
			panic(fmt.Sprintf("create transport error %s", err))
		}

		err = createTransportDependencies(rabbitMqTransport)
		if err != nil {
			panic(fmt.Sprintf("creating internal exchanges/queues error - %v", err))
		}

		rabbitMqTransport = rabbitMq
		supported = append(supported, "rabbitmq")
	}

	var httpTransport scheduler.HttpTransport
	if viper.IsSet("transport.http") && viper.GetBool("transport.http.enabled") {
		httpTransport = scheduler.HttpTransport{}
		supported = append(supported, "http")
	}

	return Application{
		Scheduler: scheduler.Start(ctx, pgStorage, rabbitMqTransport, httpTransport,
			supported),
	}
}

func createTransportDependencies(rmq *scheduler.RabbitMqTransport) error {
	err := rmq.CreateExchange(string(scheduler.ExchangeJobSchedule))
	if err != nil {
		return nil
	}

	err = rmq.CreateExchange(string(scheduler.ExchangeJobStatus))
	if err != nil {
		return err
	}

	err = rmq.CreateQueue(string(scheduler.QueueJobStatus))
	if err != nil {
		return err
	}

	err = rmq.BindQueue(string(scheduler.QueueJobStatus),
		string(scheduler.ExchangeJobStatus), string(scheduler.RoutingKeyJobStatus))
	if err != nil {
		return err
	}

	return nil
}
