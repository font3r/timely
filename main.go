package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"timely/scheduler"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var supported []string

type Application struct {
	Scheduler *scheduler.Scheduler
	Logger    *zap.SugaredLogger
}

func main() {
	ctx := context.Background()
	baseLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("can't initialize zap logger: %v", err))
	}

	logger := baseLogger.Sugar()
	defer logger.Sync()

	loadConfig(logger) // TODO: config validation

	r := mux.NewRouter()
	srv := &http.Server{
		Addr: ":7468",
		Handler: handlers.CORS(
			handlers.AllowedMethods([]string{"GET", "POST", "DELETE"}),
			handlers.AllowedHeaders([]string{"Content-Type"}),
			handlers.AllowedOrigins([]string{"http://localhost:3000"}),
		)(r),
	}

	app := buildDependencies(ctx, logger)
	registerApiRoutes(r, app)

	logger.Infof("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		logger.Error(err)
	}
}

func loadConfig(log *zap.SugaredLogger) {
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			log.Panicf("no config file found - %s", err)
		} else {
			log.Panicf("config file error - %s", err)
		}
	}
}

func buildDependencies(ctx context.Context, logger *zap.SugaredLogger) Application {
	var pgStorage *scheduler.Pgsql
	if viper.IsSet("database.postgres") {
		pg, err := scheduler.NewPgsqlConnection(ctx,
			viper.GetString("database.postgres.connectionString"))
		if err != nil {
			logger.Panic(err)
		}

		pgStorage = pg
	}

	var rabbitMqTransport *scheduler.RabbitMqTransport
	if viper.IsSet("transport.rabbitmq") && viper.GetBool("transport.rabbitmq.enabled") {
		rabbitMq, err := scheduler.NewRabbitMqConnection(
			viper.GetString("transport.rabbitmq.connectionString"), logger)
		if err != nil {
			logger.Panicf(fmt.Sprintf("create transport error %s", err))
		}

		err = createTransportDependencies(rabbitMq)
		if err != nil {
			logger.Panicf(fmt.Sprintf("creating internal exchanges/queues error - %v", err))
		}

		rabbitMqTransport = rabbitMq
		supported = append(supported, "rabbitmq")
	}

	var httpTransport scheduler.HttpTransport
	if viper.IsSet("transport.http") && viper.GetBool("transport.http.enabled") {
		httpTransport = scheduler.HttpTransport{
			Logger: logger,
		}
		supported = append(supported, "http")
	}

	return Application{
		Scheduler: scheduler.Start(ctx, pgStorage, rabbitMqTransport, httpTransport,
			supported, logger),
		Logger: logger,
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
