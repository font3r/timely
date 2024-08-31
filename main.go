package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	testjobhandler "timely/cmd"
	log "timely/logger"
	"timely/scheduler"

	"github.com/spf13/viper"

	"github.com/gorilla/mux"
)

type Application struct {
	Scheduler *scheduler.Scheduler
}

func main() {
	loadConfig()

	r := mux.NewRouter()
	srv := &http.Server{
		Addr:    ":5000",
		Handler: r,
	}

	storage, err := scheduler.NewPgsqlConnection(context.Background(), viper.GetString("database.connectionString"))
	if err != nil {
		log.Logger.Fatal(err)
	}

	transport, err := scheduler.NewTransportConnection(viper.GetString("transport.rabbitmq.connectionString"))
	if err != nil {
		panic(fmt.Sprintf("create transport error %s", err))
	}

	app := &Application{
		Scheduler: scheduler.Start(context.Background(), storage, transport),
	}

	registerRoutes(r, app)
	go testjobhandler.Start()

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
			log.Logger.Panicf("No config file found - %s", err)
		} else {
			log.Logger.Panicf("Config file error - %s", err)
		}
	}
}
