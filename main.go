package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"net/http"
	testjobhandler "timely/cmd"
	"timely/commands"
	"timely/queries"
	"timely/scheduler"

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

	storage, err := scheduler.NewJobStorage(viper.GetString("database.connectionString"))
	if err != nil {
		log.Fatal(err)
	}

	transport, err := scheduler.NewConnection(viper.GetString("transport.rabbitmq.connectionString"))
	if err != nil {
		panic(fmt.Sprintf("create transport error %s", err))
	}

	app := &Application{
		Scheduler: scheduler.Start(storage, transport),
	}

	registerRoutes(r, app)
	go testjobhandler.Start()

	log.Printf("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Println(err)
	}
}

func loadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			log.Panicf("No config file found - %s", err)
		} else {
			log.Panicf("Config file error - %s", err)
		}
	}
}

func registerRoutes(router *mux.Router, app *Application) {
	v1 := router.PathPrefix("/api/v1").Subrouter()

	v1.HandleFunc("/schedules/{id}", func(w http.ResponseWriter, req *http.Request) {
		result, err := queries.GetSchedule(req, app.Scheduler.Storage)
		if err != nil {
			problem(w, err)
			return
		}

		success(w, result)
	}).Methods("GET")

	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		result, err := commands.CreateSchedule(req, app.Scheduler.Storage, app.Scheduler.Transport)
		if err != nil {
			problem(w, err)
			return
		}

		success(w, result)
	}).Headers(scheduler.ContentTypeHeader, scheduler.ApplicationJson).Methods("POST")

	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		result, err := queries.GetSchedules(app.Scheduler.Storage)
		if err != nil {
			problem(w, err)
			return
		}

		success(w, result)
	}).Methods("GET")
}

func success(w http.ResponseWriter, data any) {
	w.Header().Set(scheduler.ContentTypeHeader, scheduler.ApplicationJson)

	jsonData, _ := json.Marshal(data)
	w.Write(jsonData)
}

func problem(w http.ResponseWriter, err error) {
	w.Header().Set(scheduler.ContentTypeHeader, scheduler.ApplicationJson)
	w.WriteHeader(http.StatusBadRequest)

	jsonData, _ := json.Marshal(map[string]string{"error": err.Error()})
	w.Write(jsonData)
}
