package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	testjobhandler "timely/cmd"
	"timely/commands"
	log "timely/logger"
	"timely/queries"
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

func registerRoutes(router *mux.Router, app *Application) {
	v1 := router.PathPrefix("/api/v1").Subrouter()

	v1.HandleFunc("/schedules/{id}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		id, err := uuid.Parse(vars["id"])

		if err != nil {
			problem(w, errors.New("invalid schedule id"))
			return
		}

		h := queries.GetScheduleHandler{Storage: app.Scheduler.Storage}
		result, err := h.Handle(req.Context(), queries.GetSchedule{ScheduleId: id})

		if err != nil {
			problem(w, err)
			return
		}

		if result == (queries.ScheduleDto{}) {
			notFound(w)
			return
		}

		ok(w, result)
	}).Methods("GET")

	v1.HandleFunc("/schedules/{id}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		id, err := uuid.Parse(vars["id"])

		if err != nil {
			problem(w, errors.New("invalid schedule id"))
			return
		}

		h := commands.DeleteScheduleHandler{Storage: app.Scheduler.Storage}
		err = h.Handle(req.Context(), commands.DeleteSchedule{Id: id})

		if err != nil {
			problem(w, err)
			return
		}

		noContent(w)
	}).Methods("DELETE")

	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		schedules, err := app.Scheduler.Storage.GetAll(req.Context())
		if err != nil {
			problem(w, err)
		}

		for _, schedule := range schedules {
			err = app.Scheduler.Storage.DeleteScheduleById(req.Context(), schedule.Id)
			if err != nil {
				problem(w, err)
				return
			}
		}

		noContent(w)
	}).Methods("DELETE")

	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		h := commands.CreateScheduleHandler{
			Storage:   app.Scheduler.Storage,
			Transport: app.Scheduler.Transport,
		}

		result, err := h.Handle(req.Context(), req)
		if err != nil {
			problem(w, err)
			return
		}

		ok(w, result)
	}).Headers(scheduler.ContentTypeHeader, scheduler.ApplicationJson).Methods("POST")

	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		h := queries.GetScheduleHandler{Storage: app.Scheduler.Storage}
		result, err := h.Handle(req.Context(), queries.GetSchedule{ScheduleId: uuid.New()})

		if err != nil {
			problem(w, err)
			return
		}

		ok(w, result)
	}).Methods("GET")
}

func ok(w http.ResponseWriter, data any) {
	w.Header().Set(scheduler.ContentTypeHeader, scheduler.ApplicationJson)
	w.WriteHeader(http.StatusOK)

	jsonData, _ := json.Marshal(data)
	_, _ = w.Write(jsonData)
}

func noContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

func notFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
}

func problem(w http.ResponseWriter, err error) {
	w.Header().Set(scheduler.ContentTypeHeader, scheduler.ApplicationJson)
	w.WriteHeader(http.StatusBadRequest)

	var e scheduler.Error
	var data []byte
	if castOk := errors.As(err, &e); castOk {
		data, _ = json.Marshal(map[string]string{"code": e.Code, "error": e.Msg})
	} else {
		data, _ = json.Marshal(map[string]string{"error": err.Error()})
	}

	_, _ = w.Write(data)
}
