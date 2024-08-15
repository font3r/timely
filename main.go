package main

import (
	"encoding/json"
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
	r := mux.NewRouter()
	srv := &http.Server{
		Addr:    ":5000",
		Handler: r,
	}

	storage, err := scheduler.NewJobStorage("postgres://postgres:password@127.0.0.1:5432/Timely")
	if err != nil {
		log.Fatal(err)
	}

	app := &Application{
		Scheduler: scheduler.Start(storage),
	}

	registerRoutes(r, app)
	go testjobhandler.Start()

	log.Printf("listening on %v", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Println(err)
	}
}

func registerRoutes(router *mux.Router, app *Application) {
	v1 := router.PathPrefix("/api/v1").Subrouter()

	v1.HandleFunc("/jobs/{id}", func(w http.ResponseWriter, req *http.Request) {
		result, err := queries.GetJob(req, app.Scheduler.Storage)
		if err != nil {
			problem(w, err)
			return
		}

		success(w, result)
	}).Methods("GET")

	v1.HandleFunc("/jobs", func(w http.ResponseWriter, req *http.Request) {
		result, err := commands.CreateJob(req, app.Scheduler.Storage)
		if err != nil {
			problem(w, err)
			return
		}

		success(w, result)
	}).Headers(scheduler.ContentTypeHeader, scheduler.ApplicationJson).Methods("POST")
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
