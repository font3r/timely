package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"
	"timely/commands"
	"timely/queries"
	"timely/scheduler"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

func registerApiRoutes(router *mux.Router, app Application) {
	v1 := router.PathPrefix("/api/v1").Subrouter()

	createSchedule(v1, app)
	getSchedule(v1, app)
	getSchedules(v1, app)
	deleteSchedule(v1, app)
	deleteSchedules(v1, app)

	processJobEvent(v1, app)
}

func createSchedule(v1 *mux.Router, app Application) {
	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		c, err := validateCreateSchedule(req)
		if err != nil {
			problem(w, http.StatusBadRequest, err)
			return
		}

		h := commands.CreateScheduleHandler{
			Storage:        app.Scheduler.Storage,
			AsyncTransport: app.Scheduler.AsyncTransport,
		}

		result, err := h.Handle(req.Context(), c)
		if err != nil {
			if errors.Is(err, commands.ErrJobScheduleConflict) {
				problem(w, http.StatusConflict, err)
				return
			}

			problem(w, http.StatusUnprocessableEntity, err)
			return
		}

		ok(w, result)
	}).Headers(scheduler.ContentTypeHeader, scheduler.ApplicationJson).Methods("POST")
}

func validateCreateSchedule(req *http.Request) (commands.CreateScheduleCommand, error) {
	comm := &commands.CreateScheduleCommand{}

	if err := json.NewDecoder(req.Body).Decode(&comm); err != nil {
		return commands.CreateScheduleCommand{}, err
	}

	var err error

	if comm.Description == "" {
		err = errors.Join(errors.New("invalid description"))
	}

	if comm.Frequency == "" {
		err = errors.Join(errors.New("missing frequency configuration"))
	}

	if comm.Frequency != string(scheduler.Once) {
		_, err = scheduler.CronParser.Parse(comm.Frequency)
		if err != nil {
			err = errors.Join(errors.New("invalid frequency configuration"))
		}
	}

	if comm.ScheduleStart != nil && time.Now().After(*comm.ScheduleStart) {
		err = errors.Join(errors.New("invalid schedule start"))
	}

	if comm.Job == (commands.JobConfiguration{}) {
		err = errors.Join(errors.New("missing job configuration"))
	}

	if comm.Job.Slug == "" {
		err = errors.Join(errors.New("invalid job slug"))
	}

	if comm.Configuration == (commands.ScheduleConfiguration{}) {
		err = errors.Join(errors.New("missing schedule configuration"))
	}

	// TODO: this would have to be validated against scheduler configurable available transports
	if comm.Configuration.TransportType != scheduler.Http && comm.Configuration.TransportType != scheduler.Rabbitmq {
		err = errors.Join(errors.New("invalid transport type"))
	}

	if comm.Configuration.TransportType == scheduler.Http {
		if comm.Configuration.Url == "" {
			err = errors.Join(errors.New("missing url for http transport"))
		} else {
			_, err = url.ParseRequestURI(comm.Configuration.Url)
			if err != nil {
				err = errors.Join(errors.New("invalid url for http transport"))
			}
		}
	}

	if err != nil {
		return commands.CreateScheduleCommand{}, err
	}

	return *comm, nil
}

func getSchedule(v1 *mux.Router, app Application) {
	v1.HandleFunc("/schedules/{id}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		id, err := uuid.Parse(vars["id"])

		if err != nil {
			problem(w, http.StatusBadRequest, errors.New("invalid schedule id"))
			return
		}

		h := queries.GetScheduleHandler{Storage: app.Scheduler.Storage}
		result, err := h.Handle(req.Context(), queries.GetSchedule{ScheduleId: id})

		if err != nil {
			if errors.Is(err, queries.ErrScheduleNotFound) {
				problem(w, http.StatusNotFound, err)
				return
			}

			problem(w, http.StatusUnprocessableEntity, err)
			return
		}

		ok(w, result)
	}).Methods("GET")
}

func getSchedules(v1 *mux.Router, app Application) {
	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		h := queries.GetSchedulesHandler{Storage: app.Scheduler.Storage}
		result, err := h.Handle(req.Context())

		if err != nil {
			problem(w, http.StatusUnprocessableEntity, err)
			return
		}

		ok(w, result)
	}).Methods("GET")
}

func deleteSchedule(v1 *mux.Router, app Application) {
	v1.HandleFunc("/schedules/{id}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		id, err := uuid.Parse(vars["id"])

		if err != nil {
			problem(w, http.StatusBadRequest, errors.New("invalid schedule id"))
			return
		}

		h := commands.DeleteScheduleHandler{Storage: app.Scheduler.Storage}
		err = h.Handle(req.Context(), commands.DeleteSchedule{Id: id})

		if err != nil {
			problem(w, http.StatusUnprocessableEntity, err)
			return
		}

		noContent(w)
	}).Methods("DELETE")
}

func deleteSchedules(v1 *mux.Router, app Application) {
	v1.HandleFunc("/schedules", func(w http.ResponseWriter, req *http.Request) {
		schedules, err := app.Scheduler.Storage.GetAll(req.Context())
		if err != nil {
			problem(w, http.StatusUnprocessableEntity, err)
			return
		}

		for _, schedule := range schedules {
			err = app.Scheduler.Storage.DeleteScheduleById(req.Context(), schedule.Id)
			if err != nil {
				problem(w, http.StatusUnprocessableEntity, err)
				return
			}
		}

		noContent(w)
	}).Methods("DELETE")
}

func processJobEvent(v1 *mux.Router, app Application) {
	v1.HandleFunc("/schedules/status", func(w http.ResponseWriter, req *http.Request) {
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = scheduler.HandleJobEvent(req.Context(), payload, app.Scheduler.Storage)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})
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

func problem(w http.ResponseWriter, statusCode int, err error) {
	w.Header().Set(scheduler.ContentTypeHeader, scheduler.ApplicationJson)
	w.WriteHeader(statusCode)

	var e scheduler.Error
	var data []byte
	if castOk := errors.As(err, &e); castOk {
		data, _ = json.Marshal(map[string]string{"code": e.Code, "error": e.Msg})
	} else {
		data, _ = json.Marshal(map[string]string{"error": err.Error()})
	}

	_, _ = w.Write(data)
}
