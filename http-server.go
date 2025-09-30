package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
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
			problem(w, http.StatusUnprocessableEntity, err)
			return
		}

		ok(w, result)
	}).Headers(scheduler.ContentTypeHeader, scheduler.ApplicationJson).Methods("POST")
}

// TODO: Cleanup validation for multiple invalid fields
func validateCreateSchedule(req *http.Request) (commands.CreateScheduleCommand, error) {
	comm := &commands.CreateScheduleCommand{}

	if err := json.NewDecoder(req.Body).Decode(&comm); err != nil {
		return commands.CreateScheduleCommand{}, err
	}

	var err error

	if comm.Description == "" {
		err = errors.Join(err, errors.New("invalid description"))
	}

	if comm.Frequency == "" {
		err = errors.Join(err, errors.New("missing frequency configuration"))
	}

	if comm.Frequency != string(scheduler.Once) {
		_, cronErr := scheduler.CronParser.Parse(comm.Frequency)
		if cronErr != nil {
			err = errors.Join(err, errors.New("invalid frequency configuration"))
		}
	}

	if comm.ScheduleStart != nil && time.Now().After(*comm.ScheduleStart) {
		err = errors.Join(err, errors.New("invalid schedule start"))
	}

	if comm.Job == (commands.JobConfiguration{}) {
		err = errors.Join(err, errors.New("missing job configuration"))
	}

	if comm.Job.Slug == "" {
		err = errors.Join(err, errors.New("invalid job slug"))
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
		vars := req.URL.Query()
		page, err := strconv.Atoi(vars.Get("page"))
		if err != nil || page <= 0 {
			problem(w, http.StatusBadRequest, errors.New("invalid page"))
			return
		}

		pageSize, err := strconv.Atoi(vars.Get("pageSize"))
		if err != nil || pageSize > 100 || pageSize <= 0 {
			problem(w, http.StatusBadRequest, errors.New("invalid pageSize"))
			return
		}

		q := queries.GetSchedules{Page: page, PageSize: pageSize}
		h := queries.GetSchedulesHandler{Storage: app.Scheduler.Storage}
		result, err := h.Handle(req.Context(), q)

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

		h := commands.DeleteScheduleHandler{
			AsyncTransport: app.Scheduler.AsyncTransport,
			Storage:        app.Scheduler.Storage,
			Logger:         app.Logger,
		}
		err = h.Handle(req.Context(), commands.DeleteSchedule{Id: id})

		if err != nil {
			problem(w, http.StatusUnprocessableEntity, err)
			return
		}

		noContent(w)
	}).Methods("DELETE")
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
