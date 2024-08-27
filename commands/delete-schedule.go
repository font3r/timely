package commands

import (
	"errors"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"net/http"
	"timely/scheduler"
)

func DeleteSchedule(req *http.Request, str *scheduler.Pgsql) error {
	vars := mux.Vars(req)

	id, err := uuid.Parse(vars["id"])
	if err != nil {
		return errors.New("invalid schedule id")
	}

	err = str.DeleteScheduleById(id)
	if err != nil {
		return err
	}

	return nil
}
