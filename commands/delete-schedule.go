package commands

import (
	"context"
	"timely/scheduler"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DeleteSchedule struct {
	Id uuid.UUID
}

type DeleteScheduleHandler struct {
	AsyncTransport scheduler.AsyncTransportDriver
	Storage        scheduler.StorageDriver
	Logger         *zap.SugaredLogger
}

var (
	ErrTransportError = scheduler.Error{
		Code: "TRANSPORT_ERROR",
		Msg:  "transport driver error",
	}
	ErrScheduleNotFound = scheduler.Error{
		Code: "SCHEDULE_NOT_FOUND",
		Msg:  "schedule not found",
	}
)

func (h DeleteScheduleHandler) Handle(ctx context.Context, c DeleteSchedule) error {
	sch, err := h.Storage.GetScheduleById(ctx, c.Id)
	if err != nil {
		return err
	}

	if sch == nil {
		return ErrScheduleNotFound
	}

	err = h.Storage.DeleteScheduleById(ctx, sch.Id)
	if err != nil {
		return err
	}

	err = h.AsyncTransport.DeleteQueue(sch.Job.Slug)
	if err != nil {
		h.Logger.Errorf("error during deleting queue - %s", err)
		return ErrTransportError
	}

	return nil
}
