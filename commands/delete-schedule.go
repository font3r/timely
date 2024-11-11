package commands

import (
	"context"
	"timely/scheduler"

	"github.com/google/uuid"
)

type DeleteSchedule struct {
	Id uuid.UUID
}

type DeleteScheduleHandler struct {
	Storage scheduler.StorageDriver
}

func (h DeleteScheduleHandler) Handle(ctx context.Context, c DeleteSchedule) error {
	err := h.Storage.DeleteScheduleById(ctx, c.Id)
	if err != nil {
		return err
	}

	return nil
}
