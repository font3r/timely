package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type SyncTransportDriver interface {
	Start(ctx context.Context, endpoint string, request ScheduleJobRequest) error
}

type HttpTransport struct {
	Logger *zap.SugaredLogger
}

type ScheduleJobRequest struct {
	ScheduleId uuid.UUID       `json:"scheduleId"`
	GroupId    uuid.UUID       `json:"groupId"`
	JobRunId   uuid.UUID       `json:"jobRunId"`
	Job        string          `json:"job"`
	Data       *map[string]any `json:"data"`
}

var InvalidScheduleStartResponse = Error{
	Code: "INVALID_SCHEDULE_START_RESPONSE",
	Msg:  "invalid http response"}

// TODO: Authorization, maybe with secret as header created during job registration
// Probably we should merge jobs for client and specify secret during client creation
func (ht HttpTransport) Start(ctx context.Context, url string, request ScheduleJobRequest) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, ApplicationJson, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("error during sending post to %s - %w", url, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return InvalidScheduleStartResponse
	}

	return nil
}
