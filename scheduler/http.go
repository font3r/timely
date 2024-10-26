package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	log "timely/logger"

	"github.com/google/uuid"
)

type SyncTransportDriver interface {
	Start(ctx context.Context, endpoint string, request ScheduleJobRequest) error
}

type HttpTransport struct {
}

type ScheduleJobRequest struct {
	ScheduleId uuid.UUID       `json:"schedule_id"`
	JobRunId   uuid.UUID       `json:"job_run_id"`
	Job        string          `json:"job"`
	Data       *map[string]any `json:"data"`
}

var InvalidScheduleStartResponse = Error{
	Code: "INVALID_SCHEDULE_START_RESPONSE",
	Msg:  "invalid http response"}

// TODO: Authorization, maybe with secret as header created during job registration
func (ht HttpTransport) Start(ctx context.Context, url string, request ScheduleJobRequest) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	log.Logger.Printf("sending schedule start http request to %s\n", url)
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
