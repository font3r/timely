package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/wiremock/go-wiremock"
	. "github.com/wiremock/wiremock-testcontainers-go"
)

func TestHttpSchedule(t *testing.T) {
	ctx := context.Background()
	wiremockCtr, err := startWiremock(ctx)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := wiremockCtr.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	endpoint, err := wiremockCtr.Endpoint(ctx, "http")
	if err != nil {
		t.Fatal(err)
	}

	wiremockClient := wiremock.NewClient(endpoint)
	defer wiremockClient.Reset()
	wiremockClient.StubFor(wiremock.Get(wiremock.URLPathEqualTo("/user")).
		WillReturnResponse(
			wiremock.NewResponse().
				WithJSONBody(map[string]interface{}{
					"code":   http.StatusBadRequest,
					"detail": "detail",
				}).
				WithHeader("Content-Type", "application/json").
				WithStatus(http.StatusBadRequest),
		))

	res, err := http.Get(fmt.Sprintf("%s/user", endpoint))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("did not receive expected status code - received %s", res.Status)
	}
}

func startWiremock(ctx context.Context) (*WireMockContainer, error) {
	wiremockCtr, err := RunContainer(ctx, WithImage("wiremock/wiremock:3.9.2"))
	if err != nil {
		return nil, err
	}

	return wiremockCtr, nil
}
