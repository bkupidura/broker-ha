package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"net/http"

	"broker/discovery"

	"github.com/alexliesenfeld/health"
	"github.com/go-chi/render"
	mqtt "github.com/mochi-co/mqtt/server"
)

// readyHandler returns handler with health.Checker used by /ready endpoint.
// It checks discovery (memberlist) healthscore.
func readyHandler(disco *discovery.Discovery) http.Handler {
	readinessProbe := health.NewChecker(
		health.WithCacheDuration(1*time.Second),
		health.WithTimeout(10*time.Second),
		health.WithCheck(health.Check{
			Name:    "readiness_cluster_health",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				healthScore := disco.GetHealthScore()
				if healthScore > 0 {
					return fmt.Errorf("health score %d", healthScore)
				}
				return nil
			},
		}),
	)
	return health.NewHandler(readinessProbe)
}

// healthzHandler returns handler with health.Checker used by /healthz endpoint.
// It checks discovery (memberlist) healthscore.
// It checks discovery (memberlist) members count.
// It checks if member is part of the cluster.
func healthzHandler(disco *discovery.Discovery, expectedMembers int) http.Handler {
	livenessProbe := health.NewChecker(
		health.WithCacheDuration(1*time.Second),
		health.WithTimeout(10*time.Second),
		health.WithCheck(health.Check{
			Name:    "liveness_cluster_health",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				healthScore := disco.GetHealthScore()
				if healthScore > 0 {
					return fmt.Errorf("health score %d", healthScore)
				}
				return nil
			},
		}),
		health.WithCheck(health.Check{
			Name:    "liveness_cluster_discovered_members",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				// Safe guard in case whole cluster shutdown
				discoveredMembers := len(disco.Members(true))
				if discoveredMembers == 1 && expectedMembers > 1 {
					return fmt.Errorf("not enough discovered members %d", discoveredMembers)
				}
				return nil
			},
		}),
		health.WithCheck(health.Check{
			Name:    "liveness_cluster_in_cluster",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				discoveredMembersWithSelf := len(disco.Members(true))
				discoveredMembersWithoutSelf := len(disco.Members(false))
				if discoveredMembersWithSelf == discoveredMembersWithoutSelf {
					return fmt.Errorf("we are not part of the cluster")
				}
				return nil
			},
		}),
	)
	return health.NewHandler(livenessProbe)
}

// apiErrResponse describes error response for any API call.
type apiErrResponse struct {
	Err            error  `json:"-"`               // low-level runtime error
	HTTPStatusCode int    `json:"-"`               // http response status code
	StatusText     string `json:"status"`          // user-level status message
	AppCode        int64  `json:"code,omitempty"`  // application-specific error code
	ErrorText      string `json:"error,omitempty"` // application-level error message, for debugging
}

// Render response.
func (e *apiErrResponse) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, e.HTTPStatusCode)
	return nil
}

// apiInvalidRequestError returns 400 http error in case wrong requests parameters
// are sent to API endpoint.
func apiInvalidRequestError(err error) render.Renderer {
	return &apiErrResponse{
		Err:            err,
		HTTPStatusCode: 400,
		StatusText:     "Invalid request.",
		ErrorText:      err.Error(),
	}
}

// apiApplicationError returns 500 http error in case API is not able to perform
// requested action.
func apiApplicationError(err error) render.Renderer {
	return &apiErrResponse{
		Err:            err,
		HTTPStatusCode: 500,
		StatusText:     "Unable to perform request.",
		ErrorText:      err.Error(),
	}
}

// apiDiscoveryMembersHandler returns all discovery (memberlist) members.
// wget -O - -S -q http://localhost:8080/api/discovery/members
func apiDiscoveryMembersHandler(disco *discovery.Discovery) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, disco.Members(true))
		render.Status(r, http.StatusCreated)
	}
}

// apiMqttClientsHandler returns all mqtt clients.
// wget -O - -S -q http://localhost:8080/api/mqtt/clients
func apiMqttClientsHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, mqttServer.Clients.GetAll())
		render.Status(r, http.StatusCreated)
	}
}

// apiMqttClientIdRequest describe API request when mqtt clientId needs to be provided.
type apiMqttClientIdRequest struct {
	ClientId string `json:"client_id"`
}

// Bind validates request.
func (req *apiMqttClientIdRequest) Bind(r *http.Request) error {
	if req.ClientId == "" {
		return errors.New("client_id is required")
	}
	return nil
}

// apiMqttClientStopHandler will stop (disconnect) mqtt client.
// wget -O - -S -q http://localhost:8080/api/mqtt/client/stop \
// --post-data '{"client_id": "cc16d0v002aeifmbddo0"}' --header 'Content-Type: application/json'
// apiMqttClientIdRequest should be passed.
func apiMqttClientStopHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiMqttClientIdRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		client, ok := mqttServer.Clients.Get(request.ClientId)
		if !ok {
			render.Render(w, r, apiInvalidRequestError(errors.New("unknown client")))
			return
		}
		client.Stop(errors.New("stopped by API"))
		render.Status(r, http.StatusCreated)
	}
}

// apiMqttClientInflightHandler will return Inflight messages for client.
// wget -O - -S -q http://localhost:8080/api/mqtt/client/inflight \
// --post-data '{"client_id": "cc16d0v002aeifmbddo0"}' --header 'Content-Type: application/json'
// apiMqttClientIdRequest should be passed.
func apiMqttClientInflightHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiMqttClientIdRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		client, ok := mqttServer.Clients.Get(request.ClientId)
		if !ok {
			render.Render(w, r, apiInvalidRequestError(errors.New("unknown client")))
			return
		}
		render.JSON(w, r, client.Inflight.GetAll())
		render.Status(r, http.StatusCreated)
	}
}

// apiMqttTopicNameRequest describe API request when mqtt topic needs to be provided.
type apiMqttTopicNameRequest struct {
	Topic string `json:"topic"`
}

// Bind validates request.
func (req *apiMqttTopicNameRequest) Bind(r *http.Request) error {
	if req.Topic == "" {
		return errors.New("topic is required")
	}
	return nil
}

// apiMqttTopicMessagesHandler will return messages based on topic.
// wget -O - -S -q http://localhost:8080/api/mqtt/topic/messages \
// --post-data '{"topic": "#"}' --header 'Content-Type: application/json'
// apiMqttTopicNameRequest should be passed.
func apiMqttTopicMessagesHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiMqttTopicNameRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		render.JSON(w, r, mqttServer.Topics.Messages(request.Topic))
		render.Status(r, http.StatusCreated)
	}
}

// apiMqttTopicSubscribersHandler will return subscribers for topic.
// wget -O - -S -q http://localhost:8080/api/mqtt/topic/subscribers \
// --post-data '{"topic": "topic"}' --header 'Content-Type: application/json'
// apiMqttTopicNameRequest should be passed.
func apiMqttTopicSubscribersHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiMqttTopicNameRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		render.JSON(w, r, mqttServer.Topics.Subscribers(request.Topic))
		render.Status(r, http.StatusCreated)
	}
}
