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

// readyHandler returns health.Checker used by /ready endpoint.
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

// healthzHandler returns health.Checker used by /healthz endpoint.
// It checks discovery (memberlist) healthscore.
// It checks discovery (memberlist) members count.
// This check will be usefull when whole cluster will be restarted and all new broker-ha pods will sleep for same `random` interval.
// In this case K8s should restart PODs which dosent have any members (except self).
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

type apiErrResponse struct {
	Err            error  `json:"-"`               // low-level runtime error
	HTTPStatusCode int    `json:"-"`               // http response status code
	StatusText     string `json:"status"`          // user-level status message
	AppCode        int64  `json:"code,omitempty"`  // application-specific error code
	ErrorText      string `json:"error,omitempty"` // application-level error message, for debugging
}

func (e *apiErrResponse) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, e.HTTPStatusCode)
	return nil
}

func apiInvalidRequestError(err error) render.Renderer {
	return &apiErrResponse{
		Err:            err,
		HTTPStatusCode: 400,
		StatusText:     "Invalid request.",
		ErrorText:      err.Error(),
	}
}

func apiApplicationError(err error) render.Renderer {
	return &apiErrResponse{
		Err:            err,
		HTTPStatusCode: 500,
		StatusText:     "Unable to perform request.",
		ErrorText:      err.Error(),
	}
}

func apiDiscoveryMembersHandler(disco *discovery.Discovery) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, disco.Members(true))
		render.Status(r, http.StatusCreated)
	}
}

type apiDiscoveryLeaveRequest struct {
	Timeout int64 `json:"timeout"`
}

func (req *apiDiscoveryLeaveRequest) Bind(r *http.Request) error {
	if req.Timeout < 100 {
		return errors.New("timeout should be higher than 100ms")
	}
	return nil
}

func apiDiscoveryLeaveHandler(disco *discovery.Discovery) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiDiscoveryLeaveRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		if err := disco.Leave(time.Duration(request.Timeout) * time.Millisecond); err != nil {
			render.Render(w, r, apiApplicationError(err))
			return
		}
		render.Status(r, http.StatusCreated)
	}
}

type apiDiscoveryAdvertiseRequest struct {
	Timeout int64 `json:"timeout"`
}

func (req *apiDiscoveryAdvertiseRequest) Bind(r *http.Request) error {
	if req.Timeout < 100 {
		return errors.New("timeout should be higher than 100ms")
	}
	return nil
}

func apiDiscoveryAdvertiseHandler(disco *discovery.Discovery) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiDiscoveryAdvertiseRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		if err := disco.UpdateNode(time.Duration(request.Timeout) * time.Millisecond); err != nil {
			render.Render(w, r, apiApplicationError(err))
			return
		}
		render.Status(r, http.StatusCreated)
	}
}

func apiMqttClientsHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, mqttServer.Clients.GetAll())
		render.Status(r, http.StatusCreated)
	}
}

type apiMqttClientIdRequest struct {
	ClientId string `json:"client_id"`
}

func (req *apiMqttClientIdRequest) Bind(r *http.Request) error {
	if req.ClientId == "" {
		return errors.New("client id is required")
	}
	return nil
}

func apiMqttClientStopHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiMqttClientIdRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		client, ok := mqttServer.Clients.Get(request.ClientId)
		if !ok {
			render.Render(w, r, apiApplicationError(errors.New("unknown client")))
			return
		}
		client.Stop(errors.New("stopped by API"))
		render.Status(r, http.StatusCreated)
	}
}

func apiMqttClientInflightHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &apiMqttClientIdRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, apiInvalidRequestError(err))
			return
		}
		client, ok := mqttServer.Clients.Get(request.ClientId)
		if !ok {
			render.Render(w, r, apiApplicationError(errors.New("unknown client")))
			return
		}
		render.JSON(w, r, client.Inflight.GetAll())
		render.Status(r, http.StatusCreated)
	}
}

type apiMqttTopicNameRequest struct {
	Topic string `json:"topic"`
}

func (req *apiMqttTopicNameRequest) Bind(r *http.Request) error {
	if req.Topic == "" {
		return errors.New("topic is required")
	}
	return nil
}

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
