package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/render"
	mqtt "github.com/mochi-co/mqtt/server"

	"brokerha/internal/discovery"
)

// errResponse describes error response for any API call.
type errResponse struct {
	Err            error  `json:"-"`               // low-level runtime error
	HTTPStatusCode int    `json:"-"`               // http response status code
	StatusText     string `json:"status"`          // user-level status message
	AppCode        int64  `json:"code,omitempty"`  // application-specific error code
	ErrorText      string `json:"error,omitempty"` // application-level error message, for debugging
}

// Render response.
func (e *errResponse) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, e.HTTPStatusCode)
	return nil
}

// invalidRequestError returns 400 http error in case wrong requests parameters
// are sent to API endpoint.
func invalidRequestError(err error) render.Renderer {
	return &errResponse{
		Err:            err,
		HTTPStatusCode: http.StatusBadRequest,
		StatusText:     "Invalid request.",
		ErrorText:      err.Error(),
	}
}

// discoveryMembersHandler returns all discovery (memberlist) members.
// wget -O - -S -q http://localhost:8080/api/discovery/members
func discoveryMembersHandler(disco *discovery.Discovery) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, disco.Members(true))
	}
}

// mqttClientsHandler returns all mqtt clients.
// wget -O - -S -q http://localhost:8080/api/mqtt/clients
func mqttClientsHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, mqttServer.Clients.GetAll())
	}
}

// mqttClientIDRequest describe API request when mqtt clientID needs to be provided.
type mqttClientIDRequest struct {
	ClientID string `json:"client_id"`
}

// Bind validates request.
func (req *mqttClientIDRequest) Bind(r *http.Request) error {
	if req.ClientID == "" {
		return errors.New("client_id is required")
	}
	return nil
}

// mqttClientStopHandler will stop (disconnect) mqtt client.
// wget -O - -S -q http://localhost:8080/api/mqtt/client/stop \
// --post-data '{"client_id": "cc16d0v002aeifmbddo0"}' --header 'Content-Type: application/json'
// mqttClientIDRequest should be passed.
func mqttClientStopHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttClientIDRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		client, ok := mqttServer.Clients.Get(request.ClientID)
		if !ok {
			render.Render(w, r, invalidRequestError(errors.New("unknown client")))
			return
		}
		client.Stop(errors.New("stopped by API"))
	}
}

// mqttClientInflightHandler will return Inflight messages for client.
// wget -O - -S -q http://localhost:8080/api/mqtt/client/inflight \
// --post-data '{"client_id": "cc16d0v002aeifmbddo0"}' --header 'Content-Type: application/json'
// mqttClientIDRequest should be passed.
func mqttClientInflightHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttClientIDRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		client, ok := mqttServer.Clients.Get(request.ClientID)
		if !ok {
			render.Render(w, r, invalidRequestError(errors.New("unknown client")))
			return
		}
		render.JSON(w, r, client.Inflight.GetAll())
	}
}

// mqttTopicNameRequest describe API request when mqtt topic needs to be provided.
type mqttTopicNameRequest struct {
	Topic string `json:"topic"`
}

// Bind validates request.
func (req *mqttTopicNameRequest) Bind(r *http.Request) error {
	if req.Topic == "" {
		return errors.New("topic is required")
	}
	return nil
}

// mqttTopicMessagesHandler will return messages based on topic.
// wget -O - -S -q http://localhost:8080/api/mqtt/topic/messages \
// --post-data '{"topic": "#"}' --header 'Content-Type: application/json'
// mqttTopicNameRequest should be passed.
func mqttTopicMessagesHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttTopicNameRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		render.JSON(w, r, mqttServer.Topics.Messages(request.Topic))
	}
}

// mqttTopicSubscribersHandler will return subscribers for topic.
// wget -O - -S -q http://localhost:8080/api/mqtt/topic/subscribers \
// --post-data '{"topic": "topic"}' --header 'Content-Type: application/json'
// mqttTopicNameRequest should be passed.
func mqttTopicSubscribersHandler(mqttServer *mqtt.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttTopicNameRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		render.JSON(w, r, mqttServer.Topics.Subscribers(request.Topic))
	}
}
