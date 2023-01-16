package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/render"

	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
)

var (
	// How often to send keepalives in seconds over SSE.
	sseKeepalive = 60
	// List of allowed SSE channels.
	sseChannels = []string{"cluster:message_from", "cluster:message_to", "cluster:new_member"}
	// API listening port.
	HTTPPort = 8080
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

// unableToPerformError returns 500 http error in case we are not able to
// perform request.
func unableToPerformError(err error) render.Renderer {
	return &errResponse{
		Err:            err,
		HTTPStatusCode: http.StatusInternalServerError,
		StatusText:     "Unable to perform request.",
		ErrorText:      err.Error(),
	}
}

// proxyHandler will proxy request to every node in the cluster and return combined response.
func proxyHandler(d *discovery.Discovery) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		c := &http.Client{}
		mergedResponse := map[string]interface{}{}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}

		for _, member := range d.Members(true) {
			r.Body = io.NopCloser(bytes.NewBuffer(body))
			memberReq, err := http.NewRequest(r.Method, fmt.Sprintf("http://%s:%d%s", member.Addr.String(), HTTPPort, strings.TrimPrefix(r.URL.Path, "/proxy")), r.Body)
			if err != nil {
				render.Render(w, r, invalidRequestError(err))
				return
			}
			memberReq.Header = r.Header
			memberRes, err := c.Do(memberReq)
			if err != nil {
				render.Render(w, r, invalidRequestError(err))
				return
			}
			memberResBody, err := io.ReadAll(memberRes.Body)
			if err != nil {
				render.Render(w, r, invalidRequestError(err))
				return
			}
			var data interface{}
			json.Unmarshal(memberResBody, &data)
			mergedResponse[member.Name] = data
		}
		render.JSON(w, r, mergedResponse)
	}
}

// sseFilterRequest describe API requests with SSE filters.
// If not filters are provided, SSE will return all channels.
type sseFilterRequest struct {
	Filters     []string `json:"filters"`
	ChannelSize int      `json:"channel_size"`
}

// Bind validates request.
func (req *sseFilterRequest) Bind(r *http.Request) error {
	for _, reqFilterName := range req.Filters {
		allowed := false
		for _, allowedFilterName := range sseChannels {
			if reqFilterName == allowedFilterName {
				allowed = true
			}
		}
		if !allowed {
			return fmt.Errorf("filter not in allowed list %v", sseChannels)
		}
	}
	if len(req.Filters) == 0 {
		req.Filters = sseChannels
	}
	if req.ChannelSize < 100 {
		req.ChannelSize = 100
	}
	req.Filters = append(req.Filters, fmt.Sprintf("sse:keepalive:%s", r.RemoteAddr))
	return nil
}

// sseHandler starts Server Sent Events stream.
// sseFilterRequest should be passed.
func sseHandler(b *bus.Bus) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &sseFilterRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			render.Render(w, r, unableToPerformError(errors.New("streaming not supported")))
			return
		}

		subscriptions := make(map[string]chan bus.Event)

		for _, reqFilter := range request.Filters {
			ch, err := b.Subscribe(reqFilter, r.RemoteAddr, request.ChannelSize)
			if err != nil {
				render.Render(w, r, unableToPerformError(err))
			}
			defer b.Unsubscribe(reqFilter, r.RemoteAddr)
			subscriptions[reqFilter] = ch
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		clientDisconnected := r.Context().Done()
		go func() {
			for {
				select {
				case <-time.After(time.Duration(sseKeepalive) * time.Second):
					b.Publish(fmt.Sprintf("sse:keepalive:%s", r.RemoteAddr), "keepalive")
				case <-clientDisconnected:
					for subName := range subscriptions {
						b.Unsubscribe(subName, r.RemoteAddr)
					}
					return
				}
			}
		}()

		sseBusEvent := func(e bus.Event) {
			var sseEvent, sseData string
			data, err := json.Marshal(e.Data)
			if err != nil {
				log.Printf("unable to marshal SSE data %s: %+v", e.ChannelName, e.Data)
				return
			}
			sseEvent = fmt.Sprintf("event: %s\n", e.ChannelName)
			sseData = fmt.Sprintf("data: %s\n", string(data))
			w.Write([]byte(sseEvent))
			w.Write([]byte(sseData))
			flusher.Flush()
		}

		for {
			for _, ch := range subscriptions {
				select {
				case e := <-ch:
					sseBusEvent(e)
				case <-time.After(10 * time.Millisecond):
					continue
				case <-clientDisconnected:
					return
				}
			}
		}
	}
}

// discoveryMembersHandler returns all discovery (memberlist) members.
func discoveryMembersHandler(d *discovery.Discovery) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, d.Members(true))
	}
}

// mqttClientsHandler returns all mqtt clients.
func mqttClientsHandler(b *broker.Broker) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		render.JSON(w, r, b.Clients())
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
// mqttClientIDRequest should be passed.
func mqttClientStopHandler(b *broker.Broker) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttClientIDRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		if err := b.StopClient(request.ClientID, "stopped by API"); err != nil {
			render.Render(w, r, unableToPerformError(err))
		}
	}
}

// mqttClientInflightHandler will return Inflight messages for client.
// mqttClientIDRequest should be passed.
func mqttClientInflightHandler(b *broker.Broker) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttClientIDRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		messages, err := b.Inflights(request.ClientID)
		if err != nil {
			render.Render(w, r, unableToPerformError(err))
			return
		}
		render.JSON(w, r, messages)
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
// mqttTopicNameRequest should be passed.
func mqttTopicMessagesHandler(b *broker.Broker) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttTopicNameRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		render.JSON(w, r, b.Messages(request.Topic))
	}
}

// mqttTopicSubscribersHandler will return subscribers for topic.
// mqttTopicNameRequest should be passed.
func mqttTopicSubscribersHandler(b *broker.Broker) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		request := &mqttTopicNameRequest{}
		if err := render.Bind(r, request); err != nil {
			render.Render(w, r, invalidRequestError(err))
			return
		}
		render.JSON(w, r, b.Subscribers(request.Topic))
	}
}
