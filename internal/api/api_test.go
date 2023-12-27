package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/memberlist"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"

	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
	"brokerha/internal/types"
)

func TestProxyHandler(t *testing.T) {
	tests := []struct {
		inputReqFunc  func() *http.Request
		inputServers  func() []*http.Server
		expectedCode  int
		expectedError string
		expectedBody  map[string]interface{}
	}{
		{
			inputReqFunc: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/proxy/api/discovery/members", nil)
				r.Body = errReader(0)
				return r
			},
			expectedCode:  http.StatusBadRequest,
			expectedError: "test error",
		},
		{
			inputReqFunc: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/proxy/api/discovery/members", nil)
				r.Method = "*?"
				return r
			},
			expectedCode:  http.StatusBadRequest,
			expectedError: "net/http: invalid method \"*?\"",
		},
		{
			inputReqFunc: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/proxy/api/discovery/members", nil)
				return r
			},
			expectedCode:  http.StatusBadRequest,
			expectedError: "Get \"http://127.0.0.1:8080/api/discovery/members\": dial tcp 127.0.0.1:8080: connect: connection refused",
		},
		{
			inputReqFunc: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/proxy/api/discovery/members", nil)
				return r
			},
			inputServers: func() []*http.Server {
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Length", "1")
					fmt.Fprintln(w, "Hello")
				})
				s := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", HTTPPort), Handler: h}
				return []*http.Server{s}
			},
			expectedCode:  http.StatusBadRequest,
			expectedError: "unexpected EOF",
		},
		{
			inputReqFunc: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/proxy/api/discovery/members", nil)
				r.Header.Add("test-header", "test")
				return r
			},
			inputServers: func() []*http.Server {
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "test", r.Header.Get("test-header"))
					require.Equal(t, "/api/discovery/members", r.URL.Path)
					require.Equal(t, http.MethodGet, r.Method)

					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintln(w, "{\"a\": \"b\"}")
				})
				s := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", HTTPPort), Handler: h}
				return []*http.Server{s}
			},
			expectedCode: http.StatusOK,
			expectedBody: map[string]interface{}{
				"node1": map[string]interface{}{
					"a": "b",
				},
				"node2": map[string]interface{}{
					"a": "b",
				},
			},
		},
		{
			inputReqFunc: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/proxy/api/mqtt/client/inflight", strings.NewReader("{'client_id': 'test'}"))
				r.Header.Add("test-header", "test2")
				return r
			},
			inputServers: func() []*http.Server {
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "test2", r.Header.Get("test-header"))
					require.Equal(t, "/api/mqtt/client/inflight", r.URL.Path)
					require.Equal(t, http.MethodPost, r.Method)

					reqBody, err := io.ReadAll(r.Body)
					require.Nil(t, err)
					require.Equal(t, []byte("{'client_id': 'test'}"), reqBody)

					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintln(w, "{\"c\": \"d\"}")
				})
				s := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", HTTPPort), Handler: h}
				return []*http.Server{s}
			},
			expectedCode: http.StatusOK,
			expectedBody: map[string]interface{}{
				"node1": map[string]interface{}{
					"c": "d",
				},
				"node2": map[string]interface{}{
					"c": "d",
				},
			},
		},
	}
	mlConfig1 := memberlist.DefaultLocalConfig()
	mlConfig1.BindAddr = "127.0.0.1"
	mlConfig1.BindPort = 7946
	mlConfig1.AdvertisePort = 7946
	mlConfig1.Name = "node1"
	mlConfig1.LogOutput = ioutil.Discard

	evBus := bus.New()
	disco, ctxCancel, err := discovery.New(&discovery.Options{
		Domain:           "test",
		MemberListConfig: mlConfig1,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_to": 1024},
	})
	require.Nil(t, err)
	defer disco.Shutdown()
	defer ctxCancel()

	mlConfig2 := memberlist.DefaultLocalConfig()
	mlConfig2.BindAddr = "127.0.0.1"
	mlConfig2.BindPort = 7947
	mlConfig2.AdvertisePort = 7947
	mlConfig2.Name = "node2"
	mlConfig2.LogOutput = ioutil.Discard
	m2, err := memberlist.Create(mlConfig2)
	require.Nil(t, err)
	defer m2.Shutdown()

	_, err = m2.Join([]string{"127.0.0.1:7946"})
	require.Nil(t, err)

	handler := proxyHandler(disco)

	for _, test := range tests {
		w := httptest.NewRecorder()
		req := test.inputReqFunc()
		var servers []*http.Server

		if test.inputServers != nil {
			servers = test.inputServers()
			if len(servers) > 0 {
				for _, s := range servers {
					go s.ListenAndServe()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		handler(w, req)
		res := w.Result()
		defer res.Body.Close()

		if len(servers) > 0 {
			for _, s := range servers {
				s.Shutdown(context.TODO())
			}
			time.Sleep(10 * time.Millisecond)
		}

		require.Equal(t, test.expectedCode, w.Code)

		if w.Code != http.StatusOK {
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		} else {
			resp := make(map[string]interface{})
			unmarshalBody(res.Body, &resp)
			require.Equal(t, test.expectedBody, resp)
		}
	}
}

func TestSseHandler(t *testing.T) {
	tests := []struct {
		inputBusFunc          func() *bus.Bus
		inputCancelTimeout    int
		inputPublishFunc      func(*bus.Bus)
		inputResponseRecorder ResponseWriter
		inputRequest          map[string]interface{}
		expectedError         string
		expectedCode          int
		expectedBody          []string
	}{
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout:    10,
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest: map[string]interface{}{
				"filters": []string{"unknown"},
			},
			expectedCode:  http.StatusBadRequest,
			expectedError: "filter not in allowed list [cluster:message_from cluster:message_to cluster:new_member]",
			expectedBody:  []string{""},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout: 100,
			inputPublishFunc: func(b *bus.Bus) {
				time.Sleep(20 * time.Millisecond)
				err := b.Publish("cluster:message_from", "message_from")
				require.Nil(t, err)
				b.Publish("cluster:message_to", "message_to")
				b.Publish("cluster:new_member", "new_member")
			},
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest: map[string]interface{}{
				"filters": []string{"cluster:message_from"},
			},
			expectedCode: http.StatusOK,
			expectedBody: []string{
				"",
				"event: cluster:message_from",
				"data: \"message_from\"",
			},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout: 100,
			inputPublishFunc: func(b *bus.Bus) {
				time.Sleep(20 * time.Millisecond)
				b.Publish("cluster:message_from", "message_from")
				err := b.Publish("cluster:message_to", "message_to")
				require.Nil(t, err)
				b.Publish("cluster:new_member", "new_member")
			},
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest: map[string]interface{}{
				"filters": []string{"cluster:message_to"},
			},
			expectedCode: http.StatusOK,
			expectedBody: []string{
				"",
				"event: cluster:message_to",
				"data: \"message_to\"",
			},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout: 100,
			inputPublishFunc: func(b *bus.Bus) {
				time.Sleep(20 * time.Millisecond)
				b.Publish("cluster:message_from", "message_from")
				b.Publish("cluster:message_to", "message_to")
				err := b.Publish("cluster:new_member", "new_member")
				require.Nil(t, err)
			},
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest: map[string]interface{}{
				"filters": []string{"cluster:new_member"},
			},
			expectedCode: http.StatusOK,
			expectedBody: []string{
				"",
				"event: cluster:new_member",
				"data: \"new_member\"",
			},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout: 100,
			inputPublishFunc: func(b *bus.Bus) {
				time.Sleep(20 * time.Millisecond)
				err := b.Publish("cluster:message_from", "message_from")
				require.Nil(t, err)
				err = b.Publish("cluster:message_to", "message_to")
				require.Nil(t, err)
				err = b.Publish("cluster:new_member", "new_member")
				require.Nil(t, err)
			},
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest: map[string]interface{}{
				"channel_size": 1,
			},
			expectedCode: http.StatusOK,
			expectedBody: []string{
				"",
				"event: cluster:message_from",
				"data: \"message_from\"",
				"event: cluster:message_to",
				"data: \"message_to\"",
				"event: cluster:new_member",
				"data: \"new_member\"",
			},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout: 100,
			inputPublishFunc: func(b *bus.Bus) {
				time.Sleep(20 * time.Millisecond)
				err := b.Publish("cluster:message_from", "message_from")
				require.Nil(t, err)
				err = b.Publish("cluster:message_to", "message_to")
				require.Nil(t, err)
				err = b.Publish("cluster:new_member", "new_member")
				require.Nil(t, err)
			},
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest:          map[string]interface{}{},
			expectedCode:          http.StatusOK,
			expectedBody: []string{
				"",
				"event: cluster:message_from",
				"data: \"message_from\"",
				"event: cluster:message_to",
				"data: \"message_to\"",
				"event: cluster:new_member",
				"data: \"new_member\"",
			},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout:    3100,
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest:          map[string]interface{}{},
			expectedCode:          http.StatusOK,
			expectedBody: []string{
				"",
				"event: sse:keepalive:1.2.3.4:60000",
				"data: \"keepalive\"",
			},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				_, err := b.Subscribe("cluster:message_from", "1.2.3.4:60000", 1024)
				require.Nil(t, err)
				return b
			},
			inputCancelTimeout:    10,
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest:          map[string]interface{}{},
			expectedCode:          http.StatusInternalServerError,
			expectedError:         "subscriber 1.2.3.4:60000 already exists",
			expectedBody:          []string{""},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				_, err := b.Subscribe("cluster:message_to", "1.2.3.4:60000", 1024)
				require.Nil(t, err)
				return b
			},
			inputCancelTimeout:    10,
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest:          map[string]interface{}{},
			expectedCode:          http.StatusInternalServerError,
			expectedError:         "subscriber 1.2.3.4:60000 already exists",
			expectedBody:          []string{""},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				_, err := b.Subscribe("cluster:new_member", "1.2.3.4:60000", 1024)
				require.Nil(t, err)
				return b
			},
			inputCancelTimeout:    10,
			inputResponseRecorder: NewResponseWriter(true),
			inputRequest:          map[string]interface{}{},
			expectedCode:          http.StatusInternalServerError,
			expectedError:         "subscriber 1.2.3.4:60000 already exists",
			expectedBody:          []string{""},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputPublishFunc: func(b *bus.Bus) {
				time.Sleep(20 * time.Millisecond)
				err := b.Publish("cluster:new_member", make(chan int))
				require.Nil(t, err)
			},
			inputCancelTimeout:    100,
			inputResponseRecorder: NewResponseWriter(true),
			expectedCode:          http.StatusOK,
			expectedBody:          []string{""},
		},
		{
			inputBusFunc: func() *bus.Bus {
				b := bus.New()
				return b
			},
			inputCancelTimeout:    10,
			inputResponseRecorder: NewResponseWriter(false),
			inputRequest:          map[string]interface{}{},
			expectedCode:          http.StatusInternalServerError,
			expectedError:         "streaming not supported",
			expectedBody:          []string{""},
		},
	}
	sseKeepalive = 3

	for _, test := range tests {
		evBus := test.inputBusFunc()
		handler := sseHandler(evBus)

		body, _ := json.Marshal(test.inputRequest)

		ctx, reqCtxCancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/api/sse", bytes.NewReader(body))
		require.Nil(t, err)

		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = "1.2.3.4:60000"

		w := test.inputResponseRecorder

		go func() {
			time.Sleep(time.Duration(test.inputCancelTimeout) * time.Millisecond)
			reqCtxCancel()
		}()
		if test.inputPublishFunc != nil {
			go test.inputPublishFunc(evBus)
		}
		handler(w, req)

		require.Equal(t, test.expectedCode, w.Code())

		res := w.Result()
		defer res.Body.Close()

		if w.Code() != http.StatusOK {
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		}
		b, err := io.ReadAll(res.Body)
		require.Nil(t, err)

		require.ElementsMatch(t, test.expectedBody, strings.Split(string(b), "\n"))

		time.Sleep(50 * time.Millisecond)
	}
}

func TestDiscoveryMembersHandler(t *testing.T) {
	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.BindPort = 7946
	mlConfig.AdvertisePort = 7946
	mlConfig.LogOutput = ioutil.Discard

	evBus := bus.New()
	disco, ctxCancel, err := discovery.New(&discovery.Options{
		Domain:           "test",
		MemberListConfig: mlConfig,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_to": 1024},
	})
	require.Nil(t, err)
	defer disco.Shutdown()
	defer ctxCancel()

	req := httptest.NewRequest(http.MethodGet, "/api/discovery/members", nil)
	handler := discoveryMembersHandler(disco)

	w := httptest.NewRecorder()
	handler(w, req)
	res := w.Result()
	defer res.Body.Close()

	members := make([]*memberlist.Node, 5)
	unmarshalBody(res.Body, &members)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, 1, len(members))
	require.Equal(t, "127.0.0.1:7946", members[0].Address())
}

func TestMqttClientsHandler(t *testing.T) {
	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetClientID("TestMqttClientsHandler").
		SetAutoReconnect(false)

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	req := httptest.NewRequest(http.MethodGet, "/api/mqtt/clients", nil)
	handler := mqttClientsHandler(b)

	w := httptest.NewRecorder()
	handler(w, req)
	res := w.Result()
	defer res.Body.Close()

	var clients []*broker.MQTTClient
	unmarshalBody(res.Body, &clients)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, 2, len(clients))
	for _, client := range clients {
		require.NotNil(t, client)
		require.Subset(t, []string{"TestMqttClientsHandler", "inline"}, []string{client.ID})
	}
}

func TestMqttClientStopHandler(t *testing.T) {
	tests := []struct {
		inputRequest       map[string]interface{}
		expectedCode       int
		expectedError      string
		expectedClientDone bool
	}{
		{
			inputRequest:       map[string]interface{}{},
			expectedCode:       http.StatusBadRequest,
			expectedError:      "client_id is required",
			expectedClientDone: false,
		},
		{
			inputRequest:       map[string]interface{}{"client_id": ""},
			expectedCode:       http.StatusBadRequest,
			expectedError:      "client_id is required",
			expectedClientDone: false,
		},
		{
			inputRequest:       map[string]interface{}{"client_id": "missing"},
			expectedCode:       http.StatusInternalServerError,
			expectedError:      "unknown client",
			expectedClientDone: false,
		},
		{
			inputRequest:       map[string]interface{}{"client_id": "TestMqttClientStopHandler"},
			expectedCode:       http.StatusOK,
			expectedClientDone: true,
		},
	}

	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetClientID("TestMqttClientStopHandler")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	handler := mqttClientStopHandler(b)

	for _, test := range tests {
		body, _ := json.Marshal(test.inputRequest)
		req := httptest.NewRequest(http.MethodPost, "/api/mqtt/client/stop", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler(w, req)

		require.Equal(t, test.expectedCode, w.Code)

		if w.Code != http.StatusOK {
			res := w.Result()
			defer res.Body.Close()
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		}

		client, err := b.Client("TestMqttClientStopHandler")
		require.Nil(t, err)
		require.Equal(t, test.expectedClientDone, client.Done)
	}
}

func TestMqttClientInflightHandler(t *testing.T) {
	tests := []struct {
		inputRequest     map[string]interface{}
		expectedCode     int
		expectedError    string
		expectedMessages []packets.Packet
	}{
		{
			inputRequest:  map[string]interface{}{},
			expectedCode:  http.StatusBadRequest,
			expectedError: "client_id is required",
		},
		{
			inputRequest:  map[string]interface{}{"client_id": ""},
			expectedCode:  http.StatusBadRequest,
			expectedError: "client_id is required",
		},
		{
			inputRequest:  map[string]interface{}{"client_id": "missing"},
			expectedCode:  http.StatusInternalServerError,
			expectedError: "unknown client",
		},
		{
			inputRequest: map[string]interface{}{"client_id": "TestMqttClientInflightHandler"},
			expectedCode: http.StatusOK,
			expectedMessages: []packets.Packet{
				{
					TopicName:       "TestMqttClientInflightHandler",
					Payload:         []byte("test"),
					Origin:          "inline",
					ProtocolVersion: 4,
					PacketID:        1,
					FixedHeader: packets.FixedHeader{
						Retain: false,
						Qos:    2,
						Type:   3,
					},
					Properties: packets.Properties{
						SubscriptionIdentifier: []int{0},
					},
				},
			},
		},
	}

	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetCleanSession(false).
		SetClientID("TestMqttClientInflightHandler")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	if token := mqttClient.Subscribe("TestMqttClientInflightHandler", byte(2), nil); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}

	mqttClient.Disconnect(10)
	time.Sleep(50 * time.Millisecond)

	evBus.Publish("cluster:message_from", types.MQTTPublishMessage{
		Topic:   "TestMqttClientInflightHandler",
		Payload: []byte("test"),
		Retain:  true,
		Qos:     2,
	})
	now := time.Now().Unix()

	handler := mqttClientInflightHandler(b)

	time.Sleep(100 * time.Millisecond)

	for _, test := range tests {
		body, _ := json.Marshal(test.inputRequest)
		req := httptest.NewRequest(http.MethodPost, "/api/mqtt/client/inflight", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler(w, req)

		require.Equal(t, test.expectedCode, w.Code)

		res := w.Result()
		defer res.Body.Close()

		for idx, pk := range test.expectedMessages {
			pk.Created = now
			pk.Expiry = now
			test.expectedMessages[idx] = pk
		}

		if w.Code != http.StatusOK {
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		} else {
			var resp []packets.Packet
			unmarshalBody(res.Body, &resp)
			require.Equal(t, test.expectedMessages, resp)
		}
	}
}

func TestMqttTopicMessagesHandler(t *testing.T) {
	tests := []struct {
		inputRequest     map[string]interface{}
		expectedCode     int
		expectedError    string
		expectedMessages []packets.Packet
	}{
		{
			inputRequest:  map[string]interface{}{},
			expectedCode:  http.StatusBadRequest,
			expectedError: "topic is required",
		},
		{
			inputRequest:  map[string]interface{}{"topic": ""},
			expectedCode:  http.StatusBadRequest,
			expectedError: "topic is required",
		},
		{
			inputRequest:     map[string]interface{}{"topic": "missing"},
			expectedCode:     http.StatusOK,
			expectedMessages: []packets.Packet{},
		},
		{
			inputRequest: map[string]interface{}{"topic": "TestMqttTopicMessagesHandler"},
			expectedCode: http.StatusOK,
			expectedMessages: []packets.Packet{
				{
					TopicName:       "TestMqttTopicMessagesHandler",
					Payload:         []byte("test"),
					Origin:          "inline",
					ProtocolVersion: 4,
					FixedHeader: packets.FixedHeader{
						Retain: true,
						Type:   3,
					},
				},
			},
		},
	}
	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	evBus.Publish("cluster:message_from", types.MQTTPublishMessage{
		Topic:   "TestMqttTopicMessagesHandler",
		Payload: []byte("test"),
		Retain:  true,
	})
	now := time.Now().Unix()

	time.Sleep(100 * time.Millisecond)

	handler := mqttTopicMessagesHandler(b)

	for _, test := range tests {
		body, _ := json.Marshal(test.inputRequest)
		req := httptest.NewRequest(http.MethodPost, "/api/mqtt/topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler(w, req)

		require.Equal(t, test.expectedCode, w.Code)

		res := w.Result()
		defer res.Body.Close()

		for idx, pk := range test.expectedMessages {
			pk.Created = now
			test.expectedMessages[idx] = pk
		}

		if w.Code != http.StatusOK {
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		} else {
			var resp []packets.Packet
			unmarshalBody(res.Body, &resp)
			require.Equal(t, test.expectedMessages, resp)
		}
	}
}

func TestMqttTopicSubscribersHandler(t *testing.T) {
	tests := []struct {
		inputRequest        map[string]interface{}
		expectedCode        int
		expectedError       string
		expectedSubscribers *mqtt.Subscribers
	}{
		{
			inputRequest:  map[string]interface{}{},
			expectedCode:  http.StatusBadRequest,
			expectedError: "topic is required",
		},
		{
			inputRequest:  map[string]interface{}{"topic": ""},
			expectedCode:  http.StatusBadRequest,
			expectedError: "topic is required",
		},
		{
			inputRequest: map[string]interface{}{"topic": "missing"},
			expectedCode: http.StatusOK,
			expectedSubscribers: &mqtt.Subscribers{
				Shared:              make(map[string]map[string]packets.Subscription),
				SharedSelected:      make(map[string]packets.Subscription),
				Subscriptions:       make(map[string]packets.Subscription),
				InlineSubscriptions: make(map[int]mqtt.InlineSubscription),
			},
		},
		{
			inputRequest: map[string]interface{}{"topic": "TestMqttTopicSubscribersHandler"},
			expectedCode: http.StatusOK,
			expectedSubscribers: &mqtt.Subscribers{
				Shared:         make(map[string]map[string]packets.Subscription),
				SharedSelected: make(map[string]packets.Subscription),
				Subscriptions: map[string]packets.Subscription{
					"TestMqttTopicSubscribersHandler": {
						Filter: "TestMqttTopicSubscribersHandler",
						Identifiers: map[string]int{
							"TestMqttTopicSubscribersHandler": 0,
						},
						Qos: 2,
					},
				},
				InlineSubscriptions: make(map[int]mqtt.InlineSubscription),
			},
		},
	}

	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetClientID("TestMqttTopicSubscribersHandler")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	if token := mqttClient.Subscribe("TestMqttTopicSubscribersHandler", byte(2), nil); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	handler := mqttTopicSubscribersHandler(b)

	for _, test := range tests {
		body, _ := json.Marshal(test.inputRequest)
		req := httptest.NewRequest(http.MethodPost, "/api/mqtt/topic/subscribers", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler(w, req)

		require.Equal(t, test.expectedCode, w.Code)

		res := w.Result()
		defer res.Body.Close()

		if w.Code != http.StatusOK {
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		} else {
			var resp *mqtt.Subscribers
			unmarshalBody(res.Body, &resp)
			require.Equal(t, test.expectedSubscribers, resp)
		}
	}
}

func unmarshalBody(body io.Reader, destination interface{}) interface{} {
	b, err := io.ReadAll(body)
	if err != nil {
		panic("unable to read body")
	}
	err = json.Unmarshal(b, &destination)
	if err != nil {
		panic("unable to unmarshal body")
	}
	return destination
}

type errReader int

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("test error")
}

func (errReader) Close() error {
	return nil
}

type ResponseWriter interface {
	http.ResponseWriter
	Code() int
	Result() *http.Response
}

func NewResponseWriter(asFlusher bool) ResponseWriter {
	if asFlusher {
		return &testResponseRecorderFlusher{Recorder: httptest.NewRecorder()}
	}
	return &testResponseRecorderWithoutFlush{Recorder: httptest.NewRecorder()}
}

type testResponseRecorderWithoutFlush struct {
	Recorder *httptest.ResponseRecorder
}

func (w *testResponseRecorderWithoutFlush) Code() int {
	return w.Recorder.Code
}

func (w *testResponseRecorderWithoutFlush) Result() *http.Response {
	return w.Recorder.Result()
}

func (w *testResponseRecorderWithoutFlush) Header() http.Header {
	return w.Recorder.Header()
}

func (w *testResponseRecorderWithoutFlush) WriteHeader(status int) {
	w.Recorder.WriteHeader(status)
}

func (w *testResponseRecorderWithoutFlush) Write(b []byte) (int, error) {
	return w.Recorder.Write(b)
}

func (w *testResponseRecorderWithoutFlush) ReadFrom(r io.Reader) (n int64, err error) {
	return io.Copy(w.Recorder, r)
}

type testResponseRecorderFlusher struct {
	Recorder *httptest.ResponseRecorder
}

func (w *testResponseRecorderFlusher) Code() int {
	return w.Recorder.Code
}

func (w *testResponseRecorderFlusher) Result() *http.Response {
	return w.Recorder.Result()
}

func (w *testResponseRecorderFlusher) Header() http.Header {
	return w.Recorder.Header()
}

func (w *testResponseRecorderFlusher) WriteHeader(status int) {
	w.Recorder.WriteHeader(status)
}

func (w *testResponseRecorderFlusher) Write(b []byte) (int, error) {
	return w.Recorder.Write(b)
}

func (w *testResponseRecorderFlusher) ReadFrom(r io.Reader) (n int64, err error) {
	return io.Copy(w.Recorder, r)
}

func (w *testResponseRecorderFlusher) Flush() {
	w.Recorder.Flush()
}
