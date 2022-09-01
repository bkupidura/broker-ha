package api

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
	"brokerha/internal/types"
)

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
		MQTTPort:  1883,
		Bus:       evBus,
		AuthUsers: map[string]string{"test": "test"},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test")

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
	require.Equal(t, 1, len(clients))
	for _, client := range clients {
		require.NotNil(t, client)
		require.Equal(t, []byte("test"), client.Username)
	}
}

func TestMqttClientStopHandler(t *testing.T) {
	tests := []struct {
		inputRequest       map[string]interface{}
		expectedCode       int
		expectedError      string
		expectedClientDone uint32
	}{
		{
			inputRequest:       map[string]interface{}{},
			expectedCode:       http.StatusBadRequest,
			expectedError:      "client_id is required",
			expectedClientDone: 0,
		},
		{
			inputRequest:       map[string]interface{}{"client_id": ""},
			expectedCode:       http.StatusBadRequest,
			expectedError:      "client_id is required",
			expectedClientDone: 0,
		},
		{
			inputRequest:       map[string]interface{}{"client_id": "missing"},
			expectedCode:       http.StatusInternalServerError,
			expectedError:      "unknown client",
			expectedClientDone: 0,
		},
		{
			inputRequest:       map[string]interface{}{"client_id": "TestMqttClientStopHandler"},
			expectedCode:       http.StatusOK,
			expectedClientDone: 1,
		},
	}

	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:  1883,
		Bus:       evBus,
		AuthUsers: map[string]string{"test": "test"},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
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
		expectedInflight int
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
			inputRequest:     map[string]interface{}{"client_id": "TestMqttClientInflightHandler"},
			expectedCode:     http.StatusOK,
			expectedInflight: 1,
		},
	}

	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:  1883,
		Bus:       evBus,
		AuthUsers: map[string]string{"test": "test"},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
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

	evBus.Publish("cluster:message_from", &types.MQTTPublishMessage{
		Topic:   "TestMqttClientInflightHandler",
		Payload: []byte("test"),
		Retain:  true,
	})

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

		if w.Code != http.StatusOK {
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		} else {
			var resp []*types.MQTTPublishMessage
			unmarshalBody(res.Body, &resp)
			require.Equal(t, test.expectedInflight, len(resp))
		}
	}
}

func TestMqttTopicMessagesHandler(t *testing.T) {
	tests := []struct {
		inputRequest     map[string]interface{}
		expectedCode     int
		expectedError    string
		expectedMessages int
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
			expectedMessages: 0,
		},
		{
			inputRequest:     map[string]interface{}{"topic": "TestMqttTopicMessagesHandler"},
			expectedCode:     http.StatusOK,
			expectedMessages: 1,
		},
	}
	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:  1883,
		Bus:       evBus,
		AuthUsers: map[string]string{"test": "test"},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	evBus.Publish("cluster:message_from", &types.MQTTPublishMessage{
		Topic:   "TestMqttTopicMessagesHandler",
		Payload: []byte("test"),
		Retain:  true,
	})

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

		if w.Code != http.StatusOK {
			resp := make(map[string]string)
			unmarshalBody(res.Body, &resp)

			if test.expectedError != "" {
				require.Equal(t, test.expectedError, resp["error"])
			}
		} else {
			var resp []*types.MQTTPublishMessage
			unmarshalBody(res.Body, &resp)
			require.Equal(t, test.expectedMessages, len(resp))
		}
	}
}

func TestMqttTopicSubscribersHandler(t *testing.T) {
	tests := []struct {
		inputRequest        map[string]interface{}
		expectedCode        int
		expectedError       string
		expectedSubscribers int
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
			inputRequest:        map[string]interface{}{"topic": "missing"},
			expectedCode:        http.StatusOK,
			expectedSubscribers: 0,
		},
		{
			inputRequest:        map[string]interface{}{"topic": "TestMqttTopicSubscribersHandler"},
			expectedCode:        http.StatusOK,
			expectedSubscribers: 1,
		},
	}

	evBus := bus.New()
	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort:  1883,
		Bus:       evBus,
		AuthUsers: map[string]string{"test": "test"},
	})
	require.Nil(t, err)
	defer b.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
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
			var resp map[string]byte
			unmarshalBody(res.Body, &resp)
			require.Equal(t, test.expectedSubscribers, len(resp))
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
