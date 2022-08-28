package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/memberlist"
	mqtt "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/stretchr/testify/require"

	"brokerha/internal/discovery"
)

func TestDiscoveryMembersHandler(t *testing.T) {
	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.BindPort = 7946
	mlConfig.AdvertisePort = 7946
	mlConfig.LogOutput = ioutil.Discard

	disco, _, err := discovery.New(&discovery.Options{
		Domain:           "test",
		MemberListConfig: mlConfig,
	})
	if err != nil {
		t.Fatalf("discovery.New error: %s", err)
	}
	defer disco.Shutdown()

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
	mqttServer := newMqttBroker()
	defer mqttServer.Close()

	mqttConnOpts := paho.NewClientOptions().AddBroker("127.0.0.1:1883").SetAutoReconnect(false)

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	req := httptest.NewRequest(http.MethodGet, "/api/mqtt/clients", nil)
	handler := mqttClientsHandler(mqttServer)

	w := httptest.NewRecorder()
	handler(w, req)
	res := w.Result()
	defer res.Body.Close()

	clients := make(map[string]interface{})
	unmarshalBody(res.Body, &clients)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, 1, len(clients))
	for clientID, client := range clients {
		c := client.(map[string]interface{})
		require.Equal(t, clientID, c["ID"])
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
			expectedCode:       http.StatusBadRequest,
			expectedError:      "unknown client",
			expectedClientDone: 0,
		},
		{
			inputRequest:       map[string]interface{}{"client_id": "TestMqttClientStopHandler"},
			expectedCode:       http.StatusOK,
			expectedClientDone: 1,
		},
	}

	mqttServer := newMqttBroker()
	defer mqttServer.Close()

	mqttConnOpts := paho.NewClientOptions().AddBroker("127.0.0.1:1883").SetAutoReconnect(false).SetClientID("TestMqttClientStopHandler")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	handler := mqttClientStopHandler(mqttServer)

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

		client, ok := mqttServer.Clients.Get("TestMqttClientStopHandler")
		if !ok {
			t.Fatalf("mqttServer.Clients.Get client missing")
		}
		require.Equal(t, test.expectedClientDone, client.State.Done)
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
			expectedCode:  http.StatusBadRequest,
			expectedError: "unknown client",
		},
		{
			inputRequest:     map[string]interface{}{"client_id": "TestMqttClientInflightHandler"},
			expectedCode:     http.StatusOK,
			expectedInflight: 1,
		},
	}

	mqttServer := newMqttBroker()
	defer mqttServer.Close()

	mqttConnOpts := paho.NewClientOptions().AddBroker("127.0.0.1:1883").SetAutoReconnect(false).SetClientID("TestMqttClientInflightHandler")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	if token := mqttClient.Subscribe("TestMqttClientInflightHandler", byte(2), nil); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}
	mqttClient.Disconnect(10)
	if err := mqttServer.Publish("TestMqttClientInflightHandler", []byte("test"), true); err != nil {
		t.Fatalf("mqttServer.Publish error: %s", err)
	}

	handler := mqttClientInflightHandler(mqttServer)

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
			resp := make(map[uint16]interface{})
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

	mqttServer := newMqttBroker()
	defer mqttServer.Close()

	if err := mqttServer.Publish("TestMqttTopicMessagesHandler", []byte("test"), true); err != nil {
		t.Fatalf("mqttServer.Publish error: %s", err)
	}

	handler := mqttTopicMessagesHandler(mqttServer)

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
			resp := make([]interface{}, 5)
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

	mqttServer := newMqttBroker()
	defer mqttServer.Close()

	mqttConnOpts := paho.NewClientOptions().AddBroker("127.0.0.1:1883").SetAutoReconnect(false).SetClientID("TestMqttTopicSubscribersHandler")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	if token := mqttClient.Subscribe("TestMqttTopicSubscribersHandler", byte(2), nil); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	handler := mqttTopicSubscribersHandler(mqttServer)

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
			resp := make(map[string]interface{})
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

func newMqttBroker() *mqtt.Server {
	mqttServer := mqtt.NewServer(nil)
	if err := mqttServer.AddListener(listeners.NewTCP("tcp", ":1883"), nil); err != nil {
		panic(fmt.Sprintf("mqttServer.AddListener error: %s", err))
	}
	go func() {
		if err := mqttServer.Serve(); err != nil {
			panic(fmt.Sprintf("mqttServer.Serve worker died: %s", err))
		}
	}()
	return mqttServer
}
