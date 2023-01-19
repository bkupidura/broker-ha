package broker

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/packets"
	"github.com/stretchr/testify/require"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

func TestAuth(t *testing.T) {
	tests := []struct {
		inputOptions       *Options
		inputMQTTClientOps *paho.ClientOptions
		expectedError      error
	}{
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Bus:              bus.New(),
				Auth:             auth.AuthRules{{Username: "test", Password: "test", Allow: true}},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputMQTTClientOps: paho.NewClientOptions().
				AddBroker("127.0.0.1:1883").
				SetAutoReconnect(false).
				SetConnectRetry(false).
				SetUsername("test").
				SetPassword("test").
				SetClientID("client1"),
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Bus:              bus.New(),
				Auth:             auth.AuthRules{{Username: "test", Password: "test", Allow: true}},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputMQTTClientOps: paho.NewClientOptions().
				AddBroker("127.0.0.1:1883").
				SetAutoReconnect(false).
				SetUsername("test").
				SetPassword("test2").
				SetClientID("client1"),
			expectedError: errors.New("not Authorized"),
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Bus:              bus.New(),
				Auth:             auth.AuthRules{{Username: "test", Password: "test", Allow: true}},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputMQTTClientOps: paho.NewClientOptions().
				AddBroker("127.0.0.1:1883").
				SetAutoReconnect(false).
				SetUsername("test2").
				SetPassword("test").
				SetClientID("client1"),
			expectedError: errors.New("not Authorized"),
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Bus:              bus.New(),
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputMQTTClientOps: paho.NewClientOptions().
				AddBroker("127.0.0.1:1883").
				SetAutoReconnect(false).
				SetUsername("test").
				SetPassword("test").
				SetClientID("client1"),
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Bus:              bus.New(),
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputMQTTClientOps: paho.NewClientOptions().
				AddBroker("127.0.0.1:1883").
				SetAutoReconnect(false).
				SetClientID("client1"),
		},
	}
	for _, test := range tests {
		b, ctxCancel, err := New(test.inputOptions)
		require.Nil(t, err)

		mqttClient := paho.NewClient(test.inputMQTTClientOps)
		token := mqttClient.Connect()
		token.Wait()

		if token.Error() == nil {
			mqttClient.Disconnect(10)
		}

		ctxCancel()
		b.Shutdown()

		require.Equal(t, test.expectedError, token.Error())

		time.Sleep(100 * time.Millisecond)
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		inputOptions    *Options
		inputBeforeTest func(*bus.Bus)
		expectedErr     string
		expectedLog     []string
	}{
		{
			inputOptions: &Options{
				MQTTPort: 1883,
			},
			expectedErr: "subscription size for cluster:message_from not provided",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"a": 10},
			},
			expectedErr: "subscription size for cluster:message_from not provided",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024},
			},
			expectedErr: "subscription size for broker:send_retained not provided",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10},
			},
			expectedErr: "subscription size for broker:pk_retained not provided",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputBeforeTest: func(b *bus.Bus) {
				_, err := b.Subscribe("cluster:message_from", "broker", 1024)
				require.Nil(t, err)
			},
			expectedErr: "subscriber broker already exists",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputBeforeTest: func(b *bus.Bus) {
				_, err := b.Subscribe("broker:send_retained", "broker", 1024)
				require.Nil(t, err)
			},
			expectedErr: "subscriber broker already exists",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			inputBeforeTest: func(b *bus.Bus) {
				_, err := b.Subscribe("broker:pk_retained", "broker", 1024)
				require.Nil(t, err)
			},
			expectedErr: "subscriber broker already exists",
		},
		{
			inputOptions: &Options{
				MQTTPort:         -1,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			expectedErr: "listen tcp: address -1: invalid port",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			expectedLog: []string{
				"",
				"cluster broker started",
				"starting eventloop",
				"auth for MQTT disabled",
			},
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Auth:             auth.AuthRules{{Username: "test", Password: "test", Allow: true}},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
			},
			expectedLog: []string{
				"",
				"cluster broker started",
				"starting eventloop",
			},
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		logOutput.Reset()

		evBus := bus.New()
		test.inputOptions.Bus = evBus
		if test.inputBeforeTest != nil {
			test.inputBeforeTest(evBus)
		}
		broker, ctxCancel, err := New(test.inputOptions)

		if err != nil {
			require.Equal(t, test.expectedErr, err.Error())
		}

		time.Sleep(50 * time.Millisecond)
		if len(test.expectedLog) > 0 {
			for _, line := range strings.Split(logOutput.String(), "\n") {
				require.Contains(t, test.expectedLog, line)
			}
		}

		if err == nil {
			ctxCancel()
			broker.Shutdown()

			// We need to ensure that handlers are closed, otherwise they will break other tests.
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestShutdown(t *testing.T) {
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer ctxCancel()

	err = broker.Shutdown()
	require.Nil(t, err)
}

func TestSystemInfo(t *testing.T) {
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	si := broker.SystemInfo()
	require.NotNil(t, si)
}

func TestMessages(t *testing.T) {
	tests := []struct {
		inputFilter      string
		expectedMessages []packets.Packet
	}{
		{
			inputFilter: "test",
			expectedMessages: []packets.Packet{
				{
					TopicName:       "test",
					Payload:         []byte("test"),
					Origin:          "inline",
					ProtocolVersion: 4,
					FixedHeader: packets.FixedHeader{
						Qos:    0,
						Retain: true,
						Type:   3,
					},
				},
			},
		},
		{
			inputFilter: "test2",
			expectedMessages: []packets.Packet{
				{
					TopicName:       "test2",
					Payload:         []byte("test2"),
					Origin:          "inline",
					ProtocolVersion: 4,
					FixedHeader: packets.FixedHeader{
						Qos:    0,
						Retain: true,
						Type:   3,
					},
				},
			},
		},
		{
			inputFilter: "#",
			expectedMessages: []packets.Packet{
				{
					TopicName:       "test",
					Payload:         []byte("test"),
					Origin:          "inline",
					ProtocolVersion: 4,
					FixedHeader: packets.FixedHeader{
						Qos:    0,
						Retain: true,
						Type:   3,
					},
				},
				{
					TopicName:       "test2",
					Payload:         []byte("test2"),
					Origin:          "inline",
					ProtocolVersion: 4,
					FixedHeader: packets.FixedHeader{
						Qos:    0,
						Retain: true,
						Type:   3,
					},
				},
			},
		},
	}
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	broker.server.Publish("test", []byte("test"), true, 0)
	broker.server.Publish("test2", []byte("test2"), true, 0)
	now := time.Now().Unix()

	time.Sleep(100 * time.Millisecond)

	for _, test := range tests {
		messages := broker.Messages(test.inputFilter)
		for idx, pk := range test.expectedMessages {
			pk.Created = now
			test.expectedMessages[idx] = pk
		}
		require.ElementsMatch(t, test.expectedMessages, messages)
	}
}

func TestClients(t *testing.T) {
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetClientID("TestClients")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	clients := broker.Clients()

	require.Equal(t, []*MQTTClient{
		{
			ID:              "TestClients",
			CleanSession:    true,
			Subscriptions:   map[string]packets.Subscription{},
			ProtocolVersion: 4,
		},
	}, clients)

}

func TestClient(t *testing.T) {
	tests := []struct {
		inputClientID  string
		expectedClient *MQTTClient
		expectedErr    error
	}{
		{
			inputClientID: "missing",
			expectedErr:   errors.New("unknown client"),
		},
		{
			inputClientID: "TestClient",
			expectedClient: &MQTTClient{
				ID:              "TestClient",
				CleanSession:    true,
				Subscriptions:   map[string]packets.Subscription{},
				ProtocolVersion: 4,
			},
		},
	}
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetClientID("TestClient")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	for _, test := range tests {
		client, err := broker.Client(test.inputClientID)
		require.Equal(t, test.expectedErr, err)
		require.Equal(t, test.expectedClient, client)
	}
}

func TestStopClient(t *testing.T) {
	tests := []struct {
		inputClientID   string
		expectedClients []*MQTTClient
		expectedErr     error
	}{
		{
			inputClientID: "missing",
			expectedErr:   errors.New("unknown client"),
			expectedClients: []*MQTTClient{
				{
					ID:              "TestStopClient",
					CleanSession:    true,
					Subscriptions:   map[string]packets.Subscription{},
					ProtocolVersion: 4,
				},
			},
		},
		{
			inputClientID: "TestStopClient",
			expectedClients: []*MQTTClient{
				{
					ID:              "TestStopClient",
					CleanSession:    true,
					Subscriptions:   map[string]packets.Subscription{},
					ProtocolVersion: 4,
					Done:            true,
				},
			},
		},
	}
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetClientID("TestStopClient")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	for _, test := range tests {
		err := broker.StopClient(test.inputClientID, "test")
		require.Equal(t, test.expectedErr, err)
		clients := broker.Clients()
		require.Equal(t, test.expectedClients, clients)
	}
}

func TestInflights(t *testing.T) {
	tests := []struct {
		inputClientID     string
		expectedInflights []packets.Packet
		expectedErr       error
	}{
		{
			inputClientID: "missing",
			expectedErr:   errors.New("unknown client"),
		},
		{
			inputClientID: "TestInflights",
			expectedInflights: []packets.Packet{
				{
					Payload:   []byte("test"),
					TopicName: "TestInflights",
					Origin:    "inline",
					Properties: packets.Properties{
						SubscriptionIdentifier: []int{0},
					},
					FixedHeader: packets.FixedHeader{
						Type:   3,
						Qos:    2,
						Retain: true,
					},
					PacketID:        1,
					ProtocolVersion: 4,
				},
			},
		},
	}
	evBus := bus.New()
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetClientID("TestInflights").
		SetCleanSession(false)

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	if token := mqttClient.Subscribe("TestInflights", byte(2), nil); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}

	mqttClient.Disconnect(10)
	time.Sleep(50 * time.Millisecond)

	evBus.Publish("cluster:message_from", types.MQTTPublishMessage{
		Topic:   "TestInflights",
		Payload: []byte("test"),
		Retain:  true,
		Qos:     2,
	})
	now := time.Now().Unix()

	time.Sleep(100 * time.Millisecond)

	for _, test := range tests {
		inflights, err := broker.Inflights(test.inputClientID)
		require.Equal(t, test.expectedErr, err)
		for idx, pk := range test.expectedInflights {
			pk.Created = now
			pk.Expiry = now + broker.server.Options.Capabilities.MaximumMessageExpiryInterval
			test.expectedInflights[idx] = pk
		}

		require.Equal(t, test.expectedInflights, inflights)
	}
}

func TestSubscribers(t *testing.T) {
	tests := []struct {
		inputFilter         string
		expectedSubscribers *mqtt.Subscribers
	}{
		{
			inputFilter: "missing",
			expectedSubscribers: &mqtt.Subscribers{
				Shared:         make(map[string]map[string]packets.Subscription),
				SharedSelected: make(map[string]packets.Subscription),
				Subscriptions:  make(map[string]packets.Subscription),
			},
		},
		{
			inputFilter: "TestSubscribers",
			expectedSubscribers: &mqtt.Subscribers{
				Shared:         make(map[string]map[string]packets.Subscription),
				SharedSelected: make(map[string]packets.Subscription),
				Subscriptions: map[string]packets.Subscription{
					"TestSubscribers": {
						Filter:      "TestSubscribers",
						Qos:         2,
						Identifiers: map[string]int{"TestSubscribers": 0},
					},
				},
			},
		},
	}
	evBus := bus.New()
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetClientID("TestSubscribers")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(10)

	if token := mqttClient.Subscribe("TestSubscribers", byte(2), nil); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}

	for _, test := range tests {
		subscribers := broker.Subscribers(test.inputFilter)
		require.Equal(t, test.expectedSubscribers, subscribers)
	}
}

func TestPublishToMQTT(t *testing.T) {
	tests := []struct {
		inputMessage     types.MQTTPublishMessage
		expectedPayload  []byte
		expectedQos      byte
		expectedTopic    string
		expectedRetained bool
		expectedLog      []string
	}{
		{
			inputMessage: types.MQTTPublishMessage{Topic: "topic#", Payload: []byte("test"), Retain: true, Qos: 2},
			expectedLog: []string{
				"",
				"unable to publish message from cluster {[116 101 115 116] topic# true 2}: protocol violation: topic contains wildcards",
			},
		},
		{
			inputMessage:     types.MQTTPublishMessage{Topic: "topic", Payload: []byte("test"), Retain: true, Qos: 2},
			expectedPayload:  []byte("test"),
			expectedTopic:    "topic",
			expectedQos:      2,
			expectedRetained: true,
		},
		{
			inputMessage:     types.MQTTPublishMessage{Topic: "topic2", Payload: []byte("test2"), Retain: true, Qos: 1},
			expectedPayload:  []byte("test2"),
			expectedTopic:    "topic2",
			expectedQos:      1,
			expectedRetained: true,
		},
		{
			inputMessage:     types.MQTTPublishMessage{Topic: "topic3", Payload: []byte("test3"), Retain: true, Qos: 0},
			expectedPayload:  []byte("test3"),
			expectedTopic:    "topic3",
			expectedQos:      0,
			expectedRetained: true,
		},
		{
			inputMessage:     types.MQTTPublishMessage{Topic: "topic4", Payload: []byte("test4"), Retain: false, Qos: 2},
			expectedPayload:  []byte("test4"),
			expectedTopic:    "topic4",
			expectedQos:      2,
			expectedRetained: false,
		},
	}

	evBus := bus.New()
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})

	require.Nil(t, err)

	defer ctxCancel()
	defer broker.Shutdown()

	mqttReceiveQueue := make(chan paho.Message, 5)
	mqttOnMessage := func(client paho.Client, message paho.Message) {
		mqttReceiveQueue <- message
	}

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("paho.NewClient error: %s", token.Error())
	}

	if token := mqttClient.Subscribe("#", byte(0), mqttOnMessage); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}
	defer mqttClient.Disconnect(10)

	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		logOutput.Reset()

		evBus.Publish("cluster:message_from", test.inputMessage)

		time.Sleep(1 * time.Millisecond)

		if test.expectedPayload != nil {
			mqttMessage := <-mqttReceiveQueue
			require.Equal(t, test.expectedTopic, mqttMessage.Topic())
			require.Equal(t, test.expectedPayload, mqttMessage.Payload())
			require.Equal(t, test.expectedQos, mqttMessage.Qos())
			require.Equal(t, test.expectedRetained, mqttMessage.Retained())
		}
		if len(test.expectedLog) > 0 {
			for _, line := range strings.Split(logOutput.String(), "\n") {
				require.Contains(t, test.expectedLog, line)
			}
		}

		time.Sleep(10 * time.Millisecond)

	}
}

func TestHandleNewMember(t *testing.T) {
	tests := []struct {
		inputNewMember  string
		expectedMessage types.DiscoveryPublishMessage
	}{
		{

			inputNewMember: "node1",
			expectedMessage: types.DiscoveryPublishMessage{
				Node:    []string{"node1"},
				Payload: []byte("test"),
				Topic:   "test",
				Retain:  true,
			},
		},
	}

	evBus := bus.New()
	ch, err := evBus.Subscribe("cluster:message_to", "t", 1024)
	require.Nil(t, err)

	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	err = broker.server.Publish("test", []byte("test"), true, 0)
	require.Nil(t, err)

	for _, test := range tests {
		evBus.Publish("broker:send_retained", test.inputNewMember)

		e := <-ch
		require.Equal(t, test.expectedMessage, e.Data.(types.DiscoveryPublishMessage))

		time.Sleep(1 * time.Millisecond)
	}
}
