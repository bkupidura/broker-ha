package broker

import (
	"bytes"
	"context"
	"errors"
	"log"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/events"
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
				AuthUsers:        map[string]string{"test": "test"},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
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
				AuthUsers:        map[string]string{"test": "test"},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
			},
			inputMQTTClientOps: paho.NewClientOptions().
				AddBroker("127.0.0.1:1883").
				SetAutoReconnect(false).
				SetUsername("test").
				SetPassword("test2").
				SetClientID("client1"),
			expectedError: errors.New("bad user name or password"),
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Bus:              bus.New(),
				AuthUsers:        map[string]string{"test": "test"},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
			},
			inputMQTTClientOps: paho.NewClientOptions().
				AddBroker("127.0.0.1:1883").
				SetAutoReconnect(false).
				SetUsername("test2").
				SetPassword("test").
				SetClientID("client1"),
			expectedError: errors.New("bad user name or password"),
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				Bus:              bus.New(),
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
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
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
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
		expectedLog     string
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
				SubscriptionSize: map[string]int{"cluster:new_member": 10},
			},
			expectedErr: "subscription size for cluster:message_from not provided",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024},
			},
			expectedErr: "subscription size for cluster:new_member not provided",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
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
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
			},
			inputBeforeTest: func(b *bus.Bus) {
				_, err := b.Subscribe("cluster:new_member", "broker", 1024)
				require.Nil(t, err)
			},
			expectedErr: "subscriber broker already exists",
		},
		{
			inputOptions: &Options{
				MQTTPort:         -1,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
			},
			expectedErr: "listen tcp: address -1: invalid port",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
			},
			expectedLog: "cluster broker started\nstarting handleNewMember worker\nstarting publishToMQTT worker\n",
		},
		{
			inputOptions: &Options{
				MQTTPort:         1883,
				AuthUsers:        map[string]string{"test": "test"},
				SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
			},
			expectedLog: "cluster broker started\nstarting handleNewMember worker\nstarting publishToMQTT worker\n",
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
		require.Equal(t, test.expectedLog, logOutput.String())

		if err == nil {
			ctxCancel()
			broker.server.Close()

			// We need to ensure that handlers are closed, otherwise they will break other tests.
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestShutdown(t *testing.T) {
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
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
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer broker.server.Close()
	defer ctxCancel()

	si := broker.SystemInfo()
	require.NotNil(t, si)
}

func TestMessages(t *testing.T) {
	tests := []struct {
		inputFilter      string
		expectedMessages []*types.MQTTPublishMessage
	}{
		{
			inputFilter: "test",
			expectedMessages: []*types.MQTTPublishMessage{
				{
					Topic:   "test",
					Payload: []byte("test"),
					Qos:     0,
					Retain:  true,
				},
			},
		},
		{
			inputFilter: "test2",
			expectedMessages: []*types.MQTTPublishMessage{
				{
					Topic:   "test2",
					Payload: []byte("test2"),
					Qos:     0,
					Retain:  true,
				},
			},
		},
		{
			inputFilter: "#",
			expectedMessages: []*types.MQTTPublishMessage{
				{
					Topic:   "test",
					Payload: []byte("test"),
					Qos:     0,
					Retain:  true,
				},
				{
					Topic:   "test2",
					Payload: []byte("test2"),
					Qos:     0,
					Retain:  true,
				},
			},
		},
	}
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer broker.server.Close()
	defer ctxCancel()

	broker.server.Publish("test", []byte("test"), true)
	broker.server.Publish("test2", []byte("test2"), true)

	time.Sleep(200 * time.Millisecond)

	for _, test := range tests {
		messages := broker.Messages(test.inputFilter)
		require.Equal(t, test.expectedMessages, messages)
	}
}

func TestClients(t *testing.T) {
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		AuthUsers:        map[string]string{"test": "test"},
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer broker.server.Close()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
		SetClientID("TestClients")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	defer mqttClient.Disconnect(1)

	clients := broker.Clients()
	require.Equal(t, []*MQTTClient{
		{
			ID:            "TestClients",
			Done:          0,
			Username:      []byte("test"),
			Subscriptions: map[string]byte{},
			CleanSession:  true,
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
				ID:            "TestClient",
				Done:          0,
				Username:      []byte("test"),
				Subscriptions: map[string]byte{},
				CleanSession:  true,
			},
		},
	}
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		AuthUsers:        map[string]string{"test": "test"},
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer broker.server.Close()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
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
					ID:            "TestStopClient",
					Done:          0,
					Username:      []byte("test"),
					Subscriptions: map[string]byte{},
					CleanSession:  true,
				},
			},
		},
		{
			inputClientID: "TestStopClient",
			expectedClients: []*MQTTClient{
				{
					ID:            "TestStopClient",
					Done:          1,
					Username:      []byte("test"),
					Subscriptions: map[string]byte{},
					CleanSession:  true,
				},
			},
		},
	}
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              bus.New(),
		AuthUsers:        map[string]string{"test": "test"},
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer broker.server.Close()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
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
		expectedInflights []*types.MQTTPublishMessage
		expectedErr       error
	}{
		{
			inputClientID: "missing",
			expectedErr:   errors.New("unknown client"),
		},
		{
			inputClientID: "TestInflights",
			expectedInflights: []*types.MQTTPublishMessage{
				{
					Payload: []byte("test"),
					Topic:   "TestInflights",
					Retain:  true,
					Qos:     2,
				},
			},
		},
	}
	evBus := bus.New()
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              evBus,
		AuthUsers:        map[string]string{"test": "test"},
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
		SetClientID("TestInflights")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Connect error: %s", token.Error())
	}
	if token := mqttClient.Subscribe("TestInflights", byte(2), nil); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}

	mqttClient.Disconnect(10)
	time.Sleep(50 * time.Millisecond)

	evBus.Publish("cluster:message_from", &types.MQTTPublishMessage{
		Topic:   "TestInflights",
		Payload: []byte("test"),
		Retain:  true,
	})

	time.Sleep(100 * time.Millisecond)

	for _, test := range tests {
		inflights, err := broker.Inflights(test.inputClientID)
		require.Equal(t, test.expectedErr, err)
		require.Equal(t, test.expectedInflights, inflights)
	}
}

func TestSubscribers(t *testing.T) {
	tests := []struct {
		inputFilter         string
		expectedSubscribers map[string]byte
	}{
		{
			inputFilter:         "missing",
			expectedSubscribers: map[string]byte{},
		},
		{
			inputFilter: "TestSubscribers",
			expectedSubscribers: map[string]byte{
				"TestSubscribers": byte(2),
			},
		},
	}
	evBus := bus.New()
	broker, ctxCancel, err := New(&Options{
		MQTTPort:         1883,
		Bus:              evBus,
		AuthUsers:        map[string]string{"test": "test"},
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	require.Nil(t, err)
	defer broker.Shutdown()
	defer ctxCancel()

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetAutoReconnect(false).
		SetUsername("test").
		SetPassword("test").
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

func TestOnMessage(t *testing.T) {
	evBus := bus.New()
	ch, err := evBus.Subscribe("cluster:message_to", "TestOnMessage", 1024)
	require.Nil(t, err)

	broker := &Broker{
		bus: evBus,
	}
	eventClient := events.Client{
		ID:       "testid",
		Remote:   "unknown",
		Listener: "testlistener",
	}
	eventPacket := events.Packet{
		Payload:   []byte("test"),
		TopicName: "topic",
	}
	broker.onMessage(eventClient, eventPacket)

	e := <-ch
	message := e.Data.(*types.DiscoveryPublishMessage)

	require.Equal(t, []byte("test"), message.Payload)
	require.Equal(t, "topic", message.Topic)
}

func TestPublishToMQTT(t *testing.T) {
	tests := []struct {
		inputMessage    *types.MQTTPublishMessage
		expectedLog     string
		expectedMessage *types.MQTTPublishMessage
	}{
		{
			inputMessage: &types.MQTTPublishMessage{Topic: "$SYS", Payload: []byte("test")},
			expectedLog:  "starting publishToMQTT worker\nunable to publish message from cluster &{[116 101 115 116] $SYS false 0}: cannot publish to $ and $SYS topics\n",
		},
		{
			inputMessage:    &types.MQTTPublishMessage{Topic: "topic", Payload: []byte("test"), Retain: true},
			expectedLog:     "starting publishToMQTT worker\n",
			expectedMessage: &types.MQTTPublishMessage{Topic: "topic", Payload: []byte("test"), Retain: true, Qos: 0},
		},
	}

	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	evBus := bus.New()
	ch, err := evBus.Subscribe("cluster:message_from", "broker", 1024)
	require.Nil(t, err)

	broker := &Broker{
		server: mqtt.NewServer(&mqtt.Options{}),
		bus:    evBus,
	}
	defer broker.server.Close()

	for _, test := range tests {
		logOutput.Reset()

		ctx, ctxCancel := context.WithCancel(context.Background())
		go broker.publishToMQTT(ctx, ch)

		evBus.Publish("cluster:message_from", test.inputMessage)

		time.Sleep(1 * time.Millisecond)
		require.Equal(t, test.expectedLog, logOutput.String())

		if test.expectedMessage != nil {
			messages := broker.Messages("#")
			require.Contains(t, messages, test.expectedMessage)
		}

		ctxCancel()
		time.Sleep(10 * time.Millisecond)

	}
}

func TestHandleNewMember(t *testing.T) {
	tests := []struct {
		inputNewMember  string
		expectedLog     string
		expectedMessage *types.DiscoveryPublishMessage
	}{
		{
			inputNewMember: "127.0.0.1:9746",
			expectedLog:    "starting handleNewMember queue worker\n",
			expectedMessage: &types.DiscoveryPublishMessage{
				Node:    []string{"127.0.0.1:9746"},
				Payload: []byte("test"),
				Topic:   "test",
				Retain:  true,
			},
		},
	}

	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	evBus := bus.New()
	ch, err := evBus.Subscribe("cluster:new_member", "broker", 10)
	require.Nil(t, err)

	ch2, err := evBus.Subscribe("cluster:message_to", "broker", 1024)
	require.Nil(t, err)

	broker := &Broker{
		server: mqtt.NewServer(&mqtt.Options{}),
		bus:    evBus,
	}
	defer broker.server.Close()

	err = broker.server.Publish("test", []byte("test"), true)
	require.Nil(t, err)

	for _, test := range tests {
		logOutput.Reset()
		ctx, ctxCancel := context.WithCancel(context.Background())

		go broker.handleNewMember(ctx, ch)

		evBus.Publish("cluster:new_member", test.inputNewMember)

		e := <-ch2
		require.Equal(t, test.expectedMessage, e.Data.(*types.DiscoveryPublishMessage))

		ctxCancel()
		time.Sleep(1 * time.Millisecond)
	}
}
