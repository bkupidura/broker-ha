package broker

import (
	"bytes"
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	mqtt "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/events"
	"github.com/stretchr/testify/require"

	"brokerha/internal/discovery"
)

func TestOnMessage(t *testing.T) {
	eventClient := events.Client{
		ID:       "testid",
		Remote:   "unknown",
		Listener: "testlistener",
	}
	eventPacket := events.Packet{
		Payload:   []byte("test"),
		TopicName: "topic",
	}
	onMessage(eventClient, eventPacket)

	message := <-discovery.MQTTPublishToCluster

	require.Equal(t, []byte("test"), message.Payload)
	require.Equal(t, "topic", message.Topic)
}

func TestNew(t *testing.T) {
	tests := []struct {
		port        int
		expectedErr string
		expectedLog string
	}{
		{
			port:        -1,
			expectedErr: "listen tcp: address -1: invalid port",
		},
		{
			port:        1883,
			expectedLog: "cluster broker started\nstarting SendRetained queue worker\nstarting MQTTPublishFromCluster queue worker\n",
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		logOutput.Reset()

		_, ctxCancel, err := New(&Options{
			MQTTPort: test.port,
		})

		time.Sleep(1 * time.Millisecond)
		if err != nil {
			require.Equal(t, test.expectedErr, err.Error())
		}
		require.Equal(t, test.expectedLog, logOutput.String())

		if err == nil {
			ctxCancel()

			// We need to ensure that handlers are closed, otherwise they will break other tests.
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestHandleMQTTPublishFromCluster(t *testing.T) {
	tests := []struct {
		inputMessage *discovery.MQTTPublishMessage
		expectedLog  string
	}{
		{
			inputMessage: &discovery.MQTTPublishMessage{Topic: "$SYS", Payload: []byte("test")},
			expectedLog:  "starting MQTTPublishFromCluster queue worker\nunable to publish message from cluster &{[] [116 101 115 116] $SYS false 0}: cannot publish to $ and $SYS topics\n",
		},
		{
			inputMessage: &discovery.MQTTPublishMessage{Topic: "topic", Payload: []byte("test")},
			expectedLog:  "starting MQTTPublishFromCluster queue worker\n",
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	mqttServer := mqtt.NewServer(&mqtt.Options{})

	for _, test := range tests {
		logOutput.Reset()
		ctx, ctxCancel := context.WithCancel(context.Background())
		go handleMQTTPublishFromCluster(ctx, mqttServer)

		discovery.MQTTPublishFromCluster <- test.inputMessage

		time.Sleep(1 * time.Millisecond)
		require.Equal(t, test.expectedLog, logOutput.String())

		logOutput.Reset()
		ctxCancel()

		time.Sleep(1 * time.Millisecond)
		require.Equal(t, "MQTTPublishFromCluster queue worker done\n", logOutput.String())
	}
}

func TestHandleSendRetained(t *testing.T) {
	tests := []struct {
		inputNode    *memberlist.Node
		expectedLog  string
		expectedNode []string
	}{
		{
			inputNode:    &memberlist.Node{Addr: net.ParseIP("127.0.0.1"), Port: 7946},
			expectedLog:  "starting SendRetained queue worker\n",
			expectedNode: []string{"127.0.0.1:7946"},
		},
	}

	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	mqttServer := mqtt.NewServer(&mqtt.Options{})
	mqttServer.Publish("topic", []byte("payload"), true)

	for _, test := range tests {
		logOutput.Reset()
		ctx, ctxCancel := context.WithCancel(context.Background())

		go handleSendRetained(ctx, mqttServer)

		discovery.MQTTSendRetained <- test.inputNode
		publishedMessage := <-discovery.MQTTPublishToCluster

		require.Equal(t, "topic", publishedMessage.Topic)
		require.Equal(t, []byte("payload"), publishedMessage.Payload)
		require.Equal(t, true, publishedMessage.Retain)
		require.Equal(t, test.expectedNode, publishedMessage.Node)
		require.Equal(t, test.expectedLog, logOutput.String())

		logOutput.Reset()
		ctxCancel()

		time.Sleep(1 * time.Millisecond)
		require.Equal(t, "SendRetained queue worker done\n", logOutput.String())
	}
}
