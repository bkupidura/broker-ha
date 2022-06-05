package server

import (
	"context"
	"log"

	"broker/discovery"

	mqtt "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/events"
	"github.com/mochi-co/mqtt/server/listeners"
)

// onMessage handler is executed when mqtt broker receives new message from clients.
// Its not executed on mqttServer.Publish().
// When new message is received, it will be send to MQTTPublishToCluster to propagate it across cluster members.
func onMessage(cl events.Client, pk events.Packet) (pkx events.Packet, err error) {
	message := &discovery.MQTTPublishMessage{
		Payload: pk.Payload,
		Topic:   pk.TopicName,
		Retain:  pk.FixedHeader.Retain,
		Qos:     pk.FixedHeader.Qos,
	}
	discovery.MQTTPublishToCluster <- message

	return pk, nil
}

// New creates and starts mqtt broker.
func New(listener listeners.Listener, auth *Auth) (*mqtt.Server, context.CancelFunc, error) {
	options := &mqtt.Options{
		BufferSize:      0,
		BufferBlockSize: 0,
	}

	mqttServer := mqtt.NewServer(options)

	if err := mqttServer.AddListener(listener, &listeners.Config{Auth: auth}); err != nil {
		return nil, nil, err
	}

	mqttServer.Events.OnMessage = onMessage

	go func() {
		if err := mqttServer.Serve(); err != nil {
			log.Fatalf("mqttServer.Serve worker died: %s", err)
		}
	}()

	// ctx is used only by tests.
	ctx, ctxCancel := context.WithCancel(context.Background())

	go handleMQTTPublishFromCluster(ctx, mqttServer)
	go handleSendRetained(ctx, mqttServer)

	log.Printf("cluster broker started")

	return mqttServer, ctxCancel, nil
}

// handleMQTTPublishFromCluster will receive messages from discovery (memberlist), and publish them to local mqtt server.
func handleMQTTPublishFromCluster(ctx context.Context, mqttServer *mqtt.Server) {
	log.Printf("starting MQTTPublishFromCluster queue worker")
	for {
		select {
		case message := <-discovery.MQTTPublishFromCluster:
			if err := mqttServer.Publish(message.Topic, message.Payload, message.Retain); err != nil {
				log.Printf("unable to publish message from cluster %v: %s", message, err)
			}
		case <-ctx.Done():
			log.Printf("MQTTPublishFromCluster queue worker done")
			return
		}
	}
}

// handleSendRetained will receive cluster member which just joined cluster.
// We will send all localy retained messages to new node and sync it with rest of the cluster.
func handleSendRetained(ctx context.Context, mqttServer *mqtt.Server) {
	log.Printf("starting SendRetained queue worker")
	for {
		select {
		case member := <-discovery.MQTTSendRetained:
			for _, retainedMessage := range mqttServer.Topics.Messages("#") {
				publishMessage := &discovery.MQTTPublishMessage{
					Payload: retainedMessage.Payload,
					Topic:   retainedMessage.TopicName,
					Retain:  retainedMessage.FixedHeader.Retain,
					Qos:     retainedMessage.FixedHeader.Qos,
					Node:    []string{member.Address()},
				}
				discovery.MQTTPublishToCluster <- publishMessage
			}
		case <-ctx.Done():
			log.Printf("SendRetained queue worker done")
			return
		}
	}
}
