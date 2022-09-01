package broker

import (
	"context"
	"errors"
	"fmt"
	"log"

	mqtt "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/events"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/mochi-co/mqtt/server/system"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

// MQTTClient stores mqtt client details.
type MQTTClient struct {
	ID            string
	Done          uint32
	Username      []byte
	Subscriptions map[string]byte
	CleanSession  bool
}

// Broker is abstraction over mqtt.Server.
type Broker struct {
	server *mqtt.Server
	bus    *bus.Bus
}

// New creates and starts Broker instance.
func New(opts *Options) (*Broker, context.CancelFunc, error) {
	chFromCluster, err := opts.Bus.Subscribe("cluster:message_from", "broker", 1024)
	if err != nil {
		return nil, nil, err
	}

	chNewMember, err := opts.Bus.Subscribe("cluster:new_member", "broker", 10)
	if err != nil {
		return nil, nil, err
	}

	mqttServer := mqtt.NewServer(&mqtt.Options{
		BufferSize:      0,
		BufferBlockSize: 0,
		InflightTTL:     60 * 5,
	})

	listener := listeners.NewTCP("tcp", fmt.Sprintf(":%d", opts.MQTTPort))

	var listenerConfig *listeners.Config
	if len(opts.AuthUsers) > 0 {
		listenerConfig = &listeners.Config{
			Auth: &Auth{
				Users:   opts.AuthUsers,
				UserACL: opts.AuthACL,
			},
		}
	} else {
		listenerConfig = nil
	}

	if err := mqttServer.AddListener(listener, listenerConfig); err != nil {
		return nil, nil, err
	}

	go func() {
		if err := mqttServer.Serve(); err != nil {
			log.Fatalf("mqttServer.Serve worker died: %s", err)
		}
	}()

	broker := &Broker{
		server: mqttServer,
		bus:    opts.Bus,
	}

	broker.server.Events.OnMessage = broker.onMessage

	// ctx is used only by tests.
	ctx, ctxCancel := context.WithCancel(context.Background())

	go broker.publishToMQTT(ctx, chFromCluster)
	go broker.handleNewMember(ctx, chNewMember)

	log.Printf("cluster broker started")

	return broker, ctxCancel, nil
}

// Shutdown broker.
func (b *Broker) Shutdown() error {
	return b.server.Close()
}

// SystemInfo returns broker system info.
func (b *Broker) SystemInfo() *system.Info {
	return b.server.System
}

// Messages returns stored messages based on filter.
// Filter can use regular MQTT wildcards (#, +).
func (b *Broker) Messages(filter string) []*types.MQTTPublishMessage {
	var messages []*types.MQTTPublishMessage
	for _, m := range b.server.Topics.Messages(filter) {
		messages = append(messages, &types.MQTTPublishMessage{
			Payload: m.Payload,
			Topic:   m.TopicName,
			Retain:  m.FixedHeader.Retain,
			Qos:     m.FixedHeader.Qos,
		})
	}
	return messages
}

// Clients returns all MQTT clients.
func (b *Broker) Clients() []*MQTTClient {
	var clients []*MQTTClient
	for _, c := range b.server.Clients.GetAll() {
		clients = append(clients, &MQTTClient{
			ID:            c.ID,
			Done:          c.State.Done,
			Username:      c.Username,
			Subscriptions: c.Subscriptions,
			CleanSession:  c.CleanSession,
		})
	}
	return clients
}

// Client returns MQTT client with specified clientID.
func (b *Broker) Client(clientID string) (*MQTTClient, error) {
	client, ok := b.server.Clients.Get(clientID)
	if !ok {
		return nil, errors.New("unknown client")
	}
	return &MQTTClient{
		ID:            client.ID,
		Done:          client.State.Done,
		Username:      client.Username,
		Subscriptions: client.Subscriptions,
		CleanSession:  client.CleanSession,
	}, nil
}

// StopClient disconnects MQTT client with specified clientID.
func (b *Broker) StopClient(clientID, reason string) error {
	client, ok := b.server.Clients.Get(clientID)
	if !ok {
		return errors.New("unknown client")
	}
	client.Stop(errors.New(reason))
	return nil
}

// Inflights returns in-flight MQTT messages for clientID.
func (b *Broker) Inflights(clientID string) ([]*types.MQTTPublishMessage, error) {
	var messages []*types.MQTTPublishMessage
	client, ok := b.server.Clients.Get(clientID)
	if !ok {
		return nil, errors.New("unknown client")
	}
	for _, m := range client.Inflight.GetAll() {
		messages = append(messages, &types.MQTTPublishMessage{
			Payload: m.Packet.Payload,
			Topic:   m.Packet.TopicName,
			Retain:  m.Packet.FixedHeader.Retain,
			Qos:     m.Packet.FixedHeader.Qos,
		})
	}
	return messages, nil
}

// Subscribers returns clientIDs subscribed for topic.
func (b *Broker) Subscribers(filter string) map[string]byte {
	return b.server.Topics.Subscribers(filter)
}

// onMessage handler is executed when mqtt broker receives new message from clients.
// Its not executed on mqttServer.Publish().
// When new message is received, it will be send to bus and propagated across cluster members.
func (b *Broker) onMessage(cl events.Client, pk events.Packet) (pkx events.Packet, err error) {
	message := &types.DiscoveryPublishMessage{
		Payload: pk.Payload,
		Topic:   pk.TopicName,
		Retain:  pk.FixedHeader.Retain,
		Qos:     pk.FixedHeader.Qos,
	}
	b.bus.Publish("cluster:message_to", message)

	return pk, nil
}

// publishToMQTT will receive messages from discovery (memberlist), and publish them to local mqtt server.
func (b *Broker) publishToMQTT(ctx context.Context, ch chan bus.Event) {
	log.Printf("starting publishToMQTT worker")
	for {
		select {
		case event := <-ch:
			message := event.Data.(*types.MQTTPublishMessage)
			if err := b.server.Publish(message.Topic, message.Payload, message.Retain); err != nil {
				log.Printf("unable to publish message from cluster %v: %s", message, err)
			}
		case <-ctx.Done():
			log.Printf("publishToMQTT worker done")
			return
		}
	}
}

// handleNewMember will receive cluster member which just joined cluster.
// We will send all localy retained messages to new node and sync it with rest of the cluster.
func (b *Broker) handleNewMember(ctx context.Context, ch chan bus.Event) {
	log.Printf("starting handleNewMember worker")
	for {
		select {
		case event := <-ch:
			member := event.Data.(string)
			for _, message := range b.Messages("#") {
				m := &types.DiscoveryPublishMessage{
					Payload: message.Payload,
					Topic:   message.Topic,
					Retain:  message.Retain,
					Qos:     message.Qos,
					Node:    []string{member},
				}
				b.bus.Publish("cluster:message_to", m)
			}
		case <-ctx.Done():
			log.Printf("handleNewMember worker done")
			return
		}
	}
}
