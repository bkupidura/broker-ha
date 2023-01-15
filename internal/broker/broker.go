package broker

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/listeners"
	"github.com/mochi-co/mqtt/v2/packets"
	"github.com/mochi-co/mqtt/v2/system"
	"github.com/rs/zerolog"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

// MQTTClient stores mqtt client details.
type MQTTClient struct {
	ID              string
	ProtocolVersion byte
	Username        string
	CleanSession    bool
	Done            bool
	Subscriptions   map[string]packets.Subscription
}

// Broker is abstraction over mqtt.Server.
type Broker struct {
	server *mqtt.Server
	bus    *bus.Bus
}

// New creates and starts Broker instance.
func New(opts *Options) (*Broker, context.CancelFunc, error) {
	fromClusterSize, ok := opts.SubscriptionSize["cluster:message_from"]
	if !ok {
		return nil, nil, fmt.Errorf("subscription size for cluster:message_from not provided")
	}

	chFromCluster, err := opts.Bus.Subscribe("cluster:message_from", "broker", fromClusterSize)
	if err != nil {
		return nil, nil, err
	}

	newMemberSize, ok := opts.SubscriptionSize["cluster:new_member"]
	if !ok {
		return nil, nil, fmt.Errorf("subscription size for cluster:new_member not provided")
	}
	chNewMember, err := opts.Bus.Subscribe("cluster:new_member", "broker", newMemberSize)
	if err != nil {
		return nil, nil, err
	}

	mqttDefaultCapabilities := mqtt.DefaultServerCapabilities
	mqttDefaultCapabilities.MaximumMessageExpiryInterval = 60 * 30

	mqttServer := mqtt.New(&mqtt.Options{
		Capabilities: mqttDefaultCapabilities,
	})
	l := mqttServer.Log.Level(zerolog.Disabled)
	mqttServer.Log = &l

	if len(opts.Auth) > 0 {
		if err := mqttServer.AddHook(new(auth.Hook), &auth.Options{
			Ledger: &auth.Ledger{
				Auth: opts.Auth,
				ACL:  opts.ACL,
			},
		}); err != nil {
			return nil, nil, err
		}
	} else {
		log.Printf("auth for mqtt disabled")
		if err := mqttServer.AddHook(new(auth.AllowHook), nil); err != nil {
			return nil, nil, err
		}
	}
	if err := mqttServer.AddHook(new(Hook), map[string]interface{}{"bus": opts.Bus}); err != nil {
		return nil, nil, err
	}

	listener := listeners.NewTCP("tcp", fmt.Sprintf(":%d", opts.MQTTPort), nil)

	if err := mqttServer.AddListener(listener); err != nil {
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
	return b.server.Info
}

// Messages returns stored messages based on filter.
// Filter can use regular MQTT wildcards (#, +).
func (b *Broker) Messages(filter string) []packets.Packet {
	return b.server.Topics.Messages(filter)
}

// Clients returns all MQTT clients.
func (b *Broker) Clients() []*MQTTClient {
	var clients []*MQTTClient
	for _, c := range b.server.Clients.GetAll() {
		clients = append(clients, &MQTTClient{
			ID:              c.ID,
			ProtocolVersion: c.Properties.ProtocolVersion,
			Username:        string(c.Properties.Username),
			CleanSession:    c.Properties.Clean,
			Done:            c.Closed(),
			Subscriptions:   c.State.Subscriptions.GetAll(),
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
		ID:              client.ID,
		ProtocolVersion: client.Properties.ProtocolVersion,
		Username:        string(client.Properties.Username),
		CleanSession:    client.Properties.Clean,
		Done:            client.Closed(),
		Subscriptions:   client.State.Subscriptions.GetAll(),
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
func (b *Broker) Inflights(clientID string) ([]packets.Packet, error) {
	client, ok := b.server.Clients.Get(clientID)
	if !ok {
		return nil, errors.New("unknown client")
	}
	return client.State.Inflight.GetAll(false), nil
}

// Subscribers returns clientIDs subscribed for topic.
func (b *Broker) Subscribers(filter string) *mqtt.Subscribers {
	return b.server.Topics.Subscribers(filter)
}

// publishToMQTT will receive messages from discovery (memberlist), and publish them to local mqtt server.
func (b *Broker) publishToMQTT(ctx context.Context, ch chan bus.Event) {
	log.Printf("starting publishToMQTT worker")
	for {
		select {
		case event := <-ch:
			message := event.Data.(types.MQTTPublishMessage)
			if err := b.server.Publish(message.Topic, message.Payload, message.Retain, message.Qos); err != nil {
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
				m := types.DiscoveryPublishMessage{
					Payload: message.Payload,
					Topic:   message.TopicName,
					Retain:  message.FixedHeader.Retain,
					Qos:     message.FixedHeader.Qos,
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
