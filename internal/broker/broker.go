package broker

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/mochi-mqtt/server/v2/system"

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
	for _, requiredSubscriptionSizeName := range []string{"cluster:message_from", "cluster:new_member"} {
		if _, ok := opts.SubscriptionSize[requiredSubscriptionSizeName]; !ok {
			return nil, nil, fmt.Errorf("subscription size for %s not provided", requiredSubscriptionSizeName)
		}
	}

	chClusterMessageFrom, err := opts.Bus.Subscribe("cluster:message_from", "broker", opts.SubscriptionSize["cluster:message_from"])
	if err != nil {
		return nil, nil, err
	}

	chClusterNewMember, err := opts.Bus.Subscribe("cluster:new_member", "broker", opts.SubscriptionSize["cluster:new_member"])
	if err != nil {
		return nil, nil, err
	}

	mqttDefaultCapabilities := mqtt.DefaultServerCapabilities
	// We wants very long expiry time not to lose any retained messages.
	mqttDefaultCapabilities.MaximumMessageExpiryInterval = 0

	mqttServer := mqtt.New(&mqtt.Options{
		Capabilities: mqttDefaultCapabilities,
		InlineClient: true,
	})

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
		log.Printf("auth for MQTT disabled")
		if err := mqttServer.AddHook(new(auth.AllowHook), nil); err != nil {
			return nil, nil, err
		}
	}
	if err := mqttServer.AddHook(new(Hook), map[string]interface{}{"bus": opts.Bus}); err != nil {
		return nil, nil, err
	}

	listener := listeners.NewTCP(listeners.Config{ID: "tcp", Address: fmt.Sprintf(":%d", opts.MQTTPort)})

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

	go broker.eventLoop(ctx, chClusterMessageFrom, chClusterNewMember)

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
func (b *Broker) publishToMQTT(message types.MQTTPublishMessage) {
	if err := b.server.Publish(message.Topic, message.Payload, message.Retain, message.Qos); err != nil {
		log.Printf("unable to publish message from cluster %v: %s", message, err)
	}
}

// sendRetained will send all retained messages to requested node.
func (b *Broker) sendRetained(member string) {
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
}

// eventLoop perform maintenance tasks.
func (b *Broker) eventLoop(ctx context.Context, chClusterMessageFrom chan bus.Event, chClusterNewMember chan bus.Event) {
	log.Printf("starting eventloop")
	for {
		select {
		case event := <-chClusterMessageFrom:
			message := event.Data.(types.MQTTPublishMessage)
			b.publishToMQTT(message)
		case event := <-chClusterNewMember:
			member := event.Data.(string)
			b.sendRetained(member)
		case <-ctx.Done():
			log.Printf("stopping eventloop")
			return
		}
	}
}
