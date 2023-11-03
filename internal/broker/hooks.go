package broker

import (
	"bytes"

	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

// Hook implements mochi-co/mqtt hooks.
type Hook struct {
	mqtt.HookBase
	bus *bus.Bus
}

// Provides list of supported hooks.
func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnPublish,
	}, []byte{b})
}

// Init hook.
func (h *Hook) Init(config any) error {
	c := config.(map[string]interface{})
	h.bus = c["bus"].(*bus.Bus)
	return nil
}

// OnPublish will be executed on receiving PUB package.
// It will push mqtt packet received from client to other cluster members.
func (h *Hook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if !cl.Net.Inline {
		message := types.DiscoveryPublishMessage{
			Payload: pk.Payload,
			Topic:   pk.TopicName,
			Retain:  pk.FixedHeader.Retain,
			Qos:     pk.FixedHeader.Qos,
			Node:    []string{"all"},
		}
		h.bus.Publish("cluster:message_to", message)
	}
	return pk, nil
}

// ID hook.
func (h *Hook) ID() string {
	return "broker-ha"
}
