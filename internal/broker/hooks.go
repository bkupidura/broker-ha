package broker

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"log"

	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/packets"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

// Hook implements mochi-co/mqtt hooks.
type Hook struct {
	mqtt.HookBase
	bus          *bus.Bus
	retainedHash hash.Hash
}

// Provides list of supported hooks.
func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnPublish,
		mqtt.OnRetainMessage,
	}, []byte{b})
}

// Init hook.
func (h *Hook) Init(config any) error {
	c := config.(map[string]interface{})
	h.bus = c["bus"].(*bus.Bus)
	h.retainedHash = sha256.New()
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

func (h *Hook) OnRetainMessage(cl *mqtt.Client, pk packets.Packet, r int64) {
	message := types.DiscoveryPublishMessage{
		Payload: pk.Payload,
		Topic:   pk.TopicName,
		Retain:  pk.FixedHeader.Retain,
		Qos:     pk.FixedHeader.Qos,
		Node:    []string{"all"},
	}
	log.Printf("new ratained message %+v", message)
	data, err := json.Marshal(message)
	if err != nil {
		return
	}
	h.retainedHash.Write(data)
	log.Printf("new retained hash %x", h.retainedHash.Sum(nil))

	h.bus.Publish("cluster:retained_hash", fmt.Sprintf("%x", h.retainedHash.Sum(nil)))
}

// ID hook.
func (h *Hook) ID() string {
	return "broker-ha"
}
