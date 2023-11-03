package broker

import (
	"testing"

	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

func TestInit(t *testing.T) {
	h := &Hook{}
	evBus := bus.New()

	err := h.Init(map[string]interface{}{"bus": evBus})
	require.Nil(t, err)
	require.Equal(t, evBus, h.bus)
}

func TestProvides(t *testing.T) {
	h := &Hook{}

	require.Equal(t, true, h.Provides(mqtt.OnPublish))
}

func TestOnPublish(t *testing.T) {
	s := mqtt.New(&mqtt.Options{})
	defer s.Close()

	tests := []struct {
		inputMqttClient *mqtt.Client
		inputMqttPacket packets.Packet
		expectedMessage types.DiscoveryPublishMessage
		readFromChannel bool
	}{
		{
			inputMqttClient: s.NewClient(nil, "local", "inline", true),
			inputMqttPacket: packets.Packet{
				FixedHeader: packets.FixedHeader{
					Type:   packets.Publish,
					Qos:    2,
					Retain: true,
				},
				TopicName: "test",
				Payload:   []byte("test"),
			},
			readFromChannel: false,
		},
		{
			inputMqttClient: s.NewClient(nil, "local", "inline", false),
			inputMqttPacket: packets.Packet{
				FixedHeader: packets.FixedHeader{
					Type:   packets.Publish,
					Qos:    2,
					Retain: true,
				},
				TopicName: "test",
				Payload:   []byte("test"),
			},
			expectedMessage: types.DiscoveryPublishMessage{
				Topic:   "test",
				Payload: []byte("test"),
				Retain:  true,
				Qos:     2,
				Node:    []string{"all"},
			},
			readFromChannel: true,
		},
		{
			inputMqttClient: s.NewClient(nil, "local", "inline", false),
			inputMqttPacket: packets.Packet{
				FixedHeader: packets.FixedHeader{
					Type:   packets.Publish,
					Qos:    1,
					Retain: false,
				},
				TopicName: "test2",
				Payload:   []byte("test2"),
			},
			expectedMessage: types.DiscoveryPublishMessage{
				Topic:   "test2",
				Payload: []byte("test2"),
				Retain:  false,
				Qos:     1,
				Node:    []string{"all"},
			},
			readFromChannel: true,
		},
	}

	evBus := bus.New()
	ch, err := evBus.Subscribe("cluster:message_to", "t", 1024)
	require.Nil(t, err)

	h := &Hook{
		bus: evBus,
	}

	for _, test := range tests {

		pk, err := h.OnPublish(test.inputMqttClient, test.inputMqttPacket)
		require.Nil(t, err)
		require.Equal(t, test.inputMqttPacket, pk)

		if test.readFromChannel {
			e := <-ch
			require.Equal(t, test.expectedMessage, e.Data.(types.DiscoveryPublishMessage))
		}
	}
}
