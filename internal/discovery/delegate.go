package discovery

import (
	"encoding/json"
	"log"

	"github.com/hashicorp/memberlist"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

// delegate implements memberlist.Delegate.
type delegate struct {
	bus *bus.Bus
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg is executed when member receives data from other cluster member.
// First message byte will be used to decide what kind of data was received.
// Everything else is treated as data.
func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}

	messageType := b[0]
	messageData := b[1:]

	switch messageType {
	case queueDataTypes["MQTTPublish"]:
		var messages []types.MQTTPublishMessage
		if err := json.Unmarshal(messageData, &messages); err != nil {
			log.Printf("received malformed message from cluster for MQTTPublish")
			return
		}
		for _, message := range messages {
			d.bus.Publish("cluster:message_from", message)
		}
	default:
		log.Printf("received unknown message type from cluster: %c", messageType)
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return [][]byte{}
}

func (d *delegate) LocalState(join bool) []byte {
	return []byte{}
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {}

// delegateEvent implements memberlist.EventDelegate.
type delegateEvent struct {
	selfAddress map[string]struct{}
	bus         *bus.Bus
}

// NotifyJoin is executed when node join cluster.
// We are skipping NotifyJoin for ourselfs.
func (d *delegateEvent) NotifyJoin(n *memberlist.Node) {
	if _, ok := d.selfAddress[n.Address()]; ok {
		return
	}
	log.Printf("new cluster member %s", n.Address())
	d.bus.Publish("cluster:new_member", n.Address())
}

// Notifyleave is executed when node leave cluster.
func (d *delegateEvent) NotifyLeave(n *memberlist.Node) {
	log.Printf("cluster member %s leaved", n.Address())
}

func (d *delegateEvent) NotifyUpdate(n *memberlist.Node) {}
