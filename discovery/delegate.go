package discovery

import (
	"log"

	"encoding/json"

	"github.com/hashicorp/memberlist"
)

// delegate implements memberlist.Delegate.
type delegate struct{}

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
		var messages []*MQTTPublishMessage
		if err := json.Unmarshal(messageData, &messages); err != nil {
			log.Printf("received malformed message from cluster for MQTTPublish")
			return
		}
		for _, message := range messages {
			MQTTPublishFromCluster <- message
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
// It stores selfAddress which is *memberlist.Node.Address() in format 'ip:port'.
type delegateEvent struct {
	selfAddress string
}

// NotifyJoin is executed when node join cluster.
// We are skipping NotifyJoin for ourselfs.
func (d *delegateEvent) NotifyJoin(n *memberlist.Node) {
	if d.selfAddress == n.Address() {
		return
	}
	log.Printf("new cluster member %s", n.Address())
	log.Printf("sending retained messages to %s", n.Address())
	MQTTSendRetained <- n
}

// Notifyleave is executed when node leave cluster.
func (d *delegateEvent) NotifyLeave(n *memberlist.Node) {
	log.Printf("cluster member %s leaved", n.Addr)
}

func (d *delegateEvent) NotifyUpdate(n *memberlist.Node) {}
