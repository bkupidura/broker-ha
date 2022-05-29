package discovery

import (
	"log"

	"encoding/json"

	"github.com/hashicorp/memberlist"
)

type delegate struct{}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}

	messageType := b[0]
	messageData := b[1:]

	switch messageType {
	case queueDataTypes["MQTTPublish"]:
		var message *MQTTPublishMessage
		if err := json.Unmarshal(messageData, &message); err != nil {
			log.Printf("received malformed message from cluster for MQTTPublish")
			return
		}
		MQTTPublishFromCluster <- message
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

type delegateEvent struct {
	selfAddress string
}

func (d *delegateEvent) NotifyJoin(n *memberlist.Node) {
	if d.selfAddress == n.Address() {
		return
	}
	log.Printf("new cluster member %s", n.Address())
	log.Printf("sending retained messages to %s", n.Address())
	MQTTSendRetained <- n
}

func (d *delegateEvent) NotifyLeave(n *memberlist.Node) {
	log.Printf("cluster member %s leaved", n.Addr)
}

func (d *delegateEvent) NotifyUpdate(n *memberlist.Node) {}
