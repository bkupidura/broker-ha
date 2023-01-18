package discovery

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

type retainedHashEntry struct {
	Hash        string
	LastUpdated int64
}

type retainedHash struct {
	mu      sync.Mutex
	hashMap map[string]retainedHashEntry
}

func (r *retainedHash) Set(node, hash string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if node == "" || hash == "" {
		return
	}

	r.hashMap[node] = retainedHashEntry{
		Hash:        hash,
		LastUpdated: time.Now().Unix(),
	}
}

func (r *retainedHash) Get(node string) retainedHashEntry {
	return r.hashMap[node]
}

func (r *retainedHash) Delete(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.hashMap, node)
}

func (r *retainedHash) PopularHash() (string, []string) {
	var mostPopularHash string
	hashNodeMap := map[string][]string{}

	for node, hashEntry := range r.hashMap {
		hashNodeMap[hashEntry.Hash] = append(hashNodeMap[hashEntry.Hash], node)
		if len(hashNodeMap[hashEntry.Hash]) > len(hashNodeMap[mostPopularHash]) {
			mostPopularHash = hashEntry.Hash
		}
	}

	return mostPopularHash, hashNodeMap[mostPopularHash]
}

// delegate implements memberlist.Delegate.
type delegate struct {
	bus          *bus.Bus
	name         string
	retainedHash *retainedHash
}

func (d *delegate) RetainedHash() *retainedHash {
	return d.retainedHash
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
	state := [2]string{
		d.name,
		d.retainedHash.Get(d.name).Hash,
	}
	data, err := json.Marshal(state)
	if err != nil {
		return []byte{}
	}
	return []byte(data)
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	state := [2]string{}
	err := json.Unmarshal(buf, &state)
	if err != nil {
		return
	}

	log.Printf("%s old retainedHash: %+v", d.name, d.retainedHash)
	d.retainedHash.Set(state[0], state[1])
	log.Printf("%s new popularHash: %+v", d.name, d.retainedHash)

	localHashEntry := d.retainedHash.Get(d.name)
	popularHash, nodes := d.retainedHash.PopularHash()

	log.Printf("%s localHash: %+v popularHash: %+v", d.name, localHashEntry.Hash, popularHash)

	if time.Now().Unix()-localHashEntry.LastUpdated < 5 {
		return
	}

	if localHashEntry.Hash != popularHash {
		log.Printf("%s sync nodes: %+v", d.name, nodes)
	}
}

// delegateEvent implements memberlist.EventDelegate.
type delegateEvent struct {
	name         string
	bus          *bus.Bus
	retainedHash *retainedHash
}

// NotifyJoin is executed when node join cluster.
// We are skipping NotifyJoin for ourselfs.
func (d *delegateEvent) NotifyJoin(n *memberlist.Node) {
	if n.String() == d.name {
		return
	}
	log.Printf("new cluster member %s", n.Address())
	d.bus.Publish("cluster:new_member", n.Address())
}

// Notifyleave is executed when node leave cluster.
func (d *delegateEvent) NotifyLeave(n *memberlist.Node) {
	d.retainedHash.Delete(n.String())
	log.Printf("cluster member %s leaved", n.Address())
}

func (d *delegateEvent) NotifyUpdate(n *memberlist.Node) {}
