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

var (
	// mocks for tests
	timeNow = time.Now
)

// retainedHashEntry stores retained messages hash and when it was changed.
type retainedHashEntry struct {
	Hash        string
	LastUpdated time.Time
}

// retainedHash stores cluster nodes and their retainedHashEntry.
type retainedHash struct {
	mu      sync.Mutex
	hashMap map[string]retainedHashEntry
}

// Set adds or updates node in hashMap.
func (r *retainedHash) Set(node, hash string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if node == "" || hash == "" {
		return
	}

	if r.hashMap[node].Hash == hash {
		return
	}

	r.hashMap[node] = retainedHashEntry{
		Hash:        hash,
		LastUpdated: timeNow(),
	}
}

// Get fetchs node from hashMap.
func (r *retainedHash) Get(node string) retainedHashEntry {
	return r.hashMap[node]
}

// GetAll fetchs all nodes from hashMap.
func (r *retainedHash) GetAll() map[string]retainedHashEntry {
	return r.hashMap
}

// Delete removes node from hashMap.
func (r *retainedHash) Delete(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.hashMap, node)
}

// delegate implements memberlist.Delegate.
type delegate struct {
	mu               sync.Mutex
	bus              *bus.Bus
	name             string
	retainedHash     *retainedHash
	pushPullInterval time.Duration
	lastSync         time.Time
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
	case queueDataTypes["SendRetained"]:
		if len(messageData) < 1 {
			log.Printf("received malformed message from cluster for SendRetained")
			return
		}
		d.bus.Publish("broker:send_retained", string(messageData))
	default:
		log.Printf("received unknown message type from cluster: %c", messageType)
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return [][]byte{}
}

// LocalState is send to other cluster members.
// It will return node name and current retained messages hash.
func (d *delegate) LocalState(join bool) []byte {
	state := [2]string{
		d.name,
		d.retainedHash.Get(d.name).Hash,
	}
	data, err := jsonMarshal(state)
	if err != nil {
		return []byte{}
	}
	return []byte(data)
}

// MergeRemoteState is executed when LocalState from other node is received.
// If our retained messages hash is different than most common hash in cluster,
// lets sync retained messages from other nodes.
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state := [2]string{}
	err := json.Unmarshal(buf, &state)
	if err != nil {
		return
	}

	d.retainedHash.Set(state[0], state[1])

	// If we synced retained messages in recently, skip this run.
	if timeNow().Sub(d.lastSync) < d.pushPullInterval*2 {
		log.Printf("skipping MergeRemoteState, synced too recently")
		return
	}

	localHash := d.retainedHash.Get(d.name)

	if localHash.Hash == state[1] {
		log.Printf("skipping MergeRemoteState, same hash already")
		return
	}

	go func(node string) {
		log.Printf("starting go func()")
		chDiscoveryRetainedHash, err := d.bus.Subscribe("discovery:retained_hash", "MergeRemoteState", 2)
		if err != nil {
			log.Printf("unable to create discovery:retained_hash subscriber: %v", err)
			return
		}
		defer d.bus.Unsubscribe("discovery:retained_hash", "MergeRemoteState")

		select {
		case e := <-chDiscoveryRetainedHash:
			localHash := e.Data.(string)
			log.Printf("received hash from chDiscoveryRetainedHash %v", localHash)
			remoteHash := d.retainedHash.Get(node)
			if localHash != remoteHash.Hash {
				log.Printf("fetching retained messages from %v", state[0])
				d.lastSync = timeNow()
				d.bus.Publish("discovery:request_retained", state[0])
			} else {
				log.Printf("skipping sync gofunc, same hash")
			}
		case <-time.After(11 * time.Second):
			log.Printf("timeout in go func()")
		}

	}(state[0])
}

// delegateEvent implements memberlist.EventDelegate.
type delegateEvent struct {
	name         string
	bus          *bus.Bus
	retainedHash *retainedHash
}

// NotifyJoin is executed when node join cluster.
func (d *delegateEvent) NotifyJoin(n *memberlist.Node) {
	if n.String() == d.name {
		return
	}
	log.Printf("new cluster member %s", n.String())
}

// Notifyleave is executed when node leave cluster.
func (d *delegateEvent) NotifyLeave(n *memberlist.Node) {
	d.retainedHash.Delete(n.String())
	log.Printf("cluster member %s leaved", n.String())
}

func (d *delegateEvent) NotifyUpdate(n *memberlist.Node) {}
