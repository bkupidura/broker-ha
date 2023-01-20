package discovery

import (
	"encoding/json"
	"log"
	"sort"
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

// Delete removes node from hashMap.
func (r *retainedHash) Delete(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.hashMap, node)
}

// PopularHash will find most popular hash in the cluster.
// Majority of nodes cant be wrong...
func (r *retainedHash) PopularHash() (string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var mostPopularHash string
	hashNodeMap := map[string][]string{}

	nodes := make([]string, 0, len(r.hashMap))
	for node := range r.hashMap {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	for _, node := range nodes {
		hashEntry := r.Get(node)
		hashNodeMap[hashEntry.Hash] = append(hashNodeMap[hashEntry.Hash], node)
		if len(hashNodeMap[hashEntry.Hash]) > len(hashNodeMap[mostPopularHash]) {
			mostPopularHash = hashEntry.Hash
		}
	}

	return mostPopularHash, hashNodeMap[mostPopularHash]
}

// delegate implements memberlist.Delegate.
type delegate struct {
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
	state := [2]string{}
	err := json.Unmarshal(buf, &state)
	if err != nil {
		return
	}

	d.retainedHash.Set(state[0], state[1])

	if join {
		return
	}

	localHashEntry := d.retainedHash.Get(d.name)

	if time.Since(d.lastSync) < d.pushPullInterval {
		return
	}

	if time.Since(localHashEntry.LastUpdated) < d.pushPullInterval*2 {
		return
	}

	popularHash, nodes := d.retainedHash.PopularHash()

	if localHashEntry.Hash != popularHash {
		log.Printf("syncing retained messages from %v", nodes)
		d.lastSync = timeNow()
		for _, node := range nodes {
			d.bus.Publish("discovery:request_retained", node)
		}
	}
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
