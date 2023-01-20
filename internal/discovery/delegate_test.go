package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

func TestRetainedHashSet(t *testing.T) {
	r := &retainedHash{
		hashMap: map[string]retainedHashEntry{},
	}

	before := time.Now()
	time.Sleep(1 * time.Millisecond)
	r.Set("node-1", "node-1-hash")
	time.Sleep(1 * time.Millisecond)
	after := time.Now()
	node1 := r.hashMap["node-1"]

	require.Equal(t, "node-1-hash", node1.Hash)
	require.Less(t, before, node1.LastUpdated)
	require.Greater(t, after, node1.LastUpdated)

	r.Set("", "hash")
	require.Equal(t, 1, len(r.hashMap))

	r.Set("a", "")
	require.Equal(t, 1, len(r.hashMap))

	time.Sleep(1 * time.Second)
	now := time.Now()

	r.Set("node-1", "node-1-hash")
	require.Equal(t, "node-1-hash", node1.Hash)
	require.Equal(t, node1.LastUpdated, node1.LastUpdated)
	require.Greater(t, now, node1.LastUpdated)
}

func TestRetainedHashGet(t *testing.T) {
	before := time.Now()
	time.Sleep(1 * time.Millisecond)
	r := &retainedHash{
		hashMap: map[string]retainedHashEntry{
			"node-2": {
				Hash:        "node-2-hash",
				LastUpdated: time.Now(),
			},
		},
	}
	time.Sleep(1 * time.Millisecond)
	after := time.Now()

	e := r.Get("node-2")
	require.Equal(t, "node-2-hash", e.Hash)
	require.Less(t, before, e.LastUpdated)
	require.Greater(t, after, e.LastUpdated)

	e = r.Get("missing")
	require.Equal(t, retainedHashEntry{}, e)
}

func TestRetainedHashDelete(t *testing.T) {

	r := &retainedHash{
		hashMap: map[string]retainedHashEntry{
			"node-2": {
				Hash:        "node-2-hash",
				LastUpdated: time.Now(),
			},
		},
	}

	r.Delete("node-1")
	require.Equal(t, 1, len(r.hashMap))
	require.Equal(t, "node-2-hash", r.hashMap["node-2"].Hash)

	r.Delete("node-2")
	require.Equal(t, 0, len(r.hashMap))
}

func TestRetainedHashPopularHash(t *testing.T) {
	tests := []struct {
		inputRetainedHash       *retainedHash
		expectedMostPopularHash string
		expectedNodes           []string
	}{
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-2": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
				},
			},
			expectedMostPopularHash: "hash1",
			expectedNodes:           []string{"node-2"},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-2": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
					"node-3": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
				},
			},
			expectedMostPopularHash: "hash1",
			expectedNodes:           []string{"node-2"},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-2": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
					"node-3": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
					"node-4": {
						Hash:        "hash3",
						LastUpdated: time.Now(),
					},
				},
			},
			expectedMostPopularHash: "hash1",
			expectedNodes:           []string{"node-2"},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-2": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
					"node-3": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
					"node-4": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
				},
			},
			expectedMostPopularHash: "hash2",
			expectedNodes:           []string{"node-3", "node-4"},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-2": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
					"node-3": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
					"node-4": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
					"node-5": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
				},
			},
			expectedMostPopularHash: "hash1",
			expectedNodes:           []string{"node-2", "node-3"},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-2": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
					"node-3": {
						Hash:        "hash1",
						LastUpdated: time.Now(),
					},
					"node-4": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
					"node-5": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
					"node-6": {
						Hash:        "hash2",
						LastUpdated: time.Now(),
					},
				},
			},
			expectedMostPopularHash: "hash2",
			expectedNodes:           []string{"node-4", "node-5", "node-6"},
		},
	}
	for _, test := range tests {
		popularHash, nodes := test.inputRetainedHash.PopularHash()
		require.Equal(t, test.expectedMostPopularHash, popularHash)
		require.Equal(t, test.expectedNodes, nodes)
	}

}

func TestDelegateNodeMeta(t *testing.T) {
	d := &delegate{}
	require.Equal(t, d.NodeMeta(10), []byte{})
}

func TestDelegateNotifyMsgMQTTPublish(t *testing.T) {
	tests := []struct {
		inputMessage    []byte
		expectedLog     string
		expectedMessage []types.MQTTPublishMessage
	}{
		{
			inputMessage: []byte{},
		},
		{
			inputMessage: append([]byte{queueDataTypes["MQTTPublish"]}, []byte("broken_msg: 1}")...),
			expectedLog:  "received malformed message from cluster for MQTTPublish\n",
		},
		{
			inputMessage: []byte("unknown"),
			expectedLog:  "received unknown message type from cluster: u\n",
		},
		{
			inputMessage: append(
				[]byte{queueDataTypes["MQTTPublish"]},
				[]byte(`[{"Payload": "dGVzdA==", "Topic": "test", "Retain": true, "Qos": 2}]`)...,
			),
			expectedMessage: []types.MQTTPublishMessage{
				{
					Payload: []byte("test"),
					Topic:   "test",
					Retain:  true,
					Qos:     2,
				},
			},
		},
		{
			inputMessage: append(
				[]byte{queueDataTypes["MQTTPublish"]},
				[]byte(`[{"Payload": "dGVzdA==", "Topic": "test", "Retain": false, "Qos": 0}, {"Payload": "dGVzdDI=", "Topic": "test2", "Retain": false, "Qos": 1}, {"Payload": "dGVzdDM=", "Topic": "test3", "Retain": true, "Qos": 2}]`)...,
			),
			expectedMessage: []types.MQTTPublishMessage{
				{
					Payload: []byte("test"),
					Topic:   "test",
					Retain:  false,
					Qos:     0,
				},
				{
					Payload: []byte("test2"),
					Topic:   "test2",
					Retain:  false,
					Qos:     1,
				},
				{
					Payload: []byte("test3"),
					Topic:   "test3",
					Retain:  true,
					Qos:     2,
				},
			},
		},
	}

	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	evBus := bus.New()
	d := &delegate{
		bus: evBus,
	}

	ch, err := evBus.Subscribe("cluster:message_from", "TestDelegateNotifyMsgMQTTPublish", 1024)
	require.Nil(t, err)

	for _, test := range tests {
		logOutput.Reset()

		d.NotifyMsg(test.inputMessage)
		require.Equal(t, test.expectedLog, logOutput.String())

		if test.expectedMessage != nil {
			for _, expectedMessage := range test.expectedMessage {
				e := <-ch
				receivedMessage := e.Data.(types.MQTTPublishMessage)
				require.Equal(t, expectedMessage, receivedMessage)
			}
		}
	}
}

func TestDelegateNotifyMsgSendRetained(t *testing.T) {
	tests := []struct {
		inputMessage    []byte
		expectedLog     string
		expectedMessage string
	}{
		{
			inputMessage: []byte{queueDataTypes["SendRetained"]},
			expectedLog:  "received malformed message from cluster for SendRetained\n",
		},
		{
			inputMessage: append(
				[]byte{queueDataTypes["SendRetained"]},
				[]byte(`node-1`)...,
			),
			expectedMessage: "node-1",
		},
	}

	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	evBus := bus.New()
	d := &delegate{
		bus: evBus,
	}

	ch, err := evBus.Subscribe("broker:send_retained", "TestDelegateNotifyMsgSendRetained", 1024)
	require.Nil(t, err)

	for _, test := range tests {
		logOutput.Reset()

		d.NotifyMsg(test.inputMessage)
		require.Equal(t, test.expectedLog, logOutput.String())

		if test.expectedMessage != "" {
			e := <-ch
			receivedMessage := e.Data.(string)
			require.Equal(t, test.expectedMessage, receivedMessage)
		}
	}
}

func TestDelegateGetBroadcasts(t *testing.T) {
	d := &delegate{}

	require.Equal(t, [][]byte{}, d.GetBroadcasts(0, 0))
}

func TestDelegateLocalState(t *testing.T) {
	d := &delegate{
		name: "TestDelegateLocalState",
		retainedHash: &retainedHash{
			hashMap: map[string]retainedHashEntry{
				"node-1": {
					Hash:        "hash1",
					LastUpdated: time.Now(),
				},
			},
		},
	}

	jsonMarshal = func(any) ([]byte, error) {
		return nil, fmt.Errorf("mock error")
	}
	s := d.LocalState(true)
	require.Equal(t, []byte{}, s)

	jsonMarshal = json.Marshal
	s = d.LocalState(false)
	require.Equal(t, `["TestDelegateLocalState",""]`, string(s))

	d.retainedHash.Set("TestDelegateLocalState", "hash2")
	s = d.LocalState(false)
	require.Equal(t, `["TestDelegateLocalState","hash2"]`, string(s))
}

func TestDelegateMergeRemoteState(t *testing.T) {
	evBus := bus.New()
	duration, err := time.ParseDuration("3s")
	require.Nil(t, err)
	pastTime := time.Now().Add(-3 * duration)
	d := &delegate{
		name:             "TestDelegateMergeRemoteState",
		bus:              evBus,
		pushPullInterval: duration,
		retainedHash: &retainedHash{
			hashMap: map[string]retainedHashEntry{
				"TestDelegateMergeRemoteState": {
					Hash:        "hash1",
					LastUpdated: pastTime,
				},
				"node-2": {
					Hash:        "hash-2",
					LastUpdated: time.Now(),
				},
			},
		},
	}

	ch, err := evBus.Subscribe("discovery:request_retained", "TestDelegateMergeRemoteState", 10)
	require.Nil(t, err)

	jsonUnmarshal = func([]byte, any) error {
		return fmt.Errorf("mock error")
	}
	d.MergeRemoteState([]byte(`["node-1", "hash-2"]`), true)
	require.Equal(t, "", d.retainedHash.Get("node-1").Hash)

	jsonUnmarshal = json.Unmarshal
	d.MergeRemoteState([]byte(`["node-1", "hash-2"]`), true)
	require.Equal(t, "hash-2", d.retainedHash.Get("node-1").Hash)
	for _, node := range []string{"node-1", "node-2"} {
		e := <-ch
		receivedMessage := e.Data.(string)
		require.Equal(t, node, receivedMessage)
	}

	d.MergeRemoteState([]byte(`["node-3", "hash-2"]`), true)
	require.Equal(t, "hash-2", d.retainedHash.Get("node-3").Hash)
	select {
	case <-ch:
		require.FailNow(t, "received unexpected message")
	case <-time.After(10 * time.Millisecond):
	}

	time.Sleep(3 * time.Second)

	d.retainedHash.Set("TestDelegateMergeRemoteState", "hash1-1")
	d.MergeRemoteState([]byte(`["node-1", "hash-2"]`), true)
	require.Equal(t, "hash-2", d.retainedHash.Get("node-1").Hash)

	select {
	case <-ch:
		require.FailNow(t, "received unexpected message")
	case <-time.After(10 * time.Millisecond):
	}

}

func TestDelegateEventNotifyJoin(t *testing.T) {
	hostname, err := os.Hostname()
	require.Nil(t, err)

	tests := []struct {
		inputMemberIP   net.IP
		inputMemberName string
		expectedLog     string
	}{
		{
			inputMemberIP:   net.ParseIP("127.0.0.1"),
			inputMemberName: hostname,
		},
		{
			inputMemberIP:   net.ParseIP("1.2.3.4"),
			inputMemberName: "test",
			expectedLog:     "new cluster member test\n",
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	evBus := bus.New()

	d := &delegateEvent{
		name:         hostname,
		bus:          evBus,
		retainedHash: &retainedHash{},
	}

	for _, test := range tests {
		logOutput.Reset()

		d.NotifyJoin(&memberlist.Node{Addr: test.inputMemberIP, Port: 7946, Name: test.inputMemberName})
		require.Equal(t, test.expectedLog, logOutput.String())
	}
}

func TestDelegateEventNotifyLeave(t *testing.T) {
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	d := &delegateEvent{
		retainedHash: &retainedHash{
			hashMap: map[string]retainedHashEntry{
				"TestDelegateEventNotifyLeave": {
					Hash:        "hash",
					LastUpdated: time.Now(),
				},
				"node-2": {
					Hash:        "hash",
					LastUpdated: time.Now(),
				},
			},
		},
	}

	require.Equal(t, 2, len(d.retainedHash.hashMap))
	d.NotifyLeave(&memberlist.Node{Addr: net.ParseIP("1.2.3.4"), Name: "TestDelegateEventNotifyLeave"})
	require.Equal(t, "cluster member TestDelegateEventNotifyLeave leaved\n", logOutput.String())
	require.Equal(t, "", d.retainedHash.Get("TestDelegateEventNotifyLeave").Hash)
	require.Equal(t, 1, len(d.retainedHash.hashMap))

}

func TestDelegateEventNotifyUpdate(t *testing.T) {}
