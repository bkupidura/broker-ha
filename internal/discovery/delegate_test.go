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
	timeNow = func() time.Time {
		return time.Date(2023, time.January, 20, 1, 2, 3, 4, time.UTC)
	}
	defer func() {
		timeNow = time.Now
	}()
	duration, err := time.ParseDuration("10s")
	require.Nil(t, err)
	pastTime100 := timeNow().Add(-10 * duration)

	tests := []struct {
		inputRetainedHash    *retainedHash
		inputNode            string
		inputHash            string
		expectedRetainedHash *retainedHash
	}{
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{},
			},
			inputHash: "hash",
			expectedRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{},
			},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{},
			},
			inputNode: "node-1",
			expectedRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{},
			},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-1": {
						Hash:        "hash",
						LastUpdated: pastTime100,
					},
				},
			},
			inputHash: "hash",
			inputNode: "node-1",
			expectedRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-1": {
						Hash:        "hash",
						LastUpdated: pastTime100,
					},
				},
			},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-1": {
						Hash:        "hash",
						LastUpdated: pastTime100,
					},
				},
			},
			inputHash: "hash2",
			inputNode: "node-1",
			expectedRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-1": {
						Hash:        "hash2",
						LastUpdated: timeNow(),
					},
				},
			},
		},
		{
			inputRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-1": {
						Hash:        "hash",
						LastUpdated: pastTime100,
					},
				},
			},
			inputHash: "hash2",
			inputNode: "node-2",
			expectedRetainedHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-1": {
						Hash:        "hash",
						LastUpdated: pastTime100,
					},
					"node-2": {
						Hash:        "hash2",
						LastUpdated: timeNow(),
					},
				},
			},
		},
	}

	for _, test := range tests {
		r := test.inputRetainedHash
		r.Set(test.inputNode, test.inputHash)
		require.Equal(t, test.expectedRetainedHash, r)
	}
}

func TestRetainedHashGet(t *testing.T) {
	timeNow = func() time.Time {
		return time.Date(2023, time.January, 20, 1, 2, 3, 4, time.UTC)
	}
	defer func() {
		timeNow = time.Now
	}()
	r := &retainedHash{
		hashMap: map[string]retainedHashEntry{
			"node-2": {
				Hash:        "node-2-hash",
				LastUpdated: timeNow(),
			},
		},
	}

	e := r.Get("node-2")
	require.Equal(t, "node-2-hash", e.Hash)
	require.Equal(t, timeNow(), e.LastUpdated)

	e = r.Get("missing")
	require.Equal(t, retainedHashEntry{}, e)
}

func TestRetainedHashGetAll(t *testing.T) {
	timeNow = func() time.Time {
		return time.Date(2023, time.January, 20, 1, 2, 3, 4, time.UTC)
	}
	defer func() {
		timeNow = time.Now
	}()
	r := &retainedHash{
		hashMap: map[string]retainedHashEntry{
			"node-2": {
				Hash:        "node-2-hash",
				LastUpdated: timeNow(),
			},
			"node-1": {
				Hash:        "node-1-hash",
				LastUpdated: timeNow(),
			},
		},
	}
	expectedRetainHash := &retainedHash{
		hashMap: map[string]retainedHashEntry{
			"node-2": {
				Hash:        "node-2-hash",
				LastUpdated: timeNow(),
			},
			"node-1": {
				Hash:        "node-1-hash",
				LastUpdated: timeNow(),
			},
		},
	}
	all := r.GetAll()
	require.Equal(t, expectedRetainHash.hashMap, all)
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

/*
func TestDelegateMergeRemoteState(t *testing.T) {
	timeNow = func() time.Time {
		return time.Date(2023, time.January, 20, 1, 2, 3, 4, time.UTC)
	}
	defer func() {
		timeNow = time.Now
	}()
	duration, err := time.ParseDuration("10s")
	require.Nil(t, err)
	pastTime100 := timeNow().Add(-10 * duration)                   // 100 seconds ago
	pastTime30 := timeNow().Add(-3 * duration)                     // 30 seconds ago
	pastTime9 := timeNow().Add(-1 * duration).Add(1 * time.Second) // 9 seconds ago

	evBus := bus.New()
	tests := []struct {
		inputDelegate      *delegate
		inputBuf           []byte
		inputJoin          bool
		expectedMessage    []string
		expectedRetainHash *retainedHash
		expectedLastSync   time.Time
	}{
		{
			inputDelegate: &delegate{
				bus:  evBus,
				name: "TestDelegateMergeRemoteState",
				retainedHash: &retainedHash{
					hashMap: map[string]retainedHashEntry{},
				},
				pushPullInterval: duration,
			},
			inputBuf: []byte(`["a""b", "c"]`),
			expectedRetainHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{},
			},
		},
		{
			inputDelegate: &delegate{
				bus:  evBus,
				name: "testDelegateMergeRemoteState-0",
				retainedHash: &retainedHash{
					hashMap: map[string]retainedHashEntry{},
				},
				pushPullInterval: duration,
				lastSync:         pastTime9,
			},
			inputBuf: []byte(`["node-1", "hash1"]`),
			expectedRetainHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"node-1": {
						Hash:        "hash1",
						LastUpdated: timeNow(),
					},
				},
			},
			expectedLastSync: pastTime9,
		},
		{
			inputDelegate: &delegate{
				bus:  evBus,
				name: "testDelegateMergeRemoteState-1",
				retainedHash: &retainedHash{
					hashMap: map[string]retainedHashEntry{
						"testDelegateMergeRemoteState-1": {
							Hash:        "hash",
							LastUpdated: timeNow(),
						},
					},
				},
				pushPullInterval: duration,
				lastSync:         pastTime30,
			},
			inputBuf: []byte(`["node-1", "hash1"]`),
			expectedRetainHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"testDelegateMergeRemoteState-1": {
						Hash:        "hash",
						LastUpdated: timeNow(),
					},
					"node-1": {
						Hash:        "hash1",
						LastUpdated: timeNow(),
					},
				},
			},
			expectedLastSync: pastTime30,
		},
		{
			inputDelegate: &delegate{
				bus:  evBus,
				name: "testDelegateMergeRemoteState-2",
				retainedHash: &retainedHash{
					hashMap: map[string]retainedHashEntry{
						"testDelegateMergeRemoteState-2": {
							Hash:        "hash",
							LastUpdated: timeNow(),
						},
					},
				},
				pushPullInterval: duration,
				lastSync:         pastTime100,
			},
			inputBuf: []byte(`["node-1", "hash1"]`),
			expectedRetainHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"testDelegateMergeRemoteState-2": {
						Hash:        "hash",
						LastUpdated: timeNow(),
					},
					"node-1": {
						Hash:        "hash1",
						LastUpdated: timeNow(),
					},
				},
			},
			expectedLastSync: timeNow(),
			expectedMessage:  []string{"node-1"},
		},
		{
			inputDelegate: &delegate{
				bus:  evBus,
				name: "testDelegateMergeRemoteState-3",
				retainedHash: &retainedHash{
					hashMap: map[string]retainedHashEntry{
						"testDelegateMergeRemoteState-3": {
							Hash:        "hash",
							LastUpdated: pastTime100,
						},
					},
				},
				pushPullInterval: duration,
				lastSync:         pastTime100,
			},
			inputBuf: []byte(`["node-1", "hash1"]`),
			expectedRetainHash: &retainedHash{
				hashMap: map[string]retainedHashEntry{
					"testDelegateMergeRemoteState-3": {
						Hash:        "hash",
						LastUpdated: pastTime100,
					},
					"node-1": {
						Hash:        "hash1",
						LastUpdated: timeNow(),
					},
				},
			},
			expectedLastSync: timeNow(),
			expectedMessage:  []string{"node-1"},
		},
	}

	ch, err := evBus.Subscribe("discovery:request_retained", "t", 1024)
	require.Nil(t, err)

	for _, test := range tests {
		d := test.inputDelegate
		d.MergeRemoteState(test.inputBuf, test.inputJoin)

		require.Equal(t, test.expectedRetainHash, d.retainedHash)
		require.Equal(t, test.expectedLastSync, d.lastSync)

		for _, expectedMessage := range test.expectedMessage {
			e := <-ch
			data := e.Data.(string)
			require.Equal(t, expectedMessage, data)
		}

	}
}*/

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
