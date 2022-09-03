package discovery

import (
	"bytes"
	"log"
	"net"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

func TestDelegateNodeMeta(t *testing.T) {
	d := &delegate{}
	require.Equal(t, d.NodeMeta(10), []byte{})
}

func TestDelegateNotifyMsg(t *testing.T) {
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

	ch, err := evBus.Subscribe("cluster:message_from", "TestDelegateNotifyMsg", 1024)
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

func TestDelegateGetBroadcasts(t *testing.T) {
	d := &delegate{}

	require.Equal(t, [][]byte{}, d.GetBroadcasts(0, 0))
}

func TestDelegateLocalState(t *testing.T) {
	d := &delegate{}

	require.Equal(t, []byte{}, d.LocalState(true))
	require.Equal(t, []byte{}, d.LocalState(false))
}

func TestDelegateMergeRemoteState(t *testing.T) {}

func TestDelegateEventNotifyJoin(t *testing.T) {
	tests := []struct {
		inputMemberIP   net.IP
		expectedLog     string
		expectedMessage string
	}{
		{
			inputMemberIP: net.ParseIP("127.0.0.1"),
		},
		{
			inputMemberIP:   net.ParseIP("1.2.3.4"),
			expectedLog:     "new cluster member 1.2.3.4:7946\n",
			expectedMessage: "1.2.3.4:7946",
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	evBus := bus.New()

	d := &delegateEvent{
		selfAddress: map[string]struct{}{
			"127.0.0.1:7946": {},
		},
		bus: evBus,
	}

	ch, err := evBus.Subscribe("cluster:new_member", "TestDelegateEventNotifyJoin", 10)
	require.Nil(t, err)

	for _, test := range tests {
		logOutput.Reset()

		d.NotifyJoin(&memberlist.Node{Addr: test.inputMemberIP, Port: 7946})
		require.Equal(t, test.expectedLog, logOutput.String())

		if test.expectedMessage != "" {
			e := <-ch
			receivedMessage := e.Data.(string)
			require.Equal(t, test.expectedMessage, receivedMessage)
		}
	}
}

func TestDelegateEventNotifyLeave(t *testing.T) {
	tests := []struct {
		inputMemberIP   net.IP
		expectedLog     string
		expectedMessage *memberlist.Node
	}{
		{
			inputMemberIP: net.ParseIP("1.2.3.4"),
			expectedLog:   "cluster member 1.2.3.4:0 leaved\n",
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	d := &delegateEvent{}

	for _, test := range tests {
		logOutput.Reset()

		d.NotifyLeave(&memberlist.Node{Addr: test.inputMemberIP})
		require.Equal(t, test.expectedLog, logOutput.String())
	}

}

func TestDelegateEventNotifyUpdate(t *testing.T) {}
