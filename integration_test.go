package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"brokerha/internal/api"

	"github.com/alexliesenfeld/health"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func TestBrokerHA(t *testing.T) {

	os.Setenv("BROKER_DISCOVERY_DOMAIN", "test")

	minInitSleep = 1
	maxInitSleep = 2

	go main()

	time.Sleep(time.Duration(maxInitSleep) * time.Second)

	mqttReceiveQueue := make(chan paho.Message, 5)
	mqttOnMessage := func(client paho.Client, message paho.Message) {
		mqttReceiveQueue <- message
	}

	mqttConnOpts := paho.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetUsername("test").
		SetPassword("test")

	mqttClient := paho.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("paho.NewClient error: %s", token.Error())
	}

	if token := mqttClient.Subscribe("from_cluster/#", byte(2), mqttOnMessage); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Subscribe error: %s", token.Error())
	}

	if token := mqttClient.Publish("to_cluster/topic_one", 0, true, "test_message_one"); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Publish error: %s", token.Error())
	}
	if token := mqttClient.Publish("to_cluster/topic_two", 0, true, "test_message_two"); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Publish error: %s", token.Error())
	}

	md1 := &mockDelegate{}
	c1 := memberlist.DefaultLocalConfig()
	c1.BindAddr = "127.0.0.1"
	c1.BindPort = 7947
	c1.AdvertisePort = 7947
	c1.Name = "node1"
	c1.LogOutput = ioutil.Discard
	c1.Delegate = md1
	m1, err := memberlist.Create(c1)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m1.Shutdown()

	md2 := &mockDelegate{}
	c2 := memberlist.DefaultLocalConfig()
	c2.BindAddr = "127.0.0.1"
	c2.BindPort = 7948
	c2.AdvertisePort = 7948
	c2.Name = "node2"
	c2.LogOutput = ioutil.Discard
	c2.Delegate = md2
	m2, err := memberlist.Create(c2)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m2.Shutdown()

	m1.Join([]string{"127.0.0.1:7948"})

	joinedNodes, err := m2.Join([]string{"127.0.0.1:7946", "127.0.0.1:7947"})
	if err != nil {
		t.Fatalf("memberlist.Join error: %s", err)
	}

	require.Equal(t, 2, joinedNodes)

	time.Sleep(2 * time.Second)

	// Broker can deliver messages in different order than sent.
	expectedMessageUnordered := []string{
		string(append([]byte{1}, []byte(`[{"Node":["127.0.0.1:7947"],"Payload":"dGVzdF9tZXNzYWdlX29uZQ==","Topic":"to_cluster/topic_one","Retain":true,"Qos":0},{"Node":["127.0.0.1:7947"],"Payload":"dGVzdF9tZXNzYWdlX3R3bw==","Topic":"to_cluster/topic_two","Retain":true,"Qos":0}]`)...)),
		string(append([]byte{1}, []byte(`[{"Node":["127.0.0.1:7947"],"Payload":"dGVzdF9tZXNzYWdlX3R3bw==","Topic":"to_cluster/topic_two","Retain":true,"Qos":0},{"Node":["127.0.0.1:7947"],"Payload":"dGVzdF9tZXNzYWdlX29uZQ==","Topic":"to_cluster/topic_one","Retain":true,"Qos":0}]`)...)),
		string(append([]byte{1}, []byte(`[{"Node":["127.0.0.1:7948"],"Payload":"dGVzdF9tZXNzYWdlX29uZQ==","Topic":"to_cluster/topic_one","Retain":true,"Qos":0},{"Node":["127.0.0.1:7948"],"Payload":"dGVzdF9tZXNzYWdlX3R3bw==","Topic":"to_cluster/topic_two","Retain":true,"Qos":0}]`)...)),
		string(append([]byte{1}, []byte(`[{"Node":["127.0.0.1:7948"],"Payload":"dGVzdF9tZXNzYWdlX3R3bw==","Topic":"to_cluster/topic_two","Retain":true,"Qos":0},{"Node":["127.0.0.1:7948"],"Payload":"dGVzdF9tZXNzYWdlX29uZQ==","Topic":"to_cluster/topic_one","Retain":true,"Qos":0}]`)...)),
	}
	require.Contains(t, expectedMessageUnordered, string(md1.GetData()))
	require.Contains(t, expectedMessageUnordered, string(md2.GetData()))

	if token := mqttClient.Publish("to_cluster/topic_three", 1, false, "test_message_three"); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Publish error: %s", token.Error())
	}

	time.Sleep(50 * time.Millisecond)
	expectedMessage := string(append([]byte{1}, []byte(`[{"Node":["all"],"Payload":"dGVzdF9tZXNzYWdlX3RocmVl","Topic":"to_cluster/topic_three","Retain":false,"Qos":1}]`)...))
	require.Equal(t, expectedMessage, string(md1.GetData()))
	require.Equal(t, expectedMessage, string(md2.GetData()))

	var brokerHAMember *memberlist.Node

	for _, member := range m1.Members() {
		if member.Address() != m1.LocalNode().Address() && member.Address() != m2.LocalNode().Address() {
			brokerHAMember = member
		}
	}

	err = m2.SendReliable(brokerHAMember, append([]byte{1}, []byte(`[{"Payload": "dGVzdA==", "Topic": "from_cluster/topic_one", "Retain": false, "Qos": 0}]`)...))
	if err != nil {
		t.Fatalf("m2.SendReliable error: %s", err)
	}

	mqttMessage := <-mqttReceiveQueue
	require.Equal(t, false, mqttMessage.Retained(), "wrong retained")
	require.Equal(t, "from_cluster/topic_one", mqttMessage.Topic(), "wrong topic")
	require.Equal(t, []byte("test"), mqttMessage.Payload(), "wrong payload")

	for _, endpoint := range []string{"ready", "healthz"} {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s", api.HTTPPort, endpoint))

		require.Equal(t, 200, resp.StatusCode, fmt.Sprintf("unexpected status code for %s endpoint", endpoint))
		require.Nil(t, err, fmt.Sprintf("unexpected error for %s endpoint", endpoint))
	}

	m1.Shutdown()
	m2.Shutdown()

	// Delay for memberlist to detect member lost.
	// Shutdown() will not immediately kill member.
	time.Sleep(15 * time.Second)

	for _, endpoint := range []string{"ready", "healthz"} {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s", api.HTTPPort, endpoint))

		require.Equal(t, 503, resp.StatusCode, fmt.Sprintf("unexpected status code for %s endpoint", endpoint))
		require.Nil(t, err, fmt.Sprintf("unexpected error for %s endpoint", endpoint))

		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("io.ReadAll error: %s", err)
		}
		healthResult := &health.CheckerResult{}
		err = json.Unmarshal(body, healthResult)
		if err != nil {
			t.Fatalf("json.Unmarshal error: %s", err)
		}

		if endpoint == "ready" {
			for checkName, check := range healthResult.Details {
				require.Equal(t, health.StatusDown, check.Status, fmt.Sprintf("unexpected status for %s", checkName))
			}
		} else {
			for _, shouldFailCheckName := range []string{"liveness_cluster_health", "liveness_cluster_discovered_members"} {
				require.Equal(t, health.StatusDown, healthResult.Details[shouldFailCheckName].Status)
			}
		}
	}
}

type mockDelegate struct {
	data []byte
}

func (d *mockDelegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *mockDelegate) NotifyMsg(b []byte) {
	d.data = b
}

func (d *mockDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return [][]byte{}
}

func (d *mockDelegate) LocalState(join bool) []byte {
	return []byte{}
}

func (d *mockDelegate) MergeRemoteState(buf []byte, join bool) {}

func (d *mockDelegate) GetData() []byte {
	return d.data
}
