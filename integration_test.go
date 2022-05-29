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

	"github.com/alexliesenfeld/health"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func TestBrokerHA(t *testing.T) {

	os.Setenv("BROKER_DISCOVERY_DOMAIN", "test")
	os.Setenv("BROKER_MQTT_USER", `{"test": "test"}`)
	maxInitSleep = 2

	go main()

	time.Sleep(time.Duration(maxInitSleep) * time.Second)

	mqttReceiveQueue := make(chan MQTT.Message, 5)
	mqttOnMessage := func(client MQTT.Client, message MQTT.Message) {
		mqttReceiveQueue <- message
	}

	mqttConnOpts := MQTT.NewClientOptions().
		AddBroker("127.0.0.1:1883").
		SetUsername("test").
		SetPassword("test")

	mqttClient := MQTT.NewClient(mqttConnOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("MQTT.NewClient error: %s", token.Error())
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

	md := &mockDelegate{}
	c1 := memberlist.DefaultLocalConfig()
	c1.BindAddr = "127.0.0.1"
	c1.BindPort = 7947
	c1.AdvertisePort = 7947
	c1.Name = "node1"
	c1.LogOutput = ioutil.Discard
	c1.Delegate = md
	c1.SecretKey = []byte("DummySecrEtKey1^")
	m1, err := memberlist.Create(c1)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m1.Shutdown()

	_, err = m1.Join([]string{"127.0.0.1:7946"})
	if err != nil {
		t.Fatalf("memberlist.Join error: %s", err)
	}

	time.Sleep(5 * time.Millisecond)
	expectedMessage := string(append([]byte{1}, []byte(`[{"Node":["127.0.0.1:7947"],"Payload":"dGVzdF9tZXNzYWdlX29uZQ==","Topic":"to_cluster/topic_one","Retain":true,"Qos":0},{"Node":["127.0.0.1:7947"],"Payload":"dGVzdF9tZXNzYWdlX3R3bw==","Topic":"to_cluster/topic_two","Retain":true,"Qos":0}]`)...))
	require.Equal(t, expectedMessage, string(md.GetData()))

	if token := mqttClient.Publish("to_cluster/topic_three", 1, false, "test_message_three"); token.Wait() && token.Error() != nil {
		t.Fatalf("mqttClient.Publish error: %s", token.Error())
	}

	time.Sleep(5 * time.Millisecond)
	expectedMessage = string(append([]byte{1}, []byte(`[{"Node":["all"],"Payload":"dGVzdF9tZXNzYWdlX3RocmVl","Topic":"to_cluster/topic_three","Retain":false,"Qos":1}]`)...))
	require.Equal(t, expectedMessage, string(md.GetData()))

	var brokerHAMember *memberlist.Node

	for _, member := range m1.Members() {
		if member != m1.LocalNode() {
			brokerHAMember = member
		}
	}

	err = m1.SendReliable(brokerHAMember, append([]byte{1}, []byte(`[{"Payload": "dGVzdA==", "Topic": "from_cluster/topic_one", "Retain": false, "Qos": 0}]`)...))
	if err != nil {
		t.Fatalf("m1.SendReliable error: %s", err)
	}

	mqttMessage := <-mqttReceiveQueue
	require.Equal(t, false, mqttMessage.Retained(), "wrong retained")
	require.Equal(t, "from_cluster/topic_one", mqttMessage.Topic(), "wrong topic")
	require.Equal(t, []byte("test"), mqttMessage.Payload(), "wrong payload")

	for _, endpoint := range []string{"ready", "healthz"} {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:8080/%s", endpoint))

		require.Equal(t, 200, resp.StatusCode, fmt.Sprintf("unexpected status code for %s endpoint", endpoint))
		require.Nil(t, err, fmt.Sprintf("unexpected error for %s endpoint", endpoint))
	}

	m1.Shutdown()

	// Delay for memberlist to detect member lost
	time.Sleep(1 * time.Second)

	for _, endpoint := range []string{"ready", "healthz"} {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:8080/%s", endpoint))

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
		for check_name, check := range *healthResult.Details {
			require.Equal(t, health.StatusDown, check.Status, fmt.Sprintf("unexpected status for %s", check_name))
		}

		// wait for LivenessProbe
		time.Sleep(1500 * time.Millisecond)
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
