package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	multierror "github.com/hashicorp/go-multierror"
)

func TestShutdown(t *testing.T) {
	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = "node1"
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.LogOutput = ioutil.Discard
	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}

	disco := &Discovery{
		domain:      "test",
		selfAddress: "127.0.0.1:7946",
		config:      mlConfig,
		ml:          ml,
	}
	err = disco.FormCluster()
	if err != nil {
		t.Fatalf("disco.FormCluster() error: %s", err)
	}

	require.Nil(t, disco.Shutdown())
}

func TestGetHealthScore(t *testing.T) {
	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = "node1"
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.LogOutput = ioutil.Discard
	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer ml.Shutdown()

	disco := &Discovery{
		domain:      "test",
		selfAddress: "127.0.0.1:7946",
		config:      mlConfig,
		ml:          ml,
	}
	err = disco.FormCluster()
	if err != nil {
		t.Fatalf("disco.FormCluster() error: %s", err)
	}

	require.Equal(t, 0, disco.GetHealthScore())
}

func TestSendReliable(t *testing.T) {
	tests := []struct {
		inputMember   *memberlist.Node
		inputDataType string
		inputData     []byte
		expectedErr   string
	}{
		{
			inputMember:   &memberlist.Node{Addr: net.ParseIP("127.0.0.1"), Port: 7946},
			inputDataType: "unknown",
			inputData:     []byte{},
			expectedErr:   "unknown data type unknown",
		},
		{
			inputMember:   &memberlist.Node{Addr: net.ParseIP("127.0.0.1"), Port: 7947},
			inputDataType: "MQTTPublish",
			inputData:     []byte("message"),
			expectedErr:   "dial tcp 127.0.0.1:7947: connect: connection refused",
		},
		{
			inputMember:   &memberlist.Node{Addr: net.ParseIP("127.0.0.1"), Port: 7946},
			inputDataType: "MQTTPublish",
			inputData:     []byte("message"),
		},
	}
	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = "node1"
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.LogOutput = ioutil.Discard
	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer ml.Shutdown()

	disco := &Discovery{
		domain:      "test",
		selfAddress: "127.0.0.1:7946",
		config:      mlConfig,
		ml:          ml,
	}
	err = disco.FormCluster()
	if err != nil {
		t.Fatalf("disco.FormCluster() error: %s", err)
	}

	for _, test := range tests {
		err = disco.SendReliable(test.inputMember, test.inputDataType, test.inputData)
		if test.expectedErr != "" {
			require.Equal(t, test.expectedErr, err.Error())
		}
	}
}

func TestMembers(t *testing.T) {
	c1 := memberlist.DefaultLocalConfig()
	c1.BindAddr = "127.0.0.1"
	c1.BindPort = 7947
	c1.AdvertisePort = 7947
	c1.Name = "node1"
	c1.LogOutput = ioutil.Discard
	m1, err := memberlist.Create(c1)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m1.Shutdown()

	netLookupSRV = func(string, string, string) (string, []*net.SRV, error) {
		a := []*net.SRV{
			&net.SRV{Target: "127-0-0-1.some-service.svc.cluster.local", Port: 7947},
		}
		return "", a, nil
	}
	netLookupIP = func(domain string) ([]net.IP, error) {
		a := []net.IP{net.ParseIP("127.0.0.1")}
		return a, nil
	}

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = "node2"
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.LogOutput = ioutil.Discard
	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer ml.Shutdown()

	disco := &Discovery{
		domain:      "test",
		selfAddress: "127.0.0.1:7946",
		config:      mlConfig,
		ml:          ml,
	}
	err = disco.FormCluster()
	if err != nil {
		t.Fatalf("disco.FormCluster() error: %s", err)
	}

	require.Equal(t, 2, len(disco.Members(true)))
	require.Equal(t, 1, len(disco.Members(false)))
}

func TestFormCluster(t *testing.T) {
	var errs error
	tests := []struct {
		mockNetLookupSRV      func(string, string, string) (string, []*net.SRV, error)
		mockNetLookupIP       func(string) ([]net.IP, error)
		inputMemberlistConfig func() *memberlist.Config
		expectedErr           error
		expectedLog           string
	}{
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				return "", nil, errors.New("mockNetLookupSRV error")
			},
			expectedLog: "unable to perform discovery: mockNetLookupSRV error\nforming new cluster\n",
			inputMemberlistConfig: func() *memberlist.Config {
				mlConfig := memberlist.DefaultLocalConfig()
				return mlConfig
			},
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "2-2-2-2.some-service.svc.cluster.local", Port: 7947},
					&net.SRV{Target: "10-10-10-10.some-service.svc.cluster.local", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(domain string) ([]net.IP, error) {
				a := []net.IP{}
				switch domain {
				case "2-2-2-2.some-service.svc.cluster.local":
					a = append(a, net.ParseIP("2.2.2.2"))
				case "10-10-10-10.some-service.svc.cluster.local":
					a = append(a, net.ParseIP("10.10.10.10"))
				}
				return a, nil
			},
			expectedLog: "joining existing cluster with [2.2.2.2:7947]\n",
			expectedErr: multierror.Append(errs, errors.New("Failed to join 2.2.2.2:7947: dial tcp 2.2.2.2:7947: i/o timeout")),
			inputMemberlistConfig: func() *memberlist.Config {
				mlConfig := memberlist.DefaultLocalConfig()
				mlConfig.BindAddr = "127.0.0.1"
				mlConfig.Name = "node2"
				return mlConfig
			},
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "127-0-0-1.some-service.svc.cluster.local", Port: 7947},
					&net.SRV{Target: "10-10-10-10.some-service.svc.cluster.local", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(domain string) ([]net.IP, error) {
				a := []net.IP{}
				switch domain {
				case "127-0-0-1.some-service.svc.cluster.local":
					a = append(a, net.ParseIP("127.0.0.1"))
				case "10-10-10-10.some-service.svc.cluster.local":
					a = append(a, net.ParseIP("10.10.10.10"))
				}
				return a, nil
			},
			expectedLog: "joining existing cluster with [127.0.0.1:7947]\n",
			inputMemberlistConfig: func() *memberlist.Config {
				mlConfig := memberlist.DefaultLocalConfig()
				mlConfig.BindAddr = "127.0.0.1"
				mlConfig.Name = "node2"
				return mlConfig
			},
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "127-0-0-1.some-service.svc.cluster.local", Port: 7947},
					&net.SRV{Target: "10-10-10-10.some-service.svc.cluster.local", Port: 7946},
					&net.SRV{Target: "2-2-2-2.some-service.svc.cluster.local", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(domain string) ([]net.IP, error) {
				a := []net.IP{}
				switch domain {
				case "127-0-0-1.some-service.svc.cluster.local":
					a = append(a, net.ParseIP("127.0.0.1"))
				case "10-10-10-10.some-service.svc.cluster.local":
					a = append(a, net.ParseIP("10.10.10.10"))
				case "2-2-2-2.some-service.svc.cluster.local":
					a = append(a, net.ParseIP("2.2.2.2"))
				}
				return a, nil
			},
			expectedLog: "joining existing cluster with [127.0.0.1:7947 2.2.2.2:7946]\n",
			inputMemberlistConfig: func() *memberlist.Config {
				mlConfig := memberlist.DefaultLocalConfig()
				mlConfig.BindAddr = "127.0.0.1"
				mlConfig.Name = "node2"
				return mlConfig
			},
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	c1 := memberlist.DefaultLocalConfig()
	c1.BindAddr = "127.0.0.1"
	c1.BindPort = 7947
	c1.AdvertisePort = 7947
	c1.Name = "node1"
	c1.LogOutput = ioutil.Discard
	m1, err := memberlist.Create(c1)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}

	defer m1.Shutdown()

	for _, test := range tests {
		errs = nil
		logOutput.Reset()

		if test.mockNetLookupSRV != nil {
			netLookupSRV = test.mockNetLookupSRV
		}
		if test.mockNetLookupIP != nil {
			netLookupIP = test.mockNetLookupIP
		}

		mlConfig := test.inputMemberlistConfig()
		mlConfig.LogOutput = ioutil.Discard
		ml, err := memberlist.Create(mlConfig)
		if err != nil {
			t.Fatalf("memberlist.Create error: %s", err)
		}

		disco := &Discovery{
			domain:      "test",
			selfAddress: "10.10.10.10:7946",
			config:      mlConfig,
			ml:          ml,
		}

		err = disco.FormCluster()
		ml.Shutdown()

		require.Equal(t, test.expectedLog, logOutput.String())
		require.Equal(t, test.expectedErr, err)
	}
}

func TestNew(t *testing.T) {
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	tests := []struct {
		mockNetInterfaceAddrs func() ([]net.Addr, error)
		mockMemberlistCreate  func(*memberlist.Config) (*memberlist.Memberlist, error)
		expectedErr           error
		expectedLog           string
		inputMemberlistConfig func() *memberlist.Config
	}{
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				return nil, errors.New("netInterfaceAddrs mock error")
			},
			expectedErr:           errors.New("netInterfaceAddrs mock error"),
			inputMemberlistConfig: memberlist.DefaultLocalConfig,
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ip1, _ := net.ParseCIDR("127.0.0.1/24")
				ip1.IP = net.ParseIP("127.0.0.1")
				return []net.Addr{ip1}, nil
			},
			expectedErr:           errors.New("more than 1 local IP available"),
			inputMemberlistConfig: memberlist.DefaultLocalConfig,
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ip1, _ := net.ParseCIDR("1.2.3.4/24")
				ip1.IP = net.ParseIP("1.2.3.4")
				_, ip2, _ := net.ParseCIDR("2.3.4.5/24")
				ip2.IP = net.ParseIP("2.3.4.5")
				return []net.Addr{ip1, ip2}, nil
			},
			expectedErr:           errors.New("more than 1 local IP available"),
			inputMemberlistConfig: memberlist.DefaultLocalConfig,
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ip1, _ := net.ParseCIDR("10.10.10.10/24")
				ip1.IP = net.ParseIP("10.10.10.10")
				return []net.Addr{ip1}, nil
			},
			mockMemberlistCreate: func(*memberlist.Config) (*memberlist.Memberlist, error) {
				return nil, errors.New("memberlistCreate mock error")
			},
			expectedErr:           errors.New("memberlistCreate mock error"),
			inputMemberlistConfig: memberlist.DefaultLocalConfig,
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ip1, _ := net.ParseCIDR("10.10.10.10/24")
				ip1.IP = net.ParseIP("10.10.10.10")
				return []net.Addr{ip1}, nil
			},
			mockMemberlistCreate: memberlist.Create,
			inputMemberlistConfig: func() *memberlist.Config {
				mockNetwork := &memberlist.MockNetwork{}
				mlConfig := memberlist.DefaultLocalConfig()
				mlConfig.Transport = mockNetwork.NewTransport("test")
				return mlConfig
			},
			expectedLog: "new cluster member 127.0.0.1:1\nsending retained messages to 127.0.0.1:1\nstarting MQTTPublishToCluster queue worker\n",
		},
	}
	for _, test := range tests {
		logOutput.Reset()
		if test.mockNetInterfaceAddrs != nil {
			netInterfaceAddrs = test.mockNetInterfaceAddrs
		}
		if test.mockMemberlistCreate != nil {
			memberlistCreate = test.mockMemberlistCreate
		}
		output, cancelFunc, err := New("test", test.inputMemberlistConfig())
		require.Equal(t, test.expectedErr, err)
		if err != nil {
			continue
		}
		defer cancelFunc()
		time.Sleep(50 * time.Millisecond)

		require.Equal(t, "test", output.domain)
		require.Equal(t, "10.10.10.10:7946", output.selfAddress)
		require.Equal(t, &delegate{}, output.config.Delegate)
		require.Equal(t, &delegateEvent{"10.10.10.10:7946"}, output.config.Events)
		require.Equal(t, test.expectedLog, logOutput.String())
	}
}

func TestPopulatePublishBatch(t *testing.T) {
	tests := []struct {
		inputMessage    *MQTTPublishMessage
		expectedMessage map[string][]*MQTTPublishMessage
	}{
		{
			inputMessage: &MQTTPublishMessage{},
			expectedMessage: map[string][]*MQTTPublishMessage{
				"all": []*MQTTPublishMessage{
					&MQTTPublishMessage{
						Node: []string{"all"},
					},
				},
			},
		},
		{
			inputMessage: &MQTTPublishMessage{Node: []string{"127.0.0.1:7946"}},
			expectedMessage: map[string][]*MQTTPublishMessage{
				"127.0.0.1:7946": []*MQTTPublishMessage{
					&MQTTPublishMessage{
						Node: []string{"127.0.0.1:7946"},
					},
				},
			},
		},
		{
			inputMessage: &MQTTPublishMessage{Node: []string{"127.0.0.1:7946", "2.2.2.2:7946"}},
			expectedMessage: map[string][]*MQTTPublishMessage{
				"127.0.0.1:7946": []*MQTTPublishMessage{
					&MQTTPublishMessage{
						Node: []string{"127.0.0.1:7946", "2.2.2.2:7946"},
					},
				},
				"2.2.2.2:7946": []*MQTTPublishMessage{
					&MQTTPublishMessage{
						Node: []string{"127.0.0.1:7946", "2.2.2.2:7946"},
					},
				},
			},
		},
	}
	for _, test := range tests {
		mqttPublishBatch := map[string][]*MQTTPublishMessage{}
		populatePublishBatch(mqttPublishBatch, test.inputMessage)
		require.Equal(t, test.expectedMessage, mqttPublishBatch)
	}
}

func TestHandleMQTTPublishToCluster(t *testing.T) {
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	ctx, ctxCancel := context.WithCancel(context.Background())

	c1 := memberlist.DefaultLocalConfig()
	c1.BindPort = 7947
	c1.Name = "node1"
	c1.BindAddr = "127.0.0.1"
	c1.LogOutput = ioutil.Discard
	m1, err := memberlist.Create(c1)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m1.Shutdown()

	c2 := memberlist.DefaultLocalConfig()
	c2.Name = "node2"
	c2.BindAddr = "127.0.0.1"
	c2.LogOutput = ioutil.Discard
	m2, err := memberlist.Create(c2)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m2.Shutdown()

	disco := &Discovery{
		ml:          m2,
		selfAddress: "127.0.0.1:7946",
		config:      c2,
		domain:      "test",
	}

	if _, err := m2.Join([]string{"127.0.0.1:7947"}); err != nil {
		t.Fatalf("memberlist.Join error: %s", err)
	}

	logOutput.Reset()

	go handleMQTTPublishToCluster(ctx, disco)

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, "starting MQTTPublishToCluster queue worker\n", logOutput.String())
	logOutput.Reset()

	jsonMarshal = func(any) ([]byte, error) {
		return nil, errors.New("mockJsonMarshal error")
	}
	MQTTPublishToCluster <- &MQTTPublishMessage{}

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, "unable to marshal to cluster message 127.0.0.1:7947: mockJsonMarshal error\n", logOutput.String())
	logOutput.Reset()

	jsonMarshal = json.Marshal

	delete(queueDataTypes, "MQTTPublish")

	MQTTPublishToCluster <- &MQTTPublishMessage{Node: []string{"127.0.0.1:7947"}}

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, "unable to publish message to cluster member 127.0.0.1:7947: unknown data type MQTTPublish\n", logOutput.String())
	logOutput.Reset()

	MQTTPublishToCluster <- &MQTTPublishMessage{}

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, "unable to publish message to cluster member 127.0.0.1:7947: unknown data type MQTTPublish\n", logOutput.String())
	logOutput.Reset()

	queueDataTypes["MQTTPublish"] = 1

	jsonMarshal = func(d any) ([]byte, error) {
		m, err := json.Marshal(d)
		if string(m) != `[{"Node":["all"],"Payload":null,"Topic":"","Retain":false,"Qos":0}]` {
			return nil, errors.New("jsonMarshal wrong marshaled data, expecing single message")
		}
		return m, err
	}

	MQTTPublishToCluster <- &MQTTPublishMessage{}

	time.Sleep(5 * time.Millisecond)

	jsonMarshal = func(d any) ([]byte, error) {
		m, err := json.Marshal(d)
		if string(m) != `[{"Node":["all"],"Payload":null,"Topic":"","Retain":false,"Qos":0},{"Node":["all"],"Payload":null,"Topic":"","Retain":false,"Qos":0}]` {
			return nil, errors.New("jsonMarshal wrong marshaled data, expecing two messages for all")
		}
		return m, err
	}

	MQTTPublishToCluster <- &MQTTPublishMessage{}
	MQTTPublishToCluster <- &MQTTPublishMessage{}

	time.Sleep(5 * time.Millisecond)

	jsonMarshal = func(d any) ([]byte, error) {
		m, err := json.Marshal(d)
		if string(m) != `[{"Node":["all"],"Payload":null,"Topic":"","Retain":false,"Qos":0},{"Node":["127.0.0.1:7947"],"Payload":null,"Topic":"","Retain":false,"Qos":0},{"Node":["127.0.0.1:7947"],"Payload":null,"Topic":"","Retain":false,"Qos":0}]` {
			return nil, errors.New("jsonMarshal wrong marshaled data, expecing three messages for 127.0.0.1:7947")
		}
		return m, err
	}

	MQTTPublishToCluster <- &MQTTPublishMessage{}
	MQTTPublishToCluster <- &MQTTPublishMessage{Node: []string{"127.0.0.1:7947"}}
	MQTTPublishToCluster <- &MQTTPublishMessage{Node: []string{"127.0.0.1:7947"}}

	time.Sleep(5 * time.Millisecond)

	jsonMarshal = func(d any) ([]byte, error) {
		m, err := json.Marshal(d)
		if string(m) != `[{"Node":["127.0.0.1:7947"],"Payload":null,"Topic":"","Retain":false,"Qos":0},{"Node":["127.0.0.1:7947"],"Payload":null,"Topic":"","Retain":false,"Qos":0}]` {
			return nil, errors.New("jsonMarshal wrong marshaled data, expecing two messages for 127.0.0.1:7947")
		}
		return m, err
	}

	MQTTPublishToCluster <- &MQTTPublishMessage{Node: []string{"127.0.0.1:7947"}}
	MQTTPublishToCluster <- &MQTTPublishMessage{Node: []string{"127.0.0.1:7947"}}
	MQTTPublishToCluster <- &MQTTPublishMessage{Node: []string{"missing:7947"}}

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, "", logOutput.String())
	logOutput.Reset()

	ctxCancel()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, "MQTTPublishToCluster queue worker done\n", logOutput.String())
}

func TestGetLocalIPs(t *testing.T) {
	tests := []struct {
		mockNetInterfaceAddrs func() ([]net.Addr, error)
		expectedErr           error
		expectedOutput        map[string]net.IP
	}{
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				return nil, errors.New("mock error")
			},
			expectedErr: errors.New("mock error"),
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ip1, _ := net.ParseCIDR("1.2.3.4/24")
				ip1.IP = net.ParseIP("1.2.3.4")
				return []net.Addr{ip1}, nil
			},
			expectedOutput: map[string]net.IP{
				"1.2.3.4": net.ParseIP("1.2.3.4"),
			},
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ipnet1, _ := net.ParseCIDR("1.2.3.4/24")
				ipnet1.IP = net.ParseIP("1.2.3.4")
				_, ipnet2, _ := net.ParseCIDR("127.0.0.1/24")
				ipnet2.IP = net.ParseIP("127.0.0.1")
				return []net.Addr{ipnet1, ipnet2}, nil
			},
			expectedOutput: map[string]net.IP{
				"1.2.3.4": net.ParseIP("1.2.3.4"),
			},
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ipnet1, _ := net.ParseCIDR("1.2.3.4/24")
				ipnet1.IP = net.ParseIP("1.2.3.4")
				_, ipnet2, _ := net.ParseCIDR("fd04:3e42:4a4e:3381::/64")
				ipnet2.IP = net.ParseIP("fd04:3e42:4a4e:3381::")
				return []net.Addr{ipnet1, ipnet2}, nil
			},
			expectedOutput: map[string]net.IP{
				"1.2.3.4": net.ParseIP("1.2.3.4"),
			},
		},
		{
			mockNetInterfaceAddrs: func() ([]net.Addr, error) {
				_, ipnet1, _ := net.ParseCIDR("1.2.3.4/24")
				ipnet1.IP = net.ParseIP("1.2.3.4")
				_, ipnet2, _ := net.ParseCIDR("2.3.4.5/24")
				ipnet2.IP = net.ParseIP("2.3.4.5")
				return []net.Addr{ipnet1, ipnet2}, nil
			},
			expectedOutput: map[string]net.IP{
				"1.2.3.4": net.ParseIP("1.2.3.4"),
				"2.3.4.5": net.ParseIP("2.3.4.5"),
			},
		},
	}
	for _, test := range tests {
		if test.mockNetInterfaceAddrs != nil {
			netInterfaceAddrs = test.mockNetInterfaceAddrs
		}
		output, err := getLocalIPs()
		require.Equal(t, test.expectedOutput, output)
		require.Equal(t, test.expectedErr, err)

	}
}

func TestGetInitialMemberIPs(t *testing.T) {
	tests := []struct {
		mockNetLookupSRV func(string, string, string) (string, []*net.SRV, error)
		mockNetLookupIP  func(string) ([]net.IP, error)
		expectedErr      error
		expectedOutput   map[string]string
		expectedLog      string
	}{
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				return "", nil, errors.New("mockNetLookupSRV error")
			},
			expectedErr: errors.New("mockNetLookupSRV error"),
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "abc", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(string) ([]net.IP, error) {
				return nil, errors.New("mockNetLookupIP error")
			},
			expectedOutput: make(map[string]string),
			expectedLog:    "unable to resolve discovered domain abc: mockNetLookupIP error\n",
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "abc", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(string) ([]net.IP, error) {
				a := []net.IP{}
				return a, nil
			},
			expectedOutput: map[string]string{},
			expectedLog:    "no IPs available for abc\n",
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "abc", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(string) ([]net.IP, error) {
				a := []net.IP{
					net.ParseIP("1.2.3.4"),
					net.ParseIP("2.3.4.5"),
				}
				return a, nil
			},
			expectedOutput: map[string]string{
				"2.3.4.5": "2.3.4.5:7946",
			},
			expectedLog: "more than 1 IP available for abc [1.2.3.4 2.3.4.5], picking last one\n",
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "abc", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(string) ([]net.IP, error) {
				a := []net.IP{
					net.ParseIP("fd04:3e42:4a4e:3381::"),
				}
				return a, nil
			},
			expectedOutput: map[string]string{},
			expectedLog:    "no IPv4 available for member abc [fd04:3e42:4a4e:3381::]\n",
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "abc", Port: 7946},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(string) ([]net.IP, error) {
				a := []net.IP{
					net.ParseIP("1.2.3.4"),
				}
				return a, nil
			},
			expectedOutput: map[string]string{
				"1.2.3.4": "1.2.3.4:7946",
			},
		},
		{
			mockNetLookupSRV: func(string, string, string) (string, []*net.SRV, error) {
				a := []*net.SRV{
					&net.SRV{Target: "abc", Port: 7946},
					&net.SRV{Target: "bsd", Port: 7947},
				}
				return "", a, nil
			},
			mockNetLookupIP: func(domain string) ([]net.IP, error) {
				a := []net.IP{}
				switch domain {
				case "abc":
					a = append(a, net.ParseIP("1.2.3.4"))
				case "bsd":
					a = append(a, net.ParseIP("2.3.4.5"))
				}

				return a, nil
			},
			expectedOutput: map[string]string{
				"1.2.3.4": "1.2.3.4:7946",
				"2.3.4.5": "2.3.4.5:7947",
			},
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		logOutput.Reset()

		if test.mockNetLookupSRV != nil {
			netLookupSRV = test.mockNetLookupSRV
		}
		if test.mockNetLookupIP != nil {
			netLookupIP = test.mockNetLookupIP
		}
		output, err := getInitialMemberIPs("test")
		require.Equal(t, test.expectedLog, logOutput.String())
		require.Equal(t, test.expectedOutput, output)
		require.Equal(t, test.expectedErr, err)

	}
}
