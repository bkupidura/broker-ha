package discovery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"

	"encoding/json"
	"io/ioutil"

	"github.com/hashicorp/memberlist"
)

var (
	// Defines allowed message types received from cluster.
	queueDataTypes = map[string]byte{"MQTTPublish": 1}
	// MQTTPublishFromCluster is used to push data received from memberlist cluster.
	// It is consumed by mqtt_server.
	MQTTPublishFromCluster = make(chan *MQTTPublishMessage, 1024)
	// MQTTPublishToCluster is used to push data received from mqtt_server.
	// It is consumed by discovery (memberlist).
	MQTTPublishToCluster = make(chan *MQTTPublishMessage, 1024)
	// MQTTSendRetained is used when new node join cluster.
	// It is consumed by mqtt_server.
	MQTTSendRetained = make(chan *memberlist.Node, 10)

	// Mocks for tests.
	netLookupSRV      = net.LookupSRV
	netLookupIP       = net.LookupIP
	netInterfaceAddrs = net.InterfaceAddrs
	jsonMarshal       = json.Marshal
	memberlistCreate  = memberlist.Create
)

// MQTTPublishMessage is storing data for MQTTPublishFromCluster and MQTTPublishToCluster.
// Basicly its MQTT message + list of nodes to which message is addressed.
// When Node is empty, message will be send to every node in cluster (except self).
type MQTTPublishMessage struct {
	Node    []string
	Payload []byte
	Topic   string
	Retain  bool
	Qos     byte
}

// Discovery is DNS member discovery and abstraction over memberlist.
// It should be created by New().
type Discovery struct {
	domain      string
	selfAddress string
	config      *memberlist.Config
	ml          *memberlist.Memberlist
}

// Shutdown memberlist connection. It will drop node from cluster.
// Its not gracefull exit, members will need to discover that node is no longer part of cluster.
func (d *Discovery) Shutdown() error {
	return d.ml.Shutdown()
}

// Returns self health status.
// 0 is best, anything higher means that there are some issues with cluster.
// Check memberlist documentation for more details.
func (d *Discovery) GetHealthScore() int {
	return d.ml.GetHealthScore()
}

// Send reliable data to cluster member.
// It will be formated for `broker-ha`, first byte is used as data type, rest of message is data itself.
func (d *Discovery) SendReliable(member *memberlist.Node, dataType string, data []byte) error {
	dataTypeByte, ok := queueDataTypes[dataType]
	if !ok {
		return errors.New(fmt.Sprintf("unknown data type %s", dataType))
	}
	message := append([]byte{dataTypeByte}, data...)

	if err := d.ml.SendReliable(member, message); err != nil {
		return err
	}
	return nil
}

// Members returns slice of member nodes from cluster.
func (d *Discovery) Members(withSelf bool) []*memberlist.Node {
	var members []*memberlist.Node
	for _, member := range d.ml.Members() {
		if !withSelf && member.Address() == d.selfAddress {
			continue
		}
		members = append(members, member)
	}
	return members
}

// FormCluster is responsible for joining existing cluster or forming new cluster.
// To check if any cluster already exists broker-ha depends on DNS SRV discovery.
// If we will receive error from DNS SRV or empty response (excluding ourselfs) - new cluster will be created.
// Otherwise discovery will join existing cluster.
//
// There is possibility that more than 1 node will form new cluster and we will endup with 2 not connected clusters.
// This scenario is possible when all cluster nodes were destroyed/killed.
// It will by fixed by 3rd cluster member or by k8s.
//
// When 3rd node will be spawned, it will connect those "independant" clusters into one synced cluster.
// If 3rd node will not be able to do that, /healthz endpoint will start reporting POD as unhealthy and POD should be killed by k8s.
func (d *Discovery) FormCluster() error {
	var members []string

	initialMemberIPs, err := getInitialMemberIPs(d.domain)
	if err != nil {
		log.Printf("unable to perform discovery: %s", err)
	}

	if initialMemberIPs != nil {
		for _, ipPortPair := range initialMemberIPs {
			if ipPortPair == d.selfAddress {
				continue
			}
			members = append(members, ipPortPair)
		}
	}

	sort.Strings(members)

	if len(members) > 0 {
		log.Printf("joining existing cluster with %s", members)
		_, err := d.ml.Join(members)
		if err != nil {
			return err
		}
	} else {
		log.Printf("forming new cluster")
	}

	return nil
}

// New creates new discovery instance.
// Currently discovery can be started only on nodes with single local IP - no multihoming.
// But this is default in K8s.
func New(domain string, mlConfig *memberlist.Config) (*Discovery, context.CancelFunc, error) {
	d := &Discovery{
		domain: domain,
		config: mlConfig,
	}

	localIPs, err := getLocalIPs()
	if err != nil {
		return nil, nil, err
	}

	if len(localIPs) == 1 {
		for _, lip := range localIPs {
			d.selfAddress = fmt.Sprintf("%s:%d", lip.String(), mlConfig.BindPort)
		}
	} else {
		return nil, nil, errors.New("more than 1 local IP available")
	}

	d.config.LogOutput = ioutil.Discard
	d.config.Events = &delegateEvent{
		selfAddress: d.selfAddress,
	}
	d.config.Delegate = &delegate{}

	ml, err := memberlistCreate(d.config)
	if err != nil {
		return nil, nil, err
	}

	d.ml = ml

	// ctx is used only by tests.
	ctx, ctxCancel := context.WithCancel(context.Background())
	go handleMQTTPublishToCluster(ctx, d)

	return d, ctxCancel, nil
}

// handleMQTTPublishToCluster will get messages from MQTTPublishToCluster and send them with memberlist to other members.
// When MQTTPublishToCluster.Node is empty, message will be send to all nodes (except self).
// Otherwise message will be send only to specific members.
func handleMQTTPublishToCluster(ctx context.Context, disco *Discovery) {
	log.Printf("starting MQTTPublishToCluster queue worker")
	for {
		select {
		case message := <-MQTTPublishToCluster:
			messageMarshal, err := jsonMarshal(message)
			if err != nil {
				log.Printf("unable to marshal to cluster message %v: %s", message, err)
				continue
			}
			var members []*memberlist.Node
			if len(message.Node) == 0 {
				members = disco.Members(false)
			} else {
				for _, memberNode := range disco.Members(false) {
					for _, messageNode := range message.Node {
						if memberNode.Address() == messageNode {
							members = append(members, memberNode)
						}
					}
				}
			}
			for _, member := range members {
				if err := disco.SendReliable(member, "MQTTPublish", messageMarshal); err != nil {
					log.Printf("unable to publish message to cluster member %s: %s", member, err)
				}
			}
		case <-ctx.Done():
			log.Printf("MQTTPublishToCluster queue worker done")
			return
		}
	}
}

// Get all IPv4 addresses except loopback.
func getLocalIPs() (map[string]net.IP, error) {
	localIPs := make(map[string]net.IP)

	adresses, err := netInterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, address := range adresses {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				localIPs[ipnet.IP.String()] = ipnet.IP
			}
		}
	}
	return localIPs, nil
}

// Get IPv4 addresses for other cluster members.
// This is only used after start to find if we should join existing cluster or form new one.
func getInitialMemberIPs(domain string) (map[string]string, error) {
	initialMemberIPs := make(map[string]string)
	_, addrs, err := netLookupSRV("", "", domain)
	if err != nil {
		return nil, err
	}

	for _, addr := range addrs {
		var memberIP string

		ips, err := netLookupIP(addr.Target)
		if err != nil {
			log.Printf("unable to resolve discovered domain %s: %s", addr.Target, err)
			continue
		}

		for _, ip := range ips {
			if ipv4 := ip.To4(); ipv4 != nil {
				memberIP = ipv4.String()
			}
		}
		if len(ips) > 1 {
			log.Printf("more than 1 IP available for %s %s, picking last one", addr.Target, ips)
		}

		if len(ips) == 0 {
			log.Printf("no IPs available for %s", addr.Target)
			continue
		}

		if memberIP == "" {
			log.Printf("no IPv4 available for member %s %s", addr.Target, ips)
			continue
		}
		initialMemberIPs[memberIP] = fmt.Sprintf("%s:%d", memberIP, addr.Port)
	}
	return initialMemberIPs, nil
}
