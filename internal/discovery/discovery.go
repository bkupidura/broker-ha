package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"sort"
	"time"

	"github.com/hashicorp/memberlist"

	"brokerha/internal/bus"
	"brokerha/internal/types"
)

var (
	// Defines allowed message types received from cluster.
	queueDataTypes = map[string]byte{"MQTTPublish": 1, "SendRetained": 2}
	// How many messages we should try to send over single memberlist.SendReliable.
	// Openning TCP session is expensive, lets try to open single session for multiple messages.
	// If there is only one message in MQTTPublishToCluster we will not wait for more.
	mergePublishToCluster = 25
	// How many times SendReliabe should be retried in case of error.
	sendRetries = 3
	// How long to wait between retries in milliseconds.
	sendRetriesInterval = 100

	// Mocks for tests.
	netLookupSRV      = net.LookupSRV
	netLookupIP       = net.LookupIP
	netInterfaceAddrs = net.InterfaceAddrs
	jsonMarshal       = json.Marshal
	memberlistCreate  = memberlist.Create
)

// Discovery is DNS member discovery and abstraction over memberlist.
// It should be created by New().
type Discovery struct {
	domain       string
	selfAddress  map[string]struct{}
	config       *memberlist.Config
	ml           *memberlist.Memberlist
	bus          *bus.Bus
	retainedHash *retainedHash
}

// New creates new discovery instance.
func New(opts *Options) (*Discovery, context.CancelFunc, error) {
	for _, requiredSubscriptionSizeName := range []string{"cluster:message_to", "discovery:request_retained", "discovery:retained_hash"} {
		if _, ok := opts.SubscriptionSize[requiredSubscriptionSizeName]; !ok {
			return nil, nil, fmt.Errorf("subscription size for %s not provided", requiredSubscriptionSizeName)
		}
	}

	chClusterMessageTo, err := opts.Bus.Subscribe("cluster:message_to", "discovery", opts.SubscriptionSize["cluster:message_to"])
	if err != nil {
		return nil, nil, err
	}

	chDiscoveryRequestRetained, err := opts.Bus.Subscribe("discovery:request_retained", "discovery", opts.SubscriptionSize["discovery:request_retained"])
	if err != nil {
		return nil, nil, err
	}

	chDiscoveryRetainedHash, err := opts.Bus.Subscribe("discovery:retained_hash", "discovery", opts.SubscriptionSize["discovery:retained_hash"])
	if err != nil {
		return nil, nil, err
	}

	d := &Discovery{
		domain:      opts.Domain,
		config:      opts.MemberListConfig,
		selfAddress: make(map[string]struct{}),
		bus:         opts.Bus,
		retainedHash: &retainedHash{
			hashMap: map[string]retainedHashEntry{},
		},
	}

	localIPs, err := getLocalIPs()
	if err != nil {
		return nil, nil, err
	}

	for _, lip := range localIPs {
		d.selfAddress[fmt.Sprintf("%s:%d", lip.String(), d.config.BindPort)] = struct{}{}
	}

	d.config.LogOutput = ioutil.Discard
	d.config.Events = &delegateEvent{
		name:         d.config.Name,
		bus:          d.bus,
		retainedHash: d.retainedHash,
	}
	d.config.Delegate = &delegate{
		name:             d.config.Name,
		bus:              d.bus,
		retainedHash:     d.retainedHash,
		pushPullInterval: d.config.PushPullInterval,
	}

	ml, err := memberlistCreate(d.config)
	if err != nil {
		return nil, nil, err
	}

	d.ml = ml

	// ctx is used only by tests.
	ctx, ctxCancel := context.WithCancel(context.Background())

	go d.eventLoop(ctx, chClusterMessageTo, chDiscoveryRequestRetained, chDiscoveryRetainedHash)

	return d, ctxCancel, nil
}

// Shutdown memberlist connection. It will drop node from cluster.
// Its not gracefull exit, members will need to detect that node is no longer part of cluster.
func (d *Discovery) Shutdown() error {
	return d.ml.Shutdown()
}

// Leave memberlist cluster.
func (d *Discovery) Leave(duration time.Duration) error {
	return d.ml.Leave(duration)
}

// Join memberlist cluster.
func (d *Discovery) Join(members []string) (int, error) {
	return d.ml.Join(members)
}

// GetHealthScore returns self health status.
// 0 is best, anything higher means that there are some issues with cluster.
// Check memberlist documentation for more details.
func (d *Discovery) GetHealthScore() int {
	return d.ml.GetHealthScore()
}

// SendReliable sends data to cluster member.
// It will be formated for `broker-ha`, first byte is used as data type, rest of message is data itself.
func (d *Discovery) SendReliable(member *memberlist.Node, dataType string, data []byte) error {
	dataTypeByte, ok := queueDataTypes[dataType]
	if !ok {
		return fmt.Errorf("unknown data type %s", dataType)
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
		if !withSelf && d.config.Name == member.String() {
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
func (d *Discovery) FormCluster(minInitSleep, maxInitSleep int) error {
	var members []string

	initialMemberIPs, err := getInitialMemberIPs(d.domain)
	if err != nil {
		log.Printf("unable to perform discovery: %s", err)

		rand.Seed(time.Now().UnixNano())
		randomSleepDuration := time.Duration(rand.Intn(maxInitSleep-minInitSleep) + minInitSleep)

		log.Printf("sleeping for %ds before forming cluster", randomSleepDuration)
		time.Sleep(randomSleepDuration * time.Second)

		// Lets try again, probably others members are already running.
		initialMemberIPs, _ = getInitialMemberIPs(d.domain)
	}

	if initialMemberIPs != nil {
		for _, ipPortPair := range initialMemberIPs {
			if _, ok := d.selfAddress[ipPortPair]; ok {
				continue
			}
			members = append(members, ipPortPair)
		}
	}

	sort.Strings(members)

	if len(members) > 0 {
		log.Printf("joining existing cluster with %s", members)
		_, err := d.Join(members)
		if err != nil {
			return err
		}
	} else {
		log.Printf("forming new cluster")
	}

	return nil
}

// publishToCluster will get messages from bus and send them with memberlist to other members.
// When message.Node is empty, message will be send to all nodes (except self).
// Otherwise message will be send only to specific members.
//
// It will try to fetch mergePublishToCluster number of messages from toClusterQueue.
// Openning TCP session for every single message is expensive, so lets try to send as much messages as we can.
// When there is no more messages in bus, we will send what we get and dont wait.
func (d *Discovery) publishToCluster(mqttPublishBatch map[string][]types.DiscoveryPublishMessage) {
	for _, member := range d.Members(false) {
		var messagesForMember []types.DiscoveryPublishMessage
		if messagesForAll, ok := mqttPublishBatch["all"]; ok {
			messagesForMember = messagesForAll
		}
		if messages, ok := mqttPublishBatch[member.String()]; ok {
			messagesForMember = append(messagesForMember, messages...)
		}
		if len(messagesForMember) > 0 {
			messageMarshal, err := jsonMarshal(messagesForMember)
			if err != nil {
				log.Printf("unable to marshal to cluster message %s: %s", member.String(), err)
				continue
			}
			go func(m *memberlist.Node, data []byte) {
				for retries := 1; retries <= sendRetries; retries++ {
					if err := d.SendReliable(m, "MQTTPublish", data); err != nil {
						log.Printf("unable to publish message to cluster member %s (retries %d/%d): %s", m.String(), retries, sendRetries, err)
						time.Sleep(time.Duration(retries*sendRetriesInterval) * time.Millisecond)
						continue
					}
					return
				}
			}(member, messageMarshal)
		}
	}
}

// populatePublishBatch will prepare map[string][]MQTTPublishMessage before sending it to cluster.
func populatePublishBatch(publishBatch map[string][]types.DiscoveryPublishMessage, message types.DiscoveryPublishMessage) {
	if len(message.Node) == 0 {
		message.Node = []string{"all"}
	}
	for _, member := range message.Node {
		publishBatch[member] = append(publishBatch[member], message)
	}
}

// eventLoop perform maintenance tasks.
func (d *Discovery) eventLoop(ctx context.Context, chClusterMessageTo chan bus.Event, chDiscoveryRequestRetained chan bus.Event, chDiscoveryRetainedHash chan bus.Event) {
	log.Printf("starting eventloop")
	for {
		select {
		case event := <-chClusterMessageTo:
			mqttPublishBatch := map[string][]types.DiscoveryPublishMessage{}
			message := event.Data.(types.DiscoveryPublishMessage)
			populatePublishBatch(mqttPublishBatch, message)

		fetchMultiple:
			for i := 0; i < mergePublishToCluster; i++ {
				select {
				case event := <-chClusterMessageTo:
					message := event.Data.(types.DiscoveryPublishMessage)
					populatePublishBatch(mqttPublishBatch, message)
				default:
					break fetchMultiple
				}
			}
			d.publishToCluster(mqttPublishBatch)
		case event := <-chDiscoveryRequestRetained:
			memberName := event.Data.(string)
			for _, member := range d.Members(false) {
				if memberName == member.String() {
					if err := d.SendReliable(member, "SendRetained", []byte(d.config.Name)); err != nil {
						log.Printf("unable to SendRetained to cluster member %s: %s", member.String(), err)
					}
					break
				}
			}
		case event := <-chDiscoveryRetainedHash:
			retainedHash := event.Data.(string)
			log.Printf("d.retainedHash.Set(d.config.Name, retainedHash) %v %v", d.config.Name, retainedHash)
			d.retainedHash.Set(d.config.Name, retainedHash)
		case <-ctx.Done():
			log.Printf("stopping eventloop")
			return
		}
	}
}

// Get all IPv4 addresses.
func getLocalIPs() (map[string]net.IP, error) {
	localIPs := make(map[string]net.IP)

	adresses, err := netInterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, address := range adresses {
		if ipnet, ok := address.(*net.IPNet); ok {
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
