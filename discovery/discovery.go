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
	queueDataTypes         = map[string]byte{"MQTTPublish": 1}
	MQTTPublishFromCluster = make(chan *MQTTPublishMessage, 1024)
	MQTTPublishToCluster   = make(chan *MQTTPublishMessage, 1024)
	MQTTSendRetained       = make(chan *memberlist.Node, 10)

	netLookupSRV      = net.LookupSRV
	netLookupIP       = net.LookupIP
	netInterfaceAddrs = net.InterfaceAddrs
	jsonMarshal       = json.Marshal
	memberlistCreate  = memberlist.Create
)

type MQTTPublishMessage struct {
	Node    []string
	Payload []byte
	Topic   string
	Retain  bool
	Qos     byte
}

type Discovery struct {
	domain      string
	selfAddress string
	config      *memberlist.Config
	ml          *memberlist.Memberlist
}

func (d *Discovery) Shutdown() error {
	return d.ml.Shutdown()
}

func (d *Discovery) GetHealthScore() int {
	return d.ml.GetHealthScore()
}

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

	ctx, ctxCancel := context.WithCancel(context.Background())
	go handleMQTTPublishToCluster(ctx, d)

	return d, ctxCancel, nil
}

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
