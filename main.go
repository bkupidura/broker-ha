package main

import (
	"log"
	"net/http"
	"sync"
	"time"

	"brokerha/internal/api"
	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
	"brokerha/internal/metric"
)

var (
	// Minimal sleep time after start.
	// This is used to introduce some random delay, in case all PODs are restarted
	// and trying to form cluster in exact same moment.
	minInitSleep = 5
	// Maximal sleep time after start.
	maxInitSleep = 60
)

// main will start discovery instance and mqtt broker instance.
func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Llongfile)

	// This sleep is needed in case container is killed by k8s.
	// Without it, there is possibility that POD will be restarted faster than memberlist will
	// be able to detect member down.
	log.Printf("sleeping for %ds before starting broker", minInitSleep)
	time.Sleep(time.Duration(minInitSleep) * time.Second)

	config, err := getConfig()
	if err != nil {
		log.Fatal(err)
	}

	subMLConfig := config.Sub("cluster.config")
	if subMLConfig == nil {
		log.Fatal("cluster.config is nil")
	}

	evBus := bus.New()

	d, _, err := discovery.New(&discovery.Options{
		Domain:           config.GetString("discovery.domain"),
		MemberListConfig: createMemberlistConfig(subMLConfig),
		Bus:              evBus,
	})
	if err != nil {
		log.Fatal(err)
	}

	mqttUserACL := make(map[string][]broker.ACL)
	config.UnmarshalKey("mqtt.acl", &mqttUserACL)

	b, _, err := broker.New(&broker.Options{
		MQTTPort:  config.GetInt("mqtt.port"),
		AuthUsers: config.GetStringMapString("mqtt.user"),
		AuthACL:   mqttUserACL,
		Bus:       evBus,
	})
	if err != nil {
		log.Fatal(err)
	}

	metric.Initialize(&metric.Options{
		Discovery: d,
		Broker:    b,
	})

	httpRouter := api.NewRouter(&api.Options{
		Discovery:              d,
		Broker:                 b,
		ClusterExpectedMembers: config.GetInt("cluster.expected_members"),
		AuthUsers:              config.GetStringMapString("api.user"),
	})

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		log.Fatal(http.ListenAndServe(":8080", httpRouter))
	}()

	if err := d.FormCluster(minInitSleep, maxInitSleep); err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
