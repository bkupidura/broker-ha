package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"brokerha/internal/api"
	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
	"brokerha/internal/metric"

	"github.com/mochi-mqtt/server/v2/hooks/auth"
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

	subscriptionSize := make(map[string]int)
	config.UnmarshalKey("discovery.subscription_size", &subscriptionSize)

	d, _, err := discovery.New(&discovery.Options{
		Domain:           config.GetString("discovery.domain"),
		MemberListConfig: createMemberlistConfig(subMLConfig),
		Bus:              evBus,
		SubscriptionSize: subscriptionSize,
	})
	if err != nil {
		log.Fatal(err)
	}

	mqttAuth := auth.AuthRules{}
	config.UnmarshalKey("mqtt.auth", &mqttAuth)
	mqttACL := auth.ACLRules{}
	config.UnmarshalKey("mqtt.acl", &mqttACL)

	config.UnmarshalKey("mqtt.subscription_size", &subscriptionSize)

	b, _, err := broker.New(&broker.Options{
		MQTTPort:         config.GetInt("mqtt.port"),
		Auth:             mqttAuth,
		ACL:              mqttACL,
		Bus:              evBus,
		SubscriptionSize: subscriptionSize,
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
		Bus:                    evBus,
		ClusterExpectedMembers: config.GetInt("cluster.expected_members"),
		AuthUsers:              config.GetStringMapString("api.user"),
	})

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", api.HTTPPort), httpRouter))
	}()

	if err := d.FormCluster(minInitSleep, maxInitSleep); err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
