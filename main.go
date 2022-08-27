package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mochi-co/mqtt/server/listeners"

	"brokerha/internal/api"
	"brokerha/internal/broker"
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

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
	mlConfig := createMemberlistConfig(subMLConfig)

	disco, _, err := discovery.New(config.GetString("discovery.domain"), mlConfig)
	if err != nil {
		log.Fatal(err)
	}

	mqttAuth := &broker.Auth{
		Users:   config.GetStringMapString("mqtt.user"),
		UserACL: make(map[string][]broker.ACL),
	}
	config.UnmarshalKey("mqtt.acl", &mqttAuth.UserACL)

	mqttListener := listeners.NewTCP("tcp", fmt.Sprintf(":%d", config.GetInt("mqtt.port")))

	mqttServer, _, err := broker.New(mqttListener, mqttAuth)
	if err != nil {
		log.Fatal(err)
	}

	metric.Initialize(disco, mqttServer)

	httpRouter := api.NewRouter(disco, mqttServer, config.GetInt("cluster.expected_members"), config.GetStringMapString("api.user"))

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		log.Fatal(http.ListenAndServe(":8080", httpRouter))
	}()

	if err := disco.FormCluster(minInitSleep, maxInitSleep); err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
