package main

import (
	"fmt"
	"log"
	"sync"

	"net/http"

	"broker/discovery"
	"broker/metric"
	"broker/mqtt_server"

	"github.com/alexliesenfeld/health"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Minimal sleep time after start.
	// This is used to introduce some random delay, in case all PODs are restarted
	// and trying to form cluster in exact same moment.
	minInitSleep = 5
	// Maximal sleep time after start.
	maxInitSleep = 60
)

// Main will start discovery instance and mqtt broker instance.
func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

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

	mqttAuth := &mqtt_server.Auth{
		Users:   config.GetStringMapString("mqtt.user"),
		UserAcl: make(map[string][]mqtt_server.Acl),
	}
	config.UnmarshalKey("mqtt.acl", &mqttAuth.UserAcl)

	mqttListener := listeners.NewTCP("t1", fmt.Sprintf(":%d", config.GetInt("mqtt.port")))

	mqttServer, _, err := mqtt_server.New(mqttListener, mqttAuth)
	if err != nil {
		log.Fatal(err)
	}

	metric.Initialize()
	go metric.Collect(disco, mqttServer)

	http.Handle("/ready", health.NewHandler(
		readinessProbe(disco),
		health.WithMiddleware(
			failedCheckLogger(),
		),
	))
	http.Handle("/healthz", health.NewHandler(
		livenessProbe(disco, config.GetInt("cluster.expected_members")),
		health.WithMiddleware(
			failedCheckLogger(),
		),
	))
	http.Handle("/metrics", promhttp.Handler())

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	if err := disco.FormCluster(minInitSleep, maxInitSleep); err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
