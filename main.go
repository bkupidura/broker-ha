package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"net/http"

	"broker/discovery"
	"broker/server"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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

	mqttAuth := &server.Auth{
		Users:   config.GetStringMapString("mqtt.user"),
		UserACL: make(map[string][]server.ACL),
	}
	config.UnmarshalKey("mqtt.acl", &mqttAuth.UserACL)

	mqttListener := listeners.NewTCP("tcp", fmt.Sprintf(":%d", config.GetInt("mqtt.port")))

	mqttServer, _, err := server.New(mqttListener, mqttAuth)
	if err != nil {
		log.Fatal(err)
	}

	initializeMetrics()
	go metricCollector(disco, mqttServer)

	httpRouter := chi.NewRouter()

	httpRouter.Group(func(r chi.Router) {
		r.Use(middleware.Recoverer)
		r.Method("GET", "/ready", readyHandler(disco))
		r.Method("GET", "/healthz", healthzHandler(disco, config.GetInt("cluster.expected_members")))
		r.Method("GET", "/metrics", promhttp.Handler())
	})
	httpRouter.Group(func(r chi.Router) {
		r.Use(middleware.Recoverer)
		r.Get("/api/discovery/members", apiDiscoveryMembersHandler(disco))
		r.Post("/api/discovery/leave", apiDiscoveryLeaveHandler(disco))
		r.Post("/api/discovery/advertise", apiDiscoveryAdvertiseHandler(disco))
		r.Get("/api/mqtt/clients", apiMqttClientsHandler(mqttServer))
		r.Post("/api/mqtt/client/stop", apiMqttClientStopHandler(mqttServer))
		r.Post("/api/mqtt/client/inflight", apiMqttClientInflightHandler(mqttServer))
		r.Post("/api/mqtt/topic/messages", apiMqttTopicMessagesHandler(mqttServer))
		r.Post("/api/mqtt/topic/subscribers", apiMqttTopicSubscribersHandler(mqttServer))
	})

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
