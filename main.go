package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"math/rand"
	"net/http"

	"broker/discovery"
	"broker/metric"
	"broker/mqtt_server"

	"github.com/alexliesenfeld/health"
	"github.com/hashicorp/memberlist"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

const (
	// Minimal sleep time after start.
	// This is used to introduce some random delay, in case all PODs are restarted
	// and trying to form cluster in exact same moment.
	minInitSleep = 1
	// Maximal sleep time after start.
	maxInitSleep = 60
)

// getConfig loads config from /config/config.yaml.
// It will also set config values based on environment variables.
// BROKER_MQTT_PORT -> mqtt.port
func getConfig() (*viper.Viper, error) {
	config := viper.New()

	config.SetConfigName("config")
	config.SetConfigType("yaml")

	config.AddConfigPath("/config")

	replacer := strings.NewReplacer(".", "_")
	config.SetEnvKeyReplacer(replacer)
	config.SetEnvPrefix("broker")
	config.AutomaticEnv()

	config.SetDefault("mqtt.port", 1883)
	config.SetDefault("mqtt.user", map[string]string{})
	config.SetDefault("mqtt.acl", map[string][]string{})

	config.SetDefault("cluster.expected_members", 3)
	config.SetDefault("cluster.secret_key", "DummySecrEtKey1^")

	if err := config.ReadInConfig(); err != nil {
		return nil, err
	}

	requiredArgs := []string{"discovery.domain"}
	for _, argName := range requiredArgs {
		if config.Get(argName) == nil {
			return nil, errors.New(fmt.Sprintf("missing required config key: %s", argName))
		}
	}

	return config, nil
}

// Main will start discovery instance and mqtt broker instance.
func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	config, err := getConfig()
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(time.Now().UnixNano())
	randomSleepDuration := time.Duration(rand.Intn(maxInitSleep-minInitSleep) + minInitSleep)

	mlConfig := memberlist.DefaultLocalConfig()
	// Lowering ProbeInterval will allow memberlist to faster detect dead nodes but will consume bandwidth.
	mlConfig.ProbeInterval = 500 * time.Millisecond
	// SecretKey is used encryption of memberlist communication. It should be 16, 24 or 32 bytes long.
	mlConfig.SecretKey = []byte(config.GetString("cluster.secret_key"))

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
		readinessProbe(disco, randomSleepDuration),
		health.WithMiddleware(
			failedCheckLogger(),
		),
	))
	http.Handle("/healthz", health.NewHandler(
		livenessProbe(disco, randomSleepDuration, config.GetInt("cluster.expected_members")),
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

	log.Printf("sleeping for %ds before forming cluster", randomSleepDuration)
	time.Sleep(randomSleepDuration * time.Second)

	if err := disco.FormCluster(); err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
