package main

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/spf13/viper"
)

// Create memberlist config and populate it from our config.
// cluster.config.tcp_timeout will be used as mlConfig.TCPTimeout.
func createMemberlistConfig(config *viper.Viper) *memberlist.Config {
	mlConfig := memberlist.DefaultLocalConfig()

	for propName, argName := range map[string]string{"TCPTimeout": "tcp_timeout", "PushPullInterval": "push_pull_interval", "ProbeInterval": "probe_interval", "ProbeTimeout": "probe_timeout", "GossipInterval": "gossip_interval", "GossipToTheDeadTime": "gossip_to_the_dead_time"} {
		if v := config.Get(argName); v != nil {
			value := time.Duration(v.(int)) * time.Millisecond
			reflect.ValueOf(mlConfig).Elem().FieldByName(propName).Set(reflect.ValueOf(value))
		}
	}

	for propName, argName := range map[string]string{"IndirectChecks": "indirect_checks", "RetransmitMult": "retransmit_mult", "SuspicionMult": "suspicion_mult"} {
		if v := config.Get(argName); v != nil {
			value := v.(int)
			reflect.ValueOf(mlConfig).Elem().FieldByName(propName).Set(reflect.ValueOf(value))
		}
	}

	for propName, argName := range map[string]string{"SecretKey": "secret_key"} {
		if v := config.Get(argName); v != nil {
			value := []byte(v.(string))
			reflect.ValueOf(mlConfig).Elem().FieldByName(propName).Set(reflect.ValueOf(value))
		}
	}

	return mlConfig
}

// getConfig loads config from /config/config.yaml.
// It will also set config values based on environment variables.
// BROKER_MQTT_PORT -> mqtt.port
func getConfig() (*viper.Viper, error) {
	config := viper.New()

	config.SetConfigName("config")
	config.SetConfigType("yaml")

	config.AddConfigPath("/config")
	config.AddConfigPath(".")

	replacer := strings.NewReplacer(".", "_")
	config.SetEnvKeyReplacer(replacer)
	config.SetEnvPrefix("broker")
	config.AutomaticEnv()

	config.SetDefault("mqtt.port", 1883)
	config.SetDefault("mqtt.subscription_size", map[string]interface{}{"cluster:message_from": 1024, "broker:send_retained": 10, "broker:pk_retained": 100})

	config.SetDefault("cluster.expected_members", 3)
	config.SetDefault("cluster.config.probe_interval", 500)
	config.SetDefault("cluster.config.push_pull_interval", 15000)

	config.SetDefault("discovery.subscription_size", map[string]interface{}{"cluster:message_to": 1024, "discovery:request_retained": 10, "discovery:retained_hash": 10})

	if err := config.ReadInConfig(); err != nil {
		log.Printf("unable to read config file, starting with defaults: %s", err)
	}

	requiredArgs := []string{"discovery.domain"}
	for _, argName := range requiredArgs {
		if config.Get(argName) == nil {
			return nil, fmt.Errorf("missing required config key: %s", argName)
		}
	}

	return config, nil
}
