package metric

import (
	mqtt "github.com/mochi-co/mqtt/server"

	"brokerha/internal/discovery"
)

// Options contains configurable options for the metric.
type Options struct {
	Broker    *mqtt.Server
	Discovery *discovery.Discovery
}
