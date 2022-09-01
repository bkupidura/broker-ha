package metric

import (
	"brokerha/internal/broker"
	"brokerha/internal/discovery"
)

// Options contains configurable options for the metric.
type Options struct {
	Broker    *broker.Broker
	Discovery *discovery.Discovery
}
