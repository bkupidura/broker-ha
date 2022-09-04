package api

import (
	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
)

// Options contains configurable options for the API.
type Options struct {
	Broker                 *broker.Broker
	Discovery              *discovery.Discovery
	Bus                    *bus.Bus
	ClusterExpectedMembers int
	AuthUsers              map[string]string
}
