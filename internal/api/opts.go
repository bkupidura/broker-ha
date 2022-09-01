package api

import (
	"brokerha/internal/broker"
	"brokerha/internal/discovery"
)

// Options contains configurable options for the API.
type Options struct {
	Broker                 *broker.Broker
	Discovery              *discovery.Discovery
	ClusterExpectedMembers int
	AuthUsers              map[string]string
}
