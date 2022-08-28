package api

import (
	mqtt "github.com/mochi-co/mqtt/server"

	"brokerha/internal/discovery"
)

// Options contains configurable options for the API.
type Options struct {
	Broker                 *mqtt.Server
	Discovery              *discovery.Discovery
	ClusterExpectedMembers int
	AuthUsers              map[string]string
}
