package broker

import (
	"brokerha/internal/bus"

	"github.com/mochi-mqtt/server/v2/hooks/auth"
)

// Options contains configurable options for the broker.
type Options struct {
	Auth             auth.AuthRules
	ACL              auth.ACLRules
	MQTTPort         int
	Bus              *bus.Bus
	SubscriptionSize map[string]int
}
