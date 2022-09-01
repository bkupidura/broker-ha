package broker

import (
	"brokerha/internal/bus"
)

// Options contains configurable options for the broker.
type Options struct {
	AuthUsers map[string]string
	AuthACL   map[string][]ACL
	MQTTPort  int
	Bus       *bus.Bus
}
