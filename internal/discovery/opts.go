package discovery

import (
	"github.com/hashicorp/memberlist"

	"brokerha/internal/bus"
)

// Options contains configurable options for the discovery.
type Options struct {
	Domain           string
	MemberListConfig *memberlist.Config
	Bus              *bus.Bus
	SubscriptionSize map[string]int
}
