package discovery

import (
	"github.com/hashicorp/memberlist"
)

// Options contains configurable options for the discovery.
type Options struct {
	Domain           string
	MemberListConfig *memberlist.Config
}
