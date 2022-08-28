package broker

// Options contains configurable options for the broker.
type Options struct {
	AuthUsers map[string]string
	AuthACL   map[string][]ACL
	MQTTPort  int
}
