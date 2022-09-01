package types

// MQTTPublishMessage is consumed by brokerha/internal/broker.
type MQTTPublishMessage struct {
	Payload []byte
	Topic   string
	Retain  bool
	Qos     byte
}

// DiscoveryPublishMessage is consumed by brokerha/internal/discovery.
// When Node is empty, message will be sent to all discovery (memberlist)
// members (except self).
type DiscoveryPublishMessage struct {
	Node    []string
	Payload []byte
	Topic   string
	Retain  bool
	Qos     byte
}
