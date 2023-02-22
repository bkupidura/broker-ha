package metric

import (
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"brokerha/internal/broker"
	"brokerha/internal/discovery"
)

var (
	clusterMembers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_cluster_members",
		Help: "Number of cluster members",
	})
	clusterMemberHealth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_cluster_member_health",
		Help: "Cluster member health",
	})
	retainedMessages = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_retained_messages",
		Help: "Number of retained messages",
	})
	subscriptions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_subscriptions",
		Help: "Number of subscriptions",
	})
	clientsConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_clients_connected",
		Help: "Number of connected clients",
	})
	brokerUptime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_uptime",
		Help: "Broker uptime",
	})
	messagesRecv = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_messages_recv",
		Help: "Total number of received messages",
	})
	messagesSent = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_messages_sent",
		Help: "Total number of sent messages",
	})
	messagesDropped = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_messages_dropped",
		Help: "Total number of publish messages dropped to slow subscriber",
	})
	inflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_inflight_messages",
		Help: "Number of in-flight messages",
	})
)

// Initialize register prometheus collectors and start collector.
func Initialize(opts *Options) {
	prometheus.MustRegister(clusterMembers)
	prometheus.MustRegister(clusterMemberHealth)
	prometheus.MustRegister(retainedMessages)
	prometheus.MustRegister(subscriptions)
	prometheus.MustRegister(clientsConnected)
	prometheus.MustRegister(brokerUptime)
	prometheus.MustRegister(messagesRecv)
	prometheus.MustRegister(messagesSent)
	prometheus.MustRegister(messagesDropped)
	prometheus.MustRegister(inflight)

	go collect(opts.Discovery, opts.Broker)
}

// collect will refresh Prometheus collectors.
func collect(disco *discovery.Discovery, broker *broker.Broker) {
	log.Printf("starting prometheus worker")
	for {
		clusterMembers.Set(float64(len(disco.Members(true))))
		clusterMemberHealth.Set(float64(disco.GetHealthScore()))
		retainedMessages.Set(float64(broker.SystemInfo().Retained))
		subscriptions.Set(float64(broker.SystemInfo().Subscriptions))
		clientsConnected.Set(float64(broker.SystemInfo().ClientsConnected))
		brokerUptime.Set(float64(broker.SystemInfo().Uptime))
		messagesRecv.Set(float64(broker.SystemInfo().MessagesReceived))
		messagesSent.Set(float64(broker.SystemInfo().MessagesSent))
		messagesDropped.Set(float64(broker.SystemInfo().MessagesDropped))
		inflight.Set(float64(broker.SystemInfo().Inflight))

		time.Sleep(10 * time.Second)
	}
}
