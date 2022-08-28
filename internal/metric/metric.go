package metric

import (
	"log"
	"time"

	mqtt "github.com/mochi-co/mqtt/server"
	"github.com/prometheus/client_golang/prometheus"

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
	clusterMQTTPublishFromCluster = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_cluster_mqtt_publish_from_cluster",
		Help: "Cluster MQTT publish from cluster queue length",
	})
	clusterMQTTPublishToCluster = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_cluster_mqtt_publish_to_cluster",
		Help: "Cluster MQTT publish to cluster queue length",
	})
	clusterMQTTRetainedQueue = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_cluster_mqtt_retained_queue",
		Help: "Cluster MQTT retained queue length",
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
	publishDropped = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_publish_dropped",
		Help: "Number of in-flight publish messages which were dropped",
	})
	publishRecv = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_publish_recv",
		Help: "Total number of received publish packets",
	})
	publishSent = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_publish_sent",
		Help: "Total number of sent publish packets",
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
	prometheus.MustRegister(clusterMQTTPublishFromCluster)
	prometheus.MustRegister(clusterMQTTPublishToCluster)
	prometheus.MustRegister(clusterMQTTRetainedQueue)
	prometheus.MustRegister(retainedMessages)
	prometheus.MustRegister(subscriptions)
	prometheus.MustRegister(clientsConnected)
	prometheus.MustRegister(brokerUptime)
	prometheus.MustRegister(messagesRecv)
	prometheus.MustRegister(messagesSent)
	prometheus.MustRegister(publishDropped)
	prometheus.MustRegister(publishRecv)
	prometheus.MustRegister(publishSent)
	prometheus.MustRegister(inflight)

	go collect(opts.Discovery, opts.Broker)
}

// collect will refresh Prometheus collectors.
func collect(disco *discovery.Discovery, mqttServer *mqtt.Server) {
	log.Printf("starting prometheus worker")
	for {
		clusterMembers.Set(float64(len(disco.Members(true))))
		clusterMemberHealth.Set(float64(disco.GetHealthScore()))
		clusterMQTTPublishFromCluster.Set(float64(len(discovery.MQTTPublishFromCluster)))
		clusterMQTTPublishToCluster.Set(float64(len(discovery.MQTTPublishToCluster)))
		clusterMQTTRetainedQueue.Set(float64(len(discovery.MQTTSendRetained)))
		subscriptions.Set(float64(mqttServer.System.Subscriptions))
		clientsConnected.Set(float64(mqttServer.System.ClientsConnected))
		retainedMessages.Set(float64(len(mqttServer.Topics.Messages("#"))))
		brokerUptime.Set(float64(mqttServer.System.Uptime))
		messagesRecv.Set(float64(mqttServer.System.MessagesRecv))
		messagesSent.Set(float64(mqttServer.System.MessagesSent))
		publishDropped.Set(float64(mqttServer.System.PublishDropped))
		publishRecv.Set(float64(mqttServer.System.PublishRecv))
		publishSent.Set(float64(mqttServer.System.PublishSent))
		inflight.Set(float64(mqttServer.System.Inflight))

		time.Sleep(10 * time.Second)
	}
}
