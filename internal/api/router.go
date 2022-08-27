package api

import (
	"log"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	mqtt "github.com/mochi-co/mqtt/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"brokerha/internal/discovery"
)

// NewRouter creates http router.
func NewRouter(disco *discovery.Discovery, mqttServer *mqtt.Server, clusterExpectedMembers int, apiUser map[string]string) *chi.Mux {

	httpRouter := chi.NewRouter()

	httpRouter.Group(func(r chi.Router) {
		r.Use(middleware.CleanPath)
		r.Use(middleware.Recoverer)
		r.Method("GET", "/ready", readyHandler(disco))
		r.Method("GET", "/healthz", healthzHandler(disco, clusterExpectedMembers))
		r.Method("GET", "/metrics", promhttp.Handler())
	})
	httpRouter.Group(func(r chi.Router) {
		r.Use(middleware.CleanPath)
		if len(apiUser) > 0 {
			log.Printf("basic auth for API HTTP endpoint enabled")
			r.Use(middleware.BasicAuth("api", apiUser))
		} else {
			log.Printf("basic auth for API HTTP endpoint disabled")
		}
		r.Use(middleware.Recoverer)

		r.Get("/api/discovery/members", discoveryMembersHandler(disco))
		r.Get("/api/mqtt/clients", mqttClientsHandler(mqttServer))
		r.Post("/api/mqtt/client/stop", mqttClientStopHandler(mqttServer))
		r.Post("/api/mqtt/client/inflight", mqttClientInflightHandler(mqttServer))
		r.Post("/api/mqtt/topic/messages", mqttTopicMessagesHandler(mqttServer))
		r.Post("/api/mqtt/topic/subscribers", mqttTopicSubscribersHandler(mqttServer))
	})

	return httpRouter
}
