package api

import (
	"log"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter creates http router.
func NewRouter(opts *Options) *chi.Mux {
	httpRouter := chi.NewRouter()

	httpRouter.Group(func(r chi.Router) {
		r.Use(middleware.CleanPath)
		r.Use(middleware.Recoverer)
		r.Method("GET", "/ready", readyHandler(opts.Discovery))
		r.Method("GET", "/healthz", healthzHandler(opts.Discovery, opts.ClusterExpectedMembers))
		r.Method("GET", "/metrics", promhttp.Handler())
	})
	httpRouter.Group(func(r chi.Router) {
		r.Use(middleware.CleanPath)
		if len(opts.AuthUsers) > 0 {
			log.Printf("basic auth for API HTTP endpoint enabled")
			r.Use(middleware.BasicAuth("api", opts.AuthUsers))
		} else {
			log.Printf("basic auth for API HTTP endpoint disabled")
		}
		r.Use(middleware.Recoverer)

		r.Post("/api/sse", sseHandler(opts.Bus))
		r.Get("/api/discovery/members", discoveryMembersHandler(opts.Discovery))
		r.Get("/api/mqtt/clients", mqttClientsHandler(opts.Broker))
		r.Post("/api/mqtt/client/stop", mqttClientStopHandler(opts.Broker))
		r.Post("/api/mqtt/client/inflight", mqttClientInflightHandler(opts.Broker))
		r.Post("/api/mqtt/topic/messages", mqttTopicMessagesHandler(opts.Broker))
		r.Post("/api/mqtt/topic/subscribers", mqttTopicSubscribersHandler(opts.Broker))
	})

	return httpRouter
}
