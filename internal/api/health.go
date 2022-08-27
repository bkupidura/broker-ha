package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/alexliesenfeld/health"

	"brokerha/internal/discovery"
)

// readyHandler returns handler with health.Checker used by /ready endpoint.
// It checks discovery (memberlist) healthscore.
func readyHandler(disco *discovery.Discovery) http.Handler {
	readinessProbe := health.NewChecker(
		health.WithCacheDuration(1*time.Second),
		health.WithTimeout(10*time.Second),
		health.WithCheck(health.Check{
			Name:    "readiness_cluster_health",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				healthScore := disco.GetHealthScore()
				if healthScore > 0 {
					return fmt.Errorf("health score %d", healthScore)
				}
				return nil
			},
		}),
	)
	return health.NewHandler(readinessProbe)
}

// healthzHandler returns handler with health.Checker used by /healthz endpoint.
// It checks discovery (memberlist) healthscore.
// It checks discovery (memberlist) members count.
// It checks if member is part of the cluster.
func healthzHandler(disco *discovery.Discovery, expectedMembers int) http.Handler {
	livenessProbe := health.NewChecker(
		health.WithCacheDuration(1*time.Second),
		health.WithTimeout(10*time.Second),
		health.WithCheck(health.Check{
			Name:    "liveness_cluster_health",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				healthScore := disco.GetHealthScore()
				if healthScore > 0 {
					return fmt.Errorf("health score %d", healthScore)
				}
				return nil
			},
		}),
		health.WithCheck(health.Check{
			Name:    "liveness_cluster_discovered_members",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				// Safe guard in case whole cluster shutdown
				discoveredMembers := len(disco.Members(true))
				if discoveredMembers == 1 && expectedMembers > 1 {
					return fmt.Errorf("not enough discovered members %d", discoveredMembers)
				}
				return nil
			},
		}),
		health.WithCheck(health.Check{
			Name:    "liveness_member_in_cluster",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				discoveredMembersWithSelf := len(disco.Members(true))
				discoveredMembersWithoutSelf := len(disco.Members(false))
				if discoveredMembersWithSelf == discoveredMembersWithoutSelf {
					return fmt.Errorf("we are not part of the cluster")
				}
				return nil
			},
		}),
	)
	return health.NewHandler(livenessProbe)
}
