package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"net/http"

	"broker/discovery"

	"github.com/alexliesenfeld/health"
)

// readinessProbe returns health.Checker used by /ready endpoint.
// It checks discovery (memberlist) healthscore.
func readinessProbe(disco *discovery.Discovery, initialSleep time.Duration) health.Checker {
	readinessProbe := health.NewChecker(
		health.WithCacheDuration(1*time.Second),
		health.WithTimeout(10*time.Second),
		health.WithPeriodicCheck(1*time.Second, initialSleep*time.Second, health.Check{
			Name:    "readiness_cluster_health",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				healthScore := disco.GetHealthScore()
				if healthScore > 0 {
					return errors.New(fmt.Sprintf("health score %d", healthScore))
				}
				return nil
			},
		}),
	)
	return readinessProbe
}

// livenessProbe returns health.Checker used by /healthz endpoint.
// It checks discovery (memberlist) healthscore.
// It checks discovery (memberlist) members count.
// This check will be usefull when whole cluster will be restarted and all new ha-broker pods will sleep for same `random` interval.
// In this case K8s should restart PODs which dosent have any members (except self).
func livenessProbe(disco *discovery.Discovery, initialSleep time.Duration, expectedMembers int) health.Checker {
	livenessProbe := health.NewChecker(
		health.WithCacheDuration(1*time.Second),
		health.WithTimeout(10*time.Second),
		health.WithPeriodicCheck(3*time.Second, (initialSleep+5)*time.Second, health.Check{
			Name:    "liveness_cluster_health",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				healthScore := disco.GetHealthScore()
				if healthScore > 0 {
					return errors.New(fmt.Sprintf("health score %d", healthScore))
				}
				return nil
			},
		}),
		health.WithPeriodicCheck(3*time.Second, (initialSleep+5)*time.Second, health.Check{
			Name:    "liveness_cluster_discovered_members",
			Timeout: 1 * time.Second,
			Check: func(ctx context.Context) error {
				// Safe guard in case whole cluster shutdown
				discoveredMembers := len(disco.Members(true))
				if discoveredMembers == 1 && expectedMembers > 1 {
					return errors.New(fmt.Sprintf("not enough discovered members %d", discoveredMembers))
				}
				return nil
			},
		}),
	)
	return livenessProbe
}

// failedCheckLogger is http middleware which will log when any healthcheck (readiness/liveness) is not healthy.
// It will be executed only on http request.
func failedCheckLogger() health.Middleware {
	return func(next health.MiddlewareFunc) health.MiddlewareFunc {
		return func(r *http.Request) health.CheckerResult {
			result := next(r)
			for k, v := range *result.Details {
				if v.Status != health.StatusUp {
					log.Printf("health check %s is in %s state", k, v.Status)
				}
			}
			return result
		}
	}
}
