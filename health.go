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
