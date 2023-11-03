package api

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alexliesenfeld/health"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"brokerha/internal/bus"
	"brokerha/internal/discovery"
)

func TestReadyHandler(t *testing.T) {
	c1 := memberlist.DefaultLocalConfig()
	c1.BindAddr = "127.0.0.1"
	c1.BindPort = 7947
	c1.AdvertisePort = 7947
	c1.Name = "node1"
	c1.LogOutput = ioutil.Discard
	m1, err := memberlist.Create(c1)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m1.Shutdown()

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = "node2"
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.ProbeInterval = 10
	mlConfig.LogOutput = ioutil.Discard

	evBus := bus.New()
	disco, _, err := discovery.New(&discovery.Options{
		Domain:           "test",
		MemberListConfig: mlConfig,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_to": 1024},
	})
	if err != nil {
		t.Fatalf("discovery.New error: %s", err)
	}
	defer disco.Shutdown()

	_, err = disco.Join([]string{"127.0.0.1:7947"})
	if err != nil {
		t.Fatalf("disco.Join error: %s", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	handler := readyHandler(disco)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	m1.Shutdown()
	time.Sleep(2 * time.Second)

	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestHealthzHandler(t *testing.T) {
	c1 := memberlist.DefaultLocalConfig()
	c1.BindAddr = "127.0.0.1"
	c1.Name = "node2"
	c1.BindPort = 7947
	c1.AdvertisePort = 7947
	c1.LogOutput = ioutil.Discard
	m1, err := memberlist.Create(c1)
	if err != nil {
		t.Fatalf("memberlist.Create error: %s", err)
	}
	defer m1.Shutdown()

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.BindPort = 7946
	mlConfig.AdvertisePort = 7946
	mlConfig.LogOutput = ioutil.Discard

	evBus := bus.New()
	disco, _, err := discovery.New(&discovery.Options{
		Domain:           "test",
		MemberListConfig: mlConfig,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_to": 1024},
	})
	if err != nil {
		t.Fatalf("discovery.New error: %s", err)
	}
	defer disco.Shutdown()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler := healthzHandler(disco, 3)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	res := w.Result()
	defer res.Body.Close()
	healthResult := getHealthResult(res.Body)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, health.StatusDown, healthResult.Details["liveness_cluster_discovered_members"].Status)
	require.Equal(t, health.StatusUp, healthResult.Details["liveness_cluster_health"].Status)
	require.Equal(t, health.StatusUp, healthResult.Details["liveness_member_in_cluster"].Status)

	_, err = disco.Join([]string{"127.0.0.1:7947"})
	if err != nil {
		t.Fatalf("disco.Join error: %s", err)
	}

	time.Sleep(1 * time.Second)

	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	m1.Shutdown()
	time.Sleep(3 * time.Second)

	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	res = w.Result()
	defer res.Body.Close()
	healthResult = getHealthResult(res.Body)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, health.StatusDown, healthResult.Details["liveness_cluster_health"].Status)
	require.Equal(t, health.StatusUp, healthResult.Details["liveness_cluster_discovered_members"].Status)
	require.Equal(t, health.StatusUp, healthResult.Details["liveness_member_in_cluster"].Status)

	disco.Leave(100)
	time.Sleep(1 * time.Second)

	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	res = w.Result()
	defer res.Body.Close()
	healthResult = getHealthResult(res.Body)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, health.StatusDown, healthResult.Details["liveness_member_in_cluster"].Status)
}

func getHealthResult(body io.Reader) *health.CheckerResult {
	b, err := io.ReadAll(body)
	if err != nil {
		panic("unable to read body")
	}
	healthResult := &health.CheckerResult{}
	err = json.Unmarshal(b, healthResult)
	if err != nil {
		panic("unable to unmarshal body")
	}
	return healthResult
}
