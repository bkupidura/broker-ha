package api

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
)

func TestNewRouter(t *testing.T) {
	tests := []struct {
		inputMethod  string
		inputPath    string
		inputAuth    [2]string
		expectedCode int
		auth         map[string]string
	}{
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/ready",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/ready",
			expectedCode: http.StatusOK,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/healthz",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/healthz",
			expectedCode: http.StatusOK,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/metrics",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/metrics",
			expectedCode: http.StatusOK,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/api/discovery/members",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/api/discovery/members",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/api/discovery/members",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusOK,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/api/mqtt/clients",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/api/mqtt/clients",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/api/mqtt/clients",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusOK,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/client/stop",
			expectedCode: http.StatusBadRequest,
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/client/stop",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/client/stop",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusBadRequest,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/client/inflight",
			expectedCode: http.StatusBadRequest,
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/client/inflight",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/client/inflight",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusBadRequest,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/topic/messages",
			expectedCode: http.StatusBadRequest,
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/topic/messages",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/topic/messages",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusBadRequest,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/topic/subscribers",
			expectedCode: http.StatusBadRequest,
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/topic/subscribers",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/mqtt/topic/subscribers",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusBadRequest,
			auth:         map[string]string{"test": "test"},
		},
	}

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = "node2"
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.ProbeInterval = 10
	mlConfig.LogOutput = ioutil.Discard

	evBus := bus.New()
	disco, ctxCancel, err := discovery.New(&discovery.Options{
		Domain:           "test",
		MemberListConfig: mlConfig,
		Bus:              evBus,
	})
	require.Nil(t, err)
	defer ctxCancel()
	defer disco.Shutdown()

	b, ctxCancel, err := broker.New(&broker.Options{
		MQTTPort: 1883,
		Bus:      evBus,
	})
	defer ctxCancel()
	defer b.Shutdown()
	require.Nil(t, err)

	for _, test := range tests {
		req := httptest.NewRequest(test.inputMethod, test.inputPath, nil)
		req.Header.Set("Content-Type", "application/json")
		if len(test.inputAuth) > 0 {
			req.SetBasicAuth(test.inputAuth[0], test.inputAuth[1])
		}
		w := httptest.NewRecorder()

		router := NewRouter(&Options{
			Discovery:              disco,
			Broker:                 b,
			ClusterExpectedMembers: 1,
			AuthUsers:              test.auth,
		})

		router.ServeHTTP(w, req)

		require.Equal(t, test.expectedCode, w.Code)
	}
}
