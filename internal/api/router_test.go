package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"brokerha/internal/broker"
	"brokerha/internal/bus"
	"brokerha/internal/discovery"
)

func TestNewRouter(t *testing.T) {
	tests := []struct {
		inputMethod        string
		inputPath          string
		inputAuth          [2]string
		inputctxCancelFunc func(func())
		expectedCode       int
		auth               map[string]string
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
			inputMethod:  http.MethodPost,
			inputPath:    "/api/sse",
			expectedCode: http.StatusOK,
			inputctxCancelFunc: func(ctxCancel func()) {
				go func() {
					time.Sleep(10 * time.Millisecond)
					ctxCancel()
				}()
			},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/api/sse",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
			inputctxCancelFunc: func(ctxCancel func()) {
				go func() {
					time.Sleep(10 * time.Millisecond)
					ctxCancel()
				}()
			},
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
			inputPath:    "/proxy/api/discovery/members",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/proxy/api/discovery/members",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/proxy/api/discovery/members",
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
			inputMethod:  http.MethodGet,
			inputPath:    "/proxy/api/mqtt/clients",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/proxy/api/mqtt/clients",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodGet,
			inputPath:    "/proxy/api/mqtt/clients",
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
			inputPath:    "/proxy/api/mqtt/client/inflight",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/proxy/api/mqtt/client/inflight",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/proxy/api/mqtt/client/inflight",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusOK,
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
			inputPath:    "/proxy/api/mqtt/topic/messages",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/proxy/api/mqtt/topic/messages",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/proxy/api/mqtt/topic/messages",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusOK,
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
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/proxy/api/mqtt/topic/subscribers",
			expectedCode: http.StatusOK,
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/proxy/api/mqtt/topic/subscribers",
			expectedCode: http.StatusUnauthorized,
			auth:         map[string]string{"test": "test"},
		},
		{
			inputMethod:  http.MethodPost,
			inputPath:    "/proxy/api/mqtt/topic/subscribers",
			inputAuth:    [2]string{"test", "test"},
			expectedCode: http.StatusOK,
			auth:         map[string]string{"test": "test"},
		},
	}

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = "node2"
	mlConfig.BindAddr = "127.0.0.1"
	mlConfig.ProbeInterval = 10
	mlConfig.LogOutput = ioutil.Discard

	evBus := bus.New()
	disco, discoCtxCancel, err := discovery.New(&discovery.Options{
		Domain:           "test",
		MemberListConfig: mlConfig,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_to": 1024},
	})
	require.Nil(t, err)
	defer discoCtxCancel()
	defer disco.Shutdown()

	b, brokerCtxCancel, err := broker.New(&broker.Options{
		MQTTPort:         1883,
		Bus:              evBus,
		SubscriptionSize: map[string]int{"cluster:message_from": 1024, "cluster:new_member": 10},
	})
	defer brokerCtxCancel()
	defer b.Shutdown()
	require.Nil(t, err)

	for _, test := range tests {
		var req *http.Request
		var err error

		body, _ := json.Marshal(map[string]interface{}{})

		if test.inputctxCancelFunc != nil {
			ctx, reqCtxCancel := context.WithCancel(context.Background())
			req, err = http.NewRequestWithContext(ctx, test.inputMethod, test.inputPath, bytes.NewReader(body))
			require.Nil(t, err)

			test.inputctxCancelFunc(reqCtxCancel)
		} else {
			req, err = http.NewRequest(test.inputMethod, test.inputPath, bytes.NewReader(body))
			require.Nil(t, err)
		}

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
			Bus:                    evBus,
		})

		httpServer := &http.Server{Addr: fmt.Sprintf(":%d", HTTPPort), Handler: router}
		go httpServer.ListenAndServe()
		defer httpServer.Shutdown(context.TODO())

		router.ServeHTTP(w, req)

		require.Equal(t, test.expectedCode, w.Code)
	}
}
