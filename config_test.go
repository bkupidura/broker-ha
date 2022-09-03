package main

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestCreateMemberlistConfig(t *testing.T) {
	tests := []struct {
		config               []byte
		expectedMLConfigFunc func() *memberlist.Config
	}{
		{
			config: []byte(`
                tcp_timeout: 100
                push_pull_interval: 15000
                probe_interval: 200
                probe_timeout: 200
                gossip_interval: 100
                gossip_to_the_dead_time: 300
                indirect_checks: 10
                retransmit_mult: 10
                suspicion_mult: 10
                secret_key: abc
                `),
			expectedMLConfigFunc: func() *memberlist.Config {
				mlConfig := memberlist.DefaultLocalConfig()
				mlConfig.TCPTimeout = 100 * time.Millisecond
				mlConfig.PushPullInterval = 15 * time.Second
				mlConfig.ProbeInterval = 200 * time.Millisecond
				mlConfig.ProbeTimeout = 200 * time.Millisecond
				mlConfig.GossipInterval = 100 * time.Millisecond
				mlConfig.GossipToTheDeadTime = 300 * time.Millisecond
				mlConfig.IndirectChecks = 10
				mlConfig.RetransmitMult = 10
				mlConfig.SuspicionMult = 10
				mlConfig.SecretKey = []byte("abc")
				return mlConfig
			},
		},
		{
			config: []byte(`
                secret_key: abc
                `),
			expectedMLConfigFunc: func() *memberlist.Config {
				mlConfig := memberlist.DefaultLocalConfig()
				mlConfig.SecretKey = []byte("abc")
				return mlConfig
			},
		},
	}

	for _, test := range tests {
		c := viper.New()
		c.SetConfigType("yaml")
		c.ReadConfig(bytes.NewBuffer(test.config))

		mlConfig := createMemberlistConfig(c)

		expectedMLConfig := test.expectedMLConfigFunc()
		require.Equal(t, expectedMLConfig, mlConfig)
	}
}

func TestGetConfig(t *testing.T) {
	tests := []struct {
		inputEnv           map[string]string
		expectedErr        error
		expectedConfigFunc func() *viper.Viper
	}{
		{
			expectedErr: errors.New("missing required config key: discovery.domain"),
		},
		{
			inputEnv: map[string]string{"BROKER_DISCOVERY_DOMAIN": "test"},
			expectedConfigFunc: func() *viper.Viper {
				c := viper.New()
				d := []byte(`
                mqtt:
                  port: 1883
                  subscription_size:
                    "cluster:message_from": 1024
                    "cluster:new_member": 10
                discovery:
                  subscription_size:
                    "cluster:message_to": 1024
                cluster:
                  expected_members: 3
                  config:
                    probe_interval: 500
                `)
				c.SetConfigType("yaml")
				c.ReadConfig(bytes.NewBuffer(d))
				return c
			},
		},
	}

	for _, test := range tests {
		for k, v := range test.inputEnv {
			os.Setenv(k, v)
		}

		c, err := getConfig()

		require.Equal(t, test.expectedErr, err)

		if test.expectedConfigFunc != nil {
			expectedConfig := test.expectedConfigFunc()
			require.Equal(t, expectedConfig.AllSettings(), c.AllSettings())
		}

		os.Clearenv()
	}
}
