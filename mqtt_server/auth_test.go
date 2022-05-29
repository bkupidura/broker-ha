package mqtt_server

import (
	"bytes"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthenticate(t *testing.T) {
	tests := []struct {
		users          map[string]string
		user           string
		password       string
		expectedResult bool
	}{
		{
			user:           "test",
			password:       "test",
			expectedResult: false,
			users:          make(map[string]string),
		},
		{
			user:           "test",
			password:       "test",
			expectedResult: false,
			users: map[string]string{
				"test2": "test2",
			},
		},
		{
			user:           "test",
			password:       "test",
			expectedResult: true,
			users: map[string]string{
				"test2": "test2",
				"test":  "test",
			},
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		auth := &Auth{Users: test.users}
		require.Equal(t, test.expectedResult, auth.Authenticate([]byte(test.user), []byte(test.password)), fmt.Sprintf("failed test for %+v", test.users))
	}
}

func TestACL(t *testing.T) {
	tests := []struct {
		acl            map[string][]Acl
		user           string
		topic          string
		expectedResult bool
	}{
		{
			user:  "test",
			topic: "test",
			acl: map[string][]Acl{
				"test": []Acl{
					Acl{Action: "deny", Prefix: "test"},
				},
				"default": []Acl{
					Acl{Action: "allow", Prefix: "test"},
				},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]Acl{
				"default": []Acl{
					Acl{Action: "deny", Prefix: "test"},
				},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]Acl{
				"test": []Acl{
					Acl{Action: "allow", Prefix: "test"},
				},
				"default": []Acl{
					Acl{Action: "deny", Prefix: "test"},
				},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]Acl{
				"test": []Acl{
					Acl{Action: "allow", Prefix: ""},
				},
				"default": []Acl{
					Acl{Action: "deny", Prefix: "test"},
				},
			},
			expectedResult: true,
		},
		{
			user:           "test",
			topic:          "test",
			acl:            map[string][]Acl{},
			expectedResult: true,
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		auth := &Auth{UserAcl: test.acl}
		require.Equal(t, test.expectedResult, auth.ACL([]byte(test.user), test.topic, true), fmt.Sprintf("failed test for %+v", test.acl))
	}
}

func TestACLCheck(t *testing.T) {
	tests := []struct {
		acl            []Acl
		user           string
		topic          string
		expectedResult bool
	}{
		{
			user:  "test",
			topic: "test",
			acl: []Acl{
				Acl{Action: "deny", Prefix: "test"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test/something/something",
			acl: []Acl{
				Acl{Action: "deny", Prefix: "test"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "something/",
			acl: []Acl{
				Acl{Action: "deny", Prefix: "test"},
				Acl{Action: "deny", Prefix: "something/"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "something",
			acl: []Acl{
				Acl{Action: "deny", Prefix: "test"},
				Acl{Action: "deny", Prefix: "something/"},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "something",
			acl: []Acl{
				Acl{Action: "deny", Prefix: "test"},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "something",
			acl: []Acl{
				Acl{Action: "deny", Prefix: "test"},
				Acl{Action: "allow", Prefix: "something"},
			},
			expectedResult: true,
		},
		{
			user:           "test",
			topic:          "test",
			acl:            []Acl{},
			expectedResult: true,
		},
	}
	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		result := aclCheck(test.acl, test.user, test.topic)
		require.Equal(t, test.expectedResult, result, fmt.Sprintf("failed test for %+v", test.acl))
	}
}
