package broker

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
		acl            map[string][]ACL
		user           string
		topic          string
		expectedResult bool
	}{
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"test": {
					{Action: "deny", Prefix: "test"},
				},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"test": {
					{Action: "allow", Prefix: "test"},
				},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"test": {
					{Action: "deny", Prefix: "test"},
				},
				"default": {
					{Action: "allow", Prefix: "test"},
				},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"test": {
					{Action: "allow", Prefix: "test"},
				},
				"default": {
					{Action: "deny", Prefix: "test"},
				},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"user": {
					{Action: "allow", Prefix: "test"},
				},
				"default": {
					{Action: "deny", Prefix: "test"},
				},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"user": {
					{Action: "deny", Prefix: "test"},
				},
				"default": {
					{Action: "allow", Prefix: "test"},
				},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"default": {
					{Action: "allow", Prefix: "test"},
				},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "test",
			acl: map[string][]ACL{
				"default": {
					{Action: "deny", Prefix: "test"},
				},
			},
			expectedResult: false,
		},
		{
			user:           "test",
			topic:          "test",
			acl:            map[string][]ACL{},
			expectedResult: true,
		},
	}

	log.SetFlags(0)
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)

	for _, test := range tests {
		auth := &Auth{UserACL: test.acl}
		require.Equal(t, test.expectedResult, auth.ACL([]byte(test.user), test.topic, true), fmt.Sprintf("failed test for %+v", test.acl))
	}
}

func TestACLCheck(t *testing.T) {
	tests := []struct {
		acl            []ACL
		user           string
		topic          string
		expectedResult bool
	}{
		{
			user:  "test",
			topic: "test",
			acl: []ACL{
				{Action: "deny", Prefix: "test"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test/something/something",
			acl: []ACL{
				{Action: "deny", Prefix: "test"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test/something/something",
			acl: []ACL{
				{Action: "deny", Prefix: ""},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "test/something/something",
			acl: []ACL{
				{Action: "allow", Prefix: ""},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "something/",
			acl: []ACL{
				{Action: "allow", Prefix: "test"},
				{Action: "deny", Prefix: "something/"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "something",
			acl: []ACL{
				{Action: "allow", Prefix: "test"},
				{Action: "deny", Prefix: "something/"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "something",
			acl: []ACL{
				{Action: "deny", Prefix: "test"},
			},
			expectedResult: false,
		},
		{
			user:  "test",
			topic: "something",
			acl: []ACL{
				{Action: "deny", Prefix: "test"},
				{Action: "allow", Prefix: "something"},
			},
			expectedResult: true,
		},
		{
			user:  "test",
			topic: "something/test",
			acl: []ACL{
				{Action: "deny", Prefix: "test"},
				{Action: "allow", Prefix: "something"},
			},
			expectedResult: true,
		},
		{
			user:           "test",
			topic:          "test",
			acl:            []ACL{},
			expectedResult: false,
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
