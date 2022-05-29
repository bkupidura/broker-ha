package mqtt_server

import (
	"log"
	"strings"
)

// Acl describe ACL rule used by mqtt broker to decide if user have permissions to given topic.
// Currently it supports two Actions - deny, allow.
// Currently it supports only one check - Prefix
//
// Prefix will check if requested topics starts with given string.
type Acl struct {
	Action string
	Prefix string
}

// Auth stores users and user acls.
// Users are provided as map[string]string{"username": "password", "username2": "password2"}.
// UserAcl are provided as map[string][]Acl{"username": []Acl{&Acl{Action: "deny", Prefix: "/"}}}.
type Auth struct {
	Users   map[string]string
	UserAcl map[string][]Acl
}

// Authenticate user.
// Just basic comparision if user is known and plaintext password is valid.
func (a *Auth) Authenticate(user, password []byte) bool {
	u := string(user)
	if pass, ok := a.Users[u]; ok && pass == string(password) {
		log.Printf("user %s authenticated", u)
		return true
	}
	log.Printf("user %s unable to authenticate", u)
	return false
}

// Check if user is allowed for given topic.
// Write parameter is currently ignored.
// If there is no acl for given user, special "default" ACL will be checked.
func (a *Auth) ACL(user []byte, topic string, write bool) bool {
	u := string(user)
	if rules, ok := a.UserAcl[u]; ok {
		return aclCheck(rules, u, topic)
	}
	if rules, ok := a.UserAcl["default"]; ok {
		return aclCheck(rules, u, topic)
	}
	return true
}

// Go thru all ACLs and check if user is allowed or not.
func aclCheck(acl []Acl, user, topic string) bool {
	for _, r := range acl {
		if strings.HasPrefix(topic, r.Prefix) {
			switch r.Action {
			case "deny":
				log.Printf("user %s not allowed for topic %s based on prefix ACL", user, topic)
				return false
			case "allow":
				return true
			}
		}
	}
	return true
}
