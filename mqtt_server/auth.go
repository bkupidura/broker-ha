package mqtt_server

import (
	"log"
	"strings"
)

type Acl struct {
	Action string
	Prefix string
}

type Auth struct {
	Users   map[string]string
	UserAcl map[string][]Acl
}

func (a *Auth) Authenticate(user, password []byte) bool {
	u := string(user)
	if pass, ok := a.Users[u]; ok && pass == string(password) {
		log.Printf("user %s authenticated", u)
		return true
	}
	log.Printf("user %s unable to authenticate", u)
	return false
}

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
