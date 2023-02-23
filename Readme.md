## Overview

Broker-HA is golang MQTT broker with clustering capabilities build for K8s.
Its based on [mochi-co/mqtt](https://github.com/mochi-co/mqtt/) and [hashicorp/memberlist](https://github.com/hashicorp/memberlist).

Features:
- Paho MQTT 3.0 / 3.1.1 / 5.0 compatible (drop-in replacement for Mosquitto [MQTT 3.0/3.1.1/5.0])
- Clustering!
- HTTP API

## Why

I was unable to find any (working) open-source MQTT broker which allows to run highly available setups.

## How it works

`broker-ha` will try to build cluster with other instances in the cluster.  
Initial cluster members are discovered with DNS SRV records.  

When new member join cluster, every other node from cluster will send `retained` messages to it.

When client will send MQTT message to any `broker-ha` instance, this instance will try to forward it to other cluster members.

DNS SRV discovery is only used to discover initial (after startup) cluster members. After that member join/leave is handled by `memberlist`.

## Limitations

- Member auto-discovery requires DNS SRV (e.g K8s headless service)


## Configuration

`broker-ha` is using [spf13/viper](https://github.com/spf13/viper) for config.

Config file should be located in `/config/config.yaml`.

```
api:
  user:
    admin: admin
discovery:
  domain: broker-headless.broker-ha.svc.cluster.local
  subscription_size:
    "cluster:message_to": 1024
mqtt:
  port: 1883
  subscription_size:
    "cluster:message_from": 1024
    "cluster:new_member": 10
  auth:
    - username: test
      password: test
      allow: true
    - client: someClientID
      allow: true
    - remote: 127.0.0.1
      allow: true
  acl:
    // 0 = deny, 1 = read only, 2 = write only, 3 = read and write
    - username: test
      filters:
        "a/#": 0
        "b/#": 2
cluster:
  expected_members: 3
  config:
    secret_key: "someSecretKey13$"
    tcp_timeout: 1000
    push_pull_interval: 15000
    probe_interval: 1000
    probe_timeout: 200
    gossip_interval: 100
    gossip_to_the_dead_time: 15000
    indirect_checks: 1
    retransmit_mult: 2
    suspicion_mult: 3
```

All `cluster.config` options are used to configure `memberlist`. For more details check [hashicorp/memberlist](https://github.com/hashicorp/memberlist/blob/master/config.go).

Every config key can be set with environment variable e.g `BROKER_MQTT_PORT`.

Default config:

```
discovery:
  subscription_size:
    "cluster:message_to": 1024
mqtt:
  port: 1883
  subscription_size:
    "cluster:message_from": 1024
    "cluster:new_member": 10
cluster:
  expected_members: 3
  config:
    probe_interval: 500
```

## K8s Manifest

`broker-headless` service is used to discover initial cluster members.

`mqtt` service is used to expose MQTT broker for clients.

```
---
apiVersion: v1
kind: Namespace
metadata:
  name: broker-ha

---
apiVersion: v1
kind: Service
metadata:
  name: broker-headless
  namespace: broker-ha
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: broker-ha
  ports:
    - protocol: TCP
      port: 7946
      targetPort: 7946

---
apiVersion: v1
kind: Service
metadata:
  name: mqtt
  namespace: broker-ha
  labels:
    app.kubernetes.io/name: mqtt
spec:
  type: LoadBalancer
  externalTrafficPolicy: Local
  publishNotReadyAddresses: false
  ports:
    - name: mqtt
      port: 1883
      protocol: TCP
  selector:
    app.kubernetes.io/name: broker-ha

---
apiVersion: v1
data:
  config: |
    discovery:
      domain: broker-headless.broker-ha.svc.cluster.local
    mqtt:
      port: 1883
      auth:
        - username: test
          password: test
          allow: true
    cluster:
      config:
        probe_interval: 500
        secret_key: "someSecretKey13$"
kind: ConfigMap
metadata:
  name: broker-ha
  namespace: broker-ha

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: broker-ha
  namespace: broker-ha
  labels:
    app.kubernetes.io/name: broker-ha
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app.kubernetes.io/name: broker-ha
  template:
    metadata:
      labels:
        app.kubernetes.io/name: broker-ha
      annotations:
        prometheus.io/port: "8080"
        prometheus.io/scrape: "true"
    spec:
      containers:
      - name: broker-ha
        image: ghcr.io/bkupidura/broker-ha:latest
        volumeMounts:
          - mountPath: /config
            name: config
        readinessProbe:
          failureThreshold: 2
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 2
          successThreshold: 1
          timeoutSeconds: 1
        livenessProbe:
          failureThreshold: 3
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 70
          periodSeconds: 5
          successThreshold: 1
          timeoutSeconds: 1
      terminationGracePeriodSeconds: 60
      dnsPolicy: ClusterFirstWithHostNet
      volumes:
        - name: config
          configMap:
            name: broker-ha
            items:
              - key: config
                path: config.yaml
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 1
              podAffinityTerm:
                labelSelector:
                  matchExpressions:
                    - key: app.kubernetes.io/name
                      operator: In
                      values:
                        - broker-ha
                topologyKey: kubernetes.io/hostname
```

## Event bus
`broker-ha` use internaly event bus (`internal/bus`) to communicate between components (`broker`, `discovery`, `api`).

Events are passed to subscribers over golang channel.

List of channels:
- `cluster:message_from` - event is generated when new MQTT message from cluster is received. Message will be published to local broker instance.
- `cluster:message_to` - event is generated when new MQTT message is published to broker. Message will be published to cluster for other instances.
- `cluster:new_member` - event is generated when new member joins cluster.

## HTTP

By default `/api` and `/debug` prefixed endpoints are available without authentication.
If you need authentication for those endpoints, add to `config.yaml` `api.user` key to enable HTTP basic auth.

```
api:
  user:
    username: password
```

All `/api` prefixed endpoints will return `200` on success, `400` on wrong request or `500` when request cant be executed.

Some `/api` endpoints are supporting `/proxy` prefix. This allow to send request to single `broker-ha` instance and get combined response from every node in the cluster.

### :8080/metrics endpoint

Expose Prometheus metrics.

cURL:
```
curl localhost:8080/metrics
```

Response:
```
# HELP broker_clients_connected Number of connected clients
# TYPE broker_clients_connected gauge
broker_clients_connected 0
# HELP broker_cluster_member_health Cluster member health
# TYPE broker_cluster_member_health gauge
broker_cluster_member_health 0
# HELP broker_cluster_members Number of cluster members
# TYPE broker_cluster_members gauge
broker_cluster_members 1
[...]
```

### :8080/ready endpoint

Expose K8s [readiness probe](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) endpoint.

cURL:
```
curl localhost:8080/ready
```

Response:
```
{"status":"up","details":{"readiness_cluster_health":{"status":"up","timestamp":"2022-09-04T08:08:27.650382Z"}}}
```

Response code:
- 200 when ready
- 503 when not ready

### :8080/healthz endpoint

Expose K8s [liveness probe](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) endpoint.

cURL:
```
curl localhost:8080/healthz
```

Response:
```
{"status":"up","details":{"liveness_cluster_discovered_members":{"status":"up","timestamp":"2022-09-04T08:11:14.993799Z"},"liveness_cluster_health":{"status":"up","timestamp":"2022-09-04T08:11:14.993785Z"},"liveness_member_in_cluster":{"status":"up","timestamp":"2022-09-04T08:11:14.993755Z"}}}
```

Response code:
- 200 when health
- 503 when not healthy

### :8080/debug endpoint

Expose [net/http/pprof](https://pkg.go.dev/net/http/pprof) endpoint.

cURL:
```
curl localhost:8080/debug
```

### :8080/api/sse endpoint

Expose Server-Sent Events stream.

cURL:
```
curl localhost:8080/api/sse -d '{"filters": []}' -H 'Content-Type: application/json'
```

Request params:
- filters - list of event bus channels we want to get messages from (default [], all channels)
- channel_size - size of subscription channel (how many messages should be buffered if they cant be delivered to SSE immediately) (default 100)

Response:
```
event: cluster:message_to
data: {"Node":["all"],"Payload":"MTY2MjI3OTQ4ODQyMg==","Topic":"dead-man-switch/mqtt-ping","Retain":false,"Qos":0}
event: sse:keepalive:[::1]:53398
data: "keepalive"
event: cluster:message_from
data: {"Payload":"MA==","Topic":"red/action/event","Retain":false,"Qos":0}
```

### :8080/api/discovery/members | :8080/proxy/api/discovery/members

Return discovery (memberlist) members.

cURL:
```
curl localhost:8080/api/discovery/members
```

Response:
```
[{"Name":"broker-ha-6ffc959cff-v5495","Addr":"10.42.2.242","Port":7946,"Meta":"","State":0,"PMin":1,"PMax":5,"PCur":2,"DMin":0,"DMax":0,"DCur":0},{"Name":"broker-ha-6ffc959cff-v62ct","Addr":"10.42.1.14","Port":7946,"Meta":"","State":0,"PMin":1,"PMax":5,"PCur":2,"DMin":0,"DMax":0,"DCur":0},{"Name":"broker-ha-6ffc959cff-mrzrk","Addr":"10.42.0.205","Port":7946,"Meta":"","State":0,"PMin":1,"PMax":5,"PCur":2,"DMin":0,"DMax":0,"DCur":0}]
```

### :8080/api/mqtt/clients | :8080/proxy/api/mqtt/clients

Return MQTT broker clients.

cURL:
```
curl localhost:8080/api/mqtt/clients
```

Response:
```
[{"ID":"cf1up11t673b49rllusg","ProtocolVersion":4,"Username":"recorder","CleanSession":true,"Done":false,"Subscriptions":{"recorder":{"ShareName":null,"Filter":"recorder","Identifier":0,"Identifiers":null,"RetainHandling":0,"Qos":2,"RetainAsPublished":false,"NoLocal":false}}}]
```

### :8080/api/mqtt/client/stop

Stop (disconnect) MQTT client.

cURL:
```
curl localhost:8080/api/mqtt/client/stop -d '{"client_id": "cf1up11t673b49rllusg"}' -H 'Content-Type: application/json'
```

Request params:
- client_id - MQTT client ID.

### :8080/api/mqtt/client/inflight | :8080/proxy/api/mqtt/client/inflight

Return MQTT in-flight messages for client.

cURL:
```
curl localhost:8080/api/mqtt/client/inflight -d '{"client_id": "akjnd9qjamz"}' -H 'Content-Type: application/json'
```

Request params:
- client_id - MQTT client ID.

Response:
```
[{"Connect":{"willProperties":{"cd":null,"si":null,"ad":null,"user":null,"ct":"","rt":"","aci":"","am":"","ri":"","sr":"","rs":"","me":0,"sei":0,"wdi":0,"mps":0,"ska":0,"rm":0,"tam":0,"ta":0,"pf":0,"fpf":false,"fsei":false,"fska":false,"rpi":0,"frpi":false,"rri":0,"fta":false,"mqos":0,"fmqos":false,"ra":0,"fra":false,"wsa":0,"fwsa":false,"sida":0,"fsida":false,"ssa":0,"fssa":false},"password":null,"username":null,"protocolName":null,"willPayload":null,"clientId":"","willTopic":"","keepalive":0,"passwordFlag":false,"usernameFlag":false,"willQos":0,"willFlag":false,"willRetain":false,"clean":false},"Properties":{"cd":null,"si":[0],"ad":null,"user":null,"ct":"","rt":"","aci":"","am":"","ri":"","sr":"","rs":"","me":0,"sei":0,"wdi":0,"mps":0,"ska":0,"rm":0,"tam":0,"ta":0,"pf":0,"fpf":false,"fsei":false,"fska":false,"rpi":0,"frpi":false,"rri":0,"fta":false,"mqos":0,"fmqos":false,"ra":0,"fra":false,"wsa":0,"fwsa":false,"sida":0,"fsida":false,"ssa":0,"fssa":false},"Payload":"dGVzdA==","ReasonCodes":null,"Filters":null,"TopicName":"TestMqttClientInflightHandler","Origin":"inline","FixedHeader":{"remaining":0,"type":3,"qos":2,"dup":false,"retain":true},"Created":1673806755,"Expiry":1673807355,"Mods":{"MaxSize":0,"DisallowProblemInfo":false,"AllowResponseInfo":false},"PacketID":1,"ProtocolVersion":4,"SessionPresent":false,"ReasonCode":0,"ReservedBit":0}]
```

### :8080/api/mqtt/topic/messages | :8080/proxy/api/mqtt/topic/messages

Return MQTT retained messages for topic.

cURL:
```
curl localhost:8080/api/mqtt/topic/messages -d '{"topic": "#"}' -H 'Content-Type: application/json'
```

Request params:
- topic - MQTT subscription topic (MQTT wildcards supported)

Response:
```
[{"Connect":{"willProperties":{"cd":null,"si":null,"ad":null,"user":null,"ct":"","rt":"","aci":"","am":"","ri":"","sr":"","rs":"","me":0,"sei":0,"wdi":0,"mps":0,"ska":0,"rm":0,"tam":0,"ta":0,"pf":0,"fpf":false,"fsei":false,"fska":false,"rpi":0,"frpi":false,"rri":0,"fta":false,"mqos":0,"fmqos":false,"ra":0,"fra":false,"wsa":0,"fwsa":false,"sida":0,"fsida":false,"ssa":0,"fssa":false},"password":null,"username":null,"protocolName":null,"willPayload":null,"clientId":"","willTopic":"","keepalive":0,"passwordFlag":false,"usernameFlag":false,"willQos":0,"willFlag":false,"willRetain":false,"clean":false},"Properties":{"cd":null,"si":null,"ad":null,"user":null,"ct":"","rt":"","aci":"","am":"","ri":"","sr":"","rs":"","me":0,"sei":0,"wdi":0,"mps":0,"ska":0,"rm":0,"tam":0,"ta":0,"pf":0,"fpf":false,"fsei":false,"fska":false,"rpi":0,"frpi":false,"rri":0,"fta":false,"mqos":0,"fmqos":false,"ra":0,"fra":false,"wsa":0,"fwsa":false,"sida":0,"fsida":false,"ssa":0,"fssa":false},"Payload":"dGVzdA==","ReasonCodes":null,"Filters":null,"TopicName":"TestMqttTopicMessagesHandler","Origin":"inline","FixedHeader":{"remaining":0,"type":3,"qos":0,"dup":false,"retain":true},"Created":1673825332,"Expiry":0,"Mods":{"MaxSize":0,"DisallowProblemInfo":false,"AllowResponseInfo":false},"PacketID":0,"ProtocolVersion":4,"SessionPresent":false,"ReasonCode":0,"ReservedBit":0}]
```

### :8080/api/mqtt/topic/subscribers | :8080/proxy/api/mqtt/topic/subscribers

Return MQTT client ID and QoS for subscription topic.

cURL:
```
curl localhost:8080/api/mqtt/topic/subscribers -d '{"topic": "something"}' -H 'Content-Type: application/json'
```

Request params:
- topic - MQTT subscription topic

Response:
```
{"Shared":{},"SharedSelected":{},"Subscriptions":{"cf1up11t673b49rllusg":{"ShareName":null,"Filter":"recorder","Identifier":0,"Identifiers":{"recorder":0},"RetainHandling":0,"Qos":2,"RetainAsPublished":false,"NoLocal":false}}}
```
