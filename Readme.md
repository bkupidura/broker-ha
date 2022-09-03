## Overview

Broker-HA is golang MQTT broker with clustering capabilities build for K8s.
Its based on [mochi-co/mqtt](https://github.com/mochi-co/mqtt/) and [hashicorp/memberlist](https://github.com/hashicorp/memberlist).

Features:
- Paho MQTT 3.0 / 3.1.1 compatible (drop-in replacement for Mosquitto [MQTT 3.0/3.1.1])
- Clustering!
- Prometheus metrics
- Readiness/liveness endpoints

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
- No MQTT 5 support


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
  user:
    test: test
    admin: admin
  acl:
    admin:
      action: "allow"
      prefix: ""
    default:
      action: "deny"
      prefix: "/restricted"
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
  loadBalancerIP: "10.0.10.43"
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
      user:
        test: test
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
          initialDelaySeconds: 10
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

## HTTP Endpoints

- GET `:8080/metrics` - Prometheus metrics endpoint
- GET `:8080/ready` - Readiness probe endpoint
- GET `:8080/healthz` - Liveness probe endpoint
- POST `:8080/api/sse` - Get Server-Sent Events
- GET `:8080/api/discovery/members` - Get discovery (memberlist) members
- GET `:8080/api/mqtt/clients` - Get mqtt clients
- POST `:8080/api/mqtt/client/stop` - Stop (disconnect) mqtt client
- POST `:8080/api/mqtt/client/inflight` - Get inflight messages for mqtt client
- POST `:8080/api/mqtt/topic/messages` - Get mqtt messages for topic
- POST `:8080/api/mqtt/topic/subscribers` - Get mqtt subscribers for topic
