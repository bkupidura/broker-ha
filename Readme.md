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
discovery:
  domain: broker-headless.broker-ha.svc.cluster.local
mqtt:
  port: 1883
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
  secret_key: "someSecretKey13$"
```

Every config key can be set with environment variable e.g `BROKER_CLUSTER_SECRET_KEY`.

## K8s Manifest

`broker-headless` service is used to discover initial cluster members.

`mqtt` service is used to expose MQTT broker for clients.

```
---
apiVersion: v1
kind: Namespace
metadata:
  name: ha-broker

---
apiVersion: v1
kind: Service
metadata:
  name: broker-headless
  namespace: ha-broker
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: ha-broker
  ports:
    - protocol: TCP
      port: 7946
      targetPort: 7946

---
apiVersion: v1
kind: Service
metadata:
  name: mqtt
  namespace: ha-broker
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
    app.kubernetes.io/name: ha-broker

---
apiVersion: v1
data:
  config: |
    discovery:
      domain: broker-headless.ha-broker.svc.cluster.local
    mqtt:
      port: 1883
      user:
        test: test
    cluster:
      secret_key: "someSecretKey13$"
kind: ConfigMap
metadata:
  name: ha-broker
  namespace: ha-broker

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ha-broker
  namespace: ha-broker
  labels:
    app.kubernetes.io/name: ha-broker
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app.kubernetes.io/name: ha-broker
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ha-broker
      annotations:
        prometheus.io/port: "8080"
        prometheus.io/scrape: "true"
    spec:
      containers:
      - name: ha-broker
        image: ghcr.io/bkupidura/broker-ha:latest
        volumeMounts:
          - mountPath: /config
            name: config
        readinessProbe:
          failureThreshold: 2
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
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
      terminationGracePeriodSeconds: 3
      dnsPolicy: ClusterFirstWithHostNet
      volumes:
        - name: config
          configMap:
            name: ha-broker
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
                        - ha-broker
                topologyKey: kubernetes.io/hostname
```

## HTTP Endpoints

- `:8080/metrics` - Prometheus metrics endpoint
- `:8080/ready` - Readiness probe endpoint
- `:8080/healthz` - Liveness probe endpoint
