# TurboStorm Load | Distributed HTTP, gRPC and WebSocket Load Testing Tool

TurboStorm Load is a distributed synthetic traffic generator and load-testing orchestrator written in Go. It is designed to generate configurable HTTP, gRPC, and WebSocket workloads, coordinate multiple workers, and help developers evaluate service behavior under controlled traffic. Use it against systems you own or are authorized to test, and report benchmark results with the environment and workload settings.


<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=0,2,24,45&height=220&section=header&text=TurboStorm%20Load&fontSize=68&fontAlignY=38&desc=Ultra-High-Throughput%20Distributed%20Traffic%20Generator%20%7C%20HTTP/gRPC/WebSocket%20%7C%20HDR%20Histogram&descAlignY=58&descAlign=50" width="100%"/>
</p>

<p align="center">
  <a href="https://github.com/ER-brocode/turbostorm-load/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/ER-brocode/turbostorm-load/ci.yml?branch=main&style=for-the-badge&logo=githubactions&logoColor=white&label=CI%20Build" alt="CI Status" />
  </a>
  <a href="https://golang.org/">
    <img src="https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go Version" />
  </a>
  <a href="https://github.com/ER-brocode/turbostorm-load/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge" alt="License" />
  </a>
  <a href="https://github.com/ER-brocode/turbostorm-load/stargazers">
    <img src="https://img.shields.io/badge/RPS-10M%2B%20Distributed-critical?style=for-the-badge&logo=speedtest&logoColor=white" alt="Capacity" />
  </a>
</p>

---

## 🚀 Key Highlights

* **Multi-Protocol Engine**: Native high-throughput clients for **HTTP/1.1**, **HTTP/2 multiplexing**, RFC 6455 **WebSocket**, and **gRPC** unary calls.
* **HDR Latency Percentiles**: Microsecond-precision **High Dynamic Range (HDR) Histogram** recording latency metrics from **p50**, **p90**, **p95**, **p99**, **p99.9** up to **p99.99**.
* **Distributed Synchronization**: Clustered master-follower coordination with wall-clock synchronized $T_0$ traffic ramp-ups.
* **Complex Traffic Profiles**: Linear ramps, sinusoidal oscillations, Poisson burst arrivals, and abrupt stress spikes.
* **Zero External Dependencies**: Pure Go standard-library implementation designed for bare-metal performance with zero allocation overhead in inner loops.

---

## 📐 Architecture & Cluster Topology

```mermaid
flowchart TD
    subgraph ControlPlane ["Distributed Control Plane"]
        Master["Master Coordinator (:9090)"]
        Plan["Traffic Ramp Planner"]
        Master --> Plan
    end

    subgraph ClusterFollowers ["Distributed Worker Fleet"]
        Node1["Follower Node 1<br/>(Worker Pool)"]
        Node2["Follower Node 2<br/>(Worker Pool)"]
        NodeN["Follower Node N<br/>(Worker Pool)"]
    end

    subgraph TargetService ["Target System Under Test (SUT)"]
        GW["API Gateway / Ingress"]
        HTTP["HTTP/REST API"]
        GRPC["gRPC Microservices"]
        WS["WebSocket Gateways"]
        GW --> HTTP
        GW --> GRPC
        GW --> WS
    end

    Plan -. Synchronized T0 Dispatch .-> Node1
    Plan -. Synchronized T0 Dispatch .-> Node2
    Plan -. Synchronized T0 Dispatch .-> NodeN

    Node1 ==>|HTTP / gRPC / WS| GW
    Node2 ==>|HTTP / gRPC / WS| GW
    NodeN ==>|HTTP / gRPC / WS| GW

    Node1 -. Heartbeat & Metrics .-> Master
    Node2 -. Heartbeat & Metrics .-> Master
    NodeN -. Heartbeat & Metrics .-> Master
```

---

## 📊 Benchmark Latency Report Preview

```text
========================================================================================
                     TURBOSTORM-LOAD PERFORMANCE BENCHMARK REPORT                      
========================================================================================
 Duration:          30.00s
 Total Requests:    300,000
 Successful:        299,985
 Errors:            15 (0.00%)
 Throughput:        10000.00 req/s (42.80 MB/s)
----------------------------------------------------------------------------------------
 LATENCY PERCENTILES (Microseconds & Milliseconds):
   Min:                  210 us  (    0.21 ms)
   Mean:                 840 us  (    0.84 ms)
   p50 (Median):         710 us  (    0.71 ms)
   p90:                 1120 us  (    1.12 ms)
   p95:                 1430 us  (    1.43 ms)
   p99:                 2350 us  (    2.35 ms)
   p99.9:               4890 us  (    4.89 ms)
   p99.99:              9120 us  (    9.12 ms)
   Max:                14250 us  (   14.25 ms)
----------------------------------------------------------------------------------------
 HTTP STATUS DISTRIBUTION:
   [200]: 299,985 requests
   [503]: 15 requests
========================================================================================
```

---

## 🛠️ Quickstart

### 1. Standalone Traffic Generation

```bash
# Build binary
go build -o turbostorm ./cmd/turbostorm

# Run a 30s ramp test up to 10,000 RPS
./turbostorm -mode=standalone \
  -url="http://127.0.0.1:8080/api/v1/resource" \
  -proto=http1 \
  -pattern=ramp \
  -initial-rps=500 \
  -rps=10000 \
  -ramp=10s \
  -duration=30s \
  -c=200
```

### 2. Distributed Cluster Mode

#### Launch Master Coordinator
```bash
./turbostorm -mode=master -listen=":9090"
```

#### Launch Follower Workers
```bash
# Worker 1
./turbostorm -mode=follower -id="node-eu-west-1" -listen=":9091" -master="127.0.0.1:9090"

# Worker 2
./turbostorm -mode=follower -id="node-us-east-1" -listen=":9092" -master="127.0.0.1:9090"
```

---

## 🧪 Automated Testing

TurboStorm Load includes comprehensive automated tests covering:
- **HDR histogram percentile calculations**: Validates mathematical distribution accuracy and dynamic range scaling against standard sorting benchmarks.
- **Traffic ramp curves**: Tests linear ramps, step functions, Poisson arrival calculations, and sinusoid schedules.
- **End-to-end synthetic traffic generation**: Full in-memory client-server loop with connection pooling and metrics verification.

```bash
go test -v -race ./...
```

---

## 📜 License

Apache License 2.0. Copyright (c) 2026 ER-brocode.
