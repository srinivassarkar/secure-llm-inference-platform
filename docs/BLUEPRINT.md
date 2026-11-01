# Secure LLM Inference Platform: Technical Specification & Blueprint

**Repository:** `https://github.com/srinivassarkar/secure-llm-inference-platform`  
**Author:** Srinivas Sarkar (`randomtechy@SrinivasSarkar`)  
**Methodology:** Measured, Derived, Borrowed (M/D/B) Verification Framework. Strictly zero unmeasured claims.

---

## 1. System Vision & The Core Enterprise Dilemma

Modern enterprise adoption of Large Language Models (LLMs) in regulated sectors (FinTech, healthcare, legal, and defense) is deadlocked between two opposing architectural forces:

1. **The Confidentiality Boundary:** Standard Kubernetes inference deployments route plaintext user prompts (containing PII, confidential medical histories, or proprietary financial transactions) through shared reverse proxies, sidecars, and cluster storage. CISOs require absolute guarantees that prompts cannot be intercepted in transit, snooped via container volume mounts, or leaked into persistent storage.
2. **The Throughput & Autoscaling Boundary:** Standard Kubernetes Horizontal Pod Autoscalers (HPA) rely on CPU or GPU memory utilization. Because modern inference engines (such as vLLM) statically pre-allocate 90%+ of GPU VRAM at boot for PagedAttention KV caches, GPU memory utilization alone is a poor replica-scaling signal. Meanwhile, scaling reactively on queue depth after slots fill results in severe cold-start latency cliffs (exceeding 30-50 seconds) that breach Time-to-First-Token (TTFT) SLOs.

### The Architectural Solution
The **Secure LLM Inference Platform** bridges this divide by delivering:
- An **Ephemeral Ingress Gateway** that decrypts authenticated application-layer encrypted payloads (X25519 + AES-256-GCM) in managed tmpfs buffers, streams encrypted tokens back with cancellation propagation, and emits Ed25519-signed verification receipts.
- A **Hardware-Accelerated Inference Fleet** running **vLLM** with continuous batching and PagedAttention on NVIDIA GPUs.
- An **Autonomous Autoscaling Plane** powered by **KEDA** and **Prometheus**, comparing reactive queue depth, capacity saturation, and predictive demand models in isolated experiments.

---

## 2. End-to-End System Architecture

```
                                  [k6 Open-Loop Traffic Injector]
                                                |
                 (Ciphertext: Authenticated Ephemeral X25519 + AES-256-GCM Payload)
                                                v
====================================================================================================
 PLANE 1: CONFIDENTIAL INGRESS DATA PLANE (The Citadel Gateway)
====================================================================================================
 [Citadel Ephemeral Ingress Gateway: Python AsyncIO]
   ├── Authenticated Handshake (Gateway Ed25519 identity certificate + ephemeral X25519 key agreement)
   ├── Bidirectional Payload Encryption (Inbound prompt + Outbound SSE streaming chunks via AES-256-GCM)
   ├── Audited Memory Boundary (Managed tmpfs /dev/shm buffers; in-place wiping; mlock swap protection)
   ├── Cancellation Propagation (Client TCP close -> immediate upstream socket termination -> vLLM abort)
   ├── Admission Control & Overload Protection (Bounded queue, concurrency limits, 503 shedding)
   └── Cryptographic Audit Logger (Ed25519-signed receipts containing transaction digest)
                                                |
                       (Plaintext HTTP Stream over private Pod network)
                                                v
====================================================================================================
 PLANE 2: ACCELERATED INFERENCE PLANE (vLLM on CUDA)
====================================================================================================
 [Kubernetes Pod Fleet: vLLM Inference Engine]
   ├── Continuous Batching Scheduler (Iteration-level prefill & decode interleaving)
   ├── PagedAttention Memory Manager (Reduces external fragmentation & KV allocation waste)
   ├── Quantized Weight Runtime (FP16 vs AWQ / GGUF model formats)
   └── Upstream Engine Exposition (Port 8000/metrics: running, waiting, cache usage)
                                                |
                       (Scraped by Prometheus ServiceMonitor every 5s)
                                                v
====================================================================================================
 PLANE 3: AUTOSCALING & SRE CONTROL PLANE (KEDA & Prometheus)
====================================================================================================
 [Prometheus Operator (kube-prometheus-stack)]
   ├── Scrapes Gateway (:9090/metrics: TTFT & TPOT histograms, disconnects, audit status)
   ├── Scrapes vLLM (:8000/metrics: num_requests_waiting, num_requests_running, gpu_cache_usage_perc)
   └── Computes Saturation & Demand Signals
                                                |
 [KEDA Operator (ScaledObject - 3 Isolated Experimental Arms)]
   ├── Arm 1: Reactive Queue Depth Trigger (waiting > 1)
   ├── Arm 2: Capacity Saturation Ratio ((running + waiting) / total_capacity > 0.75)
   └── Arm 3: Predictive Demand Model ((running + waiting + lambda * T_p) / total_capacity > 0.75)
                                                |
 [Kubernetes Scheduler & Kubelet Engine]
   └── Orchestrates pod scale-out (1 -> 2 -> 4) under real model cold-start latencies
   └── Lifecycle Phase Overlay: Pending -> ContainerCreating -> Running -> Ready
```

---

## 3. Detailed Plane Specifications

### 3.1 Plane 1: Confidential Ingress Data Plane (Citadel Gateway)

The Ingress Gateway is engineered in Python AsyncIO to provide an OpenAI-compatible `/v1/chat/completions` interface while enforcing strict zero-trust data governance.

#### A. Cryptographic Protocol Specification & Key Management

1. **Gateway Identity & Certificate Binding:**
   - The Gateway possesses a long-term Ed25519 root identity keypair (`id_ed25519`). The public key is distributed to clients out-of-band via trusted TLS bootstrap configuration.
   - The Gateway generates an ephemeral X25519 session keypair (`sk_gw`, `pk_gw`).
   - The Gateway publishes an authenticated key manifest:
     ```json
     {
       "gateway_id": "citadel-gateway-node-01",
       "key_id": "key-20261101-0400",
       "x25519_public_key": "base64-encoded-x25519-pubkey",
       "valid_from": 1730438400,
       "expires_at": 1730442000,
       "supported_ciphers": ["X25519-HKDF-AES256GCM"],
       "manifest_nonce": "16-byte-unique-nonce",
       "signature": "base64-ed25519-signature-over-canonical-manifest"
     }
     ```
   - Clients verify the Ed25519 signature before initiating key agreement, preventing Man-In-The-Middle (MITM) key substitution.

2. **Key Rotation & Lifecycle:**
   - Ephemeral session keys rotate hourly. During rotation, the Gateway retains the previous keypair for a 5-minute drain grace window to process in-flight requests tagged with the previous `key_id`.

3. **Key Agreement & Derivation:**
   - The client generates an ephemeral Curve25519 keypair (`sk_client`, `pk_client`).
   - Shared secret is computed via ECDH: $Z = \text{X25519}(sk_{\mathrm{client}}, pk_{\mathrm{gw}})$.
   - Symmetric keys are derived using HKDF-SHA256 binding the handshake transcript into `info`:
     - $\text{PRK} = \text{HKDF-Extract}(\text{salt}, Z)$
     - $K_{\mathrm{in}} = \text{HKDF-Expand}(\text{PRK}, \text{"citadel-inbound-v1"} \parallel \text{key\_id}, 32)$
     - $K_{\mathrm{out}} = \text{HKDF-Expand}(\text{PRK}, \text{"citadel-outbound-v1"} \parallel \text{key\_id}, 32)$

4. **Bidirectional Confidentiality (Inbound Request + Outbound Stream):**
   - **Inbound Request:** The client encrypts the JSON prompt payload using AES-256-GCM with key $K_{\mathrm{in}}$, binding `pk_client`, `timestamp`, and `key_id` into the Associated Authenticated Data (AAD):
     ```json
     {
       "key_id": "key-20261101-0400",
       "ephemeral_public_key": "base64-pk_client",
       "nonce": "12-byte-base64-nonce",
       "ciphertext": "base64-aes-gcm-ciphertext",
       "tag": "16-byte-auth-tag"
     }
     ```
   - **Outbound Stream:** For bidirectional confidentiality, the Gateway encrypts SSE streaming chunks using AES-256-GCM with key $K_{\mathrm{out}}$. Each streaming chunk carries a monotonic sequence number in its AAD, protecting against chunk dropping, reordering, or injection.

5. **Nonce Uniqueness & Replay Defense:**
   - The Gateway maintains an in-memory sliding-window TTL cache of observed nonces.
   - Requests with duplicate nonces or timestamps drifting beyond 60 seconds are rejected immediately with `401 Unauthorized`.

6. **Governing Zero-Persistence Scope & Memory Handling:**
   - *Governing Statement:* The gateway implements a best-effort, audited zero-persistence execution boundary for explicitly managed plaintext buffers. The audit verifies defined process, filesystem, swap, logging and container-layer conditions under a defined host threat model. It does not establish universal erasure of every plaintext copy across the host, kernel, runtime, or hardware.
   - *Memory Wiping:* Decryption buffers are allocated in tmpfs (`/dev/shm`) using mutable `bytearray` or `ctypes.create_string_buffer` and zeroized in-place via `ctypes.memset` immediately after dispatch.
   - *Swap Protection:* `mlock` is engaged on sensitive memory pools (subject to OS `RLIMIT_MEMLOCK` limits) to prevent page eviction to swap.
   - *Security Auditing:* Automated verification scripts inspect swap partitions, `/tmp`, `/var/log`, core dumps, and container writable layers under a defined test harness.
   - *Receipt Attestation:* Returns an Ed25519-signed receipt (`X-Citadel-Audit-Receipt`). Ed25519 authenticates the Gateway's attestation of its internal checks and transaction digest; it does not independently prove physical hardware memory state.
     ```json
     {
       "timestamp_epoch_ms": 1730438400123,
       "request_hash": "sha256-hash-of-ciphertext",
       "gateway_node": "citadel-gateway-node-01",
       "zero_disk_verified": true,
       "signature": "base64-ed25519-signature"
     }
     ```

#### B. Request Lifecycle & Cancellation Propagation

The Gateway manages an explicit request lifecycle:
```text
[Client Connect]
      │
      ▼
REQUEST_ACCEPTED ──► AUTHENTICATE_DECRYPT ──► ADMISSION_CHECK
                                                     │
                             ┌───────────────────────┴───────────────────────┐
                             ▼ (Capacity Available)                          ▼ (Queue Full)
                         ENQUEUED                                       REJECTED_503
                             │
                             ▼
                      VLLM_DISPATCH
                             │
                             ▼
                         STREAMING
                             │
        ┌────────────────────┴────────────────────┐
        ▼ (Stream Complete)                       ▼ (Client Socket Drop)
    COMPLETED                                 ABORTED (Upstream Terminated)
        │                                         │
        ▼                                         ▼
[Signed Audit Receipt]                    [vLLM GPU Loops Freed]
```

- **Cancellation Mechanism:** The Gateway detects client disconnection (`connection_lost` or ASGI disconnect), terminates the upstream streaming request to vLLM, and verifies whether the deployed vLLM version aborts the corresponding generation and releases its KV-cache allocations. Cancellation propagation and resource-release latency are measured experimentally.
- **Mid-Stream Timeout Semantics:** If an SLA or client timeout occurs before token generation initiates, the gateway returns HTTP 408/504; if the SSE chunk stream has already begun emitting tokens, the gateway terminates the chunk stream immediately rather than attempting an invalid mid-stream HTTP status header.
- **Empirical Hypothesis:** We hypothesize that socket-drop cancellation reduces wasted GPU decode tokens under client churn. This benefit will be measured explicitly in Phase 3 benchmarks comparing wasted tokens with and without propagation.

#### C. Overload Protection & Gateway Admission Control
To prevent the Ingress Gateway itself from becoming a queue-bloated failure domain:
- **Bounded Request Queue:** `asyncio.Queue(maxsize=128)` sheds excess requests with `503 Service Unavailable` and `Retry-After: 5`.
- **Concurrency Limiting:** Enforces maximum concurrent in-flight streams tuned to cluster capacity.
- **Client Request Deadlines:** Aborts in-flight requests that exceed SLA deadlines (e.g., 60 seconds without first token).

#### D. Drain-Aware SSE Streaming & Slow Consumers
- **Backpressure Enforcement:** The Gateway uses ASGI asynchronous streaming responses, awaiting socket drain between chunk transmissions to prevent memory buffer bloat.
- **Slow Consumer Handling:** Slow consumers unable to keep up with model token generation are buffered up to a bounded threshold (configurable 64 KiB test hypothesis). Exceeding this boundary terminates the streaming response, logs `slow_consumer`, and closes the upstream connection to abort GPU compute. (No invalid HTTP 408 is attempted on an already-open streaming connection).

---

#### D. Network Isolation & Response Confidentiality Boundary
Plaintext prompts travel strictly across the private Kubernetes pod overlay network, isolated via Kubernetes `NetworkPolicies` that drop all non-gateway ingress to vLLM.
- **Confidentiality Scope:** The current protocol protects client-to-gateway request payloads. Gateway-to-client response confidentiality is provided by the configured transport security unless a separate application-layer response-encryption mechanism is implemented.
- **Hardware Boundary:** This Application-Layer Confidential Ingress architecture guarantees zero-persistence and authenticated ingress under the audited software stack; it does not substitute for hardware-enforced confidential computing enclaves (such as AMD SEV-SNP or Intel TDX).

### 3.2 Plane 2: Accelerated Inference Plane (vLLM Engine)

#### A. KV Cache Memory Mathematical Specification
The theoretical payload memory consumption for an autoregressive sequence is defined by first principles:
\[
M_{\mathrm{KV}} = 2 \times L \times H_{\mathrm{kv}} \times D \times S \times B
\]
Where:
- \(L\): Number of transformer layers.
- \(H_{\mathrm{kv}}\): Number of Key-Value heads (accounting for Grouped-Query Attention).
- \(D\): Head dimension (\(d_{\mathrm{model}} / H_q\)).
- \(S\): Number of cached sequence tokens.
- \(B\): Bytes per element (2 bytes for FP16/BF16, 1 byte for FP8).
- Factor \(2\): Separate Key and Value tensors.

For \(N\) concurrent active sequences, total theoretical VRAM payload is \(M_{\mathrm{total}} = \sum_{i=1}^{N} M_{\mathrm{KV}}^{(i)}\). Actual engine memory allocation includes PagedAttention block table indexing overhead and block-size rounding (typically 16 or 32 tokens per block).

#### B. Architecture & Serving Engine
The inference fleet is powered by **vLLM** hosted on NVIDIA GPU hardware (L4, A10G or RTX 4090):
- **PagedAttention:** Paged memory management reducing external fragmentation and allocation waste (attributed to Kwon et al., vLLM 2023).
- **Continuous Batching:** Dynamic iteration-level scheduling combining arriving prefill requests with active decode iterations.
- **Engine Metrics Exposition:** Metrics verified against pinned vLLM release (v0.6+):
  - `vllm:num_requests_running`: Currently active requests occupying GPU slots.
  - `vllm:num_requests_waiting`: Requests waiting in the internal scheduler queue.
  - `vllm:gpu_cache_usage_perc`: Ratio of allocated KV cache blocks (percentage 0.0 - 1.0).

---

### 3.3 Plane 3: Autoscaling & SRE Control Plane (KEDA & Prometheus)

#### A. The Autoscaling Dilemma: Memory & Queue Limitations
1. **GPU Memory is Invariant:** vLLM pre-allocates 90% of VRAM on boot for PagedAttention KV blocks. Memory utilization is always 90%+, rendering GPU memory HPA a poor replica-scaling signal.
2. **CPU Utilization is Uncorrelated:** The CPU orchestrates batching iterations; it can saturate while the GPU is under-utilized or stay idle while the GPU memory bus is pegged.
3. **Queue Depth is Reactive:** Scaling when `num_requests_waiting > 0` triggers scaling only *after* requests are already waiting in queue. Given pod startup latency, TTFT immediately collapses.

#### B. Three Independent Experimental Arms (Isolated KEDA Configurations)
To ensure rigorous experimental attribution, the three scaling strategies are evaluated in **separate, isolated `ScaledObject` deployments** against identical workloads:

##### Arm 1: Reactive Queue-Depth Baseline (`deploy/keda/arm1-queue-depth.yaml`)
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: vllm-autoscaler-arm1
  namespace: secure-llm
spec:
  scaleTargetRef:
    name: vllm-worker
  minReplicaCount: 1
  maxReplicaCount: 4
  pollingInterval: 5
  cooldownPeriod: 120
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090
        metricName: vllm_requests_waiting_total
        query: sum(vllm:num_requests_waiting)
        threshold: "1"
```

* **Slot Capacity Definition:**
  Slots Per Replica (\(S_{\mathrm{replica}}\)) is determined by the engine's pre-allocated KV-cache memory divided by the maximum context sequence length (\(M_{\mathrm{KV\_alloc}} / S_{\mathrm{max}}\)), or calibrated by peak concurrent throughput before decode latency degrades beyond target SLOs. For homogeneous GPU clusters (e.g., uniform NVIDIA L4 24GB instances), capacity is uniform: \(\text{Total Slots} = N_{\mathrm{ready}} \times S_{\mathrm{replica}}\). In heterogeneous deployments, total capacity is evaluated as the sum of individual ready replica allocations: \(\sum_{i=1}^{N_{\mathrm{ready}}} S_i\).

##### Arm 2: Capacity Saturation Ratio (`deploy/keda/arm2-capacity-saturation.yaml`)
Evaluates active load plus pending queue divided by total ready capacity:
\[
S_{\mathrm{capacity}} = \frac{\sum \text{requests\_running} + \sum \text{requests\_waiting}}{\text{Ready Replicas} \times \text{Slots Per Replica}}
\]
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: vllm-autoscaler-arm2
  namespace: secure-llm
spec:
  scaleTargetRef:
    name: vllm-worker
  minReplicaCount: 1
  maxReplicaCount: 4
  pollingInterval: 5
  cooldownPeriod: 120
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090
        metricName: vllm_capacity_saturation_ratio
        query: |
          (sum(vllm:num_requests_running) + sum(vllm:num_requests_waiting))
          / (clamp_min(sum(kube_deployment_status_replicas_ready{deployment="vllm-worker"}), 1) * 16)
        threshold: "0.75"
```

##### Arm 3: Predictive Demand Model (`deploy/keda/arm3-predictive-demand.yaml`)
Hypothesis model incorporating incoming request arrival rate (\(\lambda_{\mathrm{in}}\)) and estimated provisioning latency (\(T_p\)):
\[
S_{\mathrm{predicted}} = \frac{R + W + (\lambda_{\mathrm{in}} \times T_p)}{C}
\]
* **Implementation Note:** The predictive saturation signal will initially be computed as a Prometheus recording rule or gateway-exported metric (`rate(gateway_requests_total[30s])` combined with estimated provisioning latency \(T_p\)). KEDA consumes that metric through its Prometheus scaler. The baseline and predictive policies run as separate experimental configurations (`deploy/keda/arm2-capacity-saturation.yaml` and `deploy/keda/arm3-predictive-demand.yaml`), not simultaneous triggers in one `ScaledObject`.
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: vllm-autoscaler-arm3
  namespace: secure-llm
spec:
  scaleTargetRef:
    name: vllm-worker
  minReplicaCount: 1
  maxReplicaCount: 4
  pollingInterval: 5
  cooldownPeriod: 120
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090
        metricName: vllm_predictive_demand_ratio
        query: |
          (
            sum(vllm:num_requests_running) + sum(vllm:num_requests_waiting)
            + (sum(rate(gateway_requests_total[1m])) * 35)
          ) / (clamp_min(sum(kube_deployment_status_replicas_ready{deployment="vllm-worker"}), 1) * 16)
        threshold: "0.75"
```

#### C. Estimated Provisioning Latency — Sequential Upper-Bound Model
Derived sequential delay breakdown under stated assumptions (Pre-warmed GPU Node Baseline):
```
[Traffic Spike Occurs]
  │
  ├── 1. Metric Scrape Interval (Prometheus):                 1.0 - 5.0 s
  ├── 2. KEDA Polling Interval (PromQL Evaluation):           5.0 s
  ├── 3. HPA Reconcile Sync Period:                          15.0 s
  ├── 4. Kube-Scheduler Node Filter & Binding:                1.5 s
  ├── 5. Container Runtime Init & Image Overlay:              2.0 s
  ├── 6. Model Weights Page-in (Storage to Host to VRAM):     4.0 - 8.0 s
  └── 7. Engine Warmup & First KV Allocation:                 2.0 s
  │
  └── Total Derived Sequential Upper-Bound:                  30.5 - 38.5 s
```
*(Note: Actual Kubernetes operations may partially overlap. True elapsed provisioning time will be measured explicitly from pod lifecycle timestamps: KEDA trigger timestamp, HPA desired replica change, pod creation, pod scheduled, container started, and vLLM readiness probe passed. If a cold cloud GPU node must be provisioned via Cluster Autoscaler, add 90 - 180 s).*

---

## 4. Multi-Burn-Rate SRE Alerting & Verification Gates

Following Google SRE multi-burn-rate best practices, alerts calculate true error budget consumption rather than raw compliance percentages.

### 4.1 Service Level Objectives (SLOs) & Security Release Gates
- **TTFT SLO:** 95% of requests must receive their first streaming token in $\le 350\ \mathrm{ms}$ ($P_{95}$).
- **TPOT SLO:** 99% of generated tokens must exhibit an inter-token latency of $\le 35\ \mathrm{ms}$ ($> 28.5\ \text{tok/s}$).
- **Security Release Gate (Binary):** All cryptographic verification tests, zero-persistence audit scripts, and replay rejections must pass 100% in CI before deployment (treated as a deployment-blocking gate, not an elastic SLO).

### 4.2 True 14.4x Multi-Burn-Rate Alerting Rules (`prometheus_rules.yml`)
For a 95% SLO, the error budget fraction is $1 - 0.95 = 0.05$. A 14.4x burn rate means the error ratio exceeds $14.4 \times 0.05 = 0.72$ (72% of requests breaching SLO):

```yaml
groups:
  - name: citadel_llm_slo_alerts
    rules:
      # Critical: 14.4x Burn Rate (Exhausts 100% of 30-day budget in 2 days)
      - alert: LLMTTFTSLOCriticalBurn
        expr: |
          (
            (1 - (sum(rate(gateway_ttft_seconds_bucket{le="0.350"}[5m])) / clamp_min(sum(rate(gateway_ttft_seconds_count[5m])), 1))) / 0.05 > 14.4
          ) and (
            (1 - (sum(rate(gateway_ttft_seconds_bucket{le="0.350"}[1h])) / clamp_min(sum(rate(gateway_ttft_seconds_count[1h])), 1))) / 0.05 > 14.4
          )
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "TTFT SLO error budget is burning at 14.4x in both 5m and 1h windows."

      # Warning: Capacity Pressure Signal
      - alert: LLMKVCachePressure
        expr: vllm:gpu_cache_usage_perc > 0.85
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "vLLM KV cache block allocation exceeds 85%. Request queueing or preemption imminent."
```

---

## 5. Phased Implementation Roadmap & Verification Gates

Every phase must produce a verifiable, reproducible artifact before the subsequent phase commences:

| Phase | Engineering Deliverable | Exit Evidence & Artifact |
| :--- | :--- | :--- |
| **Phase 1: Inference Baseline** | vLLM Serving + Client Streaming | Working vLLM endpoint, streaming client, raw baseline TTFT/TPOT CSV sweep. |
| **Phase 2: Cryptographic Gateway** | Citadel Ingress Gateway (X25519, AES-GCM) | Handshake test vectors, manifest validation, bidirectional encryption tests, replay rejection logs. |
| **Phase 3: Lifecycle & Cancellation** | Admission Control + Cancellation | Queue overflow 503 test, bounded memory test, disconnect-to-abort latency measurement CSV. |
| **Phase 4: Security Auditing** | Zero-Persistence Auditing | Swap/core-dump/logging/container-layer audit scripts and pass logs under defined threat model. |
| **Phase 5: Observability** | Prometheus Metrics & Grafana Dashboard | Live TTFT/TPOT histograms, saturation metrics, and verified 14.4x burn-rate alert fires. |
| **Phase 6: Kubernetes & Autoscaling** | KEDA Autoscaler (Arms 1, 2, 3) | 3 isolated ScaledObject experiments, pod lifecycle event logs, measured scale-out latency waterfall. |
| **Phase 7: Chaos & Benchmarks** | Fault Injection & k6 Open-Loop Tests | Open-loop concurrency cliff graph, failure injection TTD/MTTR matrix, blameless postmortem. |

---

## 6. Definition of Done (Strict Engineering Gates)

1. **One-Command Reproducibility:** `make up` brings up the local environment; `make test` runs the verification suite; `make results` regenerates all charts.
2. **Attribution Standard:** Every performance claim is tagged **[M]** Measured, **[D]** Derived, or **[B]** Borrowed with source CSV/JSON artifacts in `experiments/results/`.
3. **Independent Comparison:** Scaling arms are evaluated in isolated runs with identical starting conditions.
4. **Transparent Boundaries:** The difference between application-layer confidential ingress and hardware enclaves is explicitly documented.
