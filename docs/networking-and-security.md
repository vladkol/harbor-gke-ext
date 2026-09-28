# Networking and security

Running AI agent benchmarks requires executing arbitrary, untrusted code safely at scale while honoring each task's exact reachability rules. `harbor-gke-ext` addresses this at two levels:

1. **Cluster and runtime sandboxing (Kubernetes, GKE Standard & Autopilot, gVisor):** Every trial executes inside an ephemeral, single-use Pod with `automountServiceAccountToken: false`, zero `hostPath` or host runtime socket mounts, GKE admission enforcement, and optional or capability-triggered **gVisor** (`runtimeClassName: gvisor`) user-space kernel isolation.
2. **Controlled network and credential isolation:** Harbor's task-level network modes (`no-network`, `allowlist`, `public`) and `--allow-agent-host` controls map directly to Kubernetes `NetworkPolicy` and GKE Dataplane V2 `FQDNNetworkPolicy` resources, while default metadata server blocking (`169.254.169.254` and `fd00:170::2`) prevents agents from exfiltrating node IAM or Workload Identity credentials.

## Network modes

The environment creates network policies for a trial based on one of three supported network modes: `no-network`, `allowlist`, and `public`. 

### `no-network`
This mode installs a single `NetworkPolicy` with empty ingress and egress. This results in a deny-all egress policy. **DNS traffic is not excepted.** Workloads cannot resolve external hostnames.

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: harbor-netpol-<job-name>
  labels:
    session: <session_label>
    harbor.eval/session: <session_id>
spec:
  podSelector:
    matchLabels:
      session: <session_label>
  policyTypes:
    - Egress
  egress: []
```

### `allowlist`
This mode creates a `NetworkPolicy` for IP-based rules and, if the cluster supports it and hostnames are specified, an `FQDNNetworkPolicy` for hostname-based rules. The baseline `NetworkPolicy` is written unconditionally: if the allowlist is completely empty (and `allow_metadata_server=False`), it emits `egress: []` (without a DNS rule) and fails closed.

To configure the allowlist, a task author sets `allowed_hosts` in `task.toml`:
```toml
[environment]
network_mode = "allowlist"
allowed_hosts = [
  "198.51.100.0/24",  # Requires capability network_allowlist_ipv4_cidrs
  "203.0.113.10",     # Requires capability network_allowlist_ipv4_addresses
  "api.github.com",   # Requires capability network_allowlist_hostnames
  "*.google.com"      # Requires capability network_allowlist_wildcard_hostnames
]
```

The GKE environment declares the IPv4 CIDR and IPv4 address capabilities unconditionally, and the hostname and wildcard hostname capabilities only when `FQDNNetworkPolicy` support is detected or forced with `--ek enable_fqdn_network_policy=true` (see [FQDN capability detection](#fqdn-capability-detection)). It doesn't declare IPv6 allowlist capabilities, so Harbor's capability check rejects tasks with IPv6 entries.

When evaluating a non-empty allowlist:
- The base `NetworkPolicy` includes `ipBlock` peers for every IPv4 CIDR and IPv4 address (a bare address becomes `<address>/32`), plus a unified DNS egress rule (UDP/TCP port 53). If `allow_metadata_server=True`, it also appends an explicit `169.254.169.254/32` TCP port 80 egress rule.
- The `FQDNNetworkPolicy` includes `matches` blocks for exact hostnames and wildcard patterns.

#### Unified port-53 DNS egress rule

In `allowlist` (when non-empty) and `public` (metadata-blocked) modes, `_dns_egress_rule()` emits a single UDP/TCP port-53 egress rule that covers every standard GKE DNS resolver (`kube-dns`, `NodeLocal DNSCache`, `Cloud DNS for GKE`, and nested DinD containers) without requiring extra control-plane probes:

| Allowed Port-53 Peer | Purpose |
|---|---|
| `namespaceSelector: {kubernetes.io/metadata.name: kube-system}` | In-cluster `kube-dns` / `NodeLocal DNSCache` Pods |
| `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` | RFC 1918 VPC / cluster / Docker bridge DNS |
| `169.254.0.0/16` | Link-local GKE `NodeLocal DNSCache` (`169.254.20.10`) & Cloud DNS (`169.254.169.254` port 53) |
| `100.64.0.0/10` | RFC 6598 Shared / CGNAT range used in non-RFC1918 GKE Pod/Service allocations |
| `34.118.224.0/20` | GKE default reserved ClusterIP Service CIDR (where `kube-dns` ClusterIP e.g. `34.118.224.10` lives) |
| `8.8.8.8/32`, `8.8.4.4/32` | Google Public DNS fallback injected by `dockerd` (`libnetwork`) inside DinD nested containers |

If your cluster routes DNS queries to custom forwarders or VPC resolvers outside these ranges, pass `--ek dns_egress_extra_cidrs=<cidr>[,<cidr>...]` to append additional `ipBlock` peers to the port-53 rule.

**Example `NetworkPolicy` (IP allowlist + port-53 DNS egress):**
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: harbor-netpol-<job-name>
  # ... labels ...
spec:
  podSelector:
    matchLabels:
      session: <session_label>
  policyTypes:
    - Egress
  egress:
    - to:
        - ipBlock:
            cidr: 198.51.100.0/24
    - to:
        - ipBlock:
            cidr: 203.0.113.10/32
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
        - ipBlock:
            cidr: 10.0.0.0/8
        # ... plus default_dns_cidrs ...
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
```

**Example `FQDNNetworkPolicy` (Hostnames):**
```yaml
apiVersion: networking.gke.io/v1alpha1
kind: FQDNNetworkPolicy
metadata:
  name: harbor-fqdn-<job-name>
  # ... labels ...
spec:
  podSelector:
    matchLabels:
      session: <session_label>
  egress:
    - matches:
        - name: api.github.com
        - pattern: "*.google.com"
```

#### FQDN capability detection
Harbor checks whether the GKE cluster supports `FQDNNetworkPolicy` (which requires GKE Datapath V2 / Cilium) through `KubernetesClientManager.is_fqdn_network_policy_supported()`, which queries API discovery (`/apis/networking.gke.io/v1alpha1`) for the `fqdnnetworkpolicies` resource. The result is cached per cluster. You can reset this cache programmatically using `reset_fqdn_cache()`, or override detection with `--ek enable_fqdn_network_policy=true|false`.

If a task requests hostname allowlisting on a cluster without `FQDNNetworkPolicy` support, **nothing silently degrades**. The deployment fails with an error: either a `ValueError` during Harbor's capability check, or a `RuntimeError` from `apply_network_policy()` when the policy is applied (for example, if the cached probe result is stale). Harbor deliberately avoids dropping unsupportable rules to prevent unexpected fail-closed behavior at runtime.

### `public`
This mode grants unrestricted egress to the internet. 

By default (`allow_metadata_server=False`), the environment creates a `NetworkPolicy` before Job creation that permits all IPv4 (`0.0.0.0/0` except `169.254.169.254/32`) and IPv6 (`::/0` except `fd00:170::2/128`) traffic while blocking the GKE metadata server on non-DNS ports, accompanied by the unified DNS egress rule on port 53.

If you set `allow_metadata_server=True`, **no `NetworkPolicy` is created at start** (and if switched to `public` with `allow_metadata_server=True` at runtime via `_apply_network_policy()`, any existing `harbor-netpol-<job-name>` and `harbor-fqdn-<job-name>` objects are deleted). Without the metadata server carve-out, the required policy is identical to the Kubernetes namespace default, leaving ingress and egress open to namespace defaults unless the cluster enforces a separate policy.

## The metadata server boundary

`allow_metadata_server` defaults to `False` across all network modes. There is no implicit auto-enablement—allowlisting `*.googleapis.com` or other Google domains does not open access to `169.254.169.254`; you must explicitly set `--ek allow_metadata_server=true` (or `allow_metadata_server = true` in `task.toml`) when a workload requires metadata server access.

Blocking the metadata server (`169.254.169.254` and `fd00:170::2`) prevents untrusted agent code from querying the GKE node's service account credentials or Workload Identity Federation (WIF) tokens. Note that in DinD shapes (`Shape B` and `Shape C`), `dind-engine` runs in the same Pod network namespace: when `NetworkPolicy` blocks `169.254.169.254:80` (such as in `no-network` mode or `public`/`allowlist` with `allow_metadata_server=False`), `dind-engine`'s metadata token fetch fails gracefully and `dind-cache-<service>` automatically falls back from `docker pull` to `tar -cf - / | docker import` if the registry requires authentication.

## Policy lifecycle, naming stability, and settlement

NetworkPolicy and FQDNNetworkPolicy objects are named deterministically using the trial's stable Job name (`policy_key = self.job_name` -> `harbor-netpol-<job-name>` / `harbor-fqdn-<job-name>`):

- **At start-up (before Job creation):** When `network_mode != PUBLIC` or `allow_metadata_server=False`, the initial policies are created *before* the Job spawns its Pod so egress enforcement is active from the first initContainer. Because the Pod does not exist yet (`pod_uid` is unknown), start-time policies are created **without `metadata.ownerReferences`**.
- **Runtime updates (`_apply_network_policy()`):** When Harbor core updates the network policy mid-trial on a live Pod (where `pod_name` and `pod_uid` are known), the existing policy object (`harbor-netpol-<job-name>`) is replaced in place with a Pod `ownerReference` attached, and (if the Pod is already Ready) the environment sleeps for `--ek network_policy_settlement_sec` (default `2.0` s) to allow the GKE dataplane to program the new rules.
- **Deletion:** Policies are deleted explicitly when the environment calls `stop(delete=True)` (and by Kubernetes garbage collection if a runtime update attached a Pod `ownerReference`).

## Container security contexts and Service Accounts

The `securityContext` applied to containers depends on the execution mode and container role. Both single-container Pods and Compose Pods explicitly set `automountServiceAccountToken: false` at the Pod spec level so Kubernetes API tokens are never mounted into untrusted task containers by default.

You can bind a Kubernetes `ServiceAccount` to the trial Pod by supplying `--ek service_account_name=<name>`. The following table provides the authoritative security profile for every container type.

| Container Role | `runAsUser` | `privileged` | `capabilities` | `automountServiceAccountToken` |
|---|---|---|---|---|
| **Direct Pod `main`** | `0` (and `runAsGroup: 0`) | None | None | **`false`** |
| **Compose `main` (Shape A/B)** | None * | None * | None * | **`false`** |
| **Compose `main` proxy (Shape C)** | None | None | None | **`false`** |
| **Native Sidecars** | None * | None * | None * | **`false`** |
| **One-shot Inits** | None * | None * | None * | **`false`** |
| **`harbor-seed`** | None | None | None | **`false`** |
| **`dind-engine` (Default)** | None | `true` | None | **`false`** |
| **`dind-engine` (gVisor)** | None | None | `add: [SYS_ADMIN, NET_ADMIN, MKNOD]` | **`false`** |
| **`dind-cache-*`** | None | None | None | **`false`** |
| **`compose-up-gate`**| None | None | None | **`false`** |

*\* Unless explicitly overridden by `docker-compose.yaml` (`user`, `privileged`, `cap_add`, `cap_drop`). Note that `cap_add` capabilities on native containers pass through to the Kubernetes container `securityContext` subject to cluster admission policies.*

## Isolation boundaries

Harbor enforces structural isolation between the trial workload and the underlying Node infrastructure.

- **No host paths:** There are no `hostPath` volumes anywhere in the package. Volumes use `emptyDir` by default. When `--ek scratch_volume_size` is configured, Compose named volumes and the DinD storage volume use per-Pod Kubernetes generic ephemeral volumes (`ephemeral.volumeClaimTemplate`) instead. The host `containerd` or `dockerd` socket is never mounted into the Pod.
- **Docker-in-Docker (DinD) boundary:** In Shape B (Hybrid), the native `main` container has no access to `harbor-dind-socket` (which is mounted exclusively into `dind-engine`, `dind-cache-<svc>`, and `compose-up-gate`). When `main` itself mounts `/var/run/docker.sock` (`DOCKER_SOCK`) or requires other DinD-only features, the classifier selects Shape C (Main-in-DinD), where the task's `main` container runs inside `dind-engine` and accesses only the isolated per-Pod `dockerd` daemon—never the GKE Node's runtime socket—while the Pod's `main` proxy container mounts `harbor-dind-socket` to forward `docker exec` calls.
- **No TCP daemon:** The DinD `dockerd` exposes only a UNIX socket (`unix:///var/run/harbor-dind/docker.sock`), never a TCP listener. In Shape C, `connect_exec_stream()` routes trial commands via `docker exec` inside the Pod's `main` proxy container, and `ComposeServiceOpsMixin` routes per-service operations via `dind-engine`.
- **Shared volumes:** The `dind-engine` container mounts all Pod volumes referenced by any DinD service at `/harbor/pod-vols/<volume>`. This includes shared log volumes (`verifier-logs`, `agent-logs`, `artifacts`), so `main` and DinD containers share files seamlessly across Shapes B and C.
- **gVisor integration:** You can force gVisor sandbox isolation on any Pod by supplying `--ek runtime_class_name=gvisor`. In addition, `_create_pod()` automatically retries Job creation with `runtimeClassName: gvisor` whenever the cluster rejects a Pod with a capability, security context, PodSecurity, or Autopilot admission error (`_is_gvisor_retryable_error()`). See [Design decisions](design-decisions.md) for more details.

## Autopilot constraints

When executing on a GKE Autopilot cluster, `harbor-gke-ext` detects the environment and probes for cluster capabilities (`is_autopilot`, `gke_version`, `allow_net_admin`, `allowlist_paths`). If the cluster meets the requirements, the environment adjusts the Pod spec to satisfy Autopilot admission constraints:

- **Storage constraints:** `emptyDir` volumes backed by memory (`tmpfs`) are rewritten to use node ephemeral storage (`medium = ""`), and their size is clamped to 10240 MiB (`_GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB`). For Compose Pods, `--ek scratch_volume_size=<size>` backs Compose named volumes and the DinD storage volume (`/var/lib/docker`) with per-Pod Persistent Disks through generic ephemeral volumes. `dind-engine` still requests its full `ephemeral-storage` estimate from the node, so `scratch_volume_size` doesn't reduce the node ephemeral-storage reservation.
- **Resource alignment:** Harbor sets no synthesized container limits. Autopilot's admission webhook applies its own alignment; on GKE 1.35+ it forces `limits.ephemeral-storage = requests.ephemeral-storage` and leaves CPU and memory alone.
- **gVisor auto-selection:** At Compose translation time on Autopilot (`is_autopilot=True`), if a Shape A Compose service requests any Linux capability in `cap_add` outside the Kubernetes Pod Security Standards *Baseline* allowlist (`_PSS_BASELINE_CAPABILITIES`, plus default OCI runtime `NET_RAW`), the translator logs a warning naming the service and capability, records the mapping in the `harbor.dev/gvisor-capabilities` Pod annotation, and assigns `runtimeClassName: gvisor` so the elevated capability executes inside gVisor's user-space kernel rather than against the host kernel.

## Threat model and limits

This package provides namespace-level workload isolation. It relies on standard Kubernetes controls and does not implement a hardened hypervisor sandbox itself unless gVisor is utilized. 

You should be aware of the following limitations that are explicitly **not protected**:

- **Same-namespace reachability:** `NetworkPolicy` objects generated by Harbor do not restrict ingress. Concurrent trials running in the same namespace can technically reach one another if they expose internal services, unless a cluster-level default-deny ingress policy is present.
- **Node service account exposure:** Any cloud resource accessible to the Node's service account (or the namespace's default IAM binding) is reachable if an escape occurs. Ensure the Node identity is as unprivileged as possible.
- **Layer 7 filtering:** `NetworkPolicy` cannot filter traffic at Layer 7 (e.g., HTTP). Allowed IPs or FQDNs expose the entirety of those remote endpoints on the specified ports. 
- **Privileged containers are not sandboxes:** The `dind-engine` container runs as privileged (or with expansive capabilities) to facilitate Docker-in-Docker. This represents a meaningful trust boundary reduction on Standard clusters. A compromise of `dind-engine` constitutes a direct path to host compromise.
- **GPU device isolation:** In Shape B and Shape C (DinD) deployments, the `dind-engine` container enumerates and mounts every `/dev/nvidia*` device on the Node. Consequently, device-level isolation between co-tenant Pods on a shared GPU Node is not provided inside the DinD plane.
- **Compose environment variable leakage:** Compose variable interpolation evaluates `${VAR}` references in the Compose file. This interpolation includes the host's `os.environ` at translation time. If a task author references a host environment variable in their `docker-compose.yml`, that value is baked statically into the sidecar's container spec.

For more details on task architecture, see the [Architecture reference](architecture.md). For resolving common failures, check [Troubleshooting](troubleshooting.md).
For environment configuration, see the [Configuration reference](configuration.md).
