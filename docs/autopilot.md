# GKE Autopilot

This document explains how `harbor-gke-ext` detects, configures, and schedules evaluation trials on Google Kubernetes Engine (GKE) Autopilot clusters, and how Autopilot differs from GKE Standard.

## Support status

`harbor-gke-ext` supports both GKE Standard and GKE Autopilot clusters. On Autopilot, Google manages the nodes and enforces workload policies, so trials run with the following differences:

| Area | Behavior on Autopilot | Recommendation |
|---|---|---|
| Direct Pods and Shape A Compose tasks | Supported. | Apply the [`harbor-autopilot` ComputeClass](cluster-setup.md#2-apply-the-harbor-autopilot-computeclass) and pass `--ek compute_class=harbor-autopilot`. |
| Concurrency | In oracle calibration runs, throughput reached about 35 trials per minute for exec-heavy workloads and stopped increasing between `-n 50` and `-n 200`. | Run with `-n 50` to `-n 100`. For details, see [Exec admission throughput](#exec-admission-throughput). |
| Shape B and Shape C (Docker-in-Docker) | Privileged containers require a customer-owned `WorkloadAllowlist`, which is available to eligible customers. Without one, `harbor-gke-ext` rejects these tasks before Pod creation (`AUTOPILOT_DIND_UNAVAILABLE`). `harbor-gke-ext` hasn't been validated with a customer-owned allowlist. | Run DinD datasets on a Standard cluster. For details, see [DinD admissibility](#docker-in-docker-dind-admissibility-on-autopilot). |
| Node pools and machine types | Autopilot manages nodes. `--ek node_pool` raises `PlacementConflictError`, and `--ek machine_type` selects the machine family only. | Use `compute_class` or `task_compute_classes`. For details, see [ComputeClass resolution](#computeclass-resolution-and-ephemeral-storage). |
| CPU visibility | Autopilot doesn't offer `cpuManagerPolicy: static`, so `nproc` reports the node's vCPU count rather than the task budget. | For details, see the notes on [the `harbor-autopilot` ComputeClass](cluster-setup.md#2-apply-the-harbor-autopilot-computeclass). |
| Machine capacity | Scale-up reports `GCE out of resources` when a machine family has no capacity in a zone. | List several machine families in the ComputeClass. |

For large evaluations, high concurrency, or DinD datasets, a [Standard cluster](cluster-setup.md#path-a-standard-cluster-with-minimal-system-pool-scale-to-zero-worker-pools-and-nap-recommended) is the better fit.

## Exec admission throughput

On Autopilot, GKE enforces its managed security and workload policies through the GKE Warden validating admission webhook (`warden-validating.common-webhooks.networking.gke.io`), which checks every `pods/exec` connection and every Pod and Job write. The webhook has a `10`-second timeout. Harbor runs each command in a trial as a separate exec: file uploads, agent commands, verifier commands, and file downloads. As concurrency grows, so does the rate of exec admissions. When the webhook can't validate a request within its timeout, the API server returns HTTP `500` with the message `failed calling webhook ... context deadline exceeded`.

`harbor-gke-ext` adapts to this rate with a process-wide [adaptive control-plane limiter](runtime.md#adaptive-control-plane-limiter). When the API server reports a webhook call failure, the limiter halves the number of concurrent exec and create calls, down to a minimum of `4`, and the call is retried with jittered backoff. As calls succeed, the limit grows back toward its maximum of `128`. Log messages such as `Kubernetes control plane is overloaded; lowering concurrent exec/create limit from 65 to 32.` are expected on Autopilot and don't indicate failed trials. GKE Standard clusters don't run this webhook, and the limiter didn't lower its limit in Standard runs at `-n 500`.

The limiter prevents trial failures but doesn't change the admission rate. Above the throughput plateau, additional concurrent trials wait for exec admission instead of finishing sooner. Oracle calibration runs measured the following results (`-k 3`, `--max-retries 2`, `harbor-autopilot` ComputeClass, GKE Autopilot `1.35.8`, `us-central1`):

| Cluster | Dataset | `-n` | Trials per minute (steady state) | Oracle `agent_execution` p50 / p90 | Infrastructure exceptions |
|---|---|---|---|---|---|
| Autopilot | `scale-ai/swe-bench-pro` (100 trials) | `50` | At least 20 (all 100 trials finished within 5 minutes) | 5 s / 23 s | 0 |
| Autopilot | `swe-bench/swe-bench-verified` (1,488 trials) | `200` | About 35 | About 90 s / 110 s | 0 |
| Autopilot | `scale-ai/swe-bench-pro` (1,923 trials) | `400` | About 32 | 153 s / 220 s | 0 |
| Standard (`harbor-static-cpu`) | `swe-bench/swe-bench-verified` (1,488 trials) | `500` | About 177 | 5 s / 6 s | 0 |

The oracle agent runs for about `5` seconds. On Autopilot, the remainder of `agent_execution` is time spent waiting for exec admission. Increasing `-n` from `200` to `400` didn't increase throughput. The plateau begins between `-n 50` and `-n 200`; it hasn't been measured more precisely. Running the Harbor client in the same region as the cluster (Cloud Build in `us-central1`) produced the same throughput and delays as running it from a workstation.

To plan Autopilot evaluations, follow these recommendations:

- Run with `-n 50` to `-n 100`.
- Estimate wall-clock time at about 35 exec-heavy trials per minute per cluster. Agents that run commands less often than the oracle agent use admission capacity more slowly.
- Keep `--ek decoupled` at its default value, `false`. A direct stream is admitted once per command, while [decoupled polling](runtime.md#mode-b-decoupled-polling) runs a separate exec, and a separate admission, for each status poll.
- For thousands of trials at high concurrency, use a Standard cluster.

> [!NOTE]
> Whether `--cpus auto --memory auto` keeps mapping to `requests = limits = declared budget` is pending final benchmark validation. For details, see [Resource modes](#resource-modes---cpus-auto---memory-auto-vs---cpus-request---memory-request).

## Cluster detection and capability probing

`harbor-gke-ext` probes each target cluster once per process and caches the resulting `ClusterCapabilities`. Autopilot mode is part of that probe; there is no separate detection step and no override. The probe runs `gcloud container clusters describe` (which needs `container.clusters.get`, the same permission `gcloud container clusters get-credentials` needs) and reads:

- `autopilot.enabled`
- `currentMasterVersion`
- `autopilot.privilegedAdmissionConfig.allowlistPaths`
- `autopilot.workloadPolicyConfig.allowNetAdmin`
- The network configuration that decides `NetworkPolicy` enforcement
- Node pool configuration (machine types, autoscaling limits, boot disk sizes, taints) and node auto-provisioning settings

It then asks the Kubernetes API for Pod-level `spec.resources` support, the `kube-dns` ClusterIP, and `FQDNNetworkPolicy` support. If any step fails, the probe raises and nothing is cached: the environment never declares capabilities it could not verify.

## Resource modes: `--cpus auto --memory auto` vs `--cpus request --memory request`

How CPU and memory budgets from `task.toml` (`cpus`, `memory_mb`) translate into Pod specifications depends on Harbor's `--cpus` and `--memory` `ResourceMode` flags:

| Mode | What `GKEEnvironment` emits | Behavior on GKE Standard | Behavior on GKE Autopilot |
|---|---|---|---|
| `--cpus auto --memory auto` (default) | `_GKE_DEFAULT_RESOURCE_AUTO_MODE = ResourceMode.GUARANTEE`: sets `requests = limits = declared budget` (unless `cpu_limit_multiplier` or `memory_limit_multiplier` raises the limit above the request). | Capped at the declared budget (`Guaranteed` QoS on direct Pods when no multipliers are set; eligible for `cpuManagerPolicy: static`). | Capped at the declared budget (or rounded up by Autopilot Warden if below the class minimum). |
| `--cpus guarantee --memory guarantee` | Explicitly sets `requests = limits = declared budget` on the `main` container (for direct Pods; for Compose Pods the budget goes on `main` and `spec.resources` covers it plus every other container). `cpu_limit_multiplier` and `memory_limit_multiplier` are ignored. | Capped at the declared budget (`Guaranteed` QoS). | Capped at the declared budget. |
| `--cpus request --memory request` (direct Pods) | Sets `requests = declared budget` on `spec.resources` and **omits `limits`**. `cpu_limit_multiplier` and `memory_limit_multiplier` are ignored. | Uncapped (`Burstable` QoS): the Pod reserves its declared budget for scheduling but can burst into idle node CPU and memory. | Autopilot raises `requests` to its minimums. On clusters that support [Pod bursting](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/pod-bursting-gke#availability-in-gke) (GKE `1.30.2-gke.1394000` or later with cgroup v2), Autopilot doesn't set `limits`, so the Pod can burst into the node's burstable capacity (unused requests of Pods on the node). On clusters that don't support bursting, Autopilot sets `limits = requests`. GKE documents this for container resources; it hasn't been measured for Pod-level `spec.resources`. |
| `--cpus request --memory request` (Compose Pods, Shapes A, B and C) | `main` reserves the declared budget without a limit. `spec.resources.requests` is the larger of the budget and the aggregate container requests; `limits` is the larger of that request and the aggregate of per-container ceilings (limit, else request; for `dind-engine`, the daemon baseline plus the ceilings of the DinD services). With no container limits declared, `limits = requests`. See [Docker-in-Docker](docker-in-docker.md#resource-model-and-volume-topology). | Capped at the Pod-level limit. | Capped at the Pod-level limit (DinD not validated, see below). |

> [!IMPORTANT]
> Whether `--cpus auto --memory auto` remains mapped to `ResourceMode.GUARANTEE` (`requests = limits = declared budget`, matching Harbor's Docker environment) or switches back to `ResourceMode.REQUEST` is an open question pending final benchmark validation.

### Multi-container Compose budgets on Autopilot (`spec.resources`)

For every multi-container Compose Pod (Shapes A, B and C), the task budget goes on `main`, as in Harbor's Docker environment, and `build_pod_level_resources()` puts a host-wide request and ceiling on the Pod (`spec.resources`, KEP-2837, beta in Kubernetes 1.34+) that cover `main` plus every other container, rather than dividing synthetic slices across individual containers.

On GKE Autopilot, the `autopilot-default-resources-mutator` webhook injects `500m` CPU and `2Gi` memory into every container that declares neither a request nor a limit. With Pod-level resources, the API server then rejects the Pod whenever that injected aggregate exceeds `spec.resources.requests` (`spec.resources.requests[memory]: Invalid value: "4Gi": must be greater than or equal to aggregate container requests of 14Gi`). Measured on GKE Autopilot `1.35.8` with a 7-container Pod and a `2` CPU / `4Gi` budget:

| ComputeClass | Bare containers defaulted? | Result without floor |
|---|---|---|
| general-purpose (none), `Balanced`, `Scale-Out` | Yes (`Balanced`: `2` CPU / `8Gi` each) | Rejected |
| Custom `machineFamily` class (`harbor-autopilot`) | Yes | Rejected |
| `Performance`, accelerator (GPU) | No | Admitted |

To keep the task budget as the effective cap, `build_pod_level_resources(..., is_autopilot=True)` gives every container that declares neither a request nor a limit for a resource a token `1m` CPU / `1Mi` memory request, but only for resources that reach `spec.resources`. Declared Compose values are never changed, and GKE Standard is never floored. Autopilot may still raise individual containers to its own per-container minimums (measured up to `246m` / `1020Mi` on `Scale-Out`), so a task budget below those minimums can still be rejected.

DinD Pods (Shapes B and C) carry `spec.resources` too, so their undeclared Harbor infrastructure containers receive the same token requests. DinD on Autopilot also requires a `WorkloadAllowlist`, and this combination has not been measured.

## ComputeClass resolution and ephemeral storage

On GKE Autopilot, general-purpose, `Balanced` and `Scale-Out` Pods are capped at `10 GiB` (`10,240 MiB`, `_GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB`) of aggregate `ephemeral-storage`. Workloads requiring more than `10 GiB` must use the `Performance` ComputeClass or a custom `machineFamily` ComputeClass. `--ek scratch_volume_size` doesn't change this: the Pod's `ephemeral-storage` requests stay the same, so the estimate below still counts them (see [`tmpfs` and scratch volumes on Autopilot](#tmpfs-and-scratch-volumes-on-autopilot)).

`Performance` Pods run one per node (Autopilot adds `cloud.google.com/pod-slots: 1`). Because Autopilot counts `1 GiB` for every sidecar, most Compose tasks exceed `10 GiB` and would be promoted to `Performance`. For benchmark runs, apply the [`harbor-autopilot` ComputeClass](cluster-setup.md#2-apply-the-harbor-autopilot-computeclass) and pass `--ek compute_class=harbor-autopilot`. A custom class is not subject to the `10 GiB` cap (a server dry-run admitted a `200Gi` Pod), and Autopilot can place several of its Pods on one node.

Placement options behave as follows on Autopilot (measured on GKE Autopilot `1.35.8`):
- `--ek node_pool` or `--ek task_node_pools` raises `PlacementConflictError` before Pod creation (Autopilot has no user-managed node pools; use `compute_class` / `task_compute_classes`).
- `--ek machine_type` or `--ek task_machine_types` emits only a `cloud.google.com/machine-family` node selector, so Autopilot pins the family but chooses the size itself (the size suffix, for example `-4` in `c4-standard-4`, is not sent). A machine-type pin also disables automatic `Performance` promotion. Combining a machine-type pin with `compute_class` or `task_compute_classes` on the same task raises `PlacementConflictError`.
- A `compute_class` that does not exist on the cluster (for example `harbor-static-cpu` from a Standard setup) is rejected by Autopilot at Job creation (`the specified 'cloud.google.com/compute-class:harbor-static-cpu' is not supported`) and surfaces as a `RuntimeError`.
- `--ek runtime_class_name=runc` fails the same way (`RuntimeClass "runc" not found`). The Autopilot cluster defines only the `gvisor` and `confidential-linked-runner` RuntimeClasses.

Contradictory placement settings raise `PlacementConflictError` before resolution rather than being resolved silently. `GKEEnvironment._resolve_active_compute_class()` then resolves the active `ComputeClass` on Autopilot using this precedence:

1. **Priority 1 — Per-task ComputeClass (`--ek task_compute_classes`):** If the task matches an entry in `task_compute_classes`, that class is used unconditionally (and the task is exempted from `--ek max_storage_request_mb`).
2. **Priority 2 — Accelerators (`gpus > 0` or `tpu`):** Returns `None`. On Autopilot, GPU and TPU Pods use native accelerator selectors (`cloud.google.com/gke-accelerator` or `cloud.google.com/gke-tpu-accelerator`) and must not receive a CPU `ComputeClass` unless explicitly mapped via `task_compute_classes`.
3. **Priority 3 — Global ComputeClass (`--ek compute_class`):** Applies to all remaining non-accelerator tasks.
4. **Priority 4 — Machine-type pin (`--ek machine_type` or `--ek task_machine_types`):** Returns `None`, so the `cloud.google.com/machine-family` selector isn't combined with a storage-driven `Performance` class.
5. **Priority 5 — Automatic `Performance` promotion (`> 10,240 MiB`):** If `_effective_total_ephemeral_storage_mb()` exceeds `10,240 MiB`, automatically returns `"Performance"`.
6. **Priority 6 — Default:** Returns `None` (general-purpose Autopilot platform default).

### How `_effective_total_ephemeral_storage_mb()` estimates Autopilot storage

To predict whether Autopilot will reject a Pod under the `10 GiB` general-purpose ceiling, `_effective_total_ephemeral_storage_mb()` sums:
- `main`'s effective storage request (`_effective_storage_mb`, capped by `--ek max_storage_request_mb` unless the task is mapped in `task_compute_classes`),
- `1,024 MiB` (`_GKE_AUTOPILOT_DEFAULT_CONTAINER_STORAGE_MB`) for each non-`main` service across `docker-compose.yaml` and any extra Compose files (accounting for the `1 GiB` default ephemeral-storage request that Autopilot injects into containers that do not declare one), and
- `10,240 MiB` (`DIND_STORAGE_FLOOR_MB`) if the Compose task requires the DinD plane (`_compose_needs_dind()`).

For direct (non-Compose) tasks, the estimate is `main`'s effective storage request only. If the Compose files can't be parsed, the estimate assumes one sidecar and logs a warning.

Note that on Autopilot (and on Standard clusters with Node Auto-Provisioning enabled), the pre-submission node storage check `_assert_ephemeral_storage_schedulable()` is skipped (`if caps.is_autopilot or caps.node_auto_provisioning_enabled: return`) because Autopilot provisions nodes dynamically to fit admitted Pods.

### `tmpfs` and scratch volumes on Autopilot

- **Compose `tmpfs` mounts:** On GKE Standard, Compose `tmpfs` entries translate to `emptyDir: {medium: Memory}`. On GKE Autopilot (`is_autopilot=True`), `compose_translator.py` translates `tmpfs` entries to a disk-backed `emptyDir` (`medium: ""`) so they do not consume container memory budgets or trigger Warden `medium: Memory` validation rejections. A `size=` option becomes the `emptyDir` `sizeLimit` on both cluster types; on Autopilot it is capped at `10,240 MiB`. Services delegated to DinD keep Docker's native `tmpfs` in the inner Compose project instead.
- **Generic ephemeral volumes (`--ek scratch_volume_size`):** When `--ek scratch_volume_size=50Gi` is passed, Compose named volumes and `harbor-dind-storage` (`/var/lib/docker`) use Kubernetes generic ephemeral volumes (`ephemeral.volumeClaimTemplate`) backed by dynamically provisioned Persistent Disks rather than node boot disk `emptyDir`. The Pod's `ephemeral-storage` requests don't change: `dind-engine` still requests its full estimate, so `Performance` promotion and scheduling see the same reservation.

## Docker-in-Docker (DinD) admissibility on Autopilot

Running Shape B (hybrid DinD sidecars) or Shape C (Main-in-DinD) requires a privileged `dind-engine` container (`docker:28.3.3-dind`). By default, GKE Autopilot blocks privileged containers under the Pod Security Standards (PSS) baseline.

`evaluate_autopilot_dind_capability()` checks two conditions before allowing a DinD task onto an Autopilot cluster:
1. **Cluster version `>= 1.35`:** Customer-managed `WorkloadAllowlist` support for privileged containers requires GKE `1.35` or later. If `parse_gke_minor_version(gke_version) < (1, 35)`, `dind_availability` is set to `DindAvailability.DIND_UNAVAILABLE`.
2. **Configured `allowlistPaths`:** The cluster must have non-empty `autopilot.privilegedAdmissionConfig.allowlistPaths` configured in the control plane (authorized via the `container.managed.autopilotPrivilegedAdmission` organization policy and `--autopilot-privileged-admission=<path>`). `harbor-gke-ext` never installs cluster-wide policy objects itself. See [Enabling privileged DinD with a `WorkloadAllowlist`](#enabling-privileged-dind-with-a-workloadallowlist).

If a task requires DinD (`dind_sidecars` is non-empty) and `caps.dind_availability != DindAvailability.DIND_AVAILABLE`, `classify_compose_placement()` fails fast before Pod creation with `UnsupportedComposeFeatureError` (`AUTOPILOT_DIND_UNAVAILABLE`).

### Enabling privileged DinD with a `WorkloadAllowlist`

Harbor's `dind-engine` is not a GKE partner workload, so it needs a customer-owned `WorkloadAllowlist`.

> [!IMPORTANT]
> Allowlists for your own privileged workloads are **available only to eligible Google Cloud customers** and require GKE `1.35` or later. Ask [Cloud Customer Care](https://cloud.google.com/support-hub) to enable the feature before you start. If you are not eligible, run Shape B and Shape C datasets on a [Standard cluster](cluster-setup.md#path-a-standard-cluster-with-minimal-system-pool-scale-to-zero-worker-pools-and-nap-recommended).

> [!WARNING]
> `harbor-gke-ext` has not been validated against a customer-owned `WorkloadAllowlist`. Its cluster probe only checks that `autopilot.privilegedAdmissionConfig.allowlistPaths` is non-empty; it does not check that the installed allowlist actually matches Harbor's DinD Pods.

The procedure follows Google's guides [Create allowlists for privileged Autopilot workloads](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/autopilot-privileged-allowlists) and [Run privileged workloads in Autopilot](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/run-autopilot-partner-workloads):

1. **Organization policy (organization policy administrator, `roles/orgpolicy.policyAdmin`).** Add your Cloud Storage path to the `allowPaths` parameter of the `container.managed.autopilotPrivilegedAdmission` managed constraint. Keep `allowAnyGKEPath: true` if the cluster should still accept partner allowlists (`gke://` paths). See [Restrict privileged GKE workloads in organizations](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/privileged-admission-organizations).

2. **Bucket access for the GKE service agent.**

   ```bash
   export ALLOWLIST_BUCKET="my-harbor-allowlists"
   export PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)" -q)

   for ROLE in roles/storage.bucketViewer roles/storage.objectViewer; do
     gcloud storage buckets add-iam-policy-binding "gs://${ALLOWLIST_BUCKET}" \
       --member="serviceAccount:service-${PROJECT_NUMBER}@container-engine-robot.iam.gserviceaccount.com" \
       --role="${ROLE}"
   done
   ```

3. **Authorize the path on the cluster** with `--autopilot-privileged-admission`. Include `gke://*`, or partner allowlists stop working. A cluster created before your organization got access needs this update once before the `generate-allowlist` annotation in step 4 works.

   ```bash
   gcloud container clusters update "${CLUSTER_NAME}" \
     --location="${LOCATION}" \
     --project="${PROJECT_ID}" \
     --autopilot-privileged-admission="gke://*,gs://${ALLOWLIST_BUCKET}/harbor/*"
   ```

4. **Generate the `WorkloadAllowlist`.** Add the annotation `cloud.google.com/generate-allowlist: "true"` to a Harbor DinD Pod manifest and create it. GKE rejects the Pod and prints the matching `WorkloadAllowlist`. For Harbor, the privileged container is `dind-engine` (`privileged: true` on runc). Generalize `matchingCriteria.containers[*].image` with a regular expression so one allowlist covers every task, then set `metadata.name` to the file name.

5. **Upload the allowlist** to `gs://${ALLOWLIST_BUCKET}/harbor/`.

6. **Install it with an `AllowlistSynchronizer`** (requires GKE `1.32.2-gke.1652000` or later). For customer-owned buckets, `projectNumber` and `bucketName` are required. GKE re-checks the files every 10 minutes.

   ```bash
   cat <<EOF | kubectl apply -f -
   apiVersion: auto.gke.io/v1
   kind: AllowlistSynchronizer
   metadata:
     name: harbor-dind
   spec:
     projectNumber: ${PROJECT_NUMBER}
     bucketName: "${ALLOWLIST_BUCKET}"
     allowlistPaths:
     - harbor/*
   EOF

   kubectl wait --for=condition=Ready allowlistsynchronizer/harbor-dind --timeout=60s
   kubectl get workloadallowlist
   ```

Once `allowlistPaths` is set on the cluster, the probe reports `DIND_AVAILABLE` and Harbor stops rejecting Shape B and Shape C tasks up front. Whether Autopilot then admits each Pod depends on the installed allowlist matching it.

## gVisor sandbox selection for non-baseline capabilities

On GKE Autopilot, native containers cannot request Linux capabilities outside the PSS baseline (`_PSS_BASELINE_CAPABILITIES`) unless isolated in GKE Sandbox (`gVisor`):

- **Capability annotation:** In `translate_compose()`, if any native service needs gVisor because it declares a `cap_add` capability outside `_PSS_BASELINE_CAPABILITIES` (for example `NET_ADMIN` or `SYS_PTRACE`), `harbor-gke-ext` records the triggering `"<service>:<CAP>"` pairs in the `harbor.dev/gvisor-capabilities` Pod annotation. This happens on any cluster type and any Pod shape.
- **Automatic Shape A promotion:** If `caps.is_autopilot` is true, `placement.shape == "A"`, a native service needs gVisor, and no explicit `--ek runtime_class_name` is set, `harbor-gke-ext` sets `spec.runtimeClassName = "gvisor"`.
- **Admission retry fallback:** If Job creation in `_create_pod()` fails and the error message contains any of `gvisor`, `runtimeclassname`, `capabilities`, `privileged`, `podsecurity`, `autopilot`, or `securitycontext` (case-insensitive, `_is_gvisor_retryable_error()`), and the Pod doesn't already use `gvisor`, `harbor-gke-ext` sets `pod.spec.runtime_class_name = "gvisor"` and retries Job creation once. The retry isn't gated on Autopilot and applies on Standard clusters as well.

## Networking and `NetworkPolicy` on Autopilot

GKE Autopilot clusters run GKE Dataplane V2 (Cilium) by default, which enforces standard Kubernetes `NetworkPolicy` resources. To use hostname or wildcard domain allowlists (`network_mode = "allowlist"` with domain names or `--allow-agent-host`), enable FQDN Network Policies on the Autopilot cluster:

```bash
gcloud container clusters update "${CLUSTER_NAME}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --enable-fqdn-network-policy
```

Across all cluster modes (both Autopilot and Standard), `GKEEnvironment._apply_network_policy()` runs **before** the Job and Pod are created:

- **`public` mode with `allow_metadata_server=False` (default):** Creates a Pod-scoped egress `NetworkPolicy` that permits `0.0.0.0/0` except `169.254.169.254/32` and `169.254.169.252/32`, and permits DNS egress on port `53`. This blocks the Pod from querying the GKE metadata server while leaving public internet access open.
- **`public` mode with `allow_metadata_server=True`:** Skips creating any `NetworkPolicy` at trial startup — or deletes any existing Pod-scoped policies if switched mid-trial — so the Pod has unrestricted egress including `169.254.169.254`.
- **`allowlist` mode:** Creates a Pod-scoped `NetworkPolicy` for IP/CIDR rules (plus `169.254.169.254/32` TCP port `80` when `allow_metadata_server=True`, and DNS port `53` when any CIDR or hostname is allowlisted or the metadata-server rule is present) and a Pod-scoped `FQDNNetworkPolicy` when hostnames are present.
- **`no-network` mode:** Creates a Pod-scoped `NetworkPolicy` with an empty egress list (`egress: []`), blocking all egress including DNS and the metadata server.

For full details on network isolation and Workload Identity Federation, see [Networking and security](networking-and-security.md) and [Cluster setup](cluster-setup.md).
