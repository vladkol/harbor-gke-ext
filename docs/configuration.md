# Configuration reference

This page documents every option that `harbor-gke-ext` reads: environment options,
Cloud Build plugin options, the standalone pre-build CLI, the `task.toml` fields the
package consumes, and the host environment variables it inspects.

## How options reach the package

`harbor-gke-ext` receives configuration through three channels.

| Channel | Flag | Consumed by |
| :--- | :--- | :--- |
| Environment options | `--ek KEY=VALUE` (alias `--environment-kwarg`) | `GKEEnvironment` |
| Plugin options | `--plugin-kwarg KEY=VALUE` (alias `--pk`) | `CloudBuildPlugin` |
| Command-line flags | `harbor-gke-ext-prebuild --flag` | Standalone pre-builder |

Repeat `--ek` and `--plugin-kwarg` (or `--pk`) once per key:

```bash
harbor run \
  -p examples/tasks/gpu-sidecar \
  --agent oracle \
  -e harbor_gke_ext:GKEEnvironment \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${LOCATION}" \
  --ek cluster_name="${CLUSTER_NAME}"
```

### How values are parsed

Harbor parses each `--ek` value as JSON before handing it to the environment. If
the value is not valid JSON, the Python literals `True`, `False`, and `None` are
recognized; any other value is passed as a string. For example:

* `--ek autopilot=true` arrives as the boolean `True`.
* `--ek pod_ready_timeout=900` arrives as the integer `900`.
* `--ek autopilot=yes` is not valid JSON, so it arrives as the string `"yes"`.

Boolean options are normalized by `_parse_bool`, which treats `true`, `1`, `yes`, `t`, `y`, and `on` as true (case-insensitive), and treats any other string (such as `false`, `0`, or `no`) as false.

> [!NOTE]
> Unrecognized `--ek` keys emit a warning log (`Unrecognized GKE environment kwargs (--ek) ignored or forwarded to base: ...`) so typos are surfaced immediately at startup.
> Keys that start with `_`, and keys that match a `BaseEnvironment.__init__` parameter
> (for example `override_cpus`), don't produce this warning. For the environment
> settings Harbor derives from its own flags (`override_cpus`, `override_memory_mb`,
> `override_storage_mb`, `override_gpus`, `override_tpu`, `stream`), an `--ek` key of
> the same name replaces Harbor's value.

## Environment options

### Cluster identity and location

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `cluster_name` | string | Active GKE `kubectl` context (**Required** if none) | Name of the target GKE cluster. |
| `location` | string | Active GKE `kubectl` context (**Required** if none) | Cluster region or zone. Highest precedence of the three location keys. |
| `zone` | string | — | Fallback for `location`. |
| `region` | string | — | Fallback for `location`. Lowest precedence. |
| `project_id` | string | Auto-detected | Google Cloud project that hosts the cluster, Artifact Registry, and Cloud Build. |
| `namespace` | string | `default` | Kubernetes namespace for the Job, Pod, and network policies. |
| `autopilot` | boolean | Auto-detected | Overrides automatic GKE Standard vs. Autopilot detection (`false` on Standard, `true` on Autopilot). |

If `project_id`, `location` (`zone`/`region`), or `cluster_name` is omitted, `GKEEnvironment` first inspects the active `kubectl` context in `KUBECONFIG` / `~/.kube/config`. When the active context follows GKE's canonical naming (`gke_<project_id>_<location>_<cluster_name>`, written by `gcloud container clusters get-credentials`) and matches any explicitly provided `project_id`, `cluster_name`, and `location` (a zone and a region match when they share the same parent region), any omitted fields—including `project_id`—are filled from that context before checking host environment variables.

If no compatible GKE `kubectl` context is active (or when `project_id`, `cluster_name`, or `location` overrides point to a different cluster):
- Both `cluster_name` and at least one of `location`, `zone`, or `region` are **required** (omitting either raises `ValueError`). Precedence is `location` > `zone` > `region`. A zonal value such as `us-central1-a` is automatically reduced to its parent region (`us-central1`) for Artifact Registry and Cloud Build.
- `project_id` resolves from `$GOOGLE_CLOUD_PROJECT`, then `$CLOUDSDK_CORE_PROJECT`, then `$GCP_PROJECT`, then `gcloud config get-value project` (gated once per process). If all fail, it raises `ValueError`.

### Registry and image builds

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `registry_name` | string | `harbor-tasks` | Artifact Registry repository that holds task images. |
| `registry_location` | string | Cluster region | Region hosting Artifact Registry. Use this when the registry and cluster live in different regions. |
| `cloud_build_timeout_sec` | integer | `10800` | Timeout for a single inline Cloud Build (seconds). |
| `cloud_build_machine_type` | string | — | Cloud Build worker machine type, for example `E2_HIGHCPU_32`. |
| `cloud_build_disk_size_gb` | integer | — | Cloud Build worker disk size. |
| `cloud_build_worker_pool` | string | — | Private Cloud Build worker pool: a full resource name (`projects/.../locations/.../workerPools/...`), or a pool ID that is expanded to the build project and Cloud Build region. |
| `private_pool` | string | — | Alias for `cloud_build_worker_pool`. |
| `worker_pool` | string | — | Alias for `cloud_build_worker_pool`. |
| `prebuild_fail_on_incomplete` | boolean | `false` | Forwarded to `CloudBuildPlugin` when `--plugin-kwarg fail_on_incomplete` is not set. Raises `RuntimeError` if any task image fails to pre-build. |
| `image_pull_secrets` | string or list | — | `imagePullSecrets` names for private registries. Accepts a JSON array or a comma-separated list. |

> [!WARNING]
> Setting a worker pool together with `cloud_build_machine_type` raises `ValueError`
> at construction. Cloud Build does not accept both.

### Scheduling and hardware

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `machine_type` | string | — | Pins the Pod to a GCE machine family (`cloud.google.com/machine-family`, extracted from a family name like `n2` or a machine type like `n2-standard-4`). When a size suffix is present, preflight also verifies that the cluster has a pool in that family with at least that many vCPUs. Cannot be combined with `compute_class` / `task_compute_classes` on the same task. |
| `task_machine_types` | string, list, or dict | `{}` | Per-task machine family/type mapping, for example `task-a=n2-standard-8,task-b=c3-standard-8`. Also accepts `task:machine_type`. Overrides `machine_type` for the tasks it names. |
| `node_pool` | string | — | Pins the Pod to a named node pool (`cloud.google.com/gke-nodepool`). GKE Standard only; cannot be combined with `compute_class` or `task_compute_classes`. |
| `task_node_pools` | string, list, or dict | `{}` | Per-task node pool mapping, for example `task-a=pool-a,task-b=pool-b`. Also accepts `task:pool`. Overrides `node_pool` (or a job-wide `compute_class`) for the tasks it names. |
| `compute_class` | string | — | Sets `cloud.google.com/compute-class` node selector (for example a custom ComputeClass on GKE Standard or `Performance` on Autopilot). |
| `task_compute_classes` | string, list, or dict | `{}` | Per-task ComputeClass mapping, for example `task-a=class-a,task-b=class-b`. Also accepts `task:class`. |
| `gpu_override` | string | — | Forces the accelerator label. Must be a full GKE label such as `nvidia-l4`. |
| `default_gpu_type` | string | — | Accelerator used when a task requests GPUs but names no type. Accepts a short alias (`l4`) or a full label (`nvidia-l4`). |
| `default_gpu_count` | integer | — | Fallback GPU count when a Compose service requests `count: all` and `task.toml` omits `gpus`. |
| `scratch_volume_size` | string | — | When set (e.g., `50Gi`), backs Compose named volumes and DinD `/var/lib/docker` storage with Kubernetes generic ephemeral volumes (`ephemeral.volumeClaimTemplate`) instead of node-boot-disk `emptyDir`. `dind-engine` still requests its full `ephemeral-storage` estimate from the node, so this option doesn't reduce the node storage reservation. |
| `dind_storage_mb` | integer | — | Explicit `requests.ephemeral-storage` for the `dind-engine` container, in MiB. Overrides the automatic estimate described below. |
| `task_dind_storage_mb` | string, list, or dict | `{}` | Per-task `dind_storage_mb` mapping, for example `task-a=61440,task-b=20480`. Also accepts `task:mib`. Overrides `dind_storage_mb` for the tasks it names. |
| `max_storage_request_mb` | integer | — | Caps `requests.ephemeral-storage` while leaving runtime limits unconstrained. |
| `cpu_limit_multiplier` | float | — | Sets `limits.cpu = requests.cpu × multiplier` (in millicores) instead of `limits.cpu = requests.cpu`. Applies only when `--cpus` is `auto` (the default), the task declares `cpus`, and the multiplier is greater than `0`. |
| `memory_limit_multiplier` | float | — | Sets `limits.memory = requests.memory × multiplier` instead of `limits.memory = requests.memory`. Applies only when `--memory` is `auto` (the default), the task declares `memory_mb`, and the multiplier is greater than `0`. |
| `service_account_name` | string | — | Kubernetes ServiceAccount for the Pod, used with Workload Identity. |
| `runtime_class_name` | string | — | Sets `spec.runtimeClassName`, for example `gvisor`. |
| `override_entrypoint` | boolean | `false` | Replaces the image entrypoint with `sleep infinity` and disables stdin and TTY. |

#### The task-wide resource model

Harbor budgets a **task**, not an individual sidecar. `task.toml` declares `cpus` and
`memory_mb` for the task as a whole, matching how `docker run --cpus --memory` bounds a
single-container task or a Compose project on a workstation.

Under Harbor's default `--cpus auto --memory auto`, `GKEEnvironment` resolves `auto` to
`guarantee` (`_GKE_DEFAULT_RESOURCE_AUTO_MODE = ResourceMode.GUARANTEE`), setting
`requests = limits = declared budget` for both CPU and memory. Passing
`--cpus request --memory request` sets requests only: a direct Pod then runs with no
cgroup CPU or memory limit, while a Compose Pod still receives a Pod-level
`pod.spec.resources.limits` at least equal to its requests (the task budget, or the
`8192Mi` DinD memory floor). Harbor also accepts `limit` (limit only, no request) and
`ignore` (no CPU or memory value). To allow
controlled bursting above the request while remaining in `auto` mode, pass
`--ek cpu_limit_multiplier=<float>` and/or `--ek memory_limit_multiplier=<float>`.

> [!NOTE]
> **Open evaluation question:** Whether CPU and memory limits remain enabled by default (`--cpus auto --memory auto` resolving to `guarantee`, where `requests = limits`) or become request-only (`auto` resolving to `request`, with limits opt-in through `--cpus guarantee --memory guarantee`) is under final benchmark evaluation.

Where the CPU and memory budget is attached on the Pod specification depends on whether the
task is a direct (single-container) Pod or a Compose (multi-container) Pod:

| Pod type | Condition | Where CPU and memory are set | Why |
| :--- | :--- | :--- | :--- |
| **Direct Pod** (`build_direct_pod`) | Both CPU and memory have `request == limit` (default `--cpus auto --memory auto` or `--cpus guarantee --memory guarantee`) | `spec.containers[0].resources` (`main` container) | Gives the Pod `Guaranteed` QoS and makes it eligible for exclusive CPU pinning on `cpuManagerPolicy: static` node pools (the kubelet's static CPU manager ignores `pod.spec.resources` unless the `PodLevelResourceManagers` feature gate is enabled). |
| **Direct Pod** (`build_direct_pod`) | Limits are omitted (`--cpus request --memory request`) or differ from requests (e.g. via `cpu_limit_multiplier` / `memory_limit_multiplier`) | `pod.spec.resources` | Enforces the task-wide request and/or burst ceiling at the Pod cgroup level (`Burstable` QoS). |
| **Compose Pod** (`build_pod_level_resources`) | All resource modes | `pod.spec.resources` (plus any explicit per-service `deploy.resources.*`, `cpus`, or `mem_limit` on `spec.containers[].resources`) | Shares the task budget across `main`, native sidecars, and `dind-engine`. When `dind-engine` is present (Shapes B and C), `pod.spec.resources.limits.memory` has a floor of `8192Mi` (`DIND_POD_MEMORY_LIMIT_FLOOR_MB = 8192`) so the nested `dockerd` daemon and inner containers are not OOM-killed under small task memory budgets. |

For storage and accelerators:
- **`main` container**: Always carries `requests.ephemeral-storage`, because Kubernetes `pod.spec.resources` does not support `ephemeral-storage`. Harbor sets no `ephemeral-storage` limit on any container.
- **`dind-engine` sidecar**: Carries its own `requests.ephemeral-storage` (see [DinD ephemeral storage](#dind-ephemeral-storage) below) and any GPU resource allocation (`nvidia.com/gpu`) for DinD-routed services.

In a Compose Pod, a service that declares no memory limit of its own is bounded only by the
Pod-level ceiling in `pod.spec.resources`, and may use whatever the rest of the Pod is not
using.

**Kubernetes 1.34 is the minimum supported version.** Pod-level `spec.resources`
(KEP-2837) is beta and enabled by default from Kubernetes 1.34. On an older cluster the API
server silently drops the field, leaving Pod-level budgets unenforced.

Three Kubernetes API-server rules constrain `pod.spec.resources`:

1. `spec.resources.requests[X]` must be at least the aggregate container
   requests. Harbor computes that aggregate from the assembled Pod using the
   Kubernetes effective-request rule — native sidecars accumulate, ordinary
   init containers contribute via `max` — because the answer depends on the
   order init containers appear in.
2. No single container limit may exceed the matching Pod limit. Using the
   aggregate of the declared limits as the Pod's floor satisfies this, since a
   sum is never smaller than any one of its terms.
3. Only `cpu`, `memory`, and `hugepages-*` are accepted in `pod.spec.resources`.
   `ephemeral-storage` is rejected at the Pod level, which is why storage stays a
   container-level reservation.

If the Compose services between them declare more than the task budget, the Pod
ceiling is raised to the declared total and a warning names the resource and
both figures. Admitting the Pod with a lower ceiling is not an option: rule 1
would reject it.

If the task declares neither `cpus` nor `memory_mb` — both are `None` by
default in Harbor's task model, meaning unlimited — there is no budget to cap
anything with, and the Pod is admitted as **BestEffort** with a warning.
BestEffort Pods are accounted as free by the scheduler and are the first thing
the kubelet evicts under node pressure.

On GKE Autopilot, the Warden admission webhook applies its own ephemeral-storage
rules (such as setting `limits.ephemeral-storage = requests.ephemeral-storage`);
`harbor-gke-ext` doesn't override them. For CPU and memory in Compose Pods,
Autopilot injects default requests (`500m` CPU, `2Gi` memory) into every container
that declares neither a request nor a limit, which can push the aggregate above the
Pod-level budget. To keep the Pod-level budget as the effective cap, Harbor gives each
such container a token request of `1m` CPU and `1Mi` memory on Autopilot only.
Autopilot can still raise a container to its own per-container minimum, so a very
small budget can still be rejected.

#### Which container is killed when the Pod runs out of memory

In a Compose Pod where per-container limits are not synthesized, OOM selection works across the Pod cgroup whenever individual services do not declare their own memory limits.

**The condition.** A container is unbounded whenever its Compose service
declares neither `deploy.resources.limits.memory` nor the legacy `mem_limit`.
When at least one container is unbounded,

```
Σ(container memory limits)  <  spec.resources.limits.memory
```

and the Pod's memory cgroup, not any container cgroup, is the first ceiling
anything hits. The kernel then runs the OOM killer *inside the Pod cgroup* and
chooses among all of the Pod's processes.

**The choice.** The kernel ranks candidates by `oom_score`, roughly
`10 x (RSS / node memory)` plus the container's `oom_score_adj`. The kubelet
sets that adjustment ([`pkg/kubelet/qos/policy.go`][oom-policy]):

| Pod QoS | `oom_score_adj` |
| --- | --- |
| Guaranteed | `-997` |
| BestEffort | `1000` |
| Burstable | `1000 - 1000 x (containerMemReq + remainingPerContainer) / nodeMemoryCapacity` |

clamped to a minimum of `3`, with `1000` rewritten to `999`. When Pod-level
requests are set, the unclaimed remainder is shared out evenly:

```
remainingPerContainer = max(0, podMemRequest - Σ containerMemRequests)
                        / (len(spec.containers) + len(spec.initContainers))
```

Note the divisor counts **every** init container, including one-shot ones such
as `dind-cache-*` that have long since exited. A Compose project with many DinD
services therefore spreads the budget thinner per container and raises every
container's `oom_score_adj`, making the Pod as a whole a slightly more
attractive victim relative to its neighbours on the node.

**Why the offender is not necessarily the casualty.** `oom_score_adj` falls as a
container's *request* rises, so declaring a reservation buys protection. Take a
1024 MiB task on a 64 GiB node with four containers:

| Container | Declares | `containerMemReq` | `oom_score_adj` | RSS | `oom_score` |
| --- | --- | ---: | ---: | ---: | ---: |
| `leaky` | `reservations.memory: 900Mi` | 900 MiB | `1000 - 13 = 987` | 900 MiB | ~1001 |
| `innocent` | nothing | 0 | `1000 -> 999` | 200 MiB | ~1002 |

`leaky` is the one that overran, but its reservation pulled its adjustment down
by more than its extra RSS pushed its score up, so **`innocent` is killed**.
Note also that `nodeMemoryCapacity` is the denominator: on a large node every
adjustment compresses toward 1000 and the RSS term shrinks with it, so the
scores converge and the choice becomes close to arbitrary.

`dind-engine` is a partial exception. It is a native sidecar
(`restartPolicy: Always`), and the kubelet clamps a sidecar's adjustment to no
higher than that of the least-protected regular container. Killing dockerd would
take every inner container with it, so this is the behaviour Harbor wants.

**To avoid this entirely**, declare `deploy.resources.limits.memory` on the
services that can misbehave. A container with its own limit is OOM-killed inside
its own cgroup, by itself, before the Pod ceiling is ever reached.

[oom-policy]: https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/qos/policy.go


> [!IMPORTANT]
> Node pools and ComputeClasses are mutually exclusive: they emit conflicting
> node selectors (`cloud.google.com/gke-nodepool` and
> `cloud.google.com/compute-class`), so resolving a node pool disables
> ComputeClass resolution entirely. Placement selectors are resolved in this order:
>
> **Node pool (`cloud.google.com/gke-nodepool`) vs. ComputeClass (`cloud.google.com/compute-class`):**
> 1. `task_node_pools` entry matching the task name, or its basename
> 2. `node_pool`
> 3. `task_compute_classes` entry matching the task name, or its basename
> 4. On GKE Autopilot only: tasks that request GPUs or TPUs get no ComputeClass; they use the accelerator node selector instead
> 5. `compute_class`
> 6. On GKE Autopilot only: a `machine_type` / `task_machine_types` pin leaves the ComputeClass unset, so the machine-family selector applies
> 7. On GKE Autopilot only: automatic `Performance` ComputeClass promotion when the estimated aggregate Pod ephemeral storage exceeds 10,240 MiB. The estimate is the `main` storage request, plus 1,024 MiB for each Compose sidecar, plus the 10,240 MiB DinD floor when the task needs a nested Docker daemon
>
> On GKE Standard, only steps 1, 2, 3, and 5 apply.
>
> **Placement conflict checks (`PlacementConflictError`):**
> Each task must resolve to one placement mechanism. A job-wide `compute_class` may be overridden for specific tasks via `task_node_pools` (so a ComputeClass job can route an outlier task to a dedicated pool). Every other conflicting combination raises `PlacementConflictError` before the Pod is created:
> - `node_pool` or `task_node_pools` on an Autopilot cluster
> - Job-wide `node_pool` combined with `compute_class` or `task_compute_classes`
> - The same task listed in both `task_node_pools` and `task_compute_classes`
> - `machine_type` / `task_machine_types` combined with `compute_class` / `task_compute_classes` on the same task
> - `machine_type` / `task_machine_types` combined with a node pool whose probed machine family differs
>
> **Machine family (`cloud.google.com/machine-family`):**
> 1. `task_machine_types` entry matching the task name, or its basename
> 2. `machine_type`
>
> Harbor ships no built-in per-task machine types. If a task needs a specific CPU family (for example, prebuilt binaries that require AVX2 or AVX-512), pin it explicitly (`n2` or `n2-standard-4`). This emits `nodeSelector: {"cloud.google.com/machine-family": "n2"}` while Pod CPU/memory requests and limits enforce the size floor. On a GKE Standard cluster without node auto-provisioning, if the probed node pool inventory has no pool in the requested family (or only pools with fewer vCPUs than the requested size suffix), the trial fails immediately with `UnsatisfiableMachineTypeError` instead of pending until timeout.

#### DinD ephemeral storage

Compose tasks that need a nested Docker daemon (Shapes B and C) run a `dind-engine`
container with an `emptyDir` (or generic ephemeral volume when `scratch_volume_size` is set) mounted at `/var/lib/docker`. Everything the inner
daemon materialises lands there and is charged against the node's ephemeral
storage, so `dind-engine` carries an `ephemeral-storage` **request**. Without it
the scheduler treats the Pod as needing no disk, places it on a node too small
to hold the images, and the kubelet evicts it once the images are unpacked —
typically well into the trial.

When `dind_storage_mb` is not set, the reservation is estimated as:

```
max(10 GiB, 3.0 × Σ compressed image sizes + task storage budget)
```

- **`Σ compressed image sizes`** is read from each DinD service's OCI manifest
  (`layers[].size`). This costs no extra network traffic: the manifest is already
  fetched to recover the image's `ENTRYPOINT`, `CMD`, and other config that
  `docker import` discards.
- **`3.0`** is the measured ratio between an image's on-disk footprint under
  overlay2 and its compressed size. Observed range is 2.09× (large images, whose
  data blobs compress poorly) to 3.02× (small images), including a consistent
  1.05-1.065× from 4 KiB block rounding. The high end is used deliberately:
  over-reserving fails at admission, immediately and legibly, whereas
  under-reserving evicts the Pod after the work is already spent.
- **The task storage budget** is the task's own declared `storage_mb`, which is
  writable scratch for the inner containers. It is *added* rather than compared,
  because image materialisation and writable scratch are separate quantities.
  Taking `max()` would silently discard the smaller one.

No `ephemeral-storage` **limit** is set on `dind-engine`. Consistent with
Harbor's task-wide resource model, the task budget caps the Pod; a per-container
limit would evict `dind-engine` at that exact figure even when the node has room
to spare.

> [!WARNING]
> A Compose service declared with `build:` and no `image:` is built inside the
> Pod and has no registry manifest, so its footprint cannot be measured. Harbor
> logs a warning naming the service and falls back to the 10 GiB floor plus the
> task budget. If such an image is large, set `dind_storage_mb` or
> `task_dind_storage_mb` for that task.

On **GKE Standard** (recommended), the `dind-engine` `ephemeral-storage` request is scheduled against the worker node's allocatable ephemeral storage, without a ComputeClass. When `--ek scratch_volume_size` is set, `/var/lib/docker` is backed by a per-Pod Persistent Disk, but `dind-engine` still requests the same `ephemeral-storage` estimate from the node, so the storage pre-check and the scheduler see the same reservation. On **GKE Autopilot**, general-purpose Pods are limited to 10 GiB of ephemeral storage across all containers, so a DinD Pod is promoted to the `Performance` ComputeClass unless a per-task ComputeClass, an accelerator request, a job-wide `compute_class`, or a machine-type pin applies first (see the resolution order above).

Tasks listed in `task_compute_classes` are exempt from `max_storage_request_mb` and
keep their full `task.toml` storage request. See
[Accelerators](accelerators.md) for the sizing model.

The environment raises `RuntimeError` when a task requests both GPU and TPU, when
`gpu_override` is a short alias instead of a full GKE label, or when any accelerator
type is unrecognized.

### Compose and Docker-in-Docker

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `compose_placement` | string | `auto` | Placement strategy: `auto`, `native`, or `dind`. Validated. |
| `compose_mode` | string | `auto` | Legacy alias that seeds `compose_placement` when `compose_placement` is not set. Validated against `auto`, `native`, `dind`. |
| `enable_dind` | boolean | `false` | Selects `compose_placement=dind` when `compose_placement` is not set. Takes precedence over `compose_mode=native`; an explicit `compose_placement` takes precedence over `enable_dind`. |
| `compose_up_timeout_sec` | integer | `max(300, build_timeout_sec // 2)` | Bounds `docker compose up` inside the `compose-up-gate` initContainer, in Shape B and Shape C (DinD) only. Defaults to `300` when `[environment].build_timeout_sec` is `600`. |

> [!NOTE]
> These options govern *execution* placement only. The image pre-builder ignores them: a Docker-in-Docker task needs the same registry images as a native one, because Shape B and Shape C replace each sidecar `build:` with a pre-built `image:` URL.

### Networking

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `allow_metadata_server` | boolean | `false` | Controls egress to the GCE metadata server (`169.254.169.254/32` and `fd00:170::2/128`). When `false` (default), metadata egress is blocked in all network modes. When `true`, `public` mode creates no `NetworkPolicy` (and deletes any existing policy on update), while `allowlist` mode adds an explicit TCP port 80 egress rule for `169.254.169.254/32`. |
| `enable_fqdn_network_policy` | boolean | Auto-detected | Forces or disables `FQDNNetworkPolicy` support instead of probing the cluster. |
| `dns_egress_extra_cidrs` | string or list | `[]` | Comma-separated or list of additional CIDRs allowed on UDP/TCP port 53 alongside the default GKE DNS egress rule. |
| `network_policy_settlement_sec` | float | `2.0` | Pause after updating or deleting a `NetworkPolicy` on an already-running Pod (`_pod_ready=True`) so dataplane rules take effect before commands run. Not applied when creating the initial policy before the Job/Pod starts. Set to `0` to skip. |

See [Networking and security](networking-and-security.md) for the full policy model.

### Timeouts and deadlines

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `pod_ready_timeout` | integer | `max(1200, build_timeout_sec)` | Seconds to wait for the Pod to become ready. Defaults to `1200` unless `[environment].build_timeout_sec` is larger. |
| `active_deadline_seconds` | integer | Computed | Hard override for `spec.activeDeadlineSeconds`. Bypasses the computation entirely. |
| `deadline_buffer_minutes` | integer | `15` | Slack added to the computed deadline. |
| `default_agent_timeout_minutes` | integer | `1440` (24h) | Fallback agent timeout per step (in minutes) when `[agent].timeout_sec` is omitted from `task.toml`. |
| `timeout_multiplier` | float | Harbor trial `timeout_multiplier`, else `1.0` | Multiplies every computed timeout budget. |
| `agent_timeout_multiplier` | float | Harbor trial `agent_timeout_multiplier`, else `timeout_multiplier` | Multiplies the agent budget only. |
| `verifier_timeout_multiplier` | float | Harbor trial `verifier_timeout_multiplier`, else `timeout_multiplier` | Multiplies the verifier budget only. |
| `agent_setup_timeout_multiplier` | float | Harbor trial `agent_setup_timeout_multiplier`, else `timeout_multiplier` | Multiplies the setup budget only. |
| `agent_timeout_sec` | number | Harbor trial `agent.override_timeout_sec`, else `task.toml` | Replaces the agent budget of every step (`[agent].timeout_sec`, or `[steps.agent].timeout_sec` in each `[[steps]]` entry). |
| `verifier_timeout_sec` | number | Harbor trial `verifier.override_timeout_sec`, else `task.toml`, else `600` | Replaces the verifier budget of every step (`[verifier].timeout_sec`, or `[steps.verifier].timeout_sec` in each `[[steps]]` entry). |
| `agent_setup_timeout_sec` | number | Harbor trial `agent.override_setup_timeout_sec`, else `360` | Setup budget per step. |

The Pod deadline is computed as:

```text
max(60, Σ_steps(setup + agent + verifier) + deadline_buffer_minutes × 60)
```

When a task omits `[agent].timeout_sec`, `default_agent_timeout_minutes` (default `1440` minutes = 24 hours) is used so Pods always receive a bounded `activeDeadlineSeconds`.

Each agent and verifier budget is capped by the Harbor trial's `agent.max_timeout_sec`
or `verifier.max_timeout_sec` (when set) before its multiplier is applied. The
multiplier and override options are read with an "or" fallback, so a value of `0` is
treated as unset. These options only size `activeDeadlineSeconds`.

### Execution and labelling

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `decoupled` | boolean | `false` | Detaches supervised commands and polls for output instead of holding a live stream. |
| `run_id` | string | — | Tag propagated to Pod labels for grouping a benchmark batch. |
| `max_concurrent_pods` | integer | — | Per-cluster cap on in-flight trial Pods in the process, enforced by `ClusterAdmissionController` together with its probed CPU-capacity gate. Environments that target the same cluster share one controller, and the most recent non-empty value applies. The value isn't validated; `0` or a negative value admits one Pod at a time. |

Use `decoupled=true` for very long-running commands when WebSocket stream
longevity is the main failure mode. It doesn't reduce control-plane load:
each status poll is a separate exec. See
[Runtime](runtime.md#mode-b-decoupled-polling).

### Options set programmatically

`task_config` and `task` are internal test hooks. Harbor doesn't pass them, and you
don't need to set them. When neither is set, which is the case in normal runs, the
deadline is computed from the `task.toml` file in the environment directory or its
parent directory.

### Removed options

| Option | Status |
| :--- | :--- |
| `sidecar_mode` | Removed. Previously selected a sidecar topology; the package now always uses native Kubernetes sidecars. |
| `mirror_external_images` | Removed. The Cloud Build rebuild-based mirror broke digest-pinned references and was never wired into `Dockerfile FROM` or Compose service image resolution. Pull-through caching via Artifact Registry remote repositories is on the roadmap — see [Images](images.md). |

## Cloud Build plugin options

Pass these with `--plugin-kwarg` (or `--pk`). Load the plugin with
`--plugin harbor_gke_ext:CloudBuildPlugin`. When a job loads more than one plugin,
prefix each key with the plugin import path, for example
`--pk harbor_gke_ext:CloudBuildPlugin.concurrency=50`.

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `concurrency` | integer | `25` | Maximum parallel Cloud Build submissions. |
| `timeout_sec` | integer | `10800` | Per-image build timeout. |
| `build_attempts` | integer | `3` | Total build attempts per image (including the first), with exponential backoff from 5 s to 60 s between attempts. Values below `1` are treated as `1`. |
| `project_id` | string | Resolved | Google Cloud project. |
| `location` | string | Resolved | Cloud Build region. |
| `region` | string | — | Alias for `location`. |
| `registry_name` | string | `harbor-tasks` | Artifact Registry repository. |
| `registry_location` | string | Resolved region | Artifact Registry region. |
| `force_build` | boolean | `false` | Rebuilds even when the registry already has the image. |
| `cloud_build_machine_type` | string | — | Cloud Build machine type. |
| `cloud_build_disk_size_gb` | integer | — | Cloud Build disk size. A value of `0` is treated as unset. |
| `cloud_build_worker_pool` | string | — | Private worker pool: a full resource name, or a pool ID that is expanded to the build project and Cloud Build region. |
| `private_pool` | string | — | Alias for `cloud_build_worker_pool`. |
| `fail_on_incomplete` | boolean | `false` | Raises `RuntimeError` if any image fails to pre-build. |

The plugin accepts only the keys in this table. Any other `--pk` key (except keys that
start with `_`) is ignored and logged as a warning:
`Unrecognized CloudBuildPlugin kwargs (--pk) ignored: ...`. For example,
`--pk worker_pool=...` is ignored; use `--pk cloud_build_worker_pool`,
`--pk private_pool`, or `--ek worker_pool` instead.

> [!WARNING]
> As with `GKEEnvironment`, setting a worker pool (`cloud_build_worker_pool` or `private_pool`) together with `cloud_build_machine_type` raises `ValueError`.

### Resolution precedence

Most plugin options fall back to the matching `--ek` value, so you can configure the
plugin and the environment from one place.

| Setting | Precedence |
| :--- | :--- |
| Project | `--plugin-kwarg project_id` → `--ek project_id` → `$GOOGLE_CLOUD_PROJECT` / `$CLOUDSDK_CORE_PROJECT` / `$GCP_PROJECT` → active GKE `kubectl` context → `gcloud config get-value project` |
| Region | `--plugin-kwarg location` or `region` → `--ek location` → `--ek region` → `--ek zone` → active GKE `kubectl` context (**Required** if no GKE context is active). The plugin checks `--ek region` before `--ek zone`, while `GKEEnvironment` checks `zone` before `region`. If you set both to values in different regions, set `--ek location` so the Pods and the images resolve to the same region. |
| Registry name | `--plugin-kwarg registry_name` → `--ek registry_name` → `harbor-tasks` |
| Registry location | `--plugin-kwarg registry_location` → `--ek registry_location` → resolved region |
| Worker pool | `--plugin-kwarg cloud_build_worker_pool` → `--plugin-kwarg private_pool` → `--ek cloud_build_worker_pool` → `--ek private_pool` → `--ek worker_pool` |
| Machine type | `--plugin-kwarg cloud_build_machine_type` → `--ek cloud_build_machine_type` |
| Disk size | `--plugin-kwarg cloud_build_disk_size_gb` → `--ek cloud_build_disk_size_gb` |
| Fail on incomplete | `--plugin-kwarg fail_on_incomplete` → `--ek prebuild_fail_on_incomplete` |
| Force build | `--plugin-kwarg force_build` → `harbor run --force-build` |

> [!NOTE]
> The environment option that controls plugin failure behavior is named
> `prebuild_fail_on_incomplete`, not `fail_on_incomplete`. The two names are not
> interchangeable.

## Standalone pre-build CLI

The package installs a `harbor-gke-ext-prebuild` console script for building a
dataset ahead of time, outside a Harbor run.

```bash
harbor-gke-ext-prebuild /path/to/dataset \
  --project-id "${PROJECT_ID}" \
  --location "${LOCATION}" \
  --concurrency 25
```

| Flag | Default | Description |
| :--- | :--- | :--- |
| `dataset_dir` (positional) | Required | Root directory. Scanned recursively for `task.toml`. |
| `--project-id` | `$GOOGLE_CLOUD_PROJECT` / `$CLOUDSDK_CORE_PROJECT` / `$GCP_PROJECT`, then active GKE `kubectl` context, then `gcloud` | Google Cloud project. |
| `--location`, `--region` | Active GKE `kubectl` context (**Required** if none) | Cloud Build region or zone. Zones are reduced to their parent region. |
| `--registry-location` | Derived region | Artifact Registry location. |
| `--registry-name` | `harbor-tasks` | Artifact Registry repository. |
| `--concurrency` | `25` | Maximum parallel Cloud Builds. |
| `--timeout-sec` | `10800` | Per-image build timeout. |
| `--machine-type` | — | Cloud Build machine type. |
| `--worker-pool`, `--private-pool` | — | Private worker pool: a full resource name, or a pool ID that is expanded to the build project and Cloud Build region. |
| `--disk-size-gb` | — | Cloud Build disk size. |
| `--force-build` | `false` | Rebuilds regardless of cache. |

> [!NOTE]
> `--location` here is the **Artifact Registry and Cloud Build** region or a GCP zone (which is automatically reduced to its parent region, e.g. `us-central1-a` -> `us-central1`).

The CLI exits `1` if configuration or argument validation fails (for example, if `--project-id` or `--location` cannot be resolved, or if `--worker-pool` and `--machine-type` are both set). The CLI doesn't check that `dataset_dir` exists: a missing or empty directory yields zero tasks, and the CLI exits `0` after it reports `Identified 0 unique images`. Once image builds start, the CLI prints an `N/M images ready` summary and exits `0` regardless of individual build failures; it has no equivalent of `fail_on_incomplete` or `build_attempts`.

> [!NOTE]
> For the same task directories, the CLI and the plugin plan the same images, except
> that the CLI has no `--extra-docker-compose` option, so it doesn't include images
> from Compose overlays that a Harbor run adds with that flag. The CLI discovers tasks
> by scanning `dataset_dir` for `task.toml`, while the plugin uses the job's task list.
> They also differ in error handling and retries: the plugin tries each image up to
> `build_attempts` times and can fail the job through `fail_on_incomplete`, while the
> CLI does neither.

## task.toml fields

The package reads the following fields from each task definition.

| Field | Effect |
| :--- | :--- |
| `[environment].docker_image` | Prebuilt image reference, used verbatim. The task is skipped by the pre-builder entirely: there is nothing to build. `--force-build` overrides this only when the task also ships a `Dockerfile`. |
| `[environment].gpus` | GPU count. Drives accelerator validation and resolves Compose `count: all`. |
| `[environment].gpu_types` | Preferred accelerators. Overridden by `gpu_override`, consulted before `default_gpu_type`. |
| `[environment].tpu` | TPU `type` and `topology`. Mutually exclusive with GPUs. |
| `[environment].cpus`, `memory_mb` | CPU and memory budget for the Pod. |
| `[environment].storage_mb` | Ephemeral storage request for `main`, capped by `max_storage_request_mb`. On GKE Standard, the Pod's peak ephemeral-storage request (including `dind-engine`) is checked before Pod creation against a cluster-wide ceiling: the larger of an estimate from the node pool configuration (every untainted pool with `maxNodes > 0`, plus node auto-provisioning defaults) and the largest allocatable value among live nodes. The check is skipped when the ceiling is unknown, on Autopilot, and when node auto-provisioning is enabled. On Autopilot, `storage_mb` is part of the estimate that drives `Performance` promotion above 10,240 MiB. |
| `[environment].workdir` | Default working directory for `exec` in `main`. Ignored for sidecars. |
| `[environment].env` | Environment variables injected into `main` only. |
| `[environment].network_mode` | Selects the network policy applied to the Pod. |
| `[agent].timeout_sec` | Agent budget. When omitted, falls back to `default_agent_timeout_minutes` (`1440` minutes = 24h) so Pods always receive a bounded `activeDeadlineSeconds`. |
| `[verifier].timeout_sec` | Verifier budget. Defaults to `600`. |
| `[verifier].environment_mode` | Determines whether verifier time counts against this Pod's deadline. |
| `[[steps]]` | Per-step budgets, summed across all steps. |

## Host environment variables

| Variable | Purpose |
| :--- | :--- |
| `KUBECONFIG` | Kubeconfig path. Defaults to `~/.kube/config`. The file must exist: preflight exits with an error that suggests `gcloud container clusters get-credentials` when it is missing. Used to infer `project_id`, `location`, and `cluster_name` from the active GKE context when they are omitted. Harbor connects through a kubeconfig context for the target cluster and runs `gcloud container clusters get-credentials` when no matching context is available. |
| `GOOGLE_CLOUD_PROJECT` | First environment variable fallback for the default project. |
| `CLOUDSDK_CORE_PROJECT` | Second environment variable fallback for the default project. |
| `GCP_PROJECT` | Third environment variable fallback for the default project. |
| `GOOGLE_APPLICATION_CREDENTIALS` | When it points to an existing file, the `gcloud auth list` active-account check is skipped. |

The whole host environment is also copied as the base for `docker compose config`
interpolation, so shell variables referenced by a Compose file resolve as they would
locally.

> [!NOTE]
> Project resolution order differs slightly between `GKEEnvironment` and the build tools:
> - **`GKEEnvironment`**: explicit `--ek project_id` → active GKE `kubectl` context (when `cluster_name` and `location` are omitted or match that context) → `$GOOGLE_CLOUD_PROJECT` → `$CLOUDSDK_CORE_PROJECT` → `$GCP_PROJECT` → `gcloud config get-value project`.
> - **`CloudBuildPlugin`**: `--pk project_id` → `--ek project_id` → `$GOOGLE_CLOUD_PROJECT` → `$CLOUDSDK_CORE_PROJECT` → `$GCP_PROJECT` → active GKE `kubectl` context (when the resolved location and `--ek cluster_name` are omitted or match that context) → `gcloud config get-value project`.
> - **`harbor-gke-ext-prebuild`**: `--project-id` → `$GOOGLE_CLOUD_PROJECT` → `$CLOUDSDK_CORE_PROJECT` → `$GCP_PROJECT` → active GKE `kubectl` context → `gcloud config get-value project`.

## Related pages

* [Cluster setup](cluster-setup.md) — provisioning a cluster and running a first evaluation.
* [Accelerators](accelerators.md) — the sizing model behind `max_storage_request_mb`.
* [Images and builds](images.md) — what the Cloud Build plugin actually does.
* [Troubleshooting](troubleshooting.md) — symptoms and fixes.
