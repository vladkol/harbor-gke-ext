# Known issues (v0.1.0)

This page lists known behavioral limitations in `harbor-gke-ext` `0.1.0` and their workarounds. For architectural boundaries (such as unsupported Docker Compose keys or single-host TPU constraints), see [README — Limitations](../README.md#limitations) and [Compose translation](compose-translation.md).

---

## Storage and scheduling

### `--ek scratch_volume_size` does not lower the `dind-engine` node `ephemeral-storage` request

- **Behavior:** Passing `--ek scratch_volume_size=<size>` (for example, `100Gi`) replaces the `emptyDir` backing Compose named volumes and the `dind-engine` `/var/lib/docker` mount with per-Pod generic ephemeral volumes backed by Persistent Disk. However, the `dind-engine` container resource spec still requests its calculated `ephemeral-storage` estimate (3× compressed image layers plus the task's `storage_mb` budget, with a `10 GiB` floor) from the node's boot disk.
- **Impact:** Both the pre-scheduling storage check and the Kubernetes scheduler evaluate the Pod against the node's allocatable boot-disk `ephemeral-storage`, even when `/var/lib/docker` is backed by a separate Persistent Disk.
- **Workaround:** When using `--ek scratch_volume_size` to offload `/var/lib/docker` to Persistent Disk, also pass `--ek dind_storage_mb=10240` (or per-task `--ek task_dind_storage_mb="<task>=10240"`) to keep the `dind-engine` boot-disk `ephemeral-storage` request at the `10 GiB` floor, or provision worker nodes with `500 GB+` boot disks. See [Task sizing and placement](task-sizing-and-placement.md).

### Pre-scheduling ephemeral-storage check is cluster-wide rather than per target node pool

- **Behavior:** Before creating a Pod on a GKE Standard cluster without Node Auto-Provisioning (NAP), `harbor-gke-ext` compares the Pod's total `ephemeral-storage` request against the largest `allocatable["ephemeral-storage"]` across all existing nodes and node pools in the cluster, rather than scoping the check to the specific `--ek node_pool` or `--ek task_node_pools` target. When estimating allocatable storage for a scale-to-zero node pool from its GKE `NodePool` configuration, the estimator models `COS_CONTAINERD` boot-disk partitions and GKE's [local ephemeral storage reservation](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/plan-node-sizes#local_ephemeral_storage_reservation) on `diskSizeGb`, or `localSsdCount` when the pool uses Local SSD-backed ephemeral storage (`ephemeralStorageLocalSsdConfig` or `ephemeralStorageConfig`, before Local SSD filesystem formatting).
- **Impact:** If a cluster has one node pool with large disks and a smaller target node pool pinned via `--ek node_pool`, the pre-check passes based on the larger pool and the Pod remains `Pending` until `pod_Wait` times out or the cluster autoscaler emits `NotTriggerScaleUp`.
- **Workaround:** Size the boot disk (`--disk-size`) of the target worker node pool to accommodate the task's storage request (see [Cluster setup — Storage arithmetic](cluster-setup.md#storage-arithmetic-and-computeclass-resolution)), or keep at least one node active in the target pool so live node `status.allocatable["ephemeral-storage"]` is read directly.

---

## Networking

### Bare IPv6 addresses in `allowlist` mode are normalized with a `/32` prefix

- **Behavior:** When `network_mode = "allowlist"` is configured, bare IP literals in `[environment.network].allowlist` or `--allow-agent-host` that do not include a `/` CIDR suffix have `/32` appended automatically.
- **Impact:** Passing a bare IPv6 literal without a prefix length (for example, `2001:db8::1`) produces `2001:db8::1/32` instead of `/128`, or fails Kubernetes `NetworkPolicy` validation.
- **Workaround:** Always include an explicit prefix length (such as `/128`) when specifying IPv6 literals in `allowlist` or `--allow-agent-host` (for example, `2001:db8::1/128`). Note that the GKE IPv6 metadata server address `fd00:170::2` is recognized and handled as `/128` automatically.

---

## Runtime and Pod lifecycle

### Pod creation capability-error retry attempts `runtimeClassName: gvisor` on all clusters

- **Behavior:** If the Kubernetes API server rejects Pod creation with a message matching `capabilities`, `securitycontext`, `privileged`, `forbidden`, `podsecurity`, or `baseline`, `harbor-gke-ext` retries Pod creation once with `runtimeClassName: gvisor`, regardless of whether the target cluster is Autopilot or Standard.
- **Impact:** On a GKE Standard cluster that does not have a gVisor-enabled node pool (`--sandbox type=gvisor`), if initial Pod creation fails with an admission error matching those keywords, the retried Pod is created with `runtimeClassName: gvisor` and remains `Pending` (`Unschedulable`) until startup timeout.
- **Workaround:** On GKE Standard clusters without a gVisor node pool, inspect the warning log emitted before the retry (`Pod creation rejected by admission controller (...); retrying with runtimeClassName='gvisor'`) to see the underlying admission rejection, or provision a gVisor node pool if gVisor sandboxing is desired.

### `reset_cluster_autopilot_cache()` does not clear the cluster capabilities cache

- **Behavior:** Calling `harbor_gke_ext.client.reset_cluster_autopilot_cache()` clears the cached Autopilot detection flag (`_CLUSTER_AUTOPILOT_CACHE`) but does not clear `_CLUSTER_CAPABILITIES_CACHE` in `harbor_gke_ext.environment`.
- **Impact:** Standard CLI runs (`harbor run`) target a single cluster per process and are unaffected. Long-lived Python processes or custom test harnesses that switch target clusters within the same process and call `reset_cluster_autopilot_cache()` may retain cached `ClusterCapabilities` from the previous cluster.
- **Workaround:** If switching clusters inside a single Python process, also call `harbor_gke_ext.environment._CLUSTER_CAPABILITIES_CACHE.clear()`.

---

## Image building and CLI

### Inline Cloud Build fallback retries deterministic errors

- **Behavior:** When `CloudBuildPlugin` is not enabled and a trial worker builds a missing task image inline during `GKEEnvironment.start()`, the 3-attempt retry wrapper retries all exceptions (`Exception`), including deterministic failures such as a missing `Dockerfile` (`FileNotFoundError`) or invalid build configuration (`ValueError`). By contrast, `CloudBuildPlugin` fast-fails deterministic errors on the first attempt.
- **Impact:** A task with a broken or missing `Dockerfile` running without `--plugin harbor_gke_ext:CloudBuildPlugin` attempts the build 3 times (adding ~6 seconds of backoff) before failing the trial.
- **Workaround:** Use `--plugin harbor_gke_ext:CloudBuildPlugin` when running datasets that require image builds.

### `harbor-gke-ext-prebuild` exits with code `0` when the target directory does not exist

- **Behavior:** Running `harbor-gke-ext-prebuild <dataset_dir>` with a nonexistent path logs an error (`Directory does not exist: ...`) and returns normally with exit code `0`.
- **Impact:** CI scripts invoking `harbor-gke-ext-prebuild` as a standalone step will not fail on a mistyped dataset path unless they check that the directory exists beforehand.
- **Workaround:** Verify that `<dataset_dir>` exists (`test -d "${DATASET_DIR}"`) in shell or CI pipelines before invoking `harbor-gke-ext-prebuild`, or run prebuilds via `harbor run --plugin harbor_gke_ext:CloudBuildPlugin`.

---

## GKE Autopilot

### `exec` admission throughput at high concurrency

- **Behavior:** GKE Autopilot validates every `pods/exec` request through the GKE Warden admission webhook (`warden-validating.common-webhooks.networking.gke.io`) as part of its managed policy enforcement. In oracle calibration runs, throughput plateaued at approximately 35 exec-heavy trials per minute per cluster. Without a custom ComputeClass, `harbor-gke-ext` promotes Pods whose aggregate ephemeral storage exceeds `10 GiB` to the `Performance` ComputeClass (unless an accelerator or a machine-type pin applies), and Autopilot runs `Performance` Pods one per node, so each such trial waits for its own node. Shape B and Shape C (Docker-in-Docker) tasks require a customer-owned `WorkloadAllowlist`.
- **Workaround:** Apply the [`harbor-autopilot` ComputeClass](cluster-setup.md#2-apply-the-harbor-autopilot-computeclass) and pass `--ek compute_class=harbor-autopilot` so Autopilot places several trial Pods on each node. Run with `--n-concurrent 50` to `--n-concurrent 100`, and keep `--ek decoupled` at its default, `false`: [decoupled polling](runtime.md#mode-b-decoupled-polling) runs a separate exec, and a separate admission, for each status poll. For high-concurrency (`--n-concurrent 200+`) or Docker-in-Docker evaluations, a GKE Standard cluster is the better fit. See [Autopilot](autopilot.md) for details.
