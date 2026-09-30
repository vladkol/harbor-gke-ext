# Troubleshooting

This runbook helps you diagnose and resolve operational issues when running evaluation workloads with `harbor-gke-ext`. Entries are organized by subsystem and quote the exact error messages emitted by the environment.

## First triage

By default, `GKEEnvironment.stop()` deletes the trial's Job, Pod, and NetworkPolicies when a trial finishes (or after `ttlSecondsAfterFinished: 120` if the client disconnects). When a Pod fails during startup or execution, `harbor-gke-ext` automatically captures failure summaries and container log tails into the Harbor trial exception and log directory. To keep failed Pods alive in the cluster for interactive inspection, pass `--no-delete` to `harbor run` or `harbor trial start`.

1. **Find the Job and Pod**: Each trial runs as a Kubernetes `batch/v1` Job wrapping a single Pod labeled with `app=sandbox`, `session=<pod-name>`, `environment=<sanitized-task-name>`, and (when `run_id` is set) `run=<sanitized-run-id>`:
   ```bash
   kubectl get jobs -n <namespace> -l app=sandbox --sort-by=.status.startTime
   kubectl get pods -n <namespace> -l app=sandbox,run=<run-id>
   ```
2. **Check container logs**: View logs for the primary container (`main`), init containers (`harbor-seed`, `dind-cache-<service>`, `compose-up-gate`), or sidecars (`dind-engine`):
   ```bash
   kubectl logs -n <namespace> <pod-name> -c main
   kubectl logs -n <namespace> <pod-name> -c dind-engine
   ```
3. **Check Compose placement annotations**: For multi-container Compose tasks, `compose_translator.py` records the selected Pod shape (`A`, `B`, or `C`), per-service DinD delegation reasons, and DinD-routed services directly in Pod annotations:
   ```bash
   kubectl get pod -n <namespace> <pod-name> \
     -o jsonpath='{.metadata.annotations.harbor\.dev/compose-placement-shape}{"\n"}{.metadata.annotations.harbor\.dev/compose-placement-summary}{"\n"}'
   ```

For architectural background, see [Architecture](architecture.md) and [Runtime](runtime.md).

## Provisioning and authentication

### Missing `gcloud`, missing `kubeconfig`, or missing `gke-gcloud-auth-plugin`

**Symptom**

During `GKEEnvironment.preflight()`, setup aborts immediately with `SystemExit` if `gcloud` or `kubeconfig` is missing:

```text
SystemExit: GKE requires the gcloud CLI to be installed. See https://docs.cloud.google.com/sdk/docs/install-sdk
```
```text
SystemExit: GKE requires Kubernetes credentials. Run 'gcloud container clusters get-credentials <CLUSTER> --location <LOCATION>' to configure kubectl, or set the KUBECONFIG environment variable.
```

If `gke-gcloud-auth-plugin` is missing from `PATH`, `preflight()` logs a warning and continues, after which the first Kubernetes API call fails with `HTTP 401 Unauthorized` or an exec-provider error:

```text
WARNING  gke-gcloud-auth-plugin is not found in PATH. Modern GKE clusters (v1.26+) require this plugin to authenticate. If you experience authentication errors, install it via 'gcloud components install gke-gcloud-auth-plugin'.
```

**Cause**

`harbor-gke-ext` requires the Google Cloud SDK (`gcloud`), a populated kubeconfig file (`$KUBECONFIG` or `~/.kube/config`), and `gke-gcloud-auth-plugin` to mint OAuth2 bearer tokens for GKE API server authentication.

**Resolution**

1. Install `gcloud` and the GKE auth plugin:
   ```bash
   gcloud components install gke-gcloud-auth-plugin
   ```
2. Populate `~/.kube/config` for your target cluster:
   ```bash
   gcloud container clusters get-credentials "${CLUSTER_NAME}" \
     --location="${REGION}" \
     --project="${PROJECT_ID}"
   ```
3. Verify cluster access:
   ```bash
   kubectl get nodes --request-timeout='5s'
   ```

## Scheduling and storage

### `FailedScaleUp`, `NotTriggerScaleUp`, or regional quota exceeded on GPU tasks

**Symptom**

When the cluster autoscaler emits a `NotTriggerScaleUp` Warning event for the Pod, the trial fails immediately with:

```text
RuntimeError: Pod cannot be scheduled and the cluster autoscaler will not scale up for it: <event message>. Harbor requested <request summary>.
```

The `<event message>` is the autoscaler's own event text (for example, `max node group size reached`). Otherwise, the Pod stays `Pending` until `pod_ready_timeout` expires, and the trial fails with:

```text
RuntimeError: Pod not ready after <N> seconds
```

Kubernetes events, logged at debug level, report:

```text
[Kubernetes Event] FailedScheduling: 0/2 nodes are available: 2 node(s) didn't match Pod's node affinity/selector.
[Kubernetes Event] FailedScaleUp: Node scale up in zones us-central1-c associated with this pod failed: GCE quota exceeded. Pod is at risk of not being scheduled.
```

**Cause**

1. **Zero or exhausted regional GPU quota:** Your Google Cloud project lacks sufficient regional quota for the requested accelerator (for example, `NVIDIA_A100_GPUS` or `NVIDIA_H100_80GB_GPUS`).
2. **Zonal capacity or node pool maximum:** Physical instances for the requested accelerator are unavailable in the configured zones, or the target node pool has reached `--total-max-nodes`.

**Resolution**

1. Inspect your regional GPU quota:
   ```bash
   gcloud compute regions describe "${REGION}" \
     --project="${PROJECT_ID}" \
     --format="table(quotas:format='table(metric, limit, usage)')" | grep -E "GPU|L4|A100|H100"
   ```
2. Request a quota increase in the Google Cloud Console (**IAM & Admin > Quotas**), or provision an autoscaling GPU node pool for an accelerator type that has quota and capacity in your region. If that type differs from the one the tasks request, apply a static GPU override across all tasks in the run with `--ek gpu_override=<accelerator-label>` (the full GKE accelerator label, such as `nvidia-l4`, not a short alias; see [Accelerators](accelerators.md)):
   ```bash
   uv run harbor run --dataset <dataset> -a <agent> \
     -e harbor_gke_ext:GKEEnvironment \
     --ek project_id="${PROJECT_ID}" \
     --ek location="${REGION}" \
     --ek cluster_name="${CLUSTER_NAME}" \
     --ek gpu_override=<accelerator-label>
   ```

### Pod stays `Pending` without scheduling warnings until the start timeout expires

**Symptom**

`trial.log` repeats `Pod status: Pending (...s elapsed)` with no `FailedScheduling` events after the Pod is placed, and the trial fails when the environment start timeout expires. For a separate verifier environment, Harbor reports this as `VerifierTimeoutError: Verifier execution timed out after N seconds` even though the verifier never started.

**Cause**

The Pod is scheduled but its image has not been pulled yet; the Pod phase stays `Pending` during image pulls. The kubelet pulls a limited number of images at once, so a small image can queue behind several large pulls on the same node. This happens when nodes run without Image Streaming, which forces full pulls of large images.

**Resolution**

1. Confirm the queueing in the node's image pull events. A long "including waiting" time next to a short pull time means the pull was queued:
   ```bash
   gcloud logging read 'jsonPayload.involvedObject.name="<pod-name>" AND jsonPayload.reason="Pulled"' \
     --project="${PROJECT_ID}" --freshness=2d \
     --format="value(timestamp,jsonPayload.message)"
   ```
   ```text
   Successfully pulled image "..." in 9.045s (11m59.019s including waiting). Image size: 126805333 bytes.
   ```
2. Enable Image Streaming at the cluster level so auto-provisioned pools inherit it (see [Cluster setup](cluster-setup.md)).
3. Place datasets with very large images on their own `ComputeClass` or node pool (`--ek compute_class=...` / `--ek task_compute_classes=...`).

### `EphemeralStorageUnschedulableError`: requested ephemeral storage exceeds all cluster nodes (GKE Standard)

**Symptom**

On a GKE Standard cluster without Node Auto-Provisioning (NAP), Pod creation fails immediately in `_assert_ephemeral_storage_schedulable()` before the Job is submitted:

```text
EphemeralStorageUnschedulableError: Task '<task>' requests <requested_mb> MiB of ephemeral storage, but the largest schedulable node in cluster '<cluster_name>' offers <ceiling_mb> MiB -- short by <diff> MiB. The Pod would stay Pending until the trial timed out. Add a node pool with a larger boot disk, or cap the reservation with `--ek task_dind_storage_mb=<task>=<mb>`.
```

**Cause**

On GKE Standard, a default `100 GB` `COS_CONTAINERD` node boot disk (`94.3 GiB` filesystem capacity after OS partitions and `ext4` metadata) provides **`43.8 GiB`** (`44,880 MiB`) of allocatable `ephemeral-storage` after GKE deducts the system reservation (`MIN(35% * D + 6 GiB, 100 GiB) = 41.0 GiB`) and the `10%` eviction threshold (`9.4 GiB`). If a Pod requests more ephemeral storage than any single node in the cluster can provide (through `task.toml` `storage_mb` on `main` or through `dind-engine`'s `/var/lib/docker` storage estimate), Kubernetes leaves the Pod `Pending` until timeout. Before submitting the Job, `harbor-gke-ext` compares the Pod's peak `ephemeral-storage` request with the largest allocatable ephemeral storage of any schedulable node and raises `EphemeralStorageUnschedulableError` instead. That ceiling is the larger of two values:

- An estimate from the cluster's node pool configuration: the allocatable storage implied by the boot disk size (or, for Local SSD-backed ephemeral storage, the Local SSD count) of every untainted node pool whose maximum node count is above zero, including pools that are currently scaled to zero.
- The largest allocatable `ephemeral-storage` among live nodes that are schedulable and have no blocking taint.

The comparison is cluster-wide, not per target node pool. The pre-check is skipped when the ceiling is unknown, on Autopilot clusters, and on Standard clusters with NAP enabled.

**Resolution**

1. Provision a worker pool with larger boot disks. A `500 GB` `pd-balanced` boot disk provides about `339.2 GiB` of allocatable `ephemeral-storage`. For larger disk sizes, see [Storage arithmetic](cluster-setup.md#storage-arithmetic-and-computeclass-resolution) in Cluster setup:
   ```bash
   gcloud container node-pools create harbor-workers \
     --cluster="${CLUSTER_NAME}" \
     --region="${REGION}" \
     --project="${PROJECT_ID}" \
     --machine-type="n2-standard-16" \
     --disk-size=500 \
     --disk-type="pd-balanced" \
     --service-account="${NODE_SA}" \
     --enable-image-streaming \
     --num-nodes=0 \
     --enable-autoscaling --total-min-nodes=0 --total-max-nodes=16
   ```
2. Or cap the ephemeral storage reservation requested on the node boot disk:
   - For a DinD task's `/var/lib/docker` reservation: `--ek task_dind_storage_mb=<task>=<mb>` or `--ek dind_storage_mb=<mb>`.
   - For `main`'s `task.toml` `storage_mb` request: `--ek max_storage_request_mb=40960`.

`--ek scratch_volume_size=<size>` backs Compose named volumes and `harbor-dind-storage` (`/var/lib/docker`) with per-Pod generic ephemeral Persistent Disks (`ephemeral.volumeClaimTemplate`, using the cluster's default storage class). It doesn't change the node reservation: `dind-engine` still requests its full `ephemeral-storage` estimate (3× the compressed image layers plus the task storage budget, at least `10,240 MiB`) from the node, so the pre-check and the scheduler see the same reservation. Only `dind_storage_mb` or `task_dind_storage_mb` lowers that request.

### GKE Warden ephemeral storage ceilings on Autopilot

**Symptom**

Job creation on GKE Autopilot fails with HTTP 400 from the Warden validating webhook:

```text
Failed to create job ...: 400 - Bad Request: admission webhook "warden-validating.common-webhooks.networking.gke.io" denied the request:
GKE Warden rejected the request because it violates one or more constraints:
Total ephemeral-storage requested by containers for workload ... is higher than the nvidia-h100-80gb-1 maximum of '588Gi'.
```

**Cause**

GKE Autopilot caps each Pod's total `ephemeral-storage` request: `10 GiB` for general-purpose, `Balanced`, and `Scale-Out` Pods; up to `56 TiB` for `Performance` and accelerator Pods on GKE `1.29.3-gke.1038000` or later; and, on hardware with Local SSDs, the Local SSD capacity minus GKE's system overhead. A100 (80GB) and H100 (80GB) GPUs always use Local SSDs for ephemeral storage. See [Resource requests in Autopilot](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/autopilot-resource-requests#min-max-requests). While `harbor-gke-ext` automatically promotes non-accelerator Pods above `10 GiB` to `Performance`, requests that exceed the ceiling of an accelerator shape or the `Performance` class are rejected by GKE Warden.

**Resolution**

Clamp the task's `storage_mb` request with `--ek max_storage_request_mb` or lower `storage_mb` in `task.toml`:

```toml
[environment]
storage_mb = 500000  # ~488 GiB
```

## Image and build

### `RESOURCE_EXHAUSTED` in Cloud Build during startup

**Symptom**

Trials fail during image build with:

```text
Quota exceeded for quota metric 'Concurrent builds' in region ...
```

**Cause**

When `CloudBuildPlugin` is not attached (or when a task image was not part of the pre-build plan), `GKEEnvironment` falls back to building the image inline during `start()` with up to 3 attempts (`wait_exponential(multiplier=2, min=5, max=60)`) and no cross-trial concurrency gate. Launching many trials at once without pre-building can exceed the Cloud Build default pool's concurrency limits: `10`–`30` concurrent builds in the `global` region and `5`–`100` concurrent E2 CPUs per region, depending on the project (see [Cloud Build quotas](https://docs.cloud.google.com/build/quotas)).

**Resolution**

1. Attach `CloudBuildPlugin` to pre-build unique task and Compose service images once before trials start, gated by a concurrency semaphore (default `25`):
   ```bash
   --plugin harbor_gke_ext:CloudBuildPlugin --pk concurrency=25
   ```
2. Or pre-build images out-of-band using the standalone CLI:
   ```bash
   uv run harbor-gke-ext-prebuild <dataset_dir> --location="${REGION}" --concurrency=25
   ```
3. The default pool's concurrency limits are system limits and can't be increased. To run more builds at once, use a private Cloud Build worker pool (`--pk private_pool=<pool-resource>` or `--ek cloud_build_worker_pool=<pool-resource>`); private pool CPU quotas are per region and can be increased.

See [Images and builds](images.md) for details.

### Cloud Build step fails with exit code 137

**Symptom**

An image build fails with `FAILURE`, and the build log shows a `RUN` step killed with `exit code: 137` (or `Killed`). Typical steps load large model weights (`torch.load`) or run many parallel compiler processes.

**Cause**

The build ran out of memory on the Cloud Build worker. Without `cloud_build_machine_type` or a private pool, Cloud Build uses its default machine type (`options.machineType` is empty in `gcloud builds describe <build-id>`).

**Resolution**

Build on a larger worker:

```bash
--plugin harbor_gke_ext:CloudBuildPlugin --pk cloud_build_machine_type=E2_HIGHCPU_32
```

When images are built inline during `start()`, pass `--ek cloud_build_machine_type=E2_HIGHCPU_32` instead. If `E2_HIGHCPU_32` is still too small, use a private worker pool configured with a larger machine type (`--pk private_pool=<pool-resource>`).

## Networking

### Metadata server egress blocked (`169.254.169.254` timeout) for Gemini Enterprise Agent Platform

**Symptom**

An agent inside the trial container fails to fetch Application Default Credentials from the Compute Engine metadata server:

```text
google.auth.exceptions.DefaultCredentialsError: Your default credentials were not found.
urllib3.exceptions.ConnectTimeoutError: Connection to 169.254.169.254 timed out.
```

**Cause**

`harbor-gke-ext` blocks egress to `169.254.169.254/32` and `169.254.169.252/32` by default across all network modes (`public`, `allowlist`, and `no-network`) so untrusted task code cannot access the node or Pod service account token. Allowlisting a Google Cloud hostname (such as `aiplatform.googleapis.com`) does **not** automatically unblock the metadata server.

**Resolution**

Explicitly pass `--ek allow_metadata_server=true` (and, in `allowlist` mode, allowlist the target API host):

```bash
--ek allow_metadata_server=true --allow-agent-host aiplatform.googleapis.com
```

If your Workload Identity setup binds a specific Kubernetes Service Account rather than the cluster-wide `principalSet`, also pass:

```bash
--ek service_account_name=<ksa-name>
```

See [Networking and security](networking-and-security.md) for details on per-mode `NetworkPolicy` rules.

### Cluster lacks `FQDNNetworkPolicy`

**Symptom**

Trial initialization fails fast in `apply_network_policy()`:

```text
RuntimeError: Task requires hostname-based network allowlisting, but the target GKE cluster does not support FQDNNetworkPolicy (GKE Datapath V2 / Cilium is required).
```

**Cause**

The task (or `--allow-agent-host`) specifies hostname or wildcard domain entries in `network_mode = "allowlist"`, but the cluster does not have the `FQDNNetworkPolicy` CRD installed (`networking.gke.io/v1alpha1`).

**Resolution**

1. Enable FQDN Network Policy on the cluster (requires GKE Dataplane V2):
   ```bash
   gcloud container clusters update "${CLUSTER_NAME}" \
     --location="${REGION}" \
     --project="${PROJECT_ID}" \
     --enable-fqdn-network-policy
   ```
2. On a GKE Standard cluster created without Dataplane V2 (`--enable-dataplane-v2`), recreate the cluster with both `--enable-dataplane-v2` and `--enable-fqdn-network-policy` (see [Cluster setup](cluster-setup.md)).

## Compose and Docker-in-Docker (DinD)

### Port collisions, static IPs, Docker socket mounts, or multi-network isolation in Compose mode

**Symptom**

A Compose task fails with `UnsupportedComposeFeatureError` (`MAIN_NEEDS_DIND`, `NATIVE_PLACEMENT_REJECTED`, `AUTOPILOT_DIND_UNAVAILABLE`, `STATIC_IP`, or `MAIN_NETWORK_SEGMENTATION`).

**Cause**

- Under `--ek compose_placement=native`, services that require Docker-in-Docker (`PRIVILEGED`, `DOCKER_SOCK`, `PORT_COLLISION`, `MULTI_NETWORK`, `DEVICE_NON_GPU`, `UNSAFE_SYSCTL`, `USER_BY_NAME`, `EXTERNAL_VOLUME`, or a code for a key in `placement.DIND_KEYS`, such as `ULIMITS:ulimits`, `INIT_PID1:init`, or `PIDS_LIMIT:pids_limit`) are rejected with `MAIN_NEEDS_DIND:<reason>` (when `main` requires DinD) or `NATIVE_PLACEMENT_REJECTED:<service>:<reasons>` (when a sidecar requires DinD).
- On GKE Autopilot without a GKE `>= 1.35` privileged `WorkloadAllowlist` (`allowlistPaths`), any task routed to Shape B or Shape C fails with `AUTOPILOT_DIND_UNAVAILABLE`.
- Static IP assignments (`ipv4_address` / `ipv6_address` -> `STATIC_IP`) and sidecars isolated on a network that fails to intersect `main`'s networks (`MAIN_NETWORK_SEGMENTATION`) are unsupported in `placement.py`.

**Resolution**

1. Use `--ek compose_placement=auto` (the default) so `classify_compose_placement()` automatically routes sidecars needing DinD into Shape B and `main` needing DinD into Shape C.
2. Run DinD workloads on a GKE Standard cluster (or configure `autopilot.privilegedAdmissionConfig.allowlistPaths` on a GKE 1.35+ Autopilot cluster).

### `dind-cache-<service>` `docker pull` denied on Artifact Registry (`falling back to rootfs direct-pipe` / `300 GiB+` disk inflation)

**Symptom**

A Shape B or Shape C DinD Pod remains in `Init:2/4` (`dind-cache-<service>`) for 15–60 minutes, consumes hundreds of GiB on `/var/lib/docker`, or times out with `EnvironmentStartTimeoutError`. Inspecting the initContainer logs (`kubectl logs <pod-name> -c dind-cache-<service>`) shows:

```text
harbor: main: docker pull us-central1-docker.pkg.dev/<project>/harbor-tasks/task-<hash>:latest unavailable (Error response from daemon: Head "https://us-central1-docker.pkg.dev/v2/<project>/harbor-tasks/task-<hash>/manifests/latest": denied: Permission 'artifactregistry.repositories.downloadArtifacts' denied on resource '//artifactregistry.googleapis.com/projects/<project>/locations/us-central1/repositories/harbor-tasks' (or it may not exist)...); falling back to rootfs direct-pipe
```

**Cause**

1. On GKE clusters with Workload Identity Federation enabled (`--workload-pool="${PROJECT_ID}.svc.id.goog"` on Standard, or by default on Autopilot), every node pool runs with `workloadMetadataConfig.mode: GKE_METADATA`.
2. While `kubelet` on the host uses the node's Compute Engine service account (`NODE_SA`) to pull images in seconds, `dind-engine` inside the Pod queries `http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token` to populate `/harbor/dind-images/.docker/config.json`. Under `GKE_METADATA`, `gke-metadata-server` intercepts that request and returns a federated token (`ya29.d....`) for the **Pod's Workload Identity principal** (`principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/<namespace>/sa/<ksa>`), **not** `NODE_SA`.
3. If `roles/artifactregistry.reader` was granted only to `serviceAccount:${NODE_SA}` and not to the cluster's Workload Identity pool, `docker pull` inside `dind-cache-<service>` fails with `Permission 'artifactregistry.repositories.downloadArtifacts' denied` and automatically falls back to `tar -cf - / | docker import`.
4. On nodes with GKE Image Streaming (`gcfs`), the overlay rootfs reports `st_nlink = 1` on every file, so `tar -cf - /` serializes every hardlink as an independent full-size file—inflating large images with hardlinked snapshots (such as 27 GB `orca-bench` images) to `300 GiB+` in `/var/lib/docker` and taking 45+ minutes instead of ~60 seconds.

**Resolution**

1. Ensure the cluster's Workload Identity pool (`${PROJECT_ID}.svc.id.goog`) has been initialized by creating the cluster with `--workload-pool="${PROJECT_ID}.svc.id.goog"` (or creating an Autopilot cluster).
2. Grant `roles/artifactregistry.reader` on the Artifact Registry repository (`harbor-tasks`) to the Workload Identity pool `principalSet`:
   ```bash
   PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
   gcloud artifacts repositories add-iam-policy-binding harbor-tasks \
     --location="${REGION}" \
     --project="${PROJECT_ID}" \
     --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/*" \
     --role="roles/artifactregistry.reader"
   ```

See [Cluster setup](cluster-setup.md) and [Docker-in-Docker](docker-in-docker.md) for details.

### `OOMKilled` (exit code 137) in DinD workloads

**Symptom**

A Shape B or Shape C DinD task fails during `dind-cache-<svc>`, `compose-up-gate`, or inner container execution with `OOMKilled` (`exit code 137`) or `TrialContainerLostError`.

**Cause**

In `dind-engine`, `/var/lib/docker` is mounted on the `harbor-dind-storage` volume — a disk-backed `emptyDir` (or a generic ephemeral Persistent Disk when `--ek scratch_volume_size` is set). Unpacked image layers consume ephemeral disk storage, not RAM. However, unlike a local workstation where `docker run` spawns sibling containers under the host daemon's root cgroup, every container inside `dind-engine` (along with `dockerd`, `containerd`, `docker compose`, and kernel page cache during layer extraction) runs inside the **Pod's memory cgroup**.

To prevent tasks with small declared budgets (such as `memory_mb = 2048`) from OOM-killing during `dockerd` startup, `build_pod_level_resources()` enforces a Pod-level memory limit floor of `8,192 MiB` (`DIND_POD_MEMORY_LIMIT_FLOOR_MB = 8192`) while keeping `spec.resources.requests.memory` at the task's declared budget. Tasks that run memory-heavy inner builds or multiple large nested services can still exceed `8 GiB`.

**Resolution**

1. Raise the task's memory budget for the run with `--override-memory-mb <MiB>` (or increase `memory_mb` in `task.toml`):
   ```bash
   uv run harbor run ... \
     -e harbor_gke_ext:GKEEnvironment \
     --override-memory-mb 16384
   ```
2. When `--memory` is in `auto` mode (`ResourceMode.AUTO`), you can also raise the Pod memory limit above the request with `--ek memory_limit_multiplier=<float>`. Note that `memory_limit_multiplier` is ignored when `--memory guarantee` is passed explicitly, and any computed limit below `8192 MiB` has no effect on a DinD Pod because of the `8,192 MiB` DinD floor.

See [Compose translation](compose-translation.md) and [Docker-in-Docker](docker-in-docker.md) for details.

### Minimal or distroless sidecar images without `/bin/sh`

**Symptom**

Command execution on a non-`main` service or image caching in `dind-cache-<service>` fails with exit code 127 or cannot start `sh`.

**Cause**

For `main`, `harbor-gke-ext` wraps commands with `bash -c`; for any container where `container != "main"`, it unconditionally uses `sh -c` and `/bin/sh` so Alpine and BusyBox images work without `bash`. However, distroless or scratch images that do not ship `/bin/sh` at all cannot accept `exec` commands, and in Shape B/C each `dind-cache-<service>` initContainer runs its own image under `sh -c` (requiring `sh`, `tar`, `sed`, `id`, and `head` if the fallback rootfs pipe is used).

**Resolution**

Ensure sidecar images targeted by `exec` or delegated to DinD include a POSIX `/bin/sh` userland (such as `alpine` or `debian-slim` rather than `gcr.io/distroless/static`).

## Exec and transport

### WebSocket idle drops / `GKEExecStreamClosedError`

**Symptom**

An exec call raises `GKEExecStreamClosedError`. For a supervised command (the default for every `exec()` call), the message is:

```text
harbor_gke_ext.constants.GKEExecStreamClosedError: Kubernetes exec stream disconnected and the status of the command in /tmp/harbor_<id> on pod <pod-name> could not be recovered
```

For an unsupervised command, the message is one of:

```text
harbor_gke_ext.constants.GKEExecStreamClosedError: Kubernetes exec stream <pod-name>/<container> ended before the command completed: <reason>
```
```text
harbor_gke_ext.constants.GKEExecStreamClosedError: Kubernetes exec stream <pod-name>/<container> ended without a command status: <reason>
```

**Cause**

Intermediate load balancers, NAT gateways, or API server proxies closed the long-lived WebSocket `exec` connection before the remote process exited and sent its status frame on channel 3.

`harbor-gke-ext` sends 10-second WebSocket pings (`_GKE_EXEC_STREAM_PING_INTERVAL_SEC = 10.0`) and configures OS-level TCP keepalives (`TCP_KEEPIDLE = 10`, `TCP_KEEPINTVL = 5`, `TCP_KEEPCNT = 3`). Every `exec()` call is supervised by default (`supervised=True`), and Harbor never opts out; only internal calls run unsupervised. A supervised command runs inside a background wrapper (started with `setsid` when the image provides it) that records output and exit status in a work directory (`/tmp/harbor_<10 hex characters>`) and a sibling `/tmp/harbor_<id>.status` file. When a supervised WebSocket stream drops mid-command, `GKEEnvironment._poll_decoupled_exec()` (which delegates to `exec_engine.poll_decoupled_exec()`) switches to polling the work directory until the command finishes. Consequently, `GKEExecStreamClosedError` surfaces to the trial only when:
- An unsupervised command's stream is severed, or
- A supervised command's status probe finds neither the work directory nor its `.status` file inside the container (`LOST`).

If the probe finds the work directory but the command's wrapper process is gone without an exit code (`DEAD`), the environment logs a warning and returns exit code `1` instead of raising an error.

For the recovery states and timing, see [Disconnect recovery](runtime.md#disconnect-recovery) in Runtime.

**Resolution**

1. Retry transient stream drops automatically with `--max-retries 2` (`GKEExecStreamClosedError` is not in Harbor's retry exclusion list).
2. For commands that run for hours across proxies that aggressively terminate long-lived WebSockets, enable decoupled polling mode (`--ek decoupled=true`), which launches supervised commands in the background immediately and polls `/tmp/harbor_<id>` instead of holding an open WebSocket stream (note: decoupled mode uses more Kubernetes API `exec` calls per trial and is intended for long-command stream longevity, not for reducing API server QPS at high concurrency):
   ```bash
   --ek decoupled=true
   ```
3. If your client accesses GKE through a Cloud NAT gateway, increase the TCP established idle timeout:
   ```bash
   gcloud compute routers nats update <nat-name> \
     --router=<router-name> --region="${REGION}" \
     --tcp-established-idle-timeout=1200s
   ```

### `DeadlineExceeded`: Pod exceeded `activeDeadlineSeconds`

**Symptom**

The Pod enters `Phase: Failed` with `reason: DeadlineExceeded`, and in-flight or subsequent `exec` calls raise `TrialContainerLostError`:

```text
TrialContainerLostError: Pod <pod-name> was evicted or lost (reason='DeadlineExceeded').
```

**Cause**

Unless you set `--ek active_deadline_seconds`, every Pod created by `harbor-gke-ext` sets `spec.activeDeadlineSeconds` to the sum of the setup timeout (`360s` default), the agent timeout (`task.toml` `[agent] timeout_sec` or `default_agent_timeout_minutes = 1440` min default), the verifier timeout (`600s` default), and `deadline_buffer_minutes` (`15` min default), floored at `60s`. The following qualifiers apply:

- For multi-step tasks, the setup, agent, and verifier budgets are summed across all steps.
- When the verifier runs in a separate environment, the trial Pod omits the verifier budget, and the dedicated verifier Pod omits the agent budget.
- Timeout overrides from the trial config replace the base budgets, timeout multipliers (`--agent-timeout-multiplier`, `--verifier-timeout-multiplier`, and the matching `--ek *_timeout_multiplier` options) scale them, and `max_timeout_sec` caps from the trial config bound the agent and verifier budgets.

Notice that `activeDeadlineSeconds` starts ticking as soon as the Pod is scheduled onto a node, so long image pulls or DinD `dind-cache-*` initialization draw from the buffer. Because `DeadlineExceeded` maps to `TrialContainerLostError`, Harbor retries the trial if `--max-retries` is configured.

**Resolution**

1. Increase the safety buffer (in minutes) to cover slow image pulls or DinD initialization:
   ```bash
   uv run harbor run --dataset <dataset> -a <agent> \
     -e harbor_gke_ext:GKEEnvironment \
     --ek project_id="${PROJECT_ID}" \
     --ek location="${REGION}" \
     --ek cluster_name="${CLUSTER_NAME}" \
     --ek deadline_buffer_minutes=45
   ```
2. Or set an explicit Pod deadline in seconds:
   ```bash
   uv run harbor run --dataset <dataset> -a <agent> \
     -e harbor_gke_ext:GKEEnvironment \
     --ek project_id="${PROJECT_ID}" \
     --ek location="${REGION}" \
     --ek cluster_name="${CLUSTER_NAME}" \
     --ek active_deadline_seconds=7200
   ```
3. Or scale the agent/verifier timeouts with `--agent-timeout-multiplier 2.0` / `--verifier-timeout-multiplier 2.0`.

### Konnectivity tunnel handshake latency (`HTTP 500: No agent available for connection`)

**Symptom**

Immediately after a Pod enters `Running` on a newly provisioned node, an `exec` handshake receives:

```text
ApiException: (500) Reason: Internal Server Error
Message: No agent available for connection
```

**Cause**

The kubelet marks the Pod `Running` before the node's `konnectivity-agent` Pod completes its tunnel registration with the GKE control plane.

**Resolution**

`connect_exec_stream()` automatically retries transient handshake errors (`HTTP 429, 500, 502, 503, 504` and socket `OSError`) up to `15` times with exponential backoff (`2.0s` initial delay, base `2.0`, capped at `20.0s` per step, with ±20% jitter — about `230s` of total wait on average). If all 15 attempts fail:

1. Check `konnectivity-agent` health in `kube-system`:
   ```bash
   kubectl get pods -n kube-system -l k8s-app=konnectivity-agent -o wide
   ```
2. Verify that VPC firewall rules allow egress from worker nodes to the control plane on TCP port `8132`.

See the [README](../README.md) for an overview of the extension.
