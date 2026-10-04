# Docker-in-Docker

The `harbor-gke-ext` environment provides a fallback Docker-in-Docker (DinD) execution plane to support Docker Compose features that lack native Kubernetes equivalents.

The DinD plane is a **fallback, not the default**. Most Compose files translate into native Kubernetes sidecars (Shape A). The DinD plane is constructed only when a service utilizes an incompatible feature—such as non-numeric users, Docker sockets, or device cgroup rules—or when port collisions occur within the Pod's shared network namespace.

## Two DinD Pod shapes: Shape B (Hybrid) and Shape C (Main-in-DinD)

When the placement classifier (`placement.py`) detects Compose features that Kubernetes cannot execute natively, it selects one of two DinD shapes depending on whether `main` itself needs Docker:

| Shape | What runs in `dind-engine` | What runs as a native Pod container | Typical use case |
| --- | --- | --- | --- |
| **Shape B — Hybrid DinD** (`harbor.dev/compose-placement-shape: "B"`) | Only the sidecars that accumulated DinD reason codes (`harbor.dev/dind-delegated-services`). | `main`, plus any native sidecars. | Multi-container tasks where two sidecars collide on a port, a sidecar uses a named user, or a sidecar is attached to multiple networks (one of which `main` shares), while `main` is a standard agent container. |
| **Shape C — Main-in-DinD** (`harbor.dev/compose-placement-shape: "C"`) | **`main` (container name `main`) and every other Compose service, including one-shot services.** | A lightweight Kubernetes `main` CLI proxy (`docker:28.3.3-dind` running `docker wait main`) that ties the Pod lifetime to the inner `main` container. | Tasks where `main` declares `privileged: true`, mounts `/var/run/docker.sock`, maps non-GPU `devices`, sets unsafe `sysctls`, mounts an external volume, or uses a `DIND_KEYS` field such as `ulimits`. `build:`, `cap_add`, `security_opt`, `network_mode`, `read_only`, and `docker` commands in `command` or `entrypoint` do not select Shape C. |

In both shapes, the set of services running inside `dind-engine` is recorded at Pod-spec creation time on the annotation `harbor.dev/dind-delegated-services` (a sorted JSON array of service names) and stored on `GKEEnvironment._dind_services` (`frozenset[str]`).

## Container roster and init sequence

In Shape B and Shape C, the translator builds an ordered sequence of `initContainers` before the Pod's `main` container starts:

```mermaid
flowchart TD
    subgraph Pod [Kubernetes Pod boundaries]
        I1("harbor-seed (extract bind mounts)")
        I2("Native Init Services & Sidecars (Shape B)")
        DE("dind-engine (restartPolicy: Always, overlay2 on harbor-dind-storage)")
        DC("dind-cache-<service> (docker pull with docker import fallback)")
        CG("compose-up-gate (docker compose up -d --wait --pull never)")
        M("main (native agent container in Shape B; docker wait main proxy in Shape C)")

        VOL1[(harbor-dind-socket)]
        VOL2[(harbor-dind-storage /var/lib/docker)]

        I1 --> I2 --> DE --> DC --> CG --> M
        DE <-->|mounts| VOL1
        DE <-->|mounts| VOL2
        DC -.->|docker pull / import via socket| VOL1
        CG -.->|docker compose up via socket| VOL1
    end
```

1. **`harbor-seed`** (`restartPolicy: None`): Extracts task bind-mount archives into shared Pod volumes. The archive is inlined as base64 when the payload is under 512 KiB, the base64 data is at most 64 KiB (`_SEED_INLINE_MAX_B64_BYTES`), and the script fits within `MAX_ARG_STRLEN`. Otherwise the orchestrator streams the archive and then creates `/harbor/compose-binds/.seed-ready`, which `harbor-seed` waits for.
2. **`dind-engine`** (`DIND_ENGINE_CONTAINER`, `restartPolicy: Always`): Runs `docker:28.3.3-dind`. Before it execs `dockerd`, it copies the static Docker CLI to `/harbor/dind-images/docker-cli` (no Compose plugin is staged; `compose-up-gate` uses the `docker compose` bundled in its own `docker:dind` image), creates host-path volume symlinks (`ln -sfn` from original task paths to `/harbor/pod-vols/<vol_name>`), and writes `/harbor/dind-images/.docker/config.json` with the node service account's Google OAuth2 bearer token for Google registries only (`gcr.io`, `us-docker.pkg.dev`, and any `*.gcr.io` or `*.pkg.dev` host that appears in a DinD service image). A background loop refreshes the token every 900 seconds. A one-shot background waiter mounts `main`'s image layers read-only at `/tmp/harbor-main-rootfs` (and symlinks `/app`) once `compose-up-gate` records them; this happens only in Shape C. `dockerd` then starts with `/var/lib/docker` mounted from `harbor-dind-storage` (an ext4 disk-backed `emptyDir`, or a generic ephemeral volume PVC when `scratch_volume_size` is set), where it auto-selects `overlay2`, and exposes the daemon socket on `unix:///var/run/harbor-dind/docker.sock`. Its `startupProbe` gates downstream initContainers until `docker info` succeeds.
3. **`dind-cache-<service>`** (`DIND_CACHE_CONTAINER_PREFIX`, `restartPolicy: None`): One initContainer per service in `dind_services` (including `dind-cache-main` in Shape C), in `sorted()` order. If the image is already present in `dind-engine`, the step exits. Otherwise it pulls the image with `DOCKER_CONFIG=/harbor/dind-images/.docker DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock /harbor/dind-images/docker-cli pull -q "$IMG"`, and falls back to `tar -cf - --exclude=/proc --exclude=/sys --exclude=/dev --exclude=/harbor --exclude=/var/run --exclude=/run --exclude=/etc/hosts --exclude=/etc/resolv.conf --exclude=/etc/hostname / | docker-cli import --change ... - "$IMG"` if the pull fails.
4. **`compose-up-gate`** (`COMPOSE_UP_GATE_CONTAINER`, `restartPolicy: None`): Verifies all required images exist in `dind-engine`, writes the translated inner Compose file (`/harbor/dind-compose.yaml`, plus `/harbor/dind-compose.gpu.yaml` when a DinD service requests GPUs), and runs `docker compose up -d --wait --pull never`. In Shape C it then waits for `/tmp/env-ready` inside `main` if `/app/entrypoint.sh` references it. If `docker compose up` fails, it dumps `docker compose logs --tail=100` to stderr.
5. **Kubernetes `main` container**: In Shape B, runs the task's native `main` container unmodified; Harbor does not add the DinD CLI to its `PATH` or set `DOCKER_HOST`. In Shape C, runs a lightweight `docker:28.3.3-dind` proxy container with `DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock` whose command is `rc=$(docker wait main 2>/dev/null || echo 1); exit ${rc:-1}`.

## The inner Compose file

To run the delegated services, `compose-up-gate` writes the translated inner `docker-compose.yaml` (`/harbor/dind-compose.yaml`, and optionally `/harbor/dind-compose.gpu.yaml`) before executing `docker compose up`:

- **Networks**: Each DinD service keeps its declared networks and is also attached to a synthesized bridge network, `harbor_dind_net` (the first free `/24` in `172.30.240.0/24`–`172.30.254.0/24`), with a deterministic static IP. Top-level network definitions are preserved. `main`'s hostnames are mapped to that network's gateway through `extra_hosts`. A service with `network_mode: service:<name>` shares its target's network and is not attached separately.
- **Volumes**: Pod volume mounts become bind mounts under `/harbor/pod-vols/<vol_name>`. A `/var/run/docker.sock` mount translates to `/var/run/harbor-dind/docker.sock:/var/run/docker.sock` so nested `docker` commands inside `main` or a sidecar talk to `dind-engine`. Placement rejects a `/run/docker.sock` bind with `ABSOLUTE_BIND`.
- **`depends_on`**: Filtered to services residing within the DinD plane.
- **Build contexts**: Stripped (`build:` removed) and replaced with the pre-built Artifact Registry `image:` URL.
- **Container names**: Assigned deterministically as the bare service name (`container_name: <service>`, e.g. `main`) so `exec_engine.py` and `ComposeServiceOpsMixin` can target them reliably.
- **GPU library and device mappings**: Compose `deploy.resources.reservations.devices` blocks are stripped from `/harbor/dind-compose.yaml`. When GPUs are requested, the service entry receives a `/usr/local/nvidia:/usr/local/nvidia:ro` bind mount and merged `LD_LIBRARY_PATH`, while `compose-up-gate` generates `/harbor/dind-compose.gpu.yaml` mapping the discovered `/dev/nvidia*` devices into the nested container.

## Image materialization (`docker pull` with `docker import` fallback)

Each `dind-cache-<service>` initContainer populates `dind-engine`'s disk-backed `/var/lib/docker` (`harbor-dind-storage`, `overlay2`) using a two-path strategy:

1. **Primary path (`docker pull`)**: `dind-engine` fetches an OAuth2 access token from the metadata server (`http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token`) and writes `/harbor/dind-images/.docker/config.json` with bearer-token auth for Google registries only: `gcr.io`, `us-docker.pkg.dev`, and any `*.gcr.io` or `*.pkg.dev` host that appears in a DinD service image. Each entry names an explicit host; there are no wildcard entries. Each `dind-cache-<service>` initContainer first checks whether the image is already present in `dind-engine` and exits if it is. Otherwise it runs `DOCKER_CONFIG=/harbor/dind-images/.docker DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock /harbor/dind-images/docker-cli pull -q "$IMG"`.
2. **Automatic fallback (`docker import`)**: If `docker pull` fails (for example, local-only image tags, external registries blocked by the Pod's `NetworkPolicy`, or missing Artifact Registry IAM permissions on the Pod's Workload Identity principal), `dind-cache-<service>` falls back to streaming its own root filesystem into `dind-engine` via `tar -cf - --exclude=/proc --exclude=/sys --exclude=/dev --exclude=/harbor --exclude=/var/run --exclude=/run --exclude=/etc/hosts --exclude=/etc/resolv.conf --exclude=/etc/hostname / | docker-cli import --change ... - "$IMG"`. The `--change` flags reconstruct `ENTRYPOINT`, `CMD`, `EXPOSE`, `LABEL`, `STOPSIGNAL`, and `HEALTHCHECK` from the image's OCI config, plus `WORKDIR`, `ENV` (excluding Kubernetes and shell variables such as `KUBERNETES_*`, `HOSTNAME`, and `HOME`), and `USER` (only when the container does not run as UID 0).
3. **Why `docker pull` is the primary path**: On GKE nodes with Image Streaming (`gcfs`), the overlay mount reports `st_nlink = 1` for every inode. When `tar -cf - /` runs over a `gcfs` rootfs, it cannot detect hardlinks and serializes every hardlinked path as an independent copy—for example, an image containing 1,623 hardlinked database snapshots inflates from ~25.6 GiB compressed to ~301 GiB in `/var/lib/docker/overlay2` and can take 45+ minutes to import. `docker pull` downloads the compressed layer tarballs directly from Artifact Registry and unpacks them inside `dind-engine`'s `overlay2` graph driver in seconds, preserving hardlinks and retaining exact OCI image metadata without `--change` reconstruction.

> [!IMPORTANT]
> **Required IAM binding for private Artifact Registry repositories when Workload Identity is enabled:**
> On GKE clusters with Workload Identity Federation enabled (`--workload-pool="${PROJECT_ID}.svc.id.goog"` on Standard, or by default on Autopilot), `gke-metadata-server` intercepts Pod requests to `169.254.169.254` and returns a token for the **Pod's Workload Identity principal** (`principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/<namespace>/sa/<ksa>`), **not** the node's Compute Engine service account (`NODE_SA`).
>
> To ensure `docker pull` inside `dind-cache-<service>` succeeds against your Artifact Registry repository (`harbor-tasks`) instead of failing with `Permission 'artifactregistry.repositories.downloadArtifacts' denied` and falling back to `docker import`, you must:
> 1. First create the cluster with `--workload-pool="${PROJECT_ID}.svc.id.goog"` (or create an Autopilot cluster) so the `workloadIdentityPools/${PROJECT_ID}.svc.id.goog` identity pool exists.
> 2. Grant `roles/artifactregistry.reader` on the `harbor-tasks` repository to that Workload Identity pool:
> ```bash
> PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
> gcloud artifacts repositories add-iam-policy-binding harbor-tasks \
>   --location="${REGION}" \
>   --project="${PROJECT_ID}" \
>   --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/*" \
>   --role="roles/artifactregistry.reader"
> ```
> See [Cluster setup](cluster-setup.md) and [Troubleshooting](troubleshooting.md) for details.

## Resource model and volume topology

DinD Pods distribute the task budget the way Harbor's local Docker environment does: the task budget (`cpus`, `memory_mb`, resolved through `--cpus` / `--memory`) applies to `main`, every other service gets what its Compose definition declares, and a container that declares nothing is bounded only by its host. Docker documents that a container without constraints "can use as much of a given resource as the host's kernel scheduler allows" ([Resource constraints](https://docs.docker.com/engine/containers/resource_constraints/)). On GKE the Pod is that host, so its ceiling must exist: without it, a task that outgrows its declared size takes memory the scheduler gave to other Pods.

The ceiling makes `memory_mb` bound the whole environment, including containers the task starts at runtime through `/var/run/docker.sock`. This is a deliberate difference from Harbor's local Docker environment, which limits only `main` and leaves socket-spawned containers bounded by the machine alone. It matches Harbor's Daytona and Modal DinD sandboxes, which size the whole sandbox from `memory_mb`. A node-sized ceiling for tasks that mount `docker.sock` was considered and rejected; see [Design decisions](design-decisions.md).

- **`main` carries the task budget.** In Shape B, the native `main` container gets the budget as container-level `requests` / `limits`. In Shape C, the inner Compose file sets `deploy.resources` on `main` (legacy `cpus`, `mem_limit` and `mem_reservation` keys are folded in first), overriding whatever the task's Compose file declares there. A ceiling `main` declares for itself survives when the task sets none (`--memory request`); the reservation is then lowered to that ceiling, because Docker refuses a memory reservation above the limit.
- **`dind-engine` holds the Docker daemon and everything it starts.** It requests a daemon baseline (`DIND_ENGINE_BASELINE_MEMORY_MB = 64`, measured: idle `dockerd` + `containerd` held 38.6 MiB of anonymous memory on GKE 1.35.6; `DIND_ENGINE_BASELINE_CPU_M = 100`, nominal) plus every reservation the DinD services declare (`deploy.resources.reservations` or `mem_reservation`; in Shape C that includes `main`'s budget). A declared limit alone reserves nothing, as on a Docker host. `dind-engine` sets **no CPU or memory limit** of its own.
- **The Pod is the host, with a ceiling.** `spec.resources` is set as for every Compose Pod (see [The task-wide resource model](configuration.md#the-task-wide-resource-model)): the request covers the task budget and every container request, and the ceiling is the sum of every container's ceiling (its limit, else its request). `dind-engine` counts as the daemon baseline plus the ceilings of the DinD services (each service's limit, else its reservation), because its cgroup holds all of them. Containers `main` starts through `/var/run/docker.sock` declare nothing, so they count against the task's `memory_mb`: a task that runs them must declare a `memory_mb` that covers them. On a Docker host they would be bounded only by the machine.
- **`singleProcessOOMKill: true` is the recommended kubelet setting for every node that runs DinD.** On cgroup v2 nodes the kubelet sets `memory.oom.group=1` on every container cgroup, so one OOM kill anywhere in `dind-engine` takes down the Docker daemon and every nested container together (measured on GKE 1.35.6). A Docker host kills single processes. `dind-engine` cannot clear the flag itself: containerd keeps it in the container's spec, and runc writes it back on every `UpdateContainerResources` call, which the static CPU manager issues seconds after start. Enable it in the kubelet configuration of the node pools (`singleProcessOomKill: true` in the `--system-config-from-file` file) or ComputeClasses (`singleProcessOOMKill: true` under `nodeSystemConfig.kubeletConfig`) that run DinD tasks; see [Per-process OOM kills for Docker-in-Docker](cluster-setup.md#5-per-process-oom-kills-for-docker-in-docker-singleprocessoomkill). Either way, the Pod ceiling keeps an OOM inside the Pod: neighbours on the node are not affected.
- **Nested containers stay inside the Pod.** A privileged container shares the node's cgroup namespace, so a default `dockerd` would create `/docker/<id>` at the node root: outside the Pod, invisible to the kubelet, and leaked after deletion (measured on GKE 1.35.6). Before anything else, `dind-engine` moves its processes into a `harbor-daemon` leaf of its own cgroup, enables `+cpu +memory +pids` (and any other available controller, best effort) in its `cgroup.subtree_control`, and starts `dockerd --cgroup-parent=<own cgroup>/docker`. Every nested container is then charged to `dind-engine`, bounded by the Pod ceiling, and removed with the Pod. If nesting fails, `dind-engine` exits with `HARBOR_ERROR: dind-engine cannot nest ...` rather than run the workload outside the Pod. A cgroup v2 node is required. gVisor is exempt, since its sandbox already contains everything it starts.
- **Teardown usage report.** Before deleting a DinD Pod, the environment reads `dind-engine`'s cgroup (`memory.current`, `memory.peak`, `memory.oom.group`, `usage_usec`) and the Pod cgroup (`memory.max`, hierarchical `oom_kill` and `oom_group_kill` from `memory.events`, and Pod-ceiling `oom` from `memory.events.local`) and logs them at INFO, including the OOM kill mode in effect (`per process` or `whole container`). Each kind of OOM kill gets its own WARNING, because each has a different fix: the Pod ceiling was reached (the task used more memory than it declares; raise `memory_mb` in `task.toml` or pass `--override-memory-mb`), a container reached its own Compose-declared limit (the same happens on Docker), or the kernel killed whole containers (enable `singleProcessOOMKill`). When the cgroup cannot be read (exec fails because `dind-engine` has exited, or hangs because it is out of memory), the report always logs a WARNING that the usage is unknown and falls back to the Pod status. containerd marks a container `OOMKilled` when any process in it was OOM-killed, so the report also reads the exit code: `dind-engine` `OOMKilled` with exit code 137 means the Docker daemon itself was killed, with every container it ran; `OOMKilled` with any other exit code means processes inside the container were OOM-killed but the container was not. Each read is bounded to 10 seconds and never blocks deletion.
- **Ephemeral storage sizing (`DIND_IMAGE_EXPANSION_RATIO = 3.0`, `DIND_STORAGE_FLOOR_MB = 10240`)**: When `dind-engine` is active, it requests `ephemeral-storage` equal to 3.0 × the sum of the compressed OCI layer sizes of the DinD service images, plus the task's storage budget, with a floor of `10240Mi` (10 GiB). This covers unpacked layers in `/var/lib/docker/overlay2`. The value is a request on `dind-engine` only, with no limit. Override it with `--ek dind_storage_mb=<MiB>` or per task with `--ek task_dind_storage_mb`.
- **Zero `hostPath` volumes**: Every volume is either a node `emptyDir` or, when `--ek scratch_volume_size=<size>` (e.g. `100Gi`) is set, a Kubernetes generic ephemeral volume (`ephemeral.volumeClaimTemplate`) backed by a per-Pod GCE Persistent Disk (`harbor-dind-storage` mounted at `/var/lib/docker`). With `scratch_volume_size`, `dind-engine` still requests the full `ephemeral-storage` estimate from the node, so the PD does not reduce the node reservation: the storage pre-check, the scheduler, and Autopilot `Performance` promotion account for `dind-engine` the same way as without it.

## Service transport (`dind_services` routing)

Remote execution and file transfers for DinD-delegated services (`GKEEnvironment._dind_services`) use two paths depending on the caller:

- **Primary trial execution and file transfers (`connect_exec_stream()` in `exec_engine.py`)**: When `env.exec()`, `upload_file()`, `upload_dir()`, `download_file()`, or `download_dir()` targets a container name in `dind_services` (such as `"main"` in Shape C), `connect_exec_stream()` opens the Kubernetes WebSocket exec stream against the Pod's `main` container and wraps the command in `docker exec [-i] [-t] <service> <command>` (targeting the bare `<service>` container name).
- **Per-service Compose operations (`ComposeServiceOpsMixin` / `_GKENativeComposeServiceTransport`)**: Helper methods that target named Compose services directly run against `dind-engine`. `service_exec()` runs `docker --host=unix:///var/run/harbor-dind/docker.sock exec [-w <workdir>] [-u <user>] [-e KEY=VAL ...] <service> sh -c ...`. `service_download_file()` and `service_download_dir()` copy out of `<service>` with `docker cp` into a staging path in `dind-engine`, download it, and remove the staging path. `stop_service()` freezes the service with `docker kill --signal=STOP`. There are no per-service upload methods.
- **Pod-rename immunity**: Because `GKEEnvironment._dind_services` is populated from the Pod spec annotation `harbor.dev/dind-delegated-services` before the Job creates the Pod, Job controller name suffixes and Spot preemption Pod replacements (`_reresolve_pod_name`) never disrupt DinD routing.

## Autopilot compatibility and capability probing

GKE Autopilot admits privileged containers only when a matching `WorkloadAllowlist` is installed on the cluster. Harbor's `dind-engine` is not a GKE partner workload, so it needs a customer-owned allowlist.

When evaluating cluster capabilities (`probe_cluster_via_gcloud()`, which calls `parse_gcloud_cluster_describe()` and `evaluate_autopilot_dind_capability()` in `cluster_probe.py`), the environment inspects the GKE cluster control-plane metadata (`autopilot.enabled`, `currentMasterVersion`, and `autopilot.privilegedAdmissionConfig.allowlistPaths`) and returns one of two `DindAvailability` states:

- `DIND_AVAILABLE`: All GKE Standard clusters, or GKE Autopilot clusters running version `>= 1.35` with at least one `allowlistPaths` entry configured under `autopilot.privilegedAdmissionConfig`.
- `DIND_UNAVAILABLE`: GKE Autopilot clusters running version `< 1.35`, Autopilot clusters without `allowlistPaths` configured, or Autopilot clusters whose control-plane metadata cannot be inspected.

If a task requires the DinD plane (`Shape B` or `Shape C`) and the probe returns `DIND_UNAVAILABLE`, the placement classifier raises a fatal `UnsupportedComposeFeatureError` with reason code `AUTOPILOT_DIND_UNAVAILABLE`.

> [!IMPORTANT]
> The `harbor-gke-ext` package **never** applies a cluster-wide `WorkloadAllowlist` itself—that is the cluster operator's responsibility. Customer-owned allowlists are available only to eligible Google Cloud customers. See [Enabling privileged DinD with a `WorkloadAllowlist`](autopilot.md#enabling-privileged-dind-with-a-workloadallowlist) for the procedure.

## Forcing and disabling the plane

You can control placement behavior via environment overrides (`--ek`):

- `--ek compose_placement=native`: Forces all services to run natively. If `main` requires DinD features, placement fails with `MAIN_NEEDS_DIND:<reason>`; if a sidecar requires DinD features, placement fails with `NATIVE_PLACEMENT_REJECTED:<service>:<reasons>`.
- `--ek compose_placement=dind`: Forces all sidecars into the DinD plane (`EXPLICIT_PLACEMENT_DIND`), regardless of whether they need it.
- `--ek enable_dind=true` or `--ek compose_mode=dind`: Legacy aliases for `compose_placement=dind`. They apply only when `compose_placement` is not set; an explicit `compose_placement` always wins. (`--ek compose_mode=native` is likewise a legacy alias for `compose_placement=native`.)

By default (`compose_placement=auto`), services are placed dynamically.

## Timeouts and failure modes

`compose_up_timeout_sec` (default `max(300, build_timeout_sec // 2)`) bounds `docker compose up` inside the **`compose-up-gate`** initContainer. On expiry the gate prints a `HARBOR_ERROR` line and exits 124.

A gate failure surfaces from the outside as an environment startup failure, because the orchestrator is blocked in `_wait_for_pod_ready`. To diagnose it, read the logs of the **`compose-up-gate`** initContainer, which contain the output of `docker compose up` and, on failure, `docker compose logs --tail=100`.

## Cost of the plane

Using the DinD plane introduces structural costs:
- **Additional image pulls**: The `dind-engine` (`docker:28.3.3-dind`) image must be pulled.
- **Cache containers**: One extra `dind-cache-<service>` initContainer is launched per delegated service.
- **Storage overhead**: `dind-engine` requires disk-backed ephemeral storage (`harbor-dind-storage` at `/var/lib/docker`) for the `overlay2` image store and inner container root filesystems.
- **Startup latency**: The DinD plane creates a serialized startup gate (`dind-engine` start -> `dind-cache-<service>` pulls -> `compose-up-gate`) before `main` can run.

For details on the materialization architecture, see [Design decisions](design-decisions.md).

## Debugging recipes

When a DinD task fails, use the following `kubectl` invocations to inspect the pipeline:

1. **Check which services were delegated**:
   Read the `harbor.dev/dind-delegated-services` annotation on the Pod:
   ```bash
   kubectl get pod <pod-name> -o jsonpath='{.metadata.annotations.harbor\.dev/dind-delegated-services}'
   ```

2. **Inspect the Docker daemon (engine)**:
   ```bash
   kubectl logs <pod-name> -c dind-engine
   ```

3. **Check image materialization**:
   Inspect a specific cache initContainer to see whether `docker pull` succeeded or fell back to `docker import`:
   ```bash
   kubectl logs <pod-name> -c dind-cache-<service_name>
   ```

4. **Review inner Compose up errors**:
   If the environment timed out or the services failed to start:
   ```bash
   kubectl logs <pod-name> -c compose-up-gate
   ```

For further troubleshooting guidance, see [Troubleshooting](troubleshooting.md).

## GPUs inside DinD

GPU passthrough inside the DinD plane is supported for both Shape B and Shape C.

Because the standard `docker:28.3.3-dind` image lacks the NVIDIA container toolkit (`nvidia-container-runtime`), Compose-level `deploy.resources.reservations.devices` blocks are stripped from `/harbor/dind-compose.yaml`. Instead:
1. `dind-engine` holds the Pod's `nvidia.com/gpu` resource allocation so the GKE NVIDIA device plugin mounts `/usr/local/nvidia` into it, and it sees the `/dev/nvidia*` device nodes because it is privileged. When at least one DinD service requests a GPU, `dind-engine` runs a non-failing probe before it execs `dockerd` and writes three files to the shared `harbor-dind-gpu` volume: `/harbor/gpu/driver-ok` (`1` when `/usr/local/nvidia/lib64` exists and is non-empty), `/harbor/gpu/devices` (the discovered device nodes), and `/harbor/gpu/diag.txt` (diagnostics).
2. The translator adds a `/usr/local/nvidia:/usr/local/nvidia:ro` bind mount and prepends `/usr/local/nvidia/lib64` to `LD_LIBRARY_PATH` on the inner Compose service definition in `/harbor/dind-compose.yaml`. `PATH` is not changed.
3. `compose-up-gate` prints `/harbor/gpu/diag.txt`, then fails with a `HARBOR_ERROR` line if `/harbor/gpu/driver-ok` is not `1` (the allocation did not reach `dind-engine`) or if `/harbor/gpu/devices` is empty. Otherwise it writes `/harbor/dind-compose.gpu.yaml` with explicit `devices:` mappings (`/dev/nvidia0`, `/dev/nvidiactl`, `/dev/nvidia-uvm`, etc.) and passes `-f /harbor/dind-compose.gpu.yaml` to `docker compose up`.

For complete details on hardware accelerator provisioning, see [Accelerators](accelerators.md).

## Related documentation

- [Configuration reference](configuration.md)
- [Architecture](architecture.md)
- [Compose translation](compose-translation.md)
- [Networking and security](networking-and-security.md)
- [Accelerators](accelerators.md)
- [Troubleshooting](troubleshooting.md)
- [Design decisions](design-decisions.md)
