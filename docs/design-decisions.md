# Design decisions

This document records the architectural choices underlying `harbor-gke-ext`, including the context, alternatives considered, technical evidence, and operational consequences for each decision.

## Job wrapping a single Pod

**Context**:
Evaluation trials require an ephemeral execution environment that runs until completion or timeout, never restarts mid-trial on container failure, and cleans up automatically if the client disconnects.

**Options considered**:
- Bare `v1/Pod`
- `apps/v1` `Deployment`
- `batch/v1` `Job` wrapping a single Pod

**Decision**:
Each trial runs as a `batch/v1` `Job` with a fixed `ttlSecondsAfterFinished: 120`, wrapping a single Pod with `restartPolicy: Never` and the `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` annotation, which is always set. Its `podFailurePolicy` lets the Job replace a Pod lost to infrastructure, up to `backoffLimit: 3` (`_GKE_JOB_BACKOFF_LIMIT`): the first rule counts Pods with the `DisruptionTarget` condition, and the second fails the Job on any non-zero container exit.

**Evidence**:
A bare Pod has no built-in TTL garbage collector on completion, while a `Deployment` restarts failed Pods indefinitely (`restartPolicy: Always`). A `Job` delegates post-termination cleanup to the Kubernetes TTL-after-finished controller, and its [Pod failure policy](https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-failure-policy) handles disruption without Harbor code. Kubernetes sets [`DisruptionTarget`](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/#pod-disruption-conditions) only for disruptions the Pod did not cause (scheduler preemption, eviction, taint-based deletion, node loss, node-pressure eviction), never for exceeding its own limits, so a task that overruns its resources is not retried. The disruption rule must come first: a disrupted Pod's containers also exit non-zero.

**Consequences**:
A trial maps 1:1 to the Pod that `start()` brought up, and never moves to another Pod. While `start()` waits for readiness it follows a replacement Pod (`_follow_job_replacement_pod`), which is safe because nothing has run yet. After `start()` returns, a lost Pod raises `TrialContainerLostError`; any replacement the Job creates sits idle until `stop()` deletes the Job. With a Pod failure policy, the Job creates a replacement only after the lost Pod is terminal, which includes its termination grace period. If the orchestrator crashes, the Job controller reaps finished resources 120 seconds after the Job finishes, and the `safe-to-evict: "false"` annotation prevents the GKE Cluster Autoscaler from evicting running trials during node scale-down.

## Three Pod shapes: Native (Shape A), Hybrid DinD (Shape B), and Main-in-DinD (Shape C)

**Context**:
Benchmark tasks range from single-container sandboxes to multi-container Docker Compose topologies, plus tasks whose `main` container expects its own Docker daemon (`/var/run/docker.sock`), `privileged: true`, non-GPU device mappings, unsafe sysctls, or Docker-specific cgroup/ulimit settings.

**Options considered**:
- Run every task inside a nested VM or a monolithic Docker-in-Docker (`docker:dind`) container.
- Run every Compose service as a native Kubernetes container and reject tasks that use Docker-specific features.
- Classify each task into one of three Pod shapes (`Shape A`, `Shape B`, `Shape C`) with the minimum necessary privilege and nesting.

**Decision**:
The placement classifier (`placement.py`) inspects the Compose topology and selects one of three Pod shapes per task:

- **Shape A (Native Kubernetes Pod)**: Default for single-container tasks and Compose tasks whose services have non-colliding ports and require no privileged or Docker-only features. `main` runs as the primary Pod container; Compose sidecars run as KEP-753 native sidecars (`initContainers` with `restartPolicy: Always`).
- **Shape B (Hybrid: Native `main` + DinD Sidecars)**: Selected when `main` has no DinD triggers, but one or more sidecars share a container port (from `expose` or a `ports` target) with another service (`PORT_COLLISION`), attach to more than one network in a project that declares more than one top-level network (`MULTI_NETWORK`), or declare DinD-only service keys. A sidecar attached only to networks that `main` isn't on is rejected as fatal (`MAIN_NETWORK_SEGMENTATION`). `main` remains a native Kubernetes container (preserving direct GKE Image Streaming and GPU/TPU device plugin mounts), while delegated sidecars run inside a `dind-engine` sidecar and are started by `compose-up-gate`.
- **Shape C (Main-in-DinD)**: Selected when `main` itself triggers a DinD reason (`dind_reasons`): `privileged: true` (`PRIVILEGED`), a `/var/run/docker.sock` bind mount (`DOCKER_SOCK`), non-GPU `devices` (`DEVICE_NON_GPU`), unsafe `sysctls` (`UNSAFE_SYSCTL`), a mounted external volume or, when the project has no sidecars, an unmounted top-level external volume (`EXTERNAL_VOLUME`), or Docker-specific `DIND_KEYS` (`ulimits`, `init`, `blkio_config`, `cpuset`, `cpu_count`, `cpu_percent`, `cpu_period`, `cpu_quota`, `cpu_rt_runtime`, `cpu_rt_period`, `oom_kill_disable`, `oom_score_adj`, `mem_swappiness`, `memswap_limit`, `pids_limit`, `storage_opt`, `device_cgroup_rules`). Note that `pid: host`, `ipc: host`, `uts`, and `userns_mode` are rejected as fatal (`HOST_NAMESPACE`), whereas `security_opt` is listed in `NATIVE_KEYS`: it is accepted and ignored (not translated) and does not trigger DinD.
  - In Shape C, both `main` and all Compose services run inside `dind-engine`.
  - The `compose-up-gate` init container runs `docker compose up`, optionally polls `docker exec main test -f /tmp/env-ready` if the inner `main` image's `/app/entrypoint.sh` references `/tmp/env-ready`, and exits once the inner topology is up.
  - The Kubernetes `main` container uses the `docker:dind` CLI image to block on the inner container's lifecycle (`rc=$(docker wait main 2>/dev/null || echo 1); exit ${rc:-1}`), mirroring the inner `main` exit code to the Pod.
  - For any service listed in `dind_services` (`harbor.dev/dind-delegated-services`), `connect_exec_stream()` routes interactive/streaming commands by executing `docker exec [-i] [-t] <service> ...` inside the Pod `main` container (which mounts `/var/run/harbor-dind`), while `ComposeServiceOpsMixin` runs service-targeted commands (`docker --host=unix:///var/run/harbor-dind/docker.sock exec ...`) and stages file transfers (`docker cp`) through `dind-engine`.

**Consequences**:
Single-container and standard Compose tasks pay zero DinD overhead (Shape A). Tasks with complex sidecar topologies keep `main` native (Shape B). Tasks that build images or invoke `docker` inside `main` run unmodified (Shape C) while preserving Harbor's `BaseEnvironment` contract.

## Image materialization inside DinD: `docker pull` with `tar | docker import` fallback

**Context**:
In Shape B and Shape C, the inner `dockerd` (`dind-engine`) must acquire the images for every service delegated to DinD before `compose-up-gate` runs `docker compose up`. Because `_apply_network_policy()` creates the Pod's `NetworkPolicy` before the Job/Pod is created, init containers (`dind-engine` and `dind-cache-<service>`) execute under the Pod's configured network policy from the start.

**Options considered**:
- Exporting unpacked layers from the node's `containerd` (`ctr images export | docker load`).
- Always archiving a mounted container's root filesystem (`tar -cf - / | docker import`).
- Attempting `docker pull` first inside `dind-cache-<service>` init containers (using short-lived GCP metadata access tokens for Artifact Registry / GCR images when reachable) and falling back to `tar -cf - / | docker import` when registry egress is unavailable.

**Decision**:
Each DinD-delegated service gets a `dind-cache-<service>` init container running that service's image. `dind-cache-<service>` first attempts `docker pull -q "$IMG"` against `dind-engine` (`DOCKER_CONFIG=/harbor/dind-images/.docker`, populated by `dind-engine`'s background `harbor_refresh_gcr_auth` loop querying `169.254.169.254` every 900s during Pod initialization). At the start of `compose-up-gate`—after all `dind-cache-<service>` init containers have completed and before `docker compose up --pull never` starts any service or `main` container—`compose-up-gate` touches `/harbor/dind-images/.auth-scrubbed` (stopping `dind-engine`'s background refresh loop) and deletes `/harbor/dind-images/.docker` so no cached OAuth token remains on the shared volume while any Compose service or agent container runs, while `dind-engine` installs `unreachable` routes for `169.254.169.254/32` and `169.254.169.252/32` when `allow_metadata_server=False`. If `docker pull` fails (for example, local-only tags), `dind-cache-<service>` falls back to streaming its own mounted root filesystem via `tar -cf - / ... | docker-cli import --change ... "$IMG"`, reconstructing `WORKDIR`, `USER`, `ENV`, and manifest-resolved `ENTRYPOINT`, `CMD`, `EXPOSE`, `LABEL`, `STOPSIGNAL`, and `HEALTHCHECK`. Inside `dind-engine`, `/var/lib/docker` is backed by the `harbor-dind-storage` volume (an ext4 node-disk `emptyDir` by default, or a generic ephemeral PVC when `scratch_volume_size` is set), where `dockerd` automatically selects the `overlay2` storage driver.

**Evidence**:
- **`ctr images export` fails on GKE**: GKE configures `containerd` with `discard_unpacked_layers = true` in `/etc/containerd/config.toml`. Once `containerd` unpacks an image, compressed blobs are deleted from the content store and `ctr images export` fails with `content digest ... not found`.
- **`tar -cf - / | docker import` inflates hardlink-heavy images on GKE Image Streaming (`gcfs`)**: GKE's `gcfs` overlay filesystem reports `st_nlink = 1` for every file, preventing `tar` from deduplicating hardlinks. On images with dense hardlink trees (such as `orca-bench` task `701e915cf705cdf6`, whose 1,623 Prometheus TSDB snapshots share hardlinks across a ~55 GiB logical tree in a 25.6 GiB compressed image), streaming the mounted rootfs via `tar | docker import` expands every hardlink into a separate file (~301 GiB inside DinD `overlay2`). See [Dataset notes](dataset-notes.md#orca-benchorca-bench-verified).
- **`docker pull` preserves layer tarballs, hardlinks, and OCI config**: Pulling the compressed manifest layers directly into `dind-engine` preserves hardlinks across layers (~55 GiB unpacked on disk) and keeps native OCI image metadata (`ENTRYPOINT`, `CMD`, `ENV`, `USER`, `WORKDIR`) without synthetic `--change` reconstruction. When network policy rules block registry or metadata access, the `tar | docker import` fallback ensures offline and `no-network` tasks still materialize their pre-pulled kubelet rootfs into `dind-engine`.

**Consequences**:
When registry egress is permitted, DinD images materialize at their true hardlink-deduplicated size with verbatim OCI metadata, and temporary registry tokens are scrubbed before `main` starts. When network egress is locked down (`no-network` or strict `allowlist`), `dind-cache-<service>` seamlessly falls back to importing the local kubelet-mounted rootfs.

## GPU driver library discovery via `ldconfig` prelude

**Context**:
GKE's NVIDIA device plugin mounts host driver libraries (`libcuda.so.1`, `libnvidia-ml.so.1`) at `/usr/local/nvidia/lib64`, which is not in glibc's default dynamic linker search path (`ld.so`). Many scientific and ML container images also define custom `ENV LD_LIBRARY_PATH` values in their Dockerfile (such as `/opt/conda/lib` or custom CUDA toolkit directories).

**Options considered**:
- Injecting `LD_LIBRARY_PATH=/usr/local/nvidia/lib64` into Kubernetes `container.env`.
- Registering `/usr/local/nvidia/lib64` in `/etc/ld.so.conf.d/harbor-nvidia.conf` and running `ldconfig` in the container startup prelude.

**Decision**:
Direct GPU Pods run an `ldconfig` prelude (`GKE_NVIDIA_LDCONFIG_SNIPPET`) before the container command and never set `LD_LIBRARY_PATH` in `container.env`.

**Evidence**:
In Kubernetes, specifying `env: [{name: LD_LIBRARY_PATH, value: ...}]` on a container spec completely overwrites the image's Dockerfile `ENV LD_LIBRARY_PATH` rather than appending to it, breaking images that rely on image-defined library search paths. Updating `/etc/ld.so.cache` via `ldconfig` makes `/usr/local/nvidia/lib64` visible system-wide without clobbering `ENV LD_LIBRARY_PATH`.

**Consequences**:
`libcuda.so.1` resolves cleanly in glibc-based GPU containers while preserving image-level `LD_LIBRARY_PATH` definitions intact.

The same reasoning applies to `PATH`: the prelude symlinks `/usr/local/nvidia/bin/*` (`nvidia-smi`, etc.) into `/usr/local/bin` rather than setting `PATH` on the Pod spec. The prelude runs only in direct GPU Pods. Compose services that request GPUs run natively (they get `nvidia.com/gpu` requests and limits) unless another DinD reason routes them into Docker-in-Docker; there is no GPU-specific DinD reason code. Native Compose containers don't get the `ldconfig` prelude, because injecting a shell prelude would require wrapping task-owned entrypoints.

## External image caching: Artifact Registry remote repositories over Cloud Build rebuilds

**Context**:
Public benchmark tasks frequently reference base and service images on external registries (`public.ecr.aws`, `ghcr.io`, `mcr.microsoft.com`) that are not directly eligible for GKE Image Streaming (`*.pkg.dev`, `gcr.io`, `docker.io`). An earlier prototype attempted to mirror external images by generating a synthetic `FROM <source>` Dockerfile and submitting a Cloud Build job per image.

**Options considered**:
- Keep the synthetic `FROM <source>` Cloud Build mirror pass.
- Remove the Cloud Build mirror pass and schedule pull-through caching via **Artifact Registry remote repositories** (`--mode=remote-repository`) + `ImageResolver` prefix rewriting.

**Decision**:
The Cloud Build mirror implementation was removed; pull-through caching via Artifact Registry remote repositories is on the roadmap.

**Evidence**:
Analyzing external image references across the Harbor benchmark corpus exposed three structural flaws in the Cloud Build rebuild approach:
1. **Unconsumed mirror builds for nested references**: `Dockerfile FROM` lines and Compose `services.*.image` references cannot consume mirrored tags unless the Dockerfile or Compose manifest itself is rewritten before build/translation.
2. **Broken digest identity**: Most prebuilt `task.toml` `docker_image` references are digest-pinned (`@sha256:...`). Rebuilding an image via `FROM <source>` in Cloud Build generates a new image config and manifest, changing its `sha256` digest.
3. **High registry concentration**: Almost all external non-Docker-Hub references across public benchmarks originate from three upstream registries (`public.ecr.aws`, `ghcr.io`, and `mcr.microsoft.com`). Artifact Registry remote repositories cover these registries with zero Cloud Build jobs, preserve exact upstream digests and multi-arch manifest lists, and are natively eligible for GKE Image Streaming.

**Consequences**:
`CloudBuildPlugin` invokes Cloud Build and Artifact Registry **only** when a task actually builds a Dockerfile (`agent_env`, `verifier`, or `compose_sidecar`), and repository creation (`ensure_artifact_registry_exists`) is gated lazily behind a cache miss on a required build.

## GPU inside Docker-in-Docker

**Context**:
Compose services that request GPUs and are routed to Docker-in-Docker for another reason (or run in a Shape C Pod) require hardware GPU passthrough into containers managed by the inner `dockerd`.

**Options considered**:
- Installing and configuring NVIDIA Container Toolkit (`--gpus all`) inside `dind-engine`.
- Explicit `--device /dev/nvidia*` passthrough paired with a read-only `/usr/local/nvidia` driver mount and an `LD_LIBRARY_PATH` entry for the driver libraries.

**Decision**:
Explicit device and driver passthrough is used. The translated Compose file bind-mounts the driver directory and sets `LD_LIBRARY_PATH` for each GPU-requesting service, and a generated Compose override (`/harbor/dind-compose.gpu.yaml`) adds the `devices` that `dind-engine` discovers at startup.

**Evidence**:
In privileged `docker:dind` containers on GKE GPU nodes, host character devices (`/dev/nvidiactl`, `/dev/nvidia-uvm`, `/dev/nvidia-uvm-tools`, `/dev/nvidia[0-9]*`, and `/dev/nvidia-caps/*`) are present in `/dev`, and the GKE NVIDIA device plugin mounts the matching userspace driver libraries at `/usr/local/nvidia`. Probing `dind-engine` at startup (the `gpu_probe_script` that runs as the `dind-engine` command), bind-mounting `/usr/local/nvidia:/usr/local/nvidia:ro` into GPU-requesting Compose services, passing the discovered devices (`/dev/nvidiactl`, `/dev/nvidia-uvm`, `/dev/nvidia-uvm-tools`, and `/dev/nvidia[0-9]*`), and prepending `/usr/local/nvidia/lib64` to each such service's `LD_LIBRARY_PATH` in its Compose `environment` (merging any existing value) enables full CUDA execution inside nested containers without installing `nvidia-container-toolkit`.

**Consequences**:
GPU-accelerated Compose services work inside DinD on GKE Standard GPU node pools. Whether a non-privileged Autopilot Pod can re-inject these devices inside a gVisor sandbox, and whether TPU passthrough works identically, remain open questions.

## Default GPU count behavior

**Context**:
Docker Compose services can request `count: all` under `deploy.resources.reservations.devices` even when `task.toml` omits `gpus`.

**Options considered**:
- Implicitly hardcode `1` GPU whenever `count: all` appears.
- Require an explicit count via `task.toml` or an explicit operator fallback flag (`--ek default_gpu_count=<int>`).

**Decision**:
When `task.toml` explicitly sets `gpus > 0`, that count resolves `count: all`. When `task.toml` omits `gpus`, the placement resolver uses `--ek default_gpu_count=<int>` if configured, or raises `UnsupportedComposeFeatureError` (`GPU_COMPOSE_ONLY`) if neither is set.

**Consequences**:
Task authors and benchmark operators can either declare `gpus` in `task.toml` or pass `--ek default_gpu_count=1 --ek default_gpu_type=L4` across a dataset where Compose files request `count: all` without `task.toml` GPU metadata.

## gVisor support

**Context**:
Running untrusted agent code on shared multi-tenant clusters requires strong kernel isolation.

**Options considered**:
- Standard `runc` only.
- Optional GKE Sandbox (`runtimeClassName: gvisor`) for native and DinD Pods.

**Decision**:
GKE Sandbox (`--ek runtime_class_name=gvisor`) is supported for native containers and DinD workloads (on GKE `>= 1.35` with `/var/lib/docker` allowlisted on Autopilot).

**Consequences**:
Operators can isolate both single-container and multi-container DinD trials inside gVisor user-space kernels while preserving GKE Image Streaming.

## No `hostPath` volumes and ephemeral scratch storage

**Context**:
Pods require temporary storage for inter-container coordination, DinD image/layer storage (`/var/lib/docker`), and task workspace seeding without violating GKE Autopilot or gVisor admission rules or exhausting fixed node boot disks.

**Options considered**:
- `hostPath` volumes.
- `emptyDir` volumes and Kubernetes generic ephemeral volumes (`ephemeral.volumeClaimTemplate`).

**Decision**:
There are zero `hostPath` volumes in `harbor-gke-ext`. By default, Pod volumes use `emptyDir`. When `--ek scratch_volume_size=<size>` (for example, `50Gi`) is set, bulk non-Harbor storage volumes (`harbor-dind-storage` for `/var/lib/docker` and Compose named volumes) use Kubernetes generic ephemeral volumes (`ephemeral.volumeClaimTemplate`), which dynamically provision per-Pod Persistent Disks through the cluster's default StorageClass (no `storageClassName` is set) and delete them automatically when the Pod terminates. `dind-engine` still requests its full `ephemeral-storage` estimate (3× the compressed image layers plus the task storage budget, at least `10,240 MiB`) from the node, so the storage pre-check, the scheduler, and Autopilot `Performance` promotion see the same reservation with or without `scratch_volume_size`. To lower that reservation, use `dind_storage_mb` or `task_dind_storage_mb`.

**Consequences**:
Avoiding `hostPath` eliminates host-filesystem escape vectors and satisfies Autopilot and gVisor admission policies. `scratch_volume_size` moves DinD image layers and Compose named-volume writes onto per-Pod Persistent Disks, but it doesn't reduce the node `ephemeral-storage` reservation, so the node still needs enough allocatable storage for the `dind-engine` request.

## Network policy timing, cluster enforcement verification, and `public` mode

**Context**:
Trial network access is controlled by three modes (`no-network`, `allowlist`, and `public`), an orthogonal metadata-server flag (`allow_metadata_server`, default `False`), and an ingress isolation flag (`allow_pod_ingress`, default `False`). However, Kubernetes `NetworkPolicy` is a passive API object that is silently ignored on GKE Standard clusters unless GKE Dataplane V2 (`--enable-dataplane-v2`) or Legacy Datapath + Calico (`--enable-network-policy`) is enabled.

**Options considered**:
- Applying `NetworkPolicy` after Pod creation without verifying cluster CNI enforcement.
- Verifying active cluster `NetworkPolicy` enforcement (`detect_network_enforcement()`) before Job creation, failing closed if the cluster has no active policy controller (or if Calico is missing `projectcalico.org/ds-ready=true` on any schedulable node), applying `NetworkPolicy` before Job/Pod creation, and only omitting the policy when both `network_mode="public"` and `allow_metadata_server=True`.

**Decision**:
- `GKEEnvironment.capabilities` and `_verify_network_enforcement()` probe the cluster's actual dataplane enforcer (`DATAPLANE_V2`, `CALICO`, or `NONE`) and fail closed in `start()` whenever `network_mode != PUBLIC` or `allow_metadata_server=False` on an unenforced or partially-ready cluster.
- For Direct Pods and Shape A native Compose Pods, `_apply_network_policy()` runs **before** the Job/Pod is created; for DinD Pods (`Shape B` and `Shape C`), `dind-engine` locks down metadata server routes right before `compose-up-gate` runs `docker compose up` and `_apply_network_policy()` applies the Pod `NetworkPolicy` right after Pod initialization completes. Start-time policies are explicitly deleted during `stop()` via `delete_network_policies()` (whereas policies applied during mid-trial updates attach `ownerReferences` to the running Pod).
- Every generated `NetworkPolicy` sets `policyTypes: ["Ingress", "Egress"]` with `ingress: []` by default (`allow_pod_ingress=False`) to block cross-trial Pod-to-Pod traffic.
- In `public` mode with `allow_metadata_server=False` (the default), `_apply_network_policy()` creates a Pod-scoped `NetworkPolicy` allowing egress to `0.0.0.0/0` except `169.254.169.254/32` and `169.254.169.252/32` (Workload Identity Calico DNAT target), plus UDP/TCP port 53 for GKE cluster/VPC DNS.
- In `public` mode with `allow_metadata_server=True`, `_apply_network_policy()` is skipped at start time (creating no `NetworkPolicy` resource) and calls `delete_network_policies()` if invoked during a mid-trial policy update.

**Consequences**:
Every trial is protected against GKE metadata server token theft (`169.254.169.254`, `169.254.169.252`) and cross-trial Pod probing on both Dataplane V2 and Calico clusters, while DinD Pods still automatically pull their `main` and sidecar images during initialization, and misconfigured clusters without an active `NetworkPolicy` controller fail closed at startup rather than running trials with silently unenforced policies.

## Where the task budget lives: the Pod, not the containers

**Context**:
Agent evaluation tasks exhibit bimodal resource demand: near-zero CPU utilization while waiting on LLM turns, followed by short, intense CPU and memory spikes during compilation and test verification (`tsc`, `esbuild`, `node-gyp`, `jest`, `pytest -n auto`, `go test`). Harbor expresses resource budgets per **task** (`cpus`, `memory_mb` in `task.toml`), not per container. A Compose project may have one container or twenty, and Harbor has no a priori basis for dividing the task budget across sidecars.

**Options considered**:
- **Divide the budget across containers**: Reserve a fixed slice per sidecar, hand `main` the remainder, and cap each container at a synthesized ceiling.
- **Put the budget on the Pod** via `spec.resources` (KEP-2837) and leave individual containers with only what Compose explicitly declares (or place Guaranteed single-container budgets directly on `main` for kubelet CPU Manager compatibility).

**Decision**:
Put the task budget at the Pod level (`spec.resources`), with two deliberate placement rules governed by `--cpus` and `--memory` (`ResourceMode`):

1. **Burstable (`auto` / `request`) vs Guaranteed (`guarantee`) modes**:
   - `--cpus auto` and `--memory auto` resolve to `ResourceMode.REQUEST` (`_GKE_DEFAULT_RESOURCE_AUTO_MODE = ResourceMode.REQUEST`), which sets `requests` to the declared task budget. On direct Pods, it omits `limits` (unless `cpu_limit_multiplier` or `memory_limit_multiplier` is set), allowing tasks to burst into idle node capacity during short compilation and test spikes while other trials on the node are waiting on LLM turns. Compose Pods (Shapes A, B and C) always get a Pod-level limit at least equal to the request (`build_pod_level_resources()`), so request mode keeps a host-wide ceiling on multi-container and DinD tasks.
   - Setting `--cpus guarantee --memory guarantee` sets `requests == limits == declared task budget` so trials run with hard CPU and memory caps matching Harbor's local Docker `--cpus` and `--memory` flags.
2. **Direct Guaranteed Pods place resources on `main`; Compose Pods put the budget on `main` and a host ceiling on `spec.resources`; Burstable direct Pods use `spec.resources`**:
   - When a direct single-container Pod has `requests == limits` for both CPU and memory (`--cpus guarantee --memory guarantee` or multiplier `1.0`), `build_direct_pod()` places the resources on the `main` container rather than `spec.resources`. The kubelet `static` CPU Manager and Memory Manager ignore Pod-level resources unless the `PodLevelResourceManagers` feature gate is enabled (alpha in Kubernetes 1.36), so placing Guaranteed resources on `main` is required to obtain exclusive pinned cores on `cpuManagerPolicy: static` node pools. Reference: [Pod-level resources limitations](https://kubernetes.io/docs/tasks/configure-pod-container/assign-pod-level-resources/#limitations).
   - For Compose Pods (Shapes A, B and C), the budget goes on `main`, as in Harbor's Docker environment, and `build_pod_level_resources()` sets `spec.resources` to the Docker host's size: the request covers the budget and every container request, and the ceiling is the sum of every container's ceiling (its limit, else its request; for `dind-engine`, the daemon baseline plus the DinD services' ceilings). Containers without a ceiling of their own share what the Pod has left, as unconstrained containers share a Docker host.
   - For Burstable direct Pods (default `--cpus auto --memory auto`, `--cpus request --memory request`, or `scaled` multipliers `> 1.0`), the task budget is placed on `spec.resources`.

**Consequences**:
- **Kubernetes 1.34+ requirement for `spec.resources`**: `spec.resources` became beta and enabled by default in Kubernetes 1.34. At startup, `probe_pod_level_resources_support()` submits a minimal Pod with a server-side dry run (`dryRun=All`), checks whether `spec.resources` survives, and records the result in `ClusterCapabilities.supports_pod_level_resources`. If the cluster drops the field, the environment logs a warning.
- **Container-level `ephemeral-storage`**: Kubernetes does not support `ephemeral-storage` in `spec.resources`, so storage requests remain on `main` and `dind-engine`. Before Pod creation, `_assert_ephemeral_storage_schedulable()` compares the Pod's peak `ephemeral-storage` request with the largest allocatable ephemeral storage of any schedulable node in the cluster and raises `EphemeralStorageUnschedulableError` if it doesn't fit. That ceiling is estimated from node pool configuration: every pool without a blocking taint and with a maximum node count above zero, including pools scaled to zero. The check is cluster-wide, not per target pool, and is skipped when the ceiling is unknown, on Autopilot, and with NAP.
- **DinD Pods are bounded by a Pod ceiling**: An earlier design gave Shape B and Shape C Pods no `spec.resources`, treating the Docker daemon as a host bounded only by the node. Measured on GKE 1.35.6, an `orca-bench` task declaring `memory_mb = 1024` reached a working set of about 4 GiB (peak 5066 MiB) inside `dind-engine`; three such Pods on one `e2-highcpu-16` node drove it to 102% memory and `NotReady`. A task that outgrows its declared size must fail inside its own Pod, never on its neighbours. So DinD Pods get the same Pod ceiling as Shape A (the sum of container ceilings), `dind-engine` counts as the daemon baseline (`64 MiB` / `100m`) plus the DinD services' ceilings, `dockerd` runs with `--cgroup-parent` under `dind-engine`'s cgroup so nested containers are charged to the Pod, and nodes that run DinD tasks should set the kubelet's `singleProcessOOMKill: true`, so an OOM kill picks one process, as on a Docker host, rather than every process in `dind-engine`. Clearing `memory.oom.group` from inside `dind-engine` was tried and rejected: containerd keeps `memory.oom.group=1` in the container's spec and runc writes it back on every `UpdateContainerResources` call, which the static CPU manager issued three seconds after start (measured on GKE 1.35.6). The `--ek dind_engine_memory_mb` / `--ek dind_engine_cpu` overrides were removed: containers a task starts through the Docker socket are part of the task, so `memory_mb` (or `--override-memory-mb`) sizes them. A teardown report separates OOM kills at the Pod ceiling (re-size the task), at a container's own Compose limit, and whole-container group kills (node configuration). This also replaces an earlier `8192 MiB` Pod memory-limit floor (`DIND_POD_MEMORY_LIMIT_FLOOR_MB`) that had no measurement behind it. See [Docker-in-Docker](docker-in-docker.md#resource-model-and-volume-topology).
- **`memory_mb` bounds the whole DinD environment**: With the Pod ceiling, containers a task starts through `/var/run/docker.sock` count against `memory_mb`. Harbor's local Docker environment limits only `services.main` and leaves those containers bounded by the machine, so this is a deliberate difference. It matches Harbor's Daytona and Modal DinD environments, which size the whole sandbox from `memory_mb`; Harbor does not define the scope beyond "RAM requested by the task". Two alternatives were rejected:
  - *A node-sized ceiling for tasks that mount `docker.sock`.* Socket-spawned containers are the one part of a task that no Compose file declares, so they were the only case for raising the ceiling. Doing so restores the measured failure above: such a task can again take memory the scheduler gave to other Pods and drive the node to `NotReady`.
  - *Failing fast on sustained memory pressure at the ceiling (PSI).* A prototype watchdog read the Pod cgroup's `memory.pressure` from `dind-engine`. On `orca-bench`, originally sized Pods peaked at `full avg60` 22% and failed through per-process OOM kills, while properly sized Pods peaked at 4%, and one sized Pod sat at its ceiling with 0% pressure, so the ceiling alone is no signal and no threshold separated a stall from normal startup. Reading Pod PSI from outside the Pod through the API server needs `nodes/proxy`, which Kubernetes documents as equivalent to exec on every node. The watchdog was removed.
- **Compose interpolation sees referenced host variables only**: `docker compose config` runs with the system keys, the host variables the task's Compose files reference (`$VAR`, `${VAR...}`, including nested defaults; `$$` escapes skipped), and task, persistent and infra env, with infra winning. Harbor's Docker environment passes the whole host environment and Modal passes the referenced variables; both give the same `${VAR}` results, so the GKE path matches them without exposing unrelated host variables. The remaining difference from Docker is the bare pass-through form (`environment: [FOO]`), which takes the host value on Docker and not on Modal or GKE.
- **Pod-level OOM victim selection**: When containers without individual `mem_limit` declarations hit a shared `spec.resources.limits.memory` ceiling, the Linux OOM killer selects a process across the Pod by `oom_score`. See [Which container is killed when the Pod runs out of memory](configuration.md#which-container-is-killed-when-the-pod-runs-out-of-memory).
- **Autopilot container floor requests**: Autopilot injects `500m` CPU / `2Gi` memory into every container that declares nothing, which pushes the container aggregate above `spec.resources.requests` and gets the Pod rejected. On Autopilot only, `build_pod_level_resources()` gives such containers a token `1m` / `1Mi` request so the task budget stays the cap. It is never applied on Standard, where floor requests were previously measured to collapse a Pod from `1 CPU / 2 GiB` to `1m / 1Mi`. See [Autopilot](autopilot.md#multi-container-compose-budgets-on-autopilot-specresources).

## Roadmap

### 1. Generating GKE NodePool specs and Autopilot ComputeClasses from a dataset

Benchmark datasets differ widely in task shapes: some are uniform 1-CPU / 4-GiB sandboxes, while others combine 16-CPU / 64-GiB EDA builds requiring `x86_64` AVX2 instructions, 100+ GiB ephemeral storage, DinD sidecars, and L4/A100 GPUs. Today operators size node pools and `ComputeClass` resources manually (see [Cluster setup](cluster-setup.md) and [Task sizing and placement](task-sizing-and-placement.md)).

Planned tooling (`harbor-gke-ext-cluster-plan`) will inspect a dataset directory ahead of time — aggregating `task.toml` budgets (`cpus`, `memory_mb`, `storage_mb`, `gpus`, `tpu`), Compose shapes (A/B/C), DinD storage footprints, and image ISA pins (`amd64` vs `arm64`) — and emit:

- **GKE Standard**: Ready-to-apply `gcloud container node-pools create` commands and Terraform/YAML `NodePool` definitions grouped into right-sized resource tiers (general pool, static-CPU pool, high-ephemeral-storage/Local-SSD DinD pool, `x86_64` AVX2 pool, and GPU pools) plus a matching `--ek task_node_pools=...` map.
- **GKE Autopilot**: Custom `ComputeClass` CRDs (`cloud.google.com/v1`) with prioritized machine families (`c3`, `n2`, `g2`), Local SSD storage, and Spot/on-demand fallback chains, plus a matching `--ek task_compute_classes=...` map.

### 2. External image pull-through caching via Artifact Registry remote repositories

Instead of copying external images (`public.ecr.aws`, `ghcr.io`, `mcr.microsoft.com`) via Cloud Build jobs, `harbor-gke-ext` will provision and target **Artifact Registry remote repositories** (`--mode=remote-repository`) per upstream registry and rewrite image prefixes at a single chokepoint inside `ImageResolver.resolve()`. This preserves upstream `sha256` digests and multi-arch manifest lists, requires zero Cloud Build jobs, covers `task.toml` `docker_image`, `Dockerfile FROM`, and Compose `services.*.image` references uniformly, and unlocks GKE Image Streaming for cached external images.

## Open questions

The following items remain open questions or unverified territory:

- **DNS fast-reject vs drop in `no-network` mode**: Should `NetworkPolicy` egress deny be paired with a local fast-reject DNS stub so tasks that inadvertently probe external hostnames at import time fail immediately with `ECONNREFUSED` / `NXDOMAIN` instead of waiting on a socket timeout?
- **TPU passthrough in DinD**: Exclusive device locking on TPU character devices across nested DinD containers remains untested.
- **GPU inside DinD inside gVisor**: Whether the inner `dockerd` can re-inject `/dev/nvidia*` devices and driver libraries inside a non-privileged gVisor sandbox remains untested.

## Related documentation

- [Architecture](architecture.md)
- [Compose translation](compose-translation.md)
- [Docker in Docker](docker-in-docker.md)
- [Networking and security](networking-and-security.md)
- [Accelerators](accelerators.md)
- [Autopilot](autopilot.md)
- [Runtime](runtime.md)
- [Task sizing and placement](task-sizing-and-placement.md)
- [Dataset notes](dataset-notes.md)
