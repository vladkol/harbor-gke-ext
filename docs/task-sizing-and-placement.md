# Task sizing and placement

This page covers how much CPU and memory to give each task, and where to run it, so that
results are fair, match Harbor's Docker environment where that matters, and are valid as
evaluation data. It has four parts:

1. [Principles](#principles) and [resource semantics](#resource-semantics-docker-vs-gke).
2. [Why tasks fail for resource reasons](#why-tasks-fail-for-resource-reasons), and how to
   diagnose each cause.
3. [A deterministic sizing and placement procedure](#a-deterministic-sizing-and-placement-procedure),
   which is also the design basis for the planned `harbor-gke-ext-cluster-plan` tool (see
   the [roadmap](design-decisions.md#1-generating-gke-nodepool-specs-and-autopilot-computeclasses-from-a-dataset)).
4. [Recipes](#recipes) you can apply today.

For dataset-specific quirks, known upstream task exclusions, and per-dataset command
templates, see [Dataset notes](dataset-notes.md).

## Principles

1. **Honest.** A task runs with the resources it declares. When you change that, the report
   says so.
2. **Docker parity.** Harbor's Docker environment is the reference most tasks were written
   and calibrated against. Match its resource semantics unless you have a documented reason
   not to.
3. **Valid.** A reward of `0` should mean the task failed, not that the platform failed.
   Infrastructure failures are retried or reported as errors, never scored.
4. **Reproducible.** The same job on the same cluster gives the same distribution of results.
   Nondeterminism that comes from the platform (noisy neighbors, placement) is removed where
   possible, and what remains is measured with repeated attempts.
5. **Measured, not guessed.** Sizing decisions come from calibration runs, not from the
   declared budget alone.

## Resource semantics: Docker vs GKE

What a single-container (direct) task actually gets under each configuration:

| | Docker (Harbor default) | GKE default (`--cpus auto --memory auto` or `guarantee`) on shared nodes | GKE default (`guarantee`) on a node with the `static` CPU manager | GKE uncapped (`--cpus request --memory request`) |
| --- | --- | --- | --- | --- |
| CPU | Capped at the declared amount by a CFS quota (`cpu.max = 100000 100000` for 1 CPU) | Capped by a CFS quota (`requests = limits = declared budget`) | One or more exclusive cores (`cpuset`), no CFS quota throttling | Reserved, not capped: can burst onto idle node cores |
| Memory | Capped (`memory.max`) | Capped (`requests = limits = declared budget`) | Capped (`requests = limits = declared budget`) | Reserved, not capped |
| CPUs the task sees (`nproc`, affinity) | All host CPUs | All node CPUs | Only its own allocated cores (`nproc = 1` for 1 CPU) | All node CPUs |
| Kubernetes QoS class | – | `Guaranteed` (container-level on direct Pods) | `Guaranteed` (container-level on direct Pods) | `Burstable` (`pod.spec.resources`) |

> [!NOTE]
> **Open evaluation question:** Whether CPU and memory limits remain enabled by default (`--cpus auto --memory auto` resolving to `guarantee`, where `requests = limits`) versus request-only (`auto` resolving to `request`, with limits opt-in via `--cpus guarantee --memory guarantee`) is an open question currently under final benchmark evaluation.

Key implementation details:

- **Default mode (`--cpus auto --memory auto`):** `GKEEnvironment` resolves `auto` to
  `guarantee` (`_GKE_DEFAULT_RESOURCE_AUTO_MODE = ResourceMode.GUARANTEE`), setting
  `requests = limits = declared budget` for both CPU and memory to match Docker's capped
  default.
- **Direct Pods vs. Compose Pods:**
  - On **direct (single-container) Pods** where both CPU and memory have `request == limit`,
    `build_direct_pod()` places the CPU and memory `requests` and `limits` directly on the
    `main` container (`spec.containers[0].resources`) rather than `pod.spec.resources`. This
    gives the Pod the Kubernetes `Guaranteed` QoS class and makes it eligible for exclusive
    CPU pinning when scheduled onto a node pool with `cpuManagerPolicy: static`. The
    kubelet's static CPU manager only grants exclusive cores to `Guaranteed` Pods that
    request whole-number CPUs on containers, and ignores `pod.spec.resources` unless the
    `PodLevelResourceManagers` feature gate is enabled (see the
    [Kubernetes feature gates reference](https://kubernetes.io/docs/reference/command-line-tools-reference/feature-gates/)
    and
    [Pod-level resources limitations](https://kubernetes.io/docs/tasks/configure-pod-container/assign-pod-level-resources/#limitations)).
  - On **direct Pods** where limits are omitted (`--cpus request --memory request`) or scaled
    above requests (via `--ek cpu_limit_multiplier=<float>` or
    `--ek memory_limit_multiplier=<float>`, which apply only under the default
    `--cpus auto` / `--memory auto` and are ignored with explicit `guarantee` or `request`),
    `build_direct_pod()` places the budget on `pod.spec.resources` (`Burstable` QoS).
  - On **Compose (multi-container) Pods**, `build_pod_level_resources()` always places the
    task-wide CPU and memory budget on `pod.spec.resources` so `main`, native sidecars, and
    `dind-engine` share one Pod cgroup budget. When `dind-engine` is present (Shapes B and
    C), `pod.spec.resources.limits.memory` enforces a floor of `8192Mi`
    (`DIND_POD_MEMORY_LIMIT_FLOOR_MB = 8192`) so the nested Docker daemon and inner
    containers are not OOM-killed under small task budgets. Compose Pods always get a
    Pod-level limit at least equal to the request, including under
    `--cpus request --memory request`. See the
    [configuration reference](configuration.md#the-task-wide-resource-model).

Comparing the two capped GKE options against local Docker:

- **Capped on shared nodes** matches Docker's cgroup CFS quota and visible CPU count (`nproc`
  equals the node vCPU count), but co-locates multiple trial Pods on a multi-tenant node.
- **Capped on `cpuManagerPolicy: static` nodes** eliminates noisy-neighbor CPU contention by
  pinning each whole-CPU `Guaranteed` Pod to dedicated physical/virtual cores, and restricts
  the Pod's CPU affinity mask (`nproc` equals the Pod's requested CPU count). That directly
  prevents runtimes that size worker pools from `nproc` from over-spawning threads inside a
  small memory limit.

## Why tasks fail for resource reasons

Five distinct causes can make a task fail under resource constraints. Because each cause
requires a different remedy, identify the mechanism before changing task sizes or timeouts.

### 1. Contention from neighbors

Test suites with tight, hardcoded wall-clock timeouts—such as Mocha's `Timeout of 30000ms`,
`ospec` timeouts of 200–500 ms, Jest's 5000 ms per-test limit, or Go's
`go test -timeout=5m`—fail when neighboring Pods on the same node contend for CPU scheduling
slices or shared boot-disk I/O. Under a strict 1-CPU CFS quota on a shared node, a throttled
container cannot borrow idle cores after a moment of scheduling latency, making wall-clock
test timeouts much more sensitive to high Pod density per node.

**Remedy:** Schedule timing-sensitive tasks onto `cpuManagerPolicy: static` nodes (so each
`Guaranteed` Pod receives exclusive cores with no CFS throttling), or reduce the number of
concurrent Pods packed onto each node. Note that `--max-retries` does not recover from test
timeout failures because a timed-out test script exits cleanly from the platform's
perspective and writes a valid-looking reward of `0`.

### 2. Runtimes that size parallelism from the visible CPU count

Many language runtimes, compilers, and test runners choose how many threads, workers, or
subprocesses to spawn based on the number of logical CPUs visible in the process affinity
mask (`nproc`, `os.cpu_count()`, `make -j`, Jest/Vitest worker pools, and Go releases prior
to 1.25), rather than reading the container's cgroup CPU quota. When a task capped at 1 CPU
and 4 GiB of memory runs on a 16-vCPU or 32-vCPU shared node, this mismatch causes two
problems:

- **CPU oversubscription.** 16 or 32 worker threads contend for 1 CPU worth of CFS quota and
  spend most of their time context-switching and throttled.
- **Multiplied peak memory.** Each parallel compiler or test worker allocates its own heap,
  so peak resident memory scales with the *node's* vCPU count rather than the *task's*
  declared CPU count.

Go illustrates this difference clearly:

- **Go 1.25 and later** set `GOMAXPROCS` from the cgroup CPU *limit* automatically (see the
  [Go 1.25 release notes](https://go.dev/doc/go1.25)).
- **Go 1.24 and earlier** set `GOMAXPROCS` from the number of CPUs in the process affinity
  mask (`nproc`), and `go build` / `go test` spawn up to `GOMAXPROCS` parallel compiler and
  test processes (`go help build`, flag `-p`).

| Execution mode | Visible CPUs (`nproc`) on a 16-vCPU node | Behavior for a 1-CPU / 4-GiB task running `go test -race` (Go < 1.25) |
| --- | --- | --- |
| Capped (`guarantee`), shared node | 16 | Spawns up to 16 parallel `compile` / `vet` / test processes inside a 4 GiB cgroup limit, causing frequent cgroup OOM kills and `TrialContainerLostError`. |
| Capped (`guarantee`), `cpuManagerPolicy: static` node | 1 | `cpuset` restricts the Pod to 1 exclusive core (`nproc = 1`). Runs 1 compile at a time; process anonymous RSS stays well within 4 GiB with zero OOM kills, though serial execution takes longer wall-clock time. |
| Uncapped (`--cpus request --memory request`), shared node, direct task | 16 | Spawns 16 parallel workers and bursts across idle node cores and memory above 4 GiB without hitting a cgroup cap. Compose tasks keep a Pod-level limit in this mode. |
| Local Docker / Podman host | Host CPU count | Spawns workers proportional to the developer's host or VM CPU count. |

**Remedy:** Choose the approach that matches your evaluation requirements:

- **Static-CPU placement (recommended for strict budget parity):** Run on
  `cpuManagerPolicy: static` nodes so the runtime sees only the CPUs it was allocated, and
  add verifier timeout headroom (`--verifier-timeout-multiplier 1.5`) if serial compilation
  approaches the task's verifier timeout.
- **Memory headroom override:** Raise the memory limit for that task group (for example via
  `--override-memory-mb 8192`, or `--ek memory_limit_multiplier=2.0` under the default
  `--memory auto`) and document the override.
- **Uncapped execution:** Pass `--cpus request --memory request` during exploratory or
  calibration runs where strict CPU/memory capping is not required. This removes limits
  from direct tasks only; Compose tasks keep a Pod-level limit equal to the request.

### 3. The declared memory is below the real peak

Some tasks genuinely require more anonymous working-set memory than their `task.toml`
declares, regardless of CPU parallelism.

When inspecting memory metrics during calibration, keep two details in mind:

- **Page cache vs. anonymous memory:** Cgroup `memory.current` includes reclaimable filesystem
  page cache, which the Linux kernel reclaims before invoking the OOM killer. Compare the
  `anon` field of `memory.stat` (or working-set metrics in Cloud Monitoring) against the
  memory limit rather than raw `memory.current`.
- **Sampling interval:** Cloud Monitoring's default 60-second sampling interval can miss
  short-lived compiler or linker memory spikes; treat sampled peaks as lower bounds.

When a container exceeds its memory limit and the kernel OOM-kills the container's root
keepalive process, the container exits and `harbor-gke-ext` raises `TrialContainerLostError`
(which Harbor automatically retries when `--max-retries` is set, rather than scoring a false
`0` reward from a missing reward file).

**Remedy:** Provide the required memory to the affected task group via `--override-memory-mb`
or `--ek memory_limit_multiplier` (which applies only under the default `--memory auto`),
and record the override in your evaluation notes.

### 4. Timeouts that assume more CPU

A verifier or agent timeout authored on a fast multi-core workstation can be too tight when
the task is strictly capped to its declared CPU budget (or when `cpuManagerPolicy: static`
serializes a build onto 1 core).

**Remedy:** Apply `--verifier-timeout-multiplier` or `--agent-timeout-multiplier` for the
affected task group, and document the multiplier. `harbor-gke-ext` reads these values from
the trial configuration to size the Pod's `activeDeadlineSeconds`. The
`--ek verifier_timeout_multiplier` / `--ek agent_timeout_multiplier` options only extend
that Pod deadline; they don't change Harbor's agent or verifier timeouts.

### 5. Not a resource problem

Rule these out before changing CPU, memory, or placement—adding resources will not fix them:

- **Verifier or task defects.** The test suite passes, but the verifier's parser or expected
  test ID list fails to match (for example, truncated test names or broken gold patches). See
  [Dataset notes](dataset-notes.md).
- **Nondeterministic tests.** Tests that depend on unseeded randomness, wall-clock ordering,
  or map/slice iteration order fail intermittently across both shared and static-CPU nodes.
- **Transient infrastructure errors.** If an `exec` stream drops unexpectedly
  (`GKEExecStreamClosedError`) or a spot/preemptible node is reclaimed
  (`TrialContainerLostError`), `harbor-gke-ext` raises an exception. Setting
  `--max-retries 2` retries only trials that raised an exception and keeps the final attempt,
  without biasing task scores.

## Measuring a dataset

Before running a large scored evaluation on a new dataset, run a short calibration workflow
to separate task defects from resource sizing requirements:

1. **Calibrate uncapped (`--cpus request --memory request -k 3` with `--agent oracle`).**
   Running the reference oracle solution without CPU or memory limits reveals each task's
   unconstrained resource profile and classifies tasks into *stable* (passes all attempts),
   *flaky* (passes some attempts), and *broken* (fails all attempts even uncapped). Request
   mode removes limits from direct tasks only: Compose tasks keep a Pod-level limit equal to
   the request, so their calibration run is still capped at the declared budget. See
   [Calibrate a dataset before scoring agents](dataset-notes.md#calibrate-a-dataset-before-scoring-agents).
2. **Profile each trial.** From Cloud Monitoring (or container cgroup metrics), record
   per-Pod peak and 90th-percentile CPU usage, peak working-set memory, and verifier duration
   across the trial's lifetime. Aggregate both per task and by task family or repository.
3. **Record runtime traits from the base images.** Check whether task images use runtimes or
   build tools that size parallelism from `nproc` (such as Go `< 1.25`, `make -j`, or Jest)
   or test suites with tight wall-clock timeouts.
4. **Calibrate at the target capped configuration (`-k 3`).** Compare task-by-task against
   step 1. A task that is stable uncapped in step 1 but fails or OOMs when capped in step 4
   is a resource sizing or placement issue; a task that is flaky in both is an upstream test
   issue.

## A deterministic sizing and placement procedure

The procedure below turns a calibration profile into groups of tasks, with resources,
placement, and timeouts for each group. It is deterministic: the same inputs give the same
plan. It is also the specification for `harbor-gke-ext-cluster-plan`.

### Inputs

- **Declared budget** per task, from `task.toml`: `cpus`, `memory_mb`, `storage_mb`, GPUs or
  TPUs, timeouts.
- **Measured profile** per task, from calibration: peak and p90 CPU, peak process memory,
  verifier and agent durations, and the stable, flaky, or broken label.
- **Runtime traits** per base image: for example `go_version < 1.25`, timing-sensitive test
  runners, `nproc`-driven parallelism.
- **Hard constraints:** CPU architecture and ISA extensions, accelerators, storage, and
  Compose shape. See [Cluster setup](cluster-setup.md).

### Step 1. Exclude and label

Drop tasks that are broken in every environment, and report the exclusion and the new
denominator. Keep flaky tasks, and report them separately or as pass@k.

### Step 2. Choose the resource policy for each task

Start from Docker parity: the default `--cpus auto --memory auto` (or explicit
`--cpus guarantee --memory guarantee`) enforces `requests = limits = declared budget`.
Deviate only when calibration evidence requires it, and record why:

| Condition (from the profile) | Decision |
| --- | --- |
| Peak process memory ≤ declared memory, and verifier p95 ≤ 0.8 × timeout at the target configuration | Declared budget, no changes |
| Peak process memory > declared memory | Memory override for this group (for example `--override-memory-mb 8192`, or `--ek memory_limit_multiplier=2.0` under the default `--memory auto`) |
| Verifier p95 > 0.8 × timeout at the target configuration | Timeout multiplier for this group (for example `--verifier-timeout-multiplier 1.5`) |
| Declared CPU is not a whole number | Round up for static placement, or place on shared nodes |

The 0.8 threshold leaves headroom for the slowest trials. Choose it once and use it for the
whole dataset.

### Step 3. Choose placement for each task

| Condition | Placement |
| --- | --- |
| Timing-sensitive tests, or runtime parallelism from the visible CPU count (`go_version < 1.25`, `nproc`) | Static-CPU nodes (`ComputeClass` or node pool with `cpuManagerPolicy: static`) |
| Neither | Shared nodes are enough. Static-CPU nodes are still the safer default for scored runs. |
| ISA, accelerator, or storage constraint | The node pool, `ComputeClass`, or machine type that satisfies it, per [Cluster setup](cluster-setup.md) |

### Step 4. Group

Tasks with the same resource policy, placement, and timeout multiplier form one group. Each
group becomes one Harbor job when resource overrides (`--override-memory-mb`) or timeout
multipliers (`--verifier-timeout-multiplier`) differ, because those flags apply job-wide.
When only placement varies across tasks, you can either route tasks within a single job using
`--ek task_compute_classes`, `--ek task_node_pools`, or `--ek task_machine_types`, or split
the dataset into separate jobs using `-i` (include) and `-x` (exclude) glob patterns.

### Step 5. Size nodes for each group

For Pods of `c` CPUs and `m` GiB on a node with allocatable CPU `C` and memory `M` (after
DaemonSets):

```text
pods_per_node = min( floor(C / c), floor(M / m) )
```

- On static-CPU nodes, count only whole cores for `C`. Check `kubectl describe node` for the
  real allocatable values.
- Disk throughput is shared by every Pod on the node. For I/O-heavy groups, include it as a
  third term. See the boot disk throughput table in [Cluster setup](cluster-setup.md).
- Pick the machine type that packs the group with the least waste. Example: an
  `e2-standard-16` node reports 15.89 allocatable CPUs and 57.3 GiB allocatable memory. For
  Pods of 1 CPU and 4 GiB that gives `min(15, 14) = 14` Pods, before DaemonSets. Memory is
  the binding term, so a static-CPU node packs as many of these Pods as a shared node.

### Step 6. Order the work

Makespan is set by the longest group. Launch the group with the longest expected trial time
(from the calibration profile) first, and give it enough node capacity to start all of its
trials concurrently so long-tail verifiers do not trail at the end of the batch.

### Step 7. Emit, then verify

- Emit one `ComputeClass` per placement (or node pool commands) and one Harbor command per
  group.
- Smoke-test one task per group, and check on the Pod: QoS class (`Guaranteed` vs
  `Burstable`), `cpuset.cpus.effective`, `cpu.max`, and `nproc`.
- Calibrate the plan (`-k 3`) and compare it task by task with the uncapped calibration.

## Recommendations

- **Keep capped resources enabled for scored runs** (the default `--cpus auto --memory auto`
  or explicit `--cpus guarantee --memory guarantee`). It matches Docker's cgroup semantics
  and keeps the declared task budget honest. Use `--cpus request --memory request` for
  calibration runs or exploratory development; it removes limits from direct tasks, while
  Compose tasks keep a Pod-level limit equal to the request.
- **Use static-CPU placement (`cpuManagerPolicy: static`)** for datasets with
  timing-sensitive tests or runtimes that size parallelism from `nproc` (such as Go `< 1.25`).
  For 1-CPU / 4-GiB tasks on standard 1:4 CPU-to-memory node shapes, static-CPU placement
  packs the same number of Pods per node as shared placement while eliminating CPU throttling
  and `nproc` oversubscription.
- **Apply resource or timeout overrides only where calibration proves them necessary**, one
  task group at a time, and document each override.
- **Keep Image Streaming on for every node pool, including auto-provisioned ones.** Without
  it, a node pulls each image in full before its Pod starts. The kubelet pulls a limited
  number of images at once (GKE nodes report `maxParallelImagePulls: 3` in their kubelet
  config), so a small image can wait behind several multi-GB pulls on the same node. That
  wait counts against the environment start timeout, which also applies to a separate
  verifier environment. See [Cluster setup](cluster-setup.md) for how to verify the
  cluster default. Datasets with very large images (tens of GB) are also good candidates for
  their own `ComputeClass` or node pool, so their pulls don't delay unrelated trials.
- **Plan for accelerators you cannot get.** A Pod waiting for capacity stays `Pending` for
  up to `pod_ready_timeout` (default `max(1200, build_timeout_sec)`), so tasks with a large
  `build_timeout_sec` can hold a trial slot for hours. Before a run, check that every GPU type
  the dataset requests has quota and a node pool or NAP limit. Otherwise remap it with
  `--ek gpu_override=<label>`, exclude those tasks with `-x`, or lower
  `--ek pod_ready_timeout` so they fail early.
- **Always set `--max-retries 2` on large runs.** Transient infrastructure errors
  (`GKEExecStreamClosedError`, `TrialContainerLostError`) are retried automatically; zero
  rewards from completed verifiers are not.
- **Calibrate with `-k 3`** at the configuration you intend to report.

## Recipes

### Static-CPU nodes with node auto-provisioning (ComputeClass)

Requires GKE Standard with node auto-provisioning enabled. `nodeSystemConfig` in a
`ComputeClass` priority rule requires GKE 1.32.1-gke.1729000 or later. See the
[ComputeClass reference](https://docs.cloud.google.com/kubernetes-engine/docs/reference/crds/computeclass).

```yaml
apiVersion: cloud.google.com/v1
kind: ComputeClass
metadata:
  name: harbor-static-cpu
spec:
  nodePoolAutoCreation:
    enabled: true
  priorities:
  - machineFamily: e2
    minCores: 16
    storage:
      bootDiskType: pd-balanced
      bootDiskSize: 500
    nodeSystemConfig:
      kubeletConfig:
        cpuManagerPolicy: static
  - machineFamily: n2
    minCores: 16
    storage:
      bootDiskType: pd-balanced
      bootDiskSize: 500
    nodeSystemConfig:
      kubeletConfig:
        cpuManagerPolicy: static
  whenUnsatisfiable: DoNotScaleUp
```

Select it for an entire job with `--ek compute_class=harbor-static-cpu`, or for specific
tasks with `--ek task_compute_classes=<task>=harbor-static-cpu`. Direct Pods running with the
default capped resources (`Guaranteed` QoS) and whole-CPU requests receive exclusive cores on
the auto-provisioned nodes.

### Static-CPU node pool (GKE Standard)

```bash
cat > static-cpu-system-config.yaml <<'EOF'
kubeletConfig:
  cpuManagerPolicy: static
EOF

gcloud container node-pools create static-cpu-pool \
  --cluster="${CLUSTER_NAME}" --region="${REGION}" \
  --service-account="${NODE_SA}" \
  --machine-type=e2-standard-16 --disk-type=pd-balanced --disk-size=500 \
  --enable-image-streaming \
  --enable-autoscaling --total-min-nodes=0 --total-max-nodes=100 \
  --system-config-from-file=static-cpu-system-config.yaml
```

Use it with `--ek node_pool=static-cpu-pool`, or for specific tasks with
`--ek task_node_pools=<task>=static-cpu-pool`. Changing `cpuManagerPolicy` on an existing
pool recreates its nodes. See
[Customizing node system configuration](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/node-system-config).

### Sizing node boot disks for GCFS Image Streaming and hardlink-heavy images

Worker node boot disks hold the containerd content store, GCFS Image Streaming cache, and
per-Pod `emptyDir` scratch storage. `--ek scratch_volume_size` moves DinD and Compose
volume writes to per-Pod GCE Persistent Disks, but the Pod still requests the same
`ephemeral-storage` from the node, so size boot disks for that reservation. Provision
**500–1,500 GB `pd-balanced`** boot disks with `--enable-image-streaming`:

- **Throughput and IOPS scale with disk size:** Per [Persistent Disk performance](https://cloud.google.com/compute/docs/disks/performance),
  a 1,500 GB `pd-balanced` disk provides up to 560 MiB/s of sustained throughput and 1,225 GiB
  of allocatable ephemeral storage after GKE's 100 GiB system reservation and 10% eviction
  margin, avoiding disk bottlenecks when 10–15 trial Pods compile code or unpack layers
  simultaneously.
- **Hardlink multiplication under GCFS Image Streaming:** Images containing thousands of
  hardlinked files (such as Python virtualenvs, `uv` / `pnpm` caches, or Conda environments)
  can materialize each hardlink as an independent file entry under GCFS overlay mounts,
  multiplying on-disk usage relative to the compressed OCI layer size and triggering
  `DiskPressure` evictions on default 100 GB boot disks.

### Routing tasks to specific machine families, node pools, or GPUs

- **Per-task CPU ISA or machine family routing:** Tasks that require x86-64 microarchitectural
  features such as `avx512f` (Intel Skylake / Cascade Lake / Ice Lake / Sapphire Rapids) can
  be pinned via `--ek machine_type=n2-standard-8` or per task via
  `--ek task_machine_types=<task>=n2-standard-8`. Harbor has no built-in per-task pins; see
  [Dataset notes](dataset-notes.md) for tasks known to need one.
- **Pre-created scale-to-zero GPU node pool and GPU override:** Because Node Auto-Provisioning
  has the longest cold-start latency when creating new GPU node pools from scratch,
  pre-create an autoscaling pool for the accelerator type that your tasks request, with
  `--total-min-nodes=0` (see
  [Cluster setup](cluster-setup.md#path-a-standard-cluster-with-minimal-system-pool-scale-to-zero-worker-pools-and-nap-recommended)).
  To run tasks that declare a different or generic GPU type on that pool, pass
  `--ek gpu_override=<accelerator>` (for example `nvidia-l4`), or set a fallback for untyped
  GPU requests with `--ek default_gpu_type=<accelerator>`.
- **Splitting jobs by task group (`-i` / `-x`):** When a subset of tasks in a dataset needs
  a higher verifier timeout or memory override alongside static-CPU placement, split the
  run into two jobs using `-i` (include) and `-x` (exclude) glob patterns (Harbor applies
  `-i` before `-x`). Patterns are matched with `fnmatch` against the full registry task name,
  `<org>/<task>` (for example `scale-ai/instance_…`). A pattern without the org prefix or a
  leading `*` matches nothing: an unmatched `-i` fails the job, but an unmatched `-x` is
  silently ignored. Confirm the resolved trial count with `--dry-run`:

```bash
COMMON=(-d <dataset> -a <agent> -n 100 --max-retries 2
  -e harbor_gke_ext:GKEEnvironment --plugin harbor_gke_ext:CloudBuildPlugin
  --ek project_id="${PROJECT_ID}" --ek location="${LOCATION}"
  --ek cluster_name="${CLUSTER_NAME}" --ek compute_class=harbor-static-cpu)

# Group 1: Long-running / heavy-compile subset launched first with extra verifier headroom
uv run harbor run "${COMMON[@]}" -i '*heavy-group*' \
  --cpus guarantee --memory guarantee \
  --verifier-timeout-multiplier 1.5 --job-name eval-heavy-group

# Group 2: Remaining tasks at default timeouts
uv run harbor run "${COMMON[@]}" -x '*heavy-group*' \
  --job-name eval-main-group
```

See [Dataset notes](dataset-notes.md) for concrete command lines and upstream exclusion lists
for `scale-ai/swe-bench-pro` and `terminal-bench@2.0`.

## Open questions

- **Default resource mode (`auto -> guarantee` vs `auto -> request`):** Under the current
  default (`_GKE_DEFAULT_RESOURCE_AUTO_MODE = ResourceMode.GUARANTEE`), `--cpus auto` and
  `--memory auto` resolve to `guarantee` (`requests = limits = declared budget`). Whether
  CPU and memory limits remain enabled by default or switch to request-only (`auto -> request`,
  with limits opt-in via `--cpus guarantee --memory guarantee`) is under final benchmark
  evaluation.
- **Pod-level resource managers (`PodLevelResourceManagers`):** When the Kubernetes
  `PodLevelResourceManagers` feature gate becomes available and enabled on GKE, the kubelet's
  `static` CPU manager will also be able to allocate exclusive cores from Pod-level
  `spec.resources`, extending static-CPU support to multi-container Compose Pods.

## Related documentation

- [Cluster setup](cluster-setup.md)
- [Configuration reference](configuration.md)
- [Dataset notes](dataset-notes.md)
- [Runtime](runtime.md)
- [Design decisions](design-decisions.md)
