# harbor-gke-ext

Run [Harbor](https://github.com/harbor-framework/harbor) agent evaluations on
Google Kubernetes Engine (GKE).

`harbor-gke-ext` is an extended alternative to Harbor's built-in `gke`
environment. It implements the same Harbor environment interface, so your existing
tasks run unchanged, and it adds Docker Compose translation, accelerator
support, network isolation, and the resilience machinery needed to keep
thousands of concurrent trials alive against a remote API server.

```bash
harbor run -t hello-world/hello-world \
  -e harbor_gke_ext:GKEEnvironment \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${LOCATION}" \
  --ek cluster_name="${CLUSTER_NAME}"
```

## What you get

**Built for scale.** Run thousands of concurrent trials as independent
Kubernetes Jobs with bounded lifetimes and content-addressed image deduplication
across datasets. An adaptive control-plane rate limiter adjusts concurrency and
retries requests automatically when the Kubernetes API server is under heavy
load.
See [Runtime](docs/runtime.md).

**Docker and Docker Compose parity.** Run both single-container tasks and
multi-service Docker Compose tasks on GKE without modifying existing task
definitions. Services start in dependency order with their healthchecks, shared
volumes, and environment variable interpolation preserved so tasks behave on GKE
the way they behave in local Docker.
See [Compose translation](docs/compose-translation.md).

**Multi-layer sandboxing for untrusted agents.** Evaluating autonomous agents
means running arbitrary, untrusted code at scale. Each trial runs in an
isolated, single-use Pod with no access to the host node filesystem, container
runtime socket, or Kubernetes API credentials, and can leverage GKE Sandbox
(**gVisor**) for user-space kernel isolation when requested or when tasks need
elevated container capabilities.
See [Networking and security](docs/networking-and-security.md).

**Harbor-controlled network policies and metadata server protection.** Beyond
container sandboxing, the environment verifies that the cluster actively enforces
Kubernetes `NetworkPolicy` (GKE Dataplane V2 or Calico, failing closed if
unenforced) and applies Harbor's per-task network modes (`no-network`,
`allowlist`, and `public`) with default cross-trial ingress isolation and GKE
Dataplane V2's `FQDNNetworkPolicy` for IP, CIDR, exact hostname, and wildcard
domain filtering. Access to the GCE and GKE metadata servers is blocked by
default to protect node and Workload Identity credentials (and temporary
Docker-in-Docker registry tokens are scrubbed before the agent container
starts), with explicit opt-in when tasks need to authenticate to Google Cloud
APIs.
See [Networking and security](docs/networking-and-security.md).

**Docker-in-Docker (DinD) support.** Tasks or sidecars that require a live
Docker daemon, privileged mode, overlapping ports, multiple Compose networks, or
other Docker-specific features are automatically routed into an isolated per-Pod
Docker-in-Docker environment—either for just the affected sidecars (**Shape B —
Hybrid**) or for the entire Compose stack (**Shape C — Main-in-DinD**)—with
transparent command and file routing.
See [Docker-in-Docker](docs/docker-in-docker.md).

**Load-balanced image pre-building and lazy registry management.** Task and
sidecar images are content-addressed and built once per unique build context on
Cloud Build with BuildKit layer caching. Pre-building warms all
Dockerfile-backed images across a dataset up front with bounded concurrency, and
Artifact Registry repositories are provisioned lazily only when a task actually
requires an image build.
See [Images and builds](docs/images.md) and
[Design decisions](docs/design-decisions.md).

**Accelerators support, including inside DinD.** Attach NVIDIA GPUs to
single-container tasks, native Compose services, or Docker-in-Docker containers
with automatic GKE node selection, tolerations, and driver wiring, or run
single-host Cloud TPU slices across TPU v3 through v7.
See [Accelerators](docs/accelerators.md).

**GKE Standard and Autopilot.** `harbor-gke-ext` fully supports GKE Standard
clusters—leveraging GKE Image Streaming, Node Auto-Provisioning, and custom
`ComputeClass`es—and also runs on GKE Autopilot clusters, with differences in
concurrency and Docker-in-Docker support (see [Autopilot](docs/autopilot.md)
and [Limitations](#limitations)). Before
scheduling trials, `harbor-gke-ext` inspects the cluster's capabilities,
available node pools, and storage capacity so configuration issues surface
immediately.
See [Cluster setup](docs/cluster-setup.md).

## Requirements

| Requirement | Notes |
| --- | --- |
| Python 3.12 or later | Same floor as Harbor. |
| `gcloud` CLI | Required. Harbor refuses to start if `gcloud` is missing or has no active authenticated account (`gcloud auth login`), unless `GOOGLE_APPLICATION_CREDENTIALS` points at a file. Version 480.0.0 or later is recommended; the version is not checked. |
| `gke-gcloud-auth-plugin` | Required by GKE 1.26 and later. Absence produces a warning, then authentication failures. |
| A kubeconfig | `~/.kube/config` must exist, or `KUBECONFIG` must point at one. `gcloud container clusters get-credentials` creates it. The package does not invoke `kubectl`. |
| A Google Cloud project | With billing enabled and the `container`, `artifactregistry`, `cloudbuild`, `compute`, and `iam` APIs turned on. |
| A GKE cluster (1.34+) | Autopilot or Standard. Kubernetes 1.34+ is required for Pod-level `spec.resources` enforcement. See [Cluster setup](docs/cluster-setup.md). |

A Docker Compose binary on the host is optional. If none is present and a task
needs one, the package downloads a pinned standalone release and verifies its
SHA-256.

## Install

Install into the same Python environment as Harbor.

If Harbor is installed as a `uv` tool:

```bash
uv tool install harbor --with git+https://github.com/vladkol/harbor-gke-ext
```

If Harbor lives in a virtual environment:

```bash
uv pip install git+https://github.com/vladkol/harbor-gke-ext
```

Verify the environment loads:

```bash
python -c "from harbor_gke_ext import GKEEnvironment; print(GKEEnvironment.type())"
```

### For developers

1. Clone `harbor-gke-ext` repository: `git clone https://github.com/vladkol/harbor-gke-ext.git && cd harbor-gke-ext`
2. Run `uv sync --all-groups` to install dependencies.

## Quick start

The recommended **GKE Standard** setup uses a 3-tier node layout:

- **System pool (`default-pool`)**: A minimal on-demand `e2-standard-4` node (`100 GB` boot disk), tainted with `CriticalAddonsOnly=true:NoSchedule` so `kube-system` is never disturbed by trial Pods.
- **Scale-to-zero CPU worker pool (`harbor-workers`)**: Pre-created `n2-standard-16` nodes with **`500 GB` boot disks (`339.2 GiB` allocatable)**. Standard benchmark tasks declare `10–30 GiB` of storage, so one node packs 7 to 14 tasks while the pool scales to `0` nodes when idle. For datasets with `300 GiB+` tasks, add a dedicated pool with larger disks (see [Storage arithmetic](docs/cluster-setup.md#storage-arithmetic-and-computeclass-resolution)).
- **Specialized pools**: Pools for tasks with specific requirements, such as tasks that need GPUs. For GPU tasks, we recommend a pre-created scale-to-zero autoscaling pool for the accelerator type that your tasks request (the example below uses `nvidia-l4`). Node Auto-Provisioning (NAP) creates other specialized pools on demand, such as custom `ComputeClass` pools (`harbor-static-cpu`). The NAP default boot disk is `100 GB`, and each `ComputeClass` sets its own disk size.

For GKE Autopilot, Workload Identity Federation, and storage sizing formulas, see [Cluster setup](docs/cluster-setup.md).

### 1. Set your variables

```bash
export PROJECT_ID="your-project-id"
export LOCATION="us-central1"
export REGION="us-central1"
export CLUSTER_NAME="harbor-eval-cluster"
export NODE_SA="harbor-gke-node-sa@${PROJECT_ID}.iam.gserviceaccount.com"
```

### 2. Enable the APIs and create the node service account

Create a dedicated least-privilege node service account (`harbor-gke-node-sa`) with `roles/artifactregistry.reader` so nodes can pull built task images from Artifact Registry without `ImagePullBackOff`:

```bash
gcloud services enable \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"

gcloud iam service-accounts create harbor-gke-node-sa \
  --display-name="Harbor GKE Node Service Account" \
  --project="${PROJECT_ID}" || true

# Wait briefly for IAM service account propagation before binding roles
sleep 10

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --role="roles/container.defaultNodeServiceAccount" \
  --member="serviceAccount:${NODE_SA}"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --role="roles/artifactregistry.reader" \
  --member="serviceAccount:${NODE_SA}"
```

### 3. Create the Artifact Registry repository

Create the target Artifact Registry repository (default `harbor-tasks`, override with `--ek registry_name=`):

```bash
gcloud artifacts repositories create harbor-tasks \
  --repository-format=docker \
  --location="${REGION}" \
  --project="${PROJECT_ID}" || true
```

### 4. Create the cluster, scale-to-zero node pools, and DinD Artifact Registry binding

Create the Node Auto-Provisioning configuration (`nap-config.yaml`) including accelerator limits, `100 GB` default `pd-balanced` boot disks (each `ComputeClass` sets its own size), and `NODE_SA`, then create the cluster (which initializes the project's Workload Identity pool `${PROJECT_ID}.svc.id.goog`), the worker pools, and the Workload Identity `roles/artifactregistry.reader` binding on `harbor-tasks`:

```bash
cat <<EOF > nap-config.yaml
resourceLimits:
  - resourceType: cpu
    minimum: 0
    maximum: 1500
  - resourceType: memory
    minimum: 0
    maximum: 6000
  - resourceType: nvidia-l4
    minimum: 0
    maximum: 16
  - resourceType: nvidia-h100-80gb
    minimum: 0
    maximum: 16
diskSizeGb: 100
diskType: pd-balanced
imageType: COS_CONTAINERD
serviceAccount: ${NODE_SA}
management:
  autoUpgrade: true
  autoRepair: true
shieldedInstanceConfig:
  enableSecureBoot: true
  enableIntegrityMonitoring: true
EOF

# 1. Create GKE Standard cluster with a minimal, system-only default-pool (100 GB, on-demand)
#    --workload-pool initializes the project's Workload Identity pool (${PROJECT_ID}.svc.id.goog)
gcloud container clusters create "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --release-channel=regular \
  --machine-type=e2-standard-4 \
  --disk-size=100 \
  --disk-type=pd-balanced \
  --num-nodes=1 \
  --enable-autoscaling --total-min-nodes=1 --total-max-nodes=2 \
  --node-taints=CriticalAddonsOnly=true:NoSchedule \
  --service-account="${NODE_SA}" \
  --enable-autoprovisioning \
  --autoprovisioning-config-file=nap-config.yaml \
  --autoscaling-profile=optimize-utilization \
  --enable-image-streaming \
  --enable-dataplane-v2 \
  --enable-fqdn-network-policy \
  --workload-pool="${PROJECT_ID}.svc.id.goog" \
  --enable-shielded-nodes \
  --cluster-dns=clouddns \
  --cluster-dns-scope=cluster \
  --addons=NodeLocalDNS

# 2. Create the scale-to-zero primary worker pool (16 vCPU, 500 GB boot disk -> 339.2 GiB allocatable)
gcloud container node-pools create harbor-workers \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --machine-type=n2-standard-16 \
  --disk-size=500 \
  --disk-type=pd-balanced \
  --service-account="${NODE_SA}" \
  --enable-image-streaming \
  --num-nodes=0 \
  --enable-autoscaling --total-min-nodes=0 --total-max-nodes=16

# 3. (Recommended for GPU tasks) Pre-create a scale-to-zero autoscaling GPU pool so GPU tasks
#    avoid cold NAP pool creation. This example uses nvidia-l4; use the accelerator type your
#    tasks request, and zones that offer it (nvidia-l4 in us-central1: a, b, c).
gcloud container node-pools create l4-gpu-pool \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --node-locations="${REGION}-a,${REGION}-b,${REGION}-c" \
  --project="${PROJECT_ID}" \
  --machine-type=g2-standard-8 \
  --accelerator="type=nvidia-l4,count=1,gpu-driver-version=default" \
  --disk-size=500 \
  --disk-type=pd-balanced \
  --service-account="${NODE_SA}" \
  --enable-image-streaming \
  --num-nodes=0 \
  --enable-autoscaling --total-min-nodes=0 --total-max-nodes=8

# 4. Grant the cluster's Workload Identity pool read access to the harbor-tasks repository
#    Required so Docker-in-Docker (dind-engine / dind-cache-*) can docker pull built images
#    from Artifact Registry when Workload Identity (GKE_METADATA) is active on the nodes.
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
gcloud artifacts repositories add-iam-policy-binding harbor-tasks \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/*" \
  --role="roles/artifactregistry.reader"

gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --location="${LOCATION}" \
  --project="${PROJECT_ID}"
```

> [!NOTE]
> - Because `default-pool` is tainted with `CriticalAddonsOnly=true:NoSchedule`, `kube-system` runs on a single `100 GB` `e2-standard-4` node, while `harbor-workers` and the GPU pool scale to **`0` nodes when no evaluations are running**.
> - Ensure your project's regional **`SSD_TOTAL_GB`** (`Persistent Disk SSD (GB)`) quota in `${REGION}` can cover your peak active worker nodes (`500 GB` per active node).
> - **Why step 4 binds `principalSet://.../workloadIdentityPools/${PROJECT_ID}.svc.id.goog/*` after cluster creation:** Creating the cluster with `--workload-pool="${PROJECT_ID}.svc.id.goog"` (or creating an Autopilot cluster) provisions the project's Workload Identity pool and enables `GKE_METADATA` on worker nodes. Under `GKE_METADATA`, `kubelet` on the host still uses `${NODE_SA}`, but `dind-engine` inside a Pod receives a Workload Identity token when querying `169.254.169.254`. Binding `roles/artifactregistry.reader` on the `harbor-tasks` repository to the Workload Identity pool lets `dind-cache-<service>` pull built task and sidecar images directly via `docker pull` instead of falling back to slow rootfs streaming (`tar -cf - / | docker import`). See [Docker-in-Docker](docs/docker-in-docker.md).

### 5. Run a task

Because `get-credentials` sets your active `kubectl` context to `gke_${PROJECT_ID}_${LOCATION}_${CLUSTER_NAME}`, `harbor-gke-ext` can infer `project_id`, `location`, and `cluster_name` automatically. Run a task from the Harbor dataset registry (or pass `-p examples/tasks/gpu-sidecar --agent oracle` to run a local example task without registry access):

```bash
harbor run -t hello-world/hello-world \
  --agent oracle \
  -e harbor_gke_ext:GKEEnvironment
```

Or pass the cluster target explicitly (recommended when environment variables and `kubectl` contexts may differ):

```bash
harbor run -t hello-world/hello-world \
  --agent oracle \
  -e harbor_gke_ext:GKEEnvironment \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${LOCATION}" \
  --ek cluster_name="${CLUSTER_NAME}"
```

The first run builds and pushes the task image, so it is slower than subsequent
runs against the same task.

If it fails, [Troubleshooting](docs/troubleshooting.md) is organized by the
literal error text you will see.

## Common recipes

**Run a whole dataset in parallel.**

```bash
harbor run --dataset terminal-bench@2.0 \
  --agent claude-code \
  --model anthropic/claude-sonnet-4-5 \
  -e harbor_gke_ext:GKEEnvironment \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${LOCATION}" \
  --ek cluster_name="${CLUSTER_NAME}" \
  --n-concurrent 64
```

**Warm every built image before the job starts.** Building images inside the
trial workers serializes a large job behind its slowest build. The plugin builds
all Dockerfile-backed task and sidecar images up front with bounded concurrency:

```bash
harbor run --dataset terminal-bench@2.0 \
  --agent claude-code \
  --model anthropic/claude-sonnet-4-5 \
  -e harbor_gke_ext:GKEEnvironment \
  --plugin harbor_gke_ext:CloudBuildPlugin \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${LOCATION}" \
  --ek cluster_name="${CLUSTER_NAME}"
```

**Control CPU and memory capping.** By default (`--cpus auto --memory auto`),
`harbor-gke-ext` resolves `auto` to `guarantee` (`requests = limits = declared budget`,
matching Docker's capped default). To omit limits so tasks can burst onto idle node
cores, or to scale limits above requests in `auto` mode:

```bash
--cpus request --memory request                              # no limits (single-container tasks)
--ek cpu_limit_multiplier=2.0 --ek memory_limit_multiplier=1.5  # limits = requests * multiplier
```

`--cpus request --memory request` removes limits for single-container tasks
only. Compose Pods always carry a Pod-level limit of at least the declared
budget, with an `8192 MiB` memory floor when `dind-engine` is present.

**Request a GPU or override GPU.** When running a dataset whose tasks declare
`gpu_types` that yiou don't have capacity for, such as
`gpu_types = ["A100"]` (or `a100-80gb`) on a cluster equipped with L4 GPUs (`l4-gpu-pool`),
override the accelerator selector at runtime with the full GKE label:

```bash
--ek gpu_override=nvidia-l4
```

See [Accelerators](docs/accelerators.md).

**Cut off the network.**

```toml
[environment]
network_mode = "no-network"
```

This installs a `NetworkPolicy` that denies all egress. DNS is not excepted,
which is deliberate; see
[Networking and security](docs/networking-and-security.md).

**Force a Compose placement.** The classifier decides automatically, but you can
pin it while debugging:

```bash
--ek compose_placement=native   # fail rather than fall back to Docker-in-Docker
--ek compose_placement=dind     # run every sidecar in the Docker-in-Docker plane
```

**Back DinD and Compose volumes with Persistent Disks.** For Compose tasks,
`--ek scratch_volume_size` replaces the `emptyDir` behind Compose named volumes
and the `dind-engine` `/var/lib/docker` volume with per-Pod generic ephemeral
volumes backed by Persistent Disk:

```bash
--ek scratch_volume_size=100Gi
```

`dind-engine` still requests its full `ephemeral-storage` estimate from the
node, so this setting does not reduce the node ephemeral-storage reservation
that the storage pre-check, the scheduler, and Autopilot see.

**Decoupled execution for long-running quiet commands.** Detach supervised
commands into background processes and poll for their output and exit status:

```bash
--ek decoupled=true
```

**Run under gVisor.**

```bash
--ek runtime_class_name=gvisor
```

Every option is catalogued in the
[Configuration reference](docs/configuration.md).

## How it works: the three Pod shapes

One trial becomes one `batch/v1` Job with `backoffLimit: 0`, owning one Pod with
`restartPolicy: Never`. `harbor-gke-ext` inspects the
task and selects one of three shapes:

| Shape | Topology | When it is chosen |
| --- | --- | --- |
| **Shape A — Native Pod** | `main` runs as the Pod's primary container; any Compose sidecars run as KEP-753 native sidecars (`initContainers` with `restartPolicy: Always`). | Default for all single-container tasks and for multi-container Compose tasks whose services have non-colliding ports and require no privileged or Docker-specific features. |
| **Shape B — Hybrid (`main` native + DinD sidecars)** | `main` runs natively as the primary Pod container; delegated sidecars run inside a `dind-engine` sidecar after `dind-cache-<svc>` pulls their images and `compose-up-gate` starts the stack. | Chosen when `main` can run natively, but one or more sidecars collide on ports, attach to more than one Compose network, declare symbolic `user:` names, or use DinD-only Compose options. Preserves direct GKE Image Streaming and GPU/TPU device passthrough on `main`. |
| **Shape C — Main-in-DinD** | Both `main` and all sidecars run inside `dind-engine` using their bare `<service>` names (e.g., `main`). The Kubernetes `main` container is a lightweight proxy that mirrors the inner `main` container's lifecycle, and Harbor commands that target `main` run inside the inner container through `docker exec`. | Chosen when `main` itself declares `privileged: true`, mounts `/var/run/docker.sock` or an `external: true` volume, or uses non-GPU `devices`, unsafe `sysctls`, or DinD-only Compose keys such as `ulimits`. |

See [Architecture](docs/architecture.md) for the full lifecycle and
[Compose translation](docs/compose-translation.md) for every classifier reason
code.

## Optimizing your cluster for datasets

Different benchmarks stress different cluster dimensions:

- **High-concurrency coding and CLI suites** (`SWE-bench`, `terminal-bench`): Dominated by Pod scheduling throughput, Image Streaming warmup, and compile/test CPU sensitivity. Under the default `--cpus auto --memory auto` (`guarantee`), direct Pods place `requests = limits` on the `main` container (`Guaranteed` QoS), making them eligible for exclusive CPU cores on `cpuManagerPolicy: static` node pools or `ComputeClass`es (see [Task sizing and placement](docs/task-sizing-and-placement.md)). For development runs where bursty `pytest` / `tsc` builds share idle node cores, pass `--cpus request --memory request`. This removes limits for single-container tasks only; Compose Pods always carry a Pod-level limit.
- **Storage- and DinD-heavy suites** (`orca-bench`, `long-horizon-terminal-bench`): Dominated by unpacked layer size inside `dind-engine` (`overlay2`), hardlink expansion under GCFS Image Streaming, and inner daemon memory overhead. Provision a dedicated worker pool with `1,500 GB+` boot disks for these datasets. `--ek scratch_volume_size=100Gi` moves `/var/lib/docker` onto a Persistent Disk, but `dind-engine` still reserves its full `ephemeral-storage` estimate on the node, so it does not replace node disk capacity. Shape B and Shape C automatically enforce an `8192 MiB` Pod memory-limit floor so `dockerd` + `containerd` do not OOM-kill during layer unpack on small-budget tasks.
- **ISA-sensitive scientific and EDA suites** (`apex-openroad-ibex-signoff`, `bespokelabs/terminal-bench-science`): Many prebuilt images are `linux/amd64`-only or execute AVX2/FMA instructions that crash with `SIGILL` on older or non-x86 CPUs. `harbor-gke-ext` does not detect image architecture or pin `kubernetes.io/arch`. Pair `--ek task_machine_types`, `--ek task_node_pools`, or `--ek task_compute_classes` with AVX2-capable x86 node pools (`n2-standard-*`, `c3-standard-*`), as shown in [Cluster setup](docs/cluster-setup.md).

See [Cluster setup](docs/cluster-setup.md), [Task sizing and placement](docs/task-sizing-and-placement.md), and [Dataset notes](docs/dataset-notes.md) for concrete node pool, `ComputeClass`, and per-dataset recipes.

## Roadmap

- **Dataset-driven NodePool and `ComputeClass` generator**: A standalone planner (`harbor-gke-ext-cluster-plan`) that scans a dataset's `task.toml` budgets, Compose shapes (A/B/C), DinD storage requirements, GPU/TPU requests, and OCI image architectures (`amd64` vs `arm64`) to emit ready-to-apply GKE Standard `NodePool` definitions, Autopilot `ComputeClass` manifests, and `--ek task_node_pools` / `task_compute_classes` mappings.
- **External image pull-through caching via Artifact Registry remote repositories**: Zero-build pull-through caching for `public.ecr.aws`, `ghcr.io`, and `mcr.microsoft.com` using Artifact Registry remote repositories (`--mode=remote-repository`) and automatic image reference rewriting, preserving upstream `sha256` digests and enabling GKE Image Streaming across external registries.

## Documentation

| Document | Contents |
| --- | --- |
| [Cluster setup](docs/cluster-setup.md) | Provisioning Autopilot and Standard clusters, dataset optimization guide, IAM, node pools, ComputeClasses. |
| [Configuration reference](docs/configuration.md) | Every `--ek` key, plugin option, and `task.toml` field, with defaults. |
| [Task sizing and placement](docs/task-sizing-and-placement.md) | Resource semantics (`guarantee` vs `request`), static-CPU placement, boot disk sizing formulas, and node packing. |
| [Dataset notes](docs/dataset-notes.md) | Per-dataset calibration rules, known upstream task issues, and recommended flags. |
| [Architecture](docs/architecture.md) | Execution model, the three Pod shapes (A, B, C), module map, invariants. |
| [Compose translation](docs/compose-translation.md) | Normalization, the placement classifier, Pod shapes, per-key fidelity. |
| [Docker-in-Docker](docs/docker-in-docker.md) | Shape B and Shape C mechanics, `docker pull` image materialization, `dind_services` routing, Autopilot admissibility. |
| [Autopilot](docs/autopilot.md) | Support status, exec admission throughput, GKE Autopilot resource rounding, `emptyDir` storage caps, `scratch_volume_size`, DinD allowlist, and gVisor. |
| [Networking and security](docs/networking-and-security.md) | Network modes, policy objects, security contexts, threat model. |
| [Accelerators](docs/accelerators.md) | GPUs, GPUs inside Compose, TPUs, storage and ComputeClass interaction. |
| [Runtime](docs/runtime.md) | Lifecycle, exec transports, retry budgets, concurrency, cleanup. |
| [Images and builds](docs/images.md) | Content addressing, lazy repository creation, BuildKit caching, Cloud Build, prebuilding. |
| [Troubleshooting](docs/troubleshooting.md) | Runbook, indexed by error message. |
| [Known issues](docs/known-issues.md) | Known v0.1.0 behavioral limitations and workarounds. |
| [Design decisions](docs/design-decisions.md) | Why the design is what it is, with the measurements behind it and the roadmap. |

## Limitations

`harbor-gke-ext` has the following architectural limitations (for v0.1.0 behavioral limitations and workarounds, see [Known issues](docs/known-issues.md)):

- Only the first entry in `gpu_types` is honoured. Additional entries are logged
  and dropped.
- GPU and TPU cannot be requested together. This is rejected at construction.
- TPU support is single-host only. Compose tasks can attach a TPU to the `main`
  service only, and there is no TPU support in the Docker-in-Docker plane.
- Task image digests do not cover file modes. Changing only a file's permission
  bits reuses the cached image. See [Images and builds](docs/images.md).
- There is no bulk cleanup pass. Finished Jobs are reclaimed by
  `ttlSecondsAfterFinished`, but objects orphaned by a hard process kill are not
  swept. See [Runtime](docs/runtime.md).
- `no-network` blocks all egress, including DNS.
- Some Compose constructs cannot be translated and therefore not supported. The fatal reason codes
  are listed in [Compose translation](docs/compose-translation.md).
- On GKE Autopilot, the GKE Warden admission webhook validates every exec as
  part of Autopilot's managed policy enforcement. In oracle calibration runs,
  throughput plateaued at about 35 exec-heavy trials per minute per cluster, so
  run Autopilot evaluations with `-n 50` to `-n 100`. Shape B and Shape C
  (Docker-in-Docker) tasks require a customer-owned `WorkloadAllowlist`. Standard
  clusters are the better fit for large, high-concurrency, or Docker-in-Docker
  evaluations. See [Autopilot support status](docs/autopilot.md#support-status).

## Development

Run the test suite, linter, formatter, and type checker from the repository root:

```bash
uv run --no-sync pytest tests/unit
uv run --no-sync ruff check --fix .
uv run --no-sync ruff format src tests
uv run --no-sync ty check
```

Examples live in [`examples/tasks/`](examples/tasks/). The
[`gpu-sidecar`](examples/tasks/gpu-sidecar) task exercises the most demanding
path in the package: two Compose services that collide on a port, which routes
them into the Docker-in-Docker plane, with a GPU attached to one of them.
