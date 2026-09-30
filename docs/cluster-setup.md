# Cluster setup and provisioning

This guide explains how to provision a Google Kubernetes Engine (GKE) cluster for running evaluation workloads with `harbor-gke-ext`.

## Prerequisites

Before provisioning a cluster, ensure your local environment has the required tools:
- Google Cloud SDK (`gcloud`), version 480.0.0 or later recommended, with `gke-gcloud-auth-plugin`
- Kubernetes CLI (`kubectl`), version 1.28 or later recommended. This guide uses `kubectl` to apply and verify cluster resources; `harbor-gke-ext` itself doesn't invoke it.
- Python 3.12 or later with `uv`

`harbor-gke-ext` doesn't check tool versions.

Export your Google Cloud project and cluster configuration into your shell. These variables are used throughout this guide:

```bash
export PROJECT_ID="your-project-id"
export LOCATION="us-central1"
export REGION="us-central1"
export CLUSTER_NAME="harbor-eval-cluster"
export NODE_SA="harbor-gke-node-sa@${PROJECT_ID}.iam.gserviceaccount.com"
```

Enable the required Google Cloud APIs (`aiplatform.googleapis.com` is needed only when agents authenticate to Gemini Enterprise Agent Platform f.k.a. "Vertex AI"):

```bash
gcloud services enable \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  aiplatform.googleapis.com \
  --project="${PROJECT_ID}"
```

(Optional) Create the Artifact Registry repository to store built task images. If omitted, `harbor-gke-ext` creates `harbor-tasks` lazily on the first build cache miss when the caller has `roles/artifactregistry.admin`. Pre-creating it allows runtime jobs to run with narrower permissions:

```bash
gcloud artifacts repositories create harbor-tasks \
  --repository-format=docker \
  --location="${REGION}" \
  --project="${PROJECT_ID}"
```

## Node service account

To adhere to the principle of least privilege, create a dedicated service account for your GKE worker nodes. This account ensures nodes can write logs, export metrics, and pull benchmark images from Artifact Registry without `ImagePullBackOff`.

```bash
# Create the service account
gcloud iam service-accounts create harbor-gke-node-sa \
  --display-name="Harbor GKE Node Service Account" \
  --project="${PROJECT_ID}" || true

# Wait briefly for IAM service account propagation before binding roles
sleep 10

# Grant minimum permissions for GKE system tasks
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --role="roles/container.defaultNodeServiceAccount" \
  --member="serviceAccount:${NODE_SA}"

# Grant read access for Artifact Registry
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --role="roles/artifactregistry.reader" \
  --member="serviceAccount:${NODE_SA}"
```

## Cluster creation

The `harbor-gke-ext` environment supports two GKE operational models. Choose the one that matches your workload:

- **Path A: Standard cluster with 3-tier pool layout (Recommended for Harbor)**. Combines a minimal on-demand system pool (`default-pool`), pre-created scale-to-zero multi-core CPU (`harbor-workers`) and GPU worker pools with **`500 GB` boot disks** (`339.2 GiB` allocatable, or `300–500 GB` on 1-GPU nodes), and **Node Auto-Provisioning (NAP)** for custom `ComputeClass` pools (`harbor-static-cpu`, which sets its own `500 GB` boot disks) and specialty accelerators. Supports all Pod shapes (direct, Compose sidecars, and privileged DinD without organizational allowlists), full `x86_64` AVX2 machine selection (`n2`/`c3`), and `$0/hr` worker cost when scaled to zero.
- **Path B: Autopilot cluster**. Google manages the node lifecycle. Best when your organization requires zero node-pool administration and your benchmarks use direct Pods or Shape A (native single-container or sidecar Pods). Apply the [`harbor-autopilot` ComputeClass](#2-apply-the-harbor-autopilot-computeclass) so Compose Pods share nodes instead of each taking a whole `Performance` node. Shape B and Shape C (DinD) need a customer-owned `WorkloadAllowlist`, which is available only to [eligible Google Cloud customers](#optional-privileged-dind-on-autopilot-workloadallowlist). In oracle calibration runs on Autopilot, throughput plateaued at about 35 exec-heavy trials per minute, so run with `-n 50` to `-n 100`; Path A is the better fit for large or high-concurrency evaluations. For details, see [Autopilot support status](autopilot.md#support-status).

For a detailed comparison of features and behaviors, see the [Architecture](architecture.md) documentation.

### Path A: Standard cluster with minimal system pool, scale-to-zero worker pools, and NAP (Recommended)

This 3-tier layout keeps idle cluster cost minimal while providing isolated worker capacity and fast boot-disk I/O:

1. **Tier 1 — System pool (`default-pool`)**: `1` on-demand `e2-standard-4` node with a `100 GB` boot disk, tainted with `CriticalAddonsOnly=true:NoSchedule` so Harbor trial Pods never land on it. Core `kube-system` components (`kube-dns`, `konnectivity-agent`, `metrics-server`) tolerate the taint and run here. Several GKE-managed components do **not** tolerate it (observed on GKE 1.35: `gke-managed-cim/kube-state-metrics`, `gmp-system/gmp-operator`, `kube-system/event-exporter-gke`, `kube-system/antrea-controller-horizontal-autoscaler`), so they run on an untainted worker node while one exists, and NAP creates one small node for them when all worker pools scale to `0`. The NAP defaults below keep that node's boot disk at `100 GB`.
2. **Tier 2 — Pre-created scale-to-zero CPU and GPU worker pools (`harbor-workers`, and a GPU pool if you run GPU tasks)**:
   - `harbor-workers`: `n2-standard-16` (16 vCPUs, 64 GiB RAM) with a **`500 GB` (`339.2 GiB` allocatable) `pd-balanced` or `pd-ssd` boot disk** and `--total-min-nodes=0 --total-max-nodes=16`. Standard benchmark tasks declare `10–20 GiB` of storage, so packing 7 to 14 tasks onto a 16-vCPU node consumes `140 GiB` (`< 45%` of `339.2 GiB` allocatable). Use `pd-ssd` (`21,000 IOPS` disk-side at `500 GB`: `6,000` baseline plus `30` per GiB, subject to the VM's per-instance limit) when packing I/O-intensive test suites (such as Node.js/TypeScript, Go, or Rust builds), or see [Storage arithmetic](#storage-arithmetic-and-computeclass-resolution) if your dataset packs multiple `300 GiB+` heavy-storage tasks per node (`1,500–2,000 GB`).
   - GPU pool (recommended for GPU tasks): an autoscaling pool for the accelerator type that your tasks request. The example `l4-gpu-pool` uses `g2-standard-8` (or `g2-standard-12`) with `1 × nvidia-l4`, `--disk-size=500` (`300–500 GB`), `--enable-image-streaming`, and `--total-min-nodes=0 --total-max-nodes=8`. Because a 1-GPU node runs a single GPU task at a time, a `500 GB` disk (`339.2 GiB` allocatable) fits even a single `300 GiB` GPU task while using `3×` less regional SSD quota than a `1,500 GB` disk.
3. **Tier 3 — Node Auto-Provisioning (`nap-config.yaml`)**: Dynamically provisions custom `ComputeClass` pools (such as `harbor-static-cpu` with `cpuManagerPolicy: static`), specialty accelerators, or overflow capacity with your least-privilege `NODE_SA`. A `ComputeClass` priority rule's `storage.bootDiskSize` overrides the NAP default, so worker disk size is set per `ComputeClass` (`500 GB` in the [`harbor-static-cpu` recipe](task-sizing-and-placement.md#static-cpu-nodes-with-node-auto-provisioning-computeclass)). The NAP default (`100 GB`) applies only to nodes that no `ComputeClass` or pre-created pool covers, such as the system overflow node above. GPU tasks that fall through to NAP also get `100 GB` (`43.8 GiB` allocatable); route GPU tasks with larger footprints to a pre-created GPU pool.

```bash
# 1. Create NAP configuration file (100 GB default boot disks; ComputeClasses set their own sizes)
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

# 2. Create the Standard cluster with a minimal, tainted on-demand default-pool
gcloud container clusters create "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --release-channel="regular" \
  --num-nodes=1 \
  --machine-type="e2-standard-4" \
  --disk-size=100 \
  --disk-type="pd-balanced" \
  --enable-autoscaling --total-min-nodes=1 --total-max-nodes=2 \
  --node-taints="CriticalAddonsOnly=true:NoSchedule" \
  --service-account="${NODE_SA}" \
  --enable-autoprovisioning \
  --autoprovisioning-config-file=nap-config.yaml \
  --autoscaling-profile="optimize-utilization" \
  --enable-image-streaming \
  --enable-dataplane-v2 \
  --enable-fqdn-network-policy \
  --workload-pool="${PROJECT_ID}.svc.id.goog" \
  --enable-shielded-nodes \
  --cluster-dns="clouddns" \
  --cluster-dns-scope="cluster" \
  --addons="NodeLocalDNS"

# 3. Create the primary scale-to-zero worker pool (16 vCPU, 500 GB disk -> 339.2 GiB allocatable)
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

# 4. (Recommended for GPU tasks) Pre-create a scale-to-zero autoscaling GPU pool so GPU tasks
#    avoid cold NAP pool creation. This example uses nvidia-l4; use the accelerator type your
#    tasks request, and zones that offer it (nvidia-l4 in us-central1: a, b, c).
gcloud container node-pools create l4-gpu-pool \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --node-locations="${REGION}-a,${REGION}-b,${REGION}-c" \
  --project="${PROJECT_ID}" \
  --machine-type="g2-standard-8" \
  --accelerator="type=nvidia-l4,count=1,gpu-driver-version=default" \
  --disk-size=500 \
  --disk-type="pd-balanced" \
  --service-account="${NODE_SA}" \
  --enable-image-streaming \
  --num-nodes=0 \
  --enable-autoscaling --total-min-nodes=0 --total-max-nodes=8

# 5. Configure kubectl
gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --location="${LOCATION}" \
  --project="${PROJECT_ID}"
```

> [!WARNING]
> Check that the NAP disk defaults were actually applied. If
> `autoprovisioningNodePoolDefaults` has no `diskSizeGb` / `diskType` (for example,
> when NAP was enabled with `--enable-autoprovisioning` flags instead of this config
> file), auto-provisioned nodes get the GKE defaults: `100 GiB` `pd-balanced` boot disks
> ([node auto-provisioning](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/node-auto-provisioning)).
> If node disk throughput plateaus at the `pd-balanced` baseline (`140 + 0.28 × 100 = 168 MiB/s`
> on a `100 GiB` disk), NAP created the node pool with default disk settings rather than your
> `autoprovisioningNodePoolDefaults`. Verify with:
>
> ```bash
> gcloud container clusters describe "${CLUSTER_NAME}" \
>   --location="${LOCATION}" --project="${PROJECT_ID}" \
>   --format="yaml(autoscaling.autoprovisioningNodePoolDefaults)"
> ```
>
> If `diskSizeGb` and `diskType` are missing, reapply the config file. The new
> defaults apply only to node pools that NAP creates afterwards:
>
> ```bash
> gcloud container clusters update "${CLUSTER_NAME}" \
>   --location="${LOCATION}" --project="${PROJECT_ID}" \
>   --enable-autoprovisioning \
>   --autoprovisioning-config-file=nap-config.yaml
> ```

> [!IMPORTANT]
> Create the cluster with `--enable-image-streaming` (step 2) so that the cluster-level
> default covers node pools that NAP creates. Per-pool `--enable-image-streaming` flags only
> cover the pools you create yourself. Verify the cluster default and each pool:
>
> ```bash
> gcloud container clusters describe "${CLUSTER_NAME}" \
>   --location="${LOCATION}" --project="${PROJECT_ID}" \
>   --format="value(nodePoolDefaults.nodeConfigDefaults.gcfsConfig.enabled)"
> gcloud container node-pools list --cluster="${CLUSTER_NAME}" \
>   --location="${LOCATION}" --project="${PROJECT_ID}" \
>   --format="table(name,config.gcfsConfig.enabled,autoscaling.autoprovisioned)"
> ```
>
> The first command must print `True`. On an existing cluster, enable it with
> `gcloud container clusters update "${CLUSTER_NAME}" --enable-image-streaming`. Without
> Image Streaming, nodes pull images in full, and large images can delay the start of
> unrelated Pods on the same node (see
> [Task sizing and placement](task-sizing-and-placement.md#recommendations)).

> [!IMPORTANT]
> If using Network Policies, **always create GKE Standard clusters with `--enable-dataplane-v2 --enable-fqdn-network-policy` (step 2).**
> On GKE Standard, the default Legacy Datapath without `--enable-dataplane-v2` or `--enable-network-policy` (Calico) accepts `NetworkPolicy` objects into the Kubernetes API server while **silently ignoring them in the dataplane**. `harbor-gke-ext` probes the cluster for an active `NetworkPolicy` controller at startup and fails closed if none is active.
> - **GKE Dataplane V2 (`--enable-dataplane-v2 --enable-fqdn-network-policy`)** is strongly recommended: it enforces `NetworkPolicy` in-kernel via eBPF before a Pod's network interface is attached, supports `FQDNNetworkPolicy` for domain and wildcard allowlists, and is compatible with GKE Sandbox (`gVisor`). Note that `--enable-dataplane-v2` must be set **at cluster creation time**.
> - **Legacy Datapath + Calico (`--enable-network-policy`)** can be enabled on an existing Legacy Datapath cluster (`gcloud container clusters update "${CLUSTER_NAME}" --update-addons=NetworkPolicy=ENABLED && gcloud container clusters update "${CLUSTER_NAME}" --enable-network-policy`) and enforces `no-network`, IP/CIDR `allowlist`, and metadata server blocking (`169.254.169.254` and `169.254.169.252`), but does **not** support `FQDNNetworkPolicy`, and requires `calico-node` (`projectcalico.org/ds-ready=true`) to be healthy on every schedulable node. See [Networking and security](networking-and-security.md#cluster-enforcement-detection-dataplane-v2-vs-calico).

### Path B: Autopilot cluster

Create a GKE Autopilot cluster. Autopilot automatically configures Dataplane V2, Workload Identity, and enforces the Pod Security Standards (PSS) baseline.

#### 1. Create the cluster

```bash
# Create the cluster
gcloud container clusters create-auto "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --service-account="${NODE_SA}" \
  --release-channel="regular"

# Configure kubectl
gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --location="${LOCATION}" \
  --project="${PROJECT_ID}"

# Enable FQDN Network Policy for domain allowlisting
gcloud container clusters update "${CLUSTER_NAME}" \
  --location="${LOCATION}" \
  --project="${PROJECT_ID}" \
  --enable-fqdn-network-policy
```

#### 2. Apply the `harbor-autopilot` ComputeClass

Without a custom class, most Compose tasks end up one Pod per node:

- Autopilot adds a `1 GiB` `ephemeral-storage` request to every container that declares none, and caps general-purpose, `Balanced` and `Scale-Out` Pods at `10 GiB` in total. A Compose task with the default `10 GiB` of storage plus any sidecar goes over that cap.
- `harbor-gke-ext` therefore promotes such Pods to the `Performance` class (see [Autopilot](autopilot.md#computeclass-resolution-and-ephemeral-storage)). Autopilot schedules `Performance` Pods one per node (it adds `cloud.google.com/pod-slots: 1` to the Pod).

A custom `ComputeClass` with `machineFamily` priorities avoids both limits. Pods on it are billed per node, Autopilot can place several Pods on one node, and the `10 GiB` cap does not apply (a server dry-run admitted a single Pod requesting `200Gi` on GKE Autopilot `1.35.8`):

```bash
cat <<EOF | kubectl apply -f -
apiVersion: cloud.google.com/v1
kind: ComputeClass
metadata:
  name: harbor-autopilot
spec:
  nodePoolAutoCreation:
    enabled: true
  priorities:
  - machineFamily: n2
    minCores: 16
    storage:
      bootDiskType: pd-balanced
      bootDiskSize: 500
  - machineFamily: n2d
    minCores: 16
    storage:
      bootDiskType: pd-balanced
      bootDiskSize: 500
  - machineFamily: c3
    minCores: 16
    storage:
      bootDiskType: pd-balanced
      bootDiskSize: 500
  - machineFamily: n4
    minCores: 16
  whenUnsatisfiable: DoNotScaleUp
EOF
```

Then pass it to every run:

```bash
harbor run ... \
  -e harbor_gke_ext:GKEEnvironment \
  --ek cluster_name="${CLUSTER_NAME}" \
  --ek compute_class=harbor-autopilot
```

Notes:

- **Why several families.** Large `n2` shapes might not have available capacity in every zone of a region. On `us-central1` we observed repeated `ScaleUpFailed ... RESOURCE_POOL_EXHAUSTED` events and a Pod pending for `852 s` with only `n2` and `n4` in the list. Autopilot tries the priorities in order.
- **Boot disks.** `storage.bootDiskSize` / `bootDiskType` on Autopilot require GKE `1.34.1-gke.1431000` or later, and Autopilot accepts only `pd-balanced`. `n4` supports only Hyperdisk boot disks, so its rule has no `storage` block and gets the Autopilot default. `500 GB` matches the Standard `harbor-workers` recommendation (see [Storage arithmetic](#storage-arithmetic-and-computeclass-resolution)).
- **`minCores: 16`** keeps nodes large enough to pack several tasks and to fit `16`-vCPU tasks. Lower it if your dataset uses small tasks only.
- **GPU tasks ignore it.** Accelerator Pods use native `cloud.google.com/gke-accelerator` selectors, and `--ek compute_class` is not applied to them.
- **CPU visibility.** Autopilot has no `cpuManagerPolicy: static` option, so `nproc` inside a task reports the node's vCPU count, not the task budget. Some toolchains size worker pools from `nproc` (for example, Go test runners); see [Task sizing and placement](task-sizing-and-placement.md).

#### Optional: Privileged DinD on Autopilot (`WorkloadAllowlist`)

Shape B and Shape C Pods run a privileged `dind-engine` container, which Autopilot rejects by default. Without a customer-owned `WorkloadAllowlist`, such tasks fail in seconds with `UnsupportedComposeFeatureError: AUTOPILOT_DIND_UNAVAILABLE`, and only direct and Shape A tasks run.

> [!IMPORTANT]
> Allowlists for your own privileged workloads are **available only to eligible Google Cloud customers** and require GKE `1.35` or later. Ask [Cloud Customer Care](https://cloud.google.com/support-hub) to enable the feature before you start. If you are not eligible, run Shape B and Shape C datasets on a [Standard cluster](#path-a-standard-cluster-with-minimal-system-pool-scale-to-zero-worker-pools-and-nap-recommended).

For the full procedure, see [Enabling privileged DinD with a `WorkloadAllowlist`](autopilot.md#enabling-privileged-dind-with-a-workloadallowlist).

## Artifact Registry access for Docker-in-Docker (`dind-engine`) under Workload Identity

When Workload Identity Federation is enabled on a GKE cluster (`--workload-pool="${PROJECT_ID}.svc.id.goog"` on Standard, or by default on Autopilot), every node pool runs with `workloadMetadataConfig.mode: GKE_METADATA`:

- **Host-level pulls (`kubelet`)** authenticate using the node's Compute Engine service account (`${NODE_SA}`).
- **In-Pod pulls (`dind-engine` / `dind-cache-<service>`)** query `http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token`, which is intercepted by `gke-metadata-server` and returns an OAuth2 token for the **Pod's Workload Identity principal** (`principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/<namespace>/sa/<ksa>`), **not** `${NODE_SA}`.

`dind-engine` fetches that token from the metadata server, so the pull path needs `--ek allow_metadata_server=true` (see [Image delivery under restricted network modes](#image-delivery-under-restricted-network-modes)). With metadata access allowed, if the Workload Identity pool does not have `roles/artifactregistry.reader` on your Artifact Registry repository (`harbor-tasks`), `docker pull` inside `dind-cache-<service>` fails with `Permission 'artifactregistry.repositories.downloadArtifacts' denied` and falls back to streaming the container rootfs via `tar -cf - / | docker import` (which loses hardlinks under GKE Image Streaming `gcfs` and can inflate large hardlink-heavy images to `300 GiB+`).

**After** creating the cluster (which initializes the Workload Identity pool `${PROJECT_ID}.svc.id.goog` in the project), create the `harbor-tasks` repository (if not already present) and grant `roles/artifactregistry.reader` scoped specifically to that repository:

```bash
export AR_REPO="harbor-tasks"
export PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)" -q)

gcloud artifacts repositories create "${AR_REPO}" \
  --repository-format=docker \
  --location="${REGION}" \
  --project="${PROJECT_ID}" || true

gcloud artifacts repositories add-iam-policy-binding "${AR_REPO}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/*" \
  --role="roles/artifactregistry.reader"
```

> [!IMPORTANT]
> You must create at least one cluster with `--workload-pool="${PROJECT_ID}.svc.id.goog"` (or an Autopilot cluster) **before** running `gcloud artifacts repositories add-iam-policy-binding`, because the `workloadIdentityPools/${PROJECT_ID}.svc.id.goog` resource does not exist until GKE initializes it. Binding `roles/artifactregistry.reader` at the repository level (`gcloud artifacts repositories add-iam-policy-binding "${AR_REPO}"`) grants read access only to task container images in `harbor-tasks`, not to any other project resource. See [Docker-in-Docker](docker-in-docker.md) for details.

### Image delivery under restricted network modes

DinD images reach the inner `dockerd` in two steps, and the trial's network mode only affects the second one:

1. **The kubelet pulls every image onto the node.** This covers the `dind-engine` image and every `dind-cache-<service>` init container, whose image is the service's own image. The kubelet pulls on the node network with `${NODE_SA}`, and `NetworkPolicy` does not apply to it, so every image reaches the node in every network mode.
2. **`dind-cache-<service>` loads the image into `dind-engine`.** The trial's `NetworkPolicy` is applied before the Pod is created, and all containers in a Pod share one network namespace, so this step runs under the trial's policy. `dind-cache-<service>` tries, in order:
   - Skip the image if it is already present in `dind-engine`.
   - `docker pull` from the registry. Registry credentials come from a metadata-server token that `dind-engine` refreshes every `900s`, and they are sent only to Google registries (`gcr.io`, `*.gcr.io`, `*.pkg.dev`).
   - If the pull fails, stream the image's root filesystem from the node-pulled container (`tar -cf - / | docker import`) and restore its OCI config (`WORKDIR`, `USER`, `ENV`, `ENTRYPOINT`, `CMD` and more) with `--change` flags.

The fallback needs no network, because its source is already on the node, so DinD tasks run in every network mode. The fallback does lose hardlinks under Image Streaming (`gcfs`) and does not share layers between images. On hardlink-dense images this multiplies disk usage (see [Dataset notes](dataset-notes.md#orca-benchorca-bench-verified)).

| Network mode | `docker pull` path | Result |
| --- | --- | --- |
| `no-network` | Blocked. Egress is empty, including DNS and the metadata server. | Always uses the rootfs fallback. |
| `allowlist` | Works only if the registry host is allowlisted. Private Artifact Registry images also need `--ek allow_metadata_server=true` for the token. | Pull, otherwise rootfs fallback. |
| `public` (default, `allow_metadata_server=false`) | The registry is reachable, but the policy blocks `169.254.169.254`, so `dind-engine` gets no token. | Public images pull anonymously. Private Artifact Registry images use the rootfs fallback. |
| `public` with `--ek allow_metadata_server=true` | Works if the Workload Identity pool has `roles/artifactregistry.reader`. | Pull. |

On GKE Dataplane V2, Pods reach the metadata server at `169.254.169.254` (port `80`), so excluding that address in the `NetworkPolicy` blocks the token request.

No image is built inside a trial. The inner Compose project runs with `--pull never`, and every `build:` section is replaced with a pre-built Artifact Registry image, which the kubelet pulls in step 1. DinD tasks therefore need no registry egress for builds.

## Agent authentication via Workload Identity Federation

Evaluation agents running inside isolated pods that call Gemini Enterprise Agent Platform directly require `roles/aiplatform.user` (together with `--ek allow_metadata_server=true` at runtime). Bind the cluster's Workload Identity pool using a cluster-level `principalSet`:

```bash
export PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)" -q)

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --role="roles/aiplatform.user" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/kubernetes.cluster/https://container.googleapis.com/v1/projects/${PROJECT_ID}/locations/${LOCATION}/clusters/${CLUSTER_NAME}"
```

## Storage arithmetic and ComputeClass resolution

Understanding how GKE manages boot disk storage and I/O throughput is essential when packing tasks with `300 GiB+` storage requirements onto multi-core nodes.

### Standard cluster storage arithmetic and vCPU bin-packing

On GKE Standard, a node's boot disk (`--disk-size` / `diskSizeGb`, specified in binary GiB, `2^30` bytes) is shared between Pod ephemeral storage (`emptyDir` and writable container layers), the GKE Image Streaming (`gcfs`) block cache, and the underlying container runtime (`imagefs`). On `COS_CONTAINERD` nodes, the boot disk reserves ~`4.2 GiB` for fixed OS partitions (`rootA`, `rootB`, `OEM`, `EFI`) and ~`1.56%` (`1/64`) for `ext4` metadata on the stateful partition, so the node's filesystem capacity (`.status.capacity.ephemeral-storage`) is `C_GiB ≈ (63 / 64) * D - 4.184 GiB`. GKE then deducts two reservations from `C_GiB`:

1. **System Reservation**: `MIN(50% * D, 35% * D + 6 GiB, 100 GiB)` — **caps at `100 GiB` once the disk reaches `269 GB`**.
2. **Eviction Threshold**: `10%` of filesystem capacity (`0.10 * C_GiB`).

When sizing node boot disks, account for three runtime effects:
- **DinD `overlay2` layer expansion**: Tasks that use `dind-engine` (Shapes B and C) unpack compressed OCI layers into `/var/lib/docker`, expanding roughly `3.0×` over compressed manifest size (plus a `10 GiB` floor per Pod).
- **Hardlink multiplication under GCFS Image Streaming**: When an OCI image stores many snapshots or build trees as hardlinks, GKE Image Streaming (`gcfs`) reports a link count of `1` (`st_nlink = 1`) on every file. Copying those files or materializing the image inside an inner Docker daemon (`dind-engine`) writes each hardlinked path as an independent full-size file, multiplying both disk space and sequential write time.
- **Sustained write throughput**: Per the [Persistent Disk performance documentation](https://cloud.google.com/compute/docs/disks/performance), a `pd-balanced` disk of `x` GiB reaches `MIN(VM limit, 140 + 0.28x, 1,200)` MiB/s and `MIN(VM limit, 3,000 + 6x, 80,000)` IOPS, where "VM limit" is the per-machine-type limit.

Because the `100 GiB` system reservation is already paid by `269 GB`, **every additional `500 GB` of disk above `500 GB` yields `~443 GiB` of pure allocatable `ephemeral-storage` (`88.6%` marginal efficiency)**, while also scaling `pd-balanced` performance. The last column shows the disk-side limit only:

| Boot Disk Size (`pd-balanced`) | Filesystem Capacity (`COS_CONTAINERD`) | System + Eviction Overhead | Node Allocatable `ephemeral-storage` | Max Concurrent `300 GiB` Tasks / Node | Max Storage / Task at **8 Tasks/Node** (`n2-standard-16`) | `pd-balanced` Disk-Side Throughput / IOPS Limit |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **100 GB** (`default-pool`) | 94.3 GiB | 50.4 GiB | **43.8 GiB** | **0** | 5.4 GiB | 168 MiB/s / 3,600 IOPS |
| **500 GB** | 488.0 GiB | 148.8 GiB | **339.2 GiB** | **1** *(strands 14 of 16 vCPUs)* | 42.4 GiB | 280 MiB/s / 6,000 IOPS |
| **1,000 GB (1 TB)** | 980.2 GiB | 198.0 GiB | **782.2 GiB** *(2.3× vs 500 GB)* | **2** | 97.7 GiB | **420 MiB/s / 9,000 IOPS** |
| **1,500 GB (1.5 TB)** | 1,472.4 GiB | 247.2 GiB | **1,225.1 GiB** *(3.6× vs 500 GB)* | **4** | 153.1 GiB | **560 MiB/s / 12,000 IOPS** |
| **2,000 GB (2 TB)** | 1,964.6 GiB | 296.5 GiB | **1,668.1 GiB** *(4.9× vs 500 GB)* | **5** | 208.5 GiB | **700 MiB/s / 15,000 IOPS** |

- **For standard benchmarks (`10–30 GiB` per task) and 1-GPU nodes**: A **`500 GB`** (`339.2 GiB` allocatable) boot disk is the right baseline. Packing 7 to 14 tasks onto a 16-vCPU node requests `140 GiB` (`< 42%` of allocatable space), and a 1-GPU node runs only 1 GPU task at a time (fitting up to a `300 GiB` GPU task in `339.2 GiB`). When packing I/O-sensitive test suites (Node.js/TypeScript, Go, Rust), prefer **`500 GB pd-ssd`** (`21,000 IOPS` disk-side, lower random I/O latency) over `pd-balanced` (`6,000 IOPS`).
- **For `300 GiB+` heavy-storage datasets (EDA, dense DinD)**: Provision a dedicated scale-to-zero pool (`heavy-cpu-storage-pool`) with **`1,500 GB`–`2,000 GB`** boot disks so a single `n2-standard-16` or `n2-standard-32` node can co-schedule **4 to 5 `300 GiB` tasks** (`560–700 MiB/s` disk-side throughput). `--ek scratch_volume_size=100Gi` moves DinD and Compose volume writes to per-Pod Persistent Disks, but doesn't lower the Pod's `ephemeral-storage` request on the node (see [Ephemeral storage schedulability](#3-ephemeral-storage-schedulability-and-generic-ephemeral-volumes)).

For further information on system reservations, consult the [GKE node sizing documentation](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/plan-node-sizes).

### Scale-to-zero ultra-dense and GPU pools

For `32-vCPU` ultra-dense packing of `300 GiB+` tasks (`2,000 GB` boot disk) or GPU benchmarks (`300–500 GB` boot disk) on a Standard cluster, pre-create dedicated scale-to-zero node pools:

```bash
# Ultra-dense 32-vCPU pool with 2 TB storage (1,668 GiB allocatable, packs five 300 GiB tasks per node)
gcloud container node-pools create heavy-cpu-storage-pool \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --machine-type="n2-standard-32" \
  --disk-size=2000 \
  --disk-type="pd-balanced" \
  --service-account="${NODE_SA}" \
  --enable-image-streaming \
  --enable-autoscaling --total-min-nodes=0 --total-max-nodes=10

# Scale-to-zero L4 GPU pool (g2-standard-8 or g2-standard-12) with 500 GB storage and Image Streaming
gcloud container node-pools create l4-gpu-pool \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --node-locations="${REGION}-a,${REGION}-b,${REGION}-c" \
  --project="${PROJECT_ID}" \
  --machine-type="g2-standard-12" \
  --accelerator="type=nvidia-l4,count=1,gpu-driver-version=default" \
  --disk-size=500 \
  --disk-type="pd-balanced" \
  --service-account="${NODE_SA}" \
  --enable-image-streaming \
  --enable-autoscaling --total-min-nodes=0 --total-max-nodes=10

# Scale-to-zero H100 GPU pool with 500 GB storage
gcloud container node-pools create heavy-gpu-h100-pool \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --machine-type="a3-highgpu-1g" \
  --accelerator="type=nvidia-h100-80gb,count=1,gpu-driver-version=default" \
  --disk-size=500 \
  --disk-type="pd-balanced" \
  --service-account="${NODE_SA}" \
  --enable-image-streaming \
  --enable-autoscaling --total-min-nodes=0 --total-max-nodes=10
```

**Why pre-create a scale-to-zero GPU pool and how to override GPU types:**
- Relying on Node Auto-Provisioning (NAP) alone for GPU tasks incurs an extra pool-creation round-trip on the first GPU Pod and depends on capacity in a single zone. A pre-created regional autoscaling pool for the accelerator type that your tasks request, with `--total-min-nodes=0`, costs `$0/hr` when idle, enables GKE Image Streaming explicitly, and lets the Cluster Autoscaler place new nodes in whichever zone has free accelerator capacity. The `l4-gpu-pool` and `heavy-gpu-h100-pool` commands above are examples.
- Many benchmark tasks declare `gpu_types = ["A100"]` (or `a100-80gb`) in `task.toml` even when their model weights fit comfortably inside the `24 GiB` VRAM of an NVIDIA L4. To schedule those tasks onto a pool with a different accelerator, such as the example `l4-gpu-pool`, without editing `task.toml`, pass the full GKE accelerator label via `--ek gpu_override=nvidia-l4`.

### ComputeClass and placement resolution

A node pool selector (`cloud.google.com/gke-nodepool`) and a `ComputeClass` selector (`cloud.google.com/compute-class`) cannot both be satisfied by the same node. Before resolution, `_validate_placement()` rejects contradictory settings with `PlacementConflictError`:

- A job-wide `--ek node_pool` combined with any ComputeClass setting (`--ek compute_class` or `--ek task_compute_classes`).
- A task mapped in both `--ek task_node_pools` and `--ek task_compute_classes`.
- A machine-type pin (`--ek machine_type` or `--ek task_machine_types`) combined with a ComputeClass (`--ek compute_class` or `--ek task_compute_classes`) on the same task. This check applies on Standard and Autopilot clusters.
- `--ek node_pool` or `--ek task_node_pools` on an Autopilot cluster.

A per-task `--ek task_node_pools` entry can override a job-wide `--ek compute_class`. When an effective node pool is resolved, `ComputeClass` resolution returns `None`.

When no node pool is set, `_resolve_active_compute_class()` resolves `cloud.google.com/compute-class` as follows:

- **On GKE Standard clusters**:
  1. **Per-task mapping**: If the task (by full name or basename) appears in `--ek task_compute_classes`, that `ComputeClass` is used.
  2. **Global override**: Otherwise, if `--ek compute_class` is set, it is used. There is no automatic `Performance` promotion on Standard clusters.

- **On GKE Autopilot clusters**:
  1. **Per-task mapping**: If the task appears in `--ek task_compute_classes`, that `ComputeClass` always wins (including on GPU/TPU tasks).
  2. **Hardware accelerators *(Autopilot only)***: If the task requests GPUs or TPUs, `ComputeClass` is left unset (`None`) so GKE schedules the Pod using native accelerator selectors (`cloud.google.com/gke-accelerator` or `cloud.google.com/gke-tpu-accelerator`) rather than a CPU `ComputeClass`.
  3. **Global override**: If `--ek compute_class` is set, it is applied to all remaining non-accelerator tasks.
  4. **Active machine-type pin *(Autopilot only)***: If an active machine type is resolved (via `--ek task_machine_types` or `--ek machine_type`), `ComputeClass` is left unset (`None`) so storage-driven `Performance` promotion does not override the machine-type selector. Because a machine-type pin combined with a ComputeClass raises `PlacementConflictError`, this step applies only when no ComputeClass is set; its effect is to suppress automatic `Performance` promotion.
  5. **Performance promotion *(Autopilot only)***: If aggregate ephemeral storage across the Pod exceeds `10 GiB` (`10,240 MiB`), Harbor promotes the Pod to the `Performance` ComputeClass.
  6. **Default**: Otherwise, `ComputeClass` is left unset (general-purpose Autopilot).

## Optimizing your cluster for benchmark datasets

Different benchmark suites stress completely different cluster dimensions. A cluster tuned for 200 concurrent 2-CPU Python sandboxes will stall or evict Pods on a dataset with 55 GiB EDA images, nested Docker builds, or AVX2-compiled binaries.

### 1. Recommended settings by dataset archetype

| Dataset archetype | Examples | Primary bottleneck | Recommended GKE setup & `--ek` flags |
| :--- | :--- | :--- | :--- |
| **High-concurrency coding & CLI** | `terminal-bench`, `SWE-bench`, `aider-polyglot` | Pod scheduling rate, image pull latency, compile/test CPU sensitivity (`pytest`, `tsc`, `cargo`, `go test`). | Enable **GKE Image Streaming** (`--enable-image-streaming` on Standard; on by default on Autopilot) and pre-warm built images with `--plugin harbor_gke_ext:CloudBuildPlugin`. By default (`--cpus auto --memory auto`), `GKEEnvironment` resolves `auto` to `guarantee` (`requests = limits = declared budget`, matching Docker's capped default); place trials on static-CPU nodes (`cpuManagerPolicy: static`) for scored runs (see [CPU isolation for scored runs](#4-cpu-isolation-for-scored-runs)). Pass `--cpus request --memory request` during development to remove limits from direct Pods. Compose Pods keep a Pod-level limit equal to the request, or higher when declared container limits or the DinD memory floor raise it. |
| **Multi-container & Docker-in-Docker (Shapes B & C)** | `orca-bench`, `long-horizon-terminal-bench`, `swelancer` | Inner `dockerd` layer unpack (`overlay2`) disk space, hardlink expansion under GCFS, and daemon/page-cache memory overhead. | Use `1,500 GB+` node boot disks. `--ek scratch_volume_size=100Gi` backs `/var/lib/docker` and Compose named volumes with per-Pod generic ephemeral PVCs, which moves those writes off the boot disk, but `dind-engine` still requests its full `ephemeral-storage` estimate from the node (see [Ephemeral storage schedulability](#3-ephemeral-storage-schedulability-and-generic-ephemeral-volumes)). Shape B/C Pods automatically enforce an `8192 MiB` Pod memory-limit floor (`DIND_POD_MEMORY_LIMIT_FLOOR_MB`). |
| **EDA, scientific & AVX2-compiled binaries** | `apex-openroad-ibex-signoff`, `bespokelabs/terminal-bench-science` | `x86_64` ISA (`amd64`), AVX2/FMA instruction support (avoiding `SIGILL`), and 50–150 GiB unpacked toolchains. | Use `n2-standard-16` or `c3-standard-16` node pools (guaranteed modern Intel x86_64 with AVX2/AVX-512) rather than `e2` (which mixes host CPU generations) or `arm64` (`c4a`/`t2a`). Route tasks with `--ek task_machine_types`, `--ek task_node_pools`, or `--ek task_compute_classes`, and use `1,500 GB+` boot disks. |
| **GPU & ML benchmarks** | `mlgym-bench`, `replicationbench` | Accelerator availability, CUDA driver discovery (`libcuda.so.1`), large model weights. | Pre-create a scale-to-zero autoscaling pool for the accelerator type that your tasks request (for example `g2-standard-8` with `nvidia-l4`, or `a3-highgpu-1g` with `nvidia-h100-80gb`) with `--disk-size=500` (`300–500 GB`) and `--enable-image-streaming`. Pass `--ek gpu_override=<accelerator>` (for example `nvidia-l4`) to run tasks that declare a different GPU type on that pool, and `--ek default_gpu_type=<accelerator> --ek default_gpu_count=1` when Compose tasks request `count: all` without `task.toml` GPU counts. |

See [Dataset notes](dataset-notes.md) for dataset-specific configurations and known upstream task issues.

### 2. CPU architecture (`amd64` vs `arm64`) and ISA requirements

Many public benchmark images are built exclusively for `linux/amd64`. `harbor-gke-ext` doesn't inspect image architectures and doesn't add a `kubernetes.io/arch` node selector. A machine-type pin emits only a `cloud.google.com/machine-family` node selector derived from the pin, and Harbor doesn't adapt it to the image architecture. Choose the node architecture yourself:

- Run `amd64`-only images on `x86_64` nodes. If your cluster has `arm64` nodes (for example `c4a` or `t2a`), route `amd64`-only tasks to `x86_64` capacity with `--ek node_pool`, `--ek machine_type`, `--ek compute_class`, or their per-task variants.
- For binaries compiled with `-march=haswell` / AVX2 (common in EDA tools like OpenROAD, Verilator, and custom scientific wheels), avoid `arm64` node pools and prefer `n2` or `c3` machine families over `e2` (using `--ek task_machine_types`, `--ek task_node_pools`, or `--ek task_compute_classes`).

### 3. Ephemeral storage schedulability and generic ephemeral volumes

Before creating a Pod, `_assert_ephemeral_storage_schedulable()` compares the Pod's peak `ephemeral-storage` request against the largest allocatable ephemeral storage of any schedulable node in the cluster, and fails fast with `EphemeralStorageUnschedulableError` if the request is larger.

- **Peak request:** computed the way the scheduler computes it. App containers and restartable init containers (sidecars) add up; one-shot init containers count as the largest single one.
- **Ceiling:** the larger of an estimate from the cluster configuration (every untainted node pool with `maxNodes > 0`, including pools scaled to zero, plus the NAP defaults when NAP is enabled) and the largest `status.allocatable["ephemeral-storage"]` among live nodes.

The comparison is cluster-wide, not per target pool. The check is skipped when the ceiling is unknown, on Autopilot, and on clusters with Node Auto-Provisioning.

You have three levers when a dataset's storage demands exceed your default node pool:

1. **Cap the scheduler reservation (`--ek max_storage_request_mb=40960`)**: Caps `main`'s `requests.ephemeral-storage` so Pods schedule onto smaller nodes when you know the task's actual runtime write footprint is well below its declared `storage_mb`. Tasks mapped in `--ek task_compute_classes` are exempt. To override the `dind-engine` reservation, use `--ek dind_storage_mb=<mb>` or `--ek task_dind_storage_mb=<task>=<mb>`.
2. **Route storage-heavy outlier tasks to a dedicated pool (`--ek task_node_pools` / `--ek task_compute_classes`)**: Keep most of your dataset on default worker nodes and pin only the heavy tasks to a scale-to-zero high-disk pool, or to a specialized pool for tasks that need one, such as GPU tasks:
   ```bash
   --ek task_node_pools="heavy-eda-task=heavy-cpu-storage-pool,protein-docking=heavy-gpu-h100-pool"
   ```
3. **Move DinD and Compose volume writes to per-Pod Persistent Disks (`--ek scratch_volume_size=100Gi`)**: For Compose tasks, converts `harbor-dind-storage` (`/var/lib/docker`) and Compose named volumes from node-boot-disk `emptyDir` mounts into Kubernetes generic ephemeral volumes (`ephemeral.volumeClaimTemplate`), dynamically provisioning a fresh GCE PD per Pod and deleting it with the Pod. This moves the bytes off the boot disk but doesn't lower the node reservation: `dind-engine` still requests the full `ephemeral-storage` estimate (`3×` the compressed layer size plus the task storage budget, with a `10,240 MiB` floor), and `main` keeps its request. The pre-check, the scheduler, and Autopilot `Performance` promotion see the same reservation. If nodes can't hold that reservation, combine it with lever 1.

### 4. CPU isolation for scored runs

Under the default `--cpus auto --memory auto` (which resolves to `guarantee`, equivalent to `--cpus guarantee --memory guarantee`), each direct Pod is `Guaranteed` and capped at its declared CPU and memory budget, matching Harbor's Docker environment. On ordinary (shared) nodes the Pod still sees every node vCPU (`nproc`), so test runners and compilers that size their worker pools from the visible CPU count (`nproc`, `os.cpu_count()`, `make -j`, Go `< 1.25`) spawn far more parallel workers than their quota allows—causing CFS throttling, multiplied memory footprints (`N ×` worker RSS), and wall-clock timeouts under neighbor contention.

Nodes that run the kubelet with the **static CPU manager policy** (`cpuManagerPolicy: static`) grant each `Guaranteed` container with an integer CPU request exclusive physical cores (`cpuset`), and the container sees only its assigned cores (`nproc` equals the requested CPU count). On memory-bound machine types such as `e2-standard-16` or `n2-standard-16`, static-CPU nodes pack the same number of `1 CPU / 4 GiB` Pods per node as shared placement while eliminating neighbor CPU contention and worker oversubscription. Full details and sizing rules are in [Task sizing and placement](task-sizing-and-placement.md).

Two ways to provision static-CPU nodes on GKE Standard:

- **ComputeClass with node auto-provisioning** (`nodeSystemConfig.kubeletConfig.cpuManagerPolicy: static` in a priority rule, GKE 1.32.1-gke.1729000 or later). Select it with `--ek compute_class=<name>`, or per task with `--ek task_compute_classes`. See the [ComputeClass recipe](task-sizing-and-placement.md#static-cpu-nodes-with-node-auto-provisioning-computeclass).
- **A dedicated node pool** created with `--system-config-from-file` containing `kubeletConfig: {cpuManagerPolicy: static}`. Select it with `--ek node_pool=<name>`, or per task with `--ek task_node_pools`. See the [node pool recipe](task-sizing-and-placement.md#static-cpu-node-pool-gke-standard).

> [!NOTE]
> The cluster-wide NAP defaults file cannot set the CPU manager policy, so nodes auto-provisioned without a ComputeClass use the default (`none`) policy. The kubelet ignores Pod-level `spec.resources` for CPU pinning unless the `PodLevelResourceManagers` feature gate is enabled, so `harbor-gke-ext` puts a Guaranteed direct Pod's budget on the `main` container. Compose Pods keep Pod-level budgets and do not get exclusive cores.

### Roadmap: Generating NodePool specs and ComputeClasses for datasets

Sizing node pools by hand requires inspecting hundreds of `task.toml` files and `docker-compose.yaml` manifests across a dataset. The manual procedure is in [Task sizing and placement](task-sizing-and-placement.md). Planned tooling (`harbor-gke-ext-cluster-plan`) will analyze a dataset directory or registry package ahead of time — aggregating CPU/memory distributions, `storage_mb` and DinD image footprints, Pod shapes (A/B/C), GPU/TPU declarations, and OCI manifest architectures — and generate:

- **GKE Standard**: Right-sized `gcloud container node-pools create` scripts and Terraform/YAML `NodePool` manifests (clustered into baseline, DinD/high-storage, x86_64 AVX2, and accelerator tiers) together with the exact `--ek task_node_pools=...` mapping for the dataset.
- **GKE Autopilot**: Ready-to-apply `ComputeClass` custom resources (`cloud.google.com/v1`) with prioritized machine families, Local SSD storage rules, and Spot-to-on-demand fallback priorities, plus the matching `--ek task_compute_classes=...` flag.

## Verification

Run the local `examples/tasks/airgapped-agent-platform-eval` validation task to verify your cluster setup, network isolation (`NetworkPolicy`), and Workload Identity access to Gemini Enterprise Agent Platform. Ensure you have applied the [Workload Identity Federation binding](#agent-authentication-via-workload-identity-federation) above (`roles/aiplatform.user`), then run:

```bash
uv run harbor run \
  -p examples/tasks/airgapped-agent-platform-eval \
  --agent oracle \
  -e harbor_gke_ext:GKEEnvironment \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${LOCATION}" \
  --ek cluster_name="${CLUSTER_NAME}"
```

If the task completes with reward `1.0`, your GKE cluster is correctly provisioned and authenticated.
