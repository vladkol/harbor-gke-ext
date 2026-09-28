# Accelerators

This document details how `harbor-gke-ext` schedules and manages GPUs and TPUs, including resource translation, Docker-in-Docker (DinD) delegation, pre-creating scale-to-zero L4 node pools, and GKE Autopilot `ComputeClass` interactions.

## Cluster prerequisites

For GPUs and TPUs to schedule, the target GKE cluster must meet these requirements:
- **Driver installation:** Node pools must have NVIDIA drivers installed (configured automatically when you pass `gpu-driver-version=default` on GKE node pools or use Autopilot) or TPU slice infrastructure enabled.
- **Regional quota:** Your Google Cloud project must have regional quota for the target accelerator family (for example, `NVIDIA_L4_GPUS`, `NVIDIA_A100_GPUS`, or `NVIDIA_H100_80GB_GPUS`).
- **Scale-to-zero headroom:** When using scale-to-zero GPU/TPU node pools or Node Auto-Provisioning (NAP), allow time for the cluster autoscaler to provision a Node before `pod_ready_timeout` (default `max(1200, build_timeout_sec)` seconds) expires.

For step-by-step cluster provisioning instructions, see [Cluster setup](cluster-setup.md).

## Requesting GPUs

You can request GPUs through `task.toml` (`gpus` and `gpu_types`), Docker Compose `deploy.resources.reservations.devices`, or CLI environment kwargs (`--ek`).

When resolving the GPU accelerator type, `harbor-gke-ext` applies a strict three-level precedence order:
1. `--ek gpu_override=<full-gke-label>`
2. The first entry in `task.toml`'s `[environment] gpu_types` list (`gpu_types[0]`).
3. `--ek default_gpu_type=<alias-or-label>`

> [!NOTE]
> Only the first item in `gpu_types` (`gpu_types[0]`) is used because a Kubernetes Pod can only target one accelerator family via `nodeSelector`. Additional entries in `gpu_types` log a debug message and are ignored (`pod_builder.py`).

### GPU aliases and `gpu_override` validation

In `task.toml` (`gpu_types`) and `--ek default_gpu_type`, you can specify either a short alias or the full GKE accelerator label (`resolve_gpu_accelerator_label()` in `constants.py`). Passing an unrecognized GPU type raises a `RuntimeError` listing all supported aliases and labels:

```text
RuntimeError: GPU type '<type>' is not supported on GKE. Supported types: a100, a100-40gb, a100-80gb, b200, gb200, h100, h100-mega, h200, l4, nvidia-a100-80gb, nvidia-b200, nvidia-gb200, nvidia-h100-80gb, nvidia-h100-mega-80gb, nvidia-h200-141gb, nvidia-l4, nvidia-rtx-pro-6000, nvidia-tesla-a100, nvidia-tesla-t4, nvidia-tesla-v100, rtx-pro-6000, t4, v100
```

By contrast, `--ek gpu_override` **must** be specified as the full GKE accelerator label (for example, `nvidia-l4`, not `l4`). If you pass a short alias to `gpu_override`, `GKEEnvironment._validate_gke_accelerator_config()` fails immediately at construction:

```text
RuntimeError: gpu_override must be specified as the full GKE accelerator type (e.g. 'nvidia-l4'), not short name 'l4'.
```

In Compose mode, `reconcile_gpu_config()` also validates `gpu_override` against the full label set and raises `ValueError("Invalid gpu_override '<value>'. Must be a full GKE accelerator label: ...")` if a non-label string is supplied.

| Short Alias | Full GKE Label (`cloud.google.com/gke-accelerator`) |
|---|---|
| `t4` | `nvidia-tesla-t4` |
| `l4` | `nvidia-l4` |
| `a100`, `a100-40gb` | `nvidia-tesla-a100` |
| `a100-80gb` | `nvidia-a100-80gb` |
| `v100` | `nvidia-tesla-v100` |
| `rtx-pro-6000` | `nvidia-rtx-pro-6000` |
| `h100` | `nvidia-h100-80gb` |
| `h100-mega` | `nvidia-h100-mega-80gb` |
| `h200` | `nvidia-h200-141gb` |
| `b200` | `nvidia-b200` |
| `gb200` | `nvidia-gb200` |

When a GPU is resolved, `harbor-gke-ext` configures the Pod spec as follows:
- **Resource requests and limits:** `nvidia.com/gpu` is set to the resolved count on the target container(s) (requests always equal limits, as required by Kubernetes extended resources).
- **Tolerations:** Adds an `nvidia.com/gpu` toleration with `operator="Exists"` and `effect="NoSchedule"`.
- **Node selectors:** Sets `cloud.google.com/gke-accelerator: <label>` on `spec.nodeSelector` whenever a GPU type is resolved and no `ComputeClass` is active (`if gpu_types and not compute_class` on direct Pods, and `if placement.gpu_config.accelerator_label and not compute_class` on Compose Pods).
- **Driver library resolution (`ldconfig` prelude):** Direct GPU Pods run `GKE_NVIDIA_LDCONFIG_SNIPPET` before `exec sleep infinity`. This snippet writes `/usr/local/nvidia/lib64` to `/etc/ld.so.conf.d/harbor-nvidia.conf` and runs `ldconfig`. `harbor-gke-ext` does not set `LD_LIBRARY_PATH` in the Pod `env` spec because a container-spec `env` entry replaces any `ENV LD_LIBRARY_PATH` defined by the image (such as `/usr/local/cuda/lib64` or Conda library paths).
- **Driver utilities on `PATH`:** The same startup prelude symlinks every executable in `/usr/local/nvidia/bin` (`nvidia-smi` and companion tools) into `/usr/local/bin` if not already present, preserving the image's `PATH`.

> [!WARNING]
> **Compose GPU services on the native path (Shape A) do not receive the `ldconfig` prelude.** A Compose service's entrypoint belongs to the task, and wrapping it would alter the process arguments and PID tree the service expects. In Shape B and Shape C, DinD-delegated GPU services receive the `/usr/local/nvidia:/usr/local/nvidia:ro` bind mount and a `LD_LIBRARY_PATH` prefixed with `/usr/local/nvidia/lib64` in `/harbor/dind-compose.yaml`; the `/harbor/dind-compose.gpu.yaml` override adds only the `devices:` list. A native Shape A GPU service must resolve `/usr/local/nvidia/lib64` and `/usr/local/nvidia/bin` in its own image or startup script.

### Pre-creating an L4 GPU node pool and substituting it for A100

Many benchmark tasks (for example in `terminal-bench`) declare `gpu_types = ["A100"]` in `task.toml` even when their VRAM footprint fits within the 24 GiB of an NVIDIA L4 GPU (`g2-standard-8` or `g2-standard-12`). On-demand `nvidia-tesla-a100` capacity depends on regional availability and quota, so you can pre-create a scale-to-zero autoscaling L4 node pool on GKE Standard and override the accelerator label at runtime with `--ek gpu_override=nvidia-l4`:

```bash
# Pre-create a scale-to-zero L4 node pool (g2-standard-8 or g2-standard-12)
# Specify zones that offer nvidia-l4 (in us-central1: a, b, c; us-central1-f does not offer L4)
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
```

Then run GPU tasks (including tasks whose `task.toml` requests `A100`) on the L4 pool by passing `--ek gpu_override=nvidia-l4`:

```bash
uv run harbor run \
  -d terminal-bench@2.0 \
  -a oracle \
  -e harbor_gke_ext:GKEEnvironment \
  --plugin harbor_gke_ext:CloudBuildPlugin \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${REGION}" \
  --ek cluster_name="${CLUSTER_NAME}" \
  --ek gpu_override=nvidia-l4
```

### Troubleshooting GPU placement in Compose mode

In Docker Compose mode, `reconcile_gpu_config()` reconciles the task-level GPU count (`task.toml` `gpus` or `--ek default_gpu_count`) against per-service GPU declarations (`deploy.resources.reservations.devices` or top-level `gpus`). When reconciliation fails, it raises `UnsupportedComposeFeatureError` with one of the following cause codes:

| Error Code | Exact Trigger Condition (`placement.py`) | How to Fix |
|---|---|---|
| `GPU_TYPE_UNRESOLVED` | Effective GPU count is `> 0` (or at least one Compose service requests GPUs), but no GPU type is specified in `task.toml` `gpu_types`, `--ek gpu_override`, or `--ek default_gpu_type`. | Add `gpu_types = ["L4"]` to `task.toml` `[environment]`, pass `--ek default_gpu_type=l4`, or pass `--ek gpu_override=nvidia-l4`. |
| `GPU_COUNT_MISMATCH` | Fires in two cases: (1) `main` declares an explicit integer GPU count in Compose and `task.toml` declares `gpus > 0`, where `gpus != main_compose_gpu + sidecar_gpu_sum`; or (2) `main` does not declare GPUs in Compose, `task.toml` declares `gpus > 0`, and `gpus < sidecar_gpu_sum`. | Ensure `gpus` in `task.toml` equals the sum of GPUs across `main` and all GPU-requesting sidecars (or exceeds `sidecar_gpu_sum` when `main` takes the remainder). |
| `GPU_COMPOSE_ONLY` | A Compose service (`main` or a sidecar) declares `count: all` (or `gpus: all`), while `task.toml` `[environment]` omits `gpus` (or sets `gpus <= 0`) and `--ek default_gpu_count` is not set. | Set an explicit integer `gpus = 1` in `task.toml` `[environment]`, or pass `--ek default_gpu_count=1 --ek default_gpu_type=l4`. |

For additional operational diagnostics, see [Troubleshooting](troubleshooting.md).

## GPUs with Compose and Docker-in-Docker

In hybrid (Shape B) and Main-in-DinD (Shape C) Compose Pods, `harbor-gke-ext` splits GPU allocations cleanly between native Kubernetes containers and the Docker-in-Docker (`dind-engine`) daemon:

- **Native GPU services keep their own `nvidia.com/gpu` allocation:** Any GPU-requesting service that runs natively on the Pod (for example, native `main` in Shape B, or any native sidecar in Shape A/B) receives its own `nvidia.com/gpu: <service_gpus[sname]>` request and limit directly on its Kubernetes container spec.
- **`dind-engine` aggregates GPUs across DinD-delegated services:** The GKE NVIDIA device plugin mounts `/usr/local/nvidia` only into the Kubernetes container that holds the `nvidia.com/gpu` resource allocation. Therefore, `placement.dind_gpu_total` sums `service_gpus[sname]` across all services in `placement.dind_sidecars`, and `_build_shape_b_dind_containers()` assigns `nvidia.com/gpu: str(placement.dind_gpu_total)` to `dind-engine`'s `requests` and `limits`.

### Walkthrough: `gpu-sidecar`

The `examples/tasks/gpu-sidecar` task illustrates how a GPU-requesting sidecar is routed into the DinD plane and wired to the host GPU.

**`task.toml` excerpt:**
```toml
[environment]
gpus = 1
gpu_types = ["L4"]
```

**`docker-compose.yaml` excerpt:**
```yaml
services:
  main:
    build: .
    command: ["sleep", "infinity"]

  model:
    image: python:3.12-slim
    expose:
      - "8080"
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]

  mini-service:
    image: python:3.12-slim
    expose:
      - "8080"
```

**Placement decision:**
1. `classify_compose_placement()` detects that `model` and `mini-service` both declare `expose: 8080`. Because native sidecars share the Pod network namespace, this triggers `PORT_COLLISION:mini-service,model:8080`.
2. Both sidecars are routed into `dind-engine` (Shape B), while `main` remains a native Kubernetes container.
3. Because `model` is in `placement.dind_sidecars` and requests `1` GPU while `main` requests `0`, `placement.dind_gpu_total` is `1`, and `dind-engine` receives `nvidia.com/gpu: "1"`.

**Runtime mechanism:**
1. **GPU probe inside `dind-engine`:** When any DinD service requests a GPU, `_build_shape_b_dind_containers()` prepends a non-failing GPU discovery probe to `dind-engine`'s command before `exec dockerd`. Because `dind-engine` holds the `nvidia.com/gpu` allocation and runs `privileged: true`, it sees both the GKE device plugin mount at `/usr/local/nvidia` and the `/dev/nvidia*` character devices. It writes `/harbor/gpu/driver-ok`, `/harbor/gpu/devices`, and `/harbor/gpu/diag.txt` to the shared `harbor-dind-gpu` `emptyDir` volume.
2. **Compose GPU override in `compose-up-gate`:** The translator has already written the `/usr/local/nvidia:/usr/local/nvidia:ro` bind mount and a `LD_LIBRARY_PATH` prefixed with `/usr/local/nvidia/lib64` into the inner service's entry in `/harbor/dind-compose.yaml`. Before running `docker compose`, `compose-up-gate` inspects `/harbor/gpu/driver-ok` and `/harbor/gpu/devices` and fails with a `HARBOR_ERROR` line if the driver directory is missing or no device node was found. Otherwise it generates `/harbor/dind-compose.gpu.yaml`, which adds only a `devices:` list with every discovered `/dev/nvidia*` device, and starts the inner project with `-f /harbor/dind-compose.yaml -f /harbor/dind-compose.gpu.yaml`.

> [!TIP]
> **Task authoring guidance for GPU sidecars:** In `examples/tasks/gpu-sidecar`, the `model` service queries GPU state lazily on HTTP requests rather than at container import time, and its Compose `healthcheck` checks TCP socket readiness without initializing CUDA. If a CUDA or driver issue occurs, the container still reaches healthy status quickly and returns structured diagnostics to the verifier instead of hanging during `compose-up-gate` startup.

## TPU support

`harbor-gke-ext` supports single-host Tensor Processing Units (TPUs) on direct Pods and on the native `main` container of Compose Pods.

### TPU aliases

You can specify either a short alias or the full GKE TPU accelerator label in `task.toml` under `[environment.tpu]`. Passing an unknown value raises a `RuntimeError` from `resolve_tpu_accelerator_label()`.

| Short Alias | Full GKE Label (`cloud.google.com/gke-tpu-accelerator`) |
|---|---|
| `v3` | `tpu-v3-slice` |
| `v3-device` | `tpu-v3-device` |
| `v4` | `tpu-v4-podslice` |
| `v5e` | `tpu-v5-lite-podslice` |
| `v5p` | `tpu-v5p-slice` |
| `v6e`, `trillium` | `tpu-v6e-slice` |
| `v7`, `ironwood` | `tpu7x` |

### Requesting TPUs

Declare an `[environment.tpu]` section in `task.toml`:

```toml
[environment.tpu]
type = "v5e"
topology = "2x2"
```

Harbor computes `chip_count` as the product of the dimensions in `topology` (for example, `2x2` = 4 chips) and configures the Pod with:
- **Resource requests and limits:** `google.com/tpu` set to `str(tpu.chip_count)`.
- **Tolerations:** A `google.com/tpu` toleration with `operator="Exists"` and `effect="NoSchedule"`.
- **Node selectors:** `cloud.google.com/gke-tpu-accelerator` and `cloud.google.com/gke-tpu-topology`.

### TPU limitations

- **Assigned only to `main` in Compose mode:** In Compose mode, `tpu_spec` is passed only to `_build_k8s_container()` for `main`. `placement.py` does not parse TPU reservations from `docker-compose.yaml`.
- **No TPU support in DinD:** Neither Shape B sidecars nor Shape C `main` receive TPU allocations inside `dind-engine`.
- **Single-host topologies only:** Each trial runs as a single-Pod `batch/v1` Job. Multi-host TPU slices requiring `JobSet` or indexed multi-Pod gang scheduling are not supported.
- **Mutually exclusive with GPUs:** Requesting both GPUs (`_effective_gpus > 0`) and TPUs (`tpu is not None`) in the same task raises a `RuntimeError` during `_validate_gke_accelerator_config()`.

## Storage and ComputeClass interaction

`ComputeClass` resolution and ephemeral storage sizing are handled in `GKEEnvironment._resolve_active_compute_class()` and `GKEEnvironment._effective_total_ephemeral_storage_mb()`:

- **Accelerator tasks on Autopilot skip global and auto-promoted ComputeClasses:** On GKE Autopilot, if a task requests GPUs (`_effective_gpus > 0`) or TPUs (`tpu is not None`), `_resolve_active_compute_class()` returns `None` unless an explicit per-task entry exists in `--ek task_compute_classes`. Global `--ek compute_class` and storage-based `Performance` promotion are skipped for accelerated tasks because GKE Autopilot schedules GPUs and TPUs via `cloud.google.com/gke-accelerator` and `cloud.google.com/gke-tpu-accelerator` node selectors rather than CPU ComputeClasses.
- **When `cloud.google.com/gke-accelerator` is omitted:** If you explicitly assign a `ComputeClass` to a GPU task (via `task_compute_classes` on Autopilot, or `compute_class` / `task_compute_classes` on Standard), `harbor-gke-ext` omits `cloud.google.com/gke-accelerator` from `nodeSelector` so the custom `ComputeClass` controls node selection without conflicting selectors.
- **Ephemeral storage estimation for non-accelerator Autopilot promotion:** General-purpose Autopilot Pods cap total ephemeral storage at `10,240 MiB` (`_GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB`). To decide whether a non-accelerator Autopilot Pod exceeds `10,240 MiB` and must be promoted to `Performance`, `_effective_total_ephemeral_storage_mb()` sums:
  1. `main`'s effective storage request (`_effective_storage_mb`),
  2. `1,024 MiB` (`_GKE_AUTOPILOT_DEFAULT_CONTAINER_STORAGE_MB`) per non-`main` Compose service to account for the default ephemeral-storage request that Autopilot's mutating webhook injects into bare containers (Harbor does not set this `1,024 MiB` figure on the containers itself), and
  3. `10,240 MiB` (`DIND_STORAGE_FLOOR_MB`) when `_compose_needs_dind()` is true.
- **When `Performance` auto-promotion is skipped:** Even when the storage estimate exceeds `10,240 MiB`, automatic `Performance` promotion on Autopilot is skipped if an accelerator, a global `compute_class`, a machine type (`machine_type` or `task_machine_types`), or a node pool is resolved for the task.
- **`max_storage_request_mb` clamp bypass:** The `--ek max_storage_request_mb` clamp has no default value; when set, it caps `_effective_storage_mb` for all tasks **except** those with a task-specific entry in `--ek task_compute_classes` (`_has_task_specific_compute_class()`). A global `--ek compute_class` does not bypass `max_storage_request_mb`.

For more details on Compose placement and DinD execution, see [Compose translation](compose-translation.md) and [Docker-in-Docker](docker-in-docker.md).
