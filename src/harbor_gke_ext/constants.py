from __future__ import annotations

import hashlib
import re

# Maps user-friendly GPU type names (from task.toml gpu_types) to GKE accelerator
# node labels used in cloud.google.com/gke-accelerator node selectors.
# Keys are lowercase for matching; values are the exact GKE label strings.
GKE_GPU_TYPE_MAP: dict[str, str] = {
    "t4": "nvidia-tesla-t4",
    "l4": "nvidia-l4",
    "a100": "nvidia-tesla-a100",
    "a100-40gb": "nvidia-tesla-a100",
    "a100-80gb": "nvidia-a100-80gb",
    "v100": "nvidia-tesla-v100",
    "rtx-pro-6000": "nvidia-rtx-pro-6000",
    "h100": "nvidia-h100-80gb",
    "h100-mega": "nvidia-h100-mega-80gb",
    "h200": "nvidia-h200-141gb",
    "b200": "nvidia-b200",
    "gb200": "nvidia-gb200",
}

# Maps user-friendly TPU aliases (from task.toml [environment.tpu].type) to GKE TPU
# accelerator node labels used in cloud.google.com/gke-tpu-accelerator node selectors.
# Keys are lowercase aliases; values are the exact GKE label strings.
GKE_TPU_TYPE_MAP: dict[str, str] = {
    "v3": "tpu-v3-slice",
    "v3-device": "tpu-v3-device",
    "v4": "tpu-v4-podslice",
    "v5e": "tpu-v5-lite-podslice",
    "v5p": "tpu-v5p-slice",
    "v6e": "tpu-v6e-slice",
    "trillium": "tpu-v6e-slice",
    "v7": "tpu7x",
    "ironwood": "tpu7x",
}


def resolve_gpu_accelerator_label(gpu_type: str) -> str:
    """Translate a user-supplied GPU type to its GKE accelerator label."""
    gpu_type_raw = gpu_type.lower().strip()
    if gpu_type_raw in GKE_GPU_TYPE_MAP:
        return GKE_GPU_TYPE_MAP[gpu_type_raw]
    if gpu_type_raw in GKE_GPU_TYPE_MAP.values():
        return gpu_type_raw
    supported = ", ".join(
        sorted(set(GKE_GPU_TYPE_MAP.keys()) | set(GKE_GPU_TYPE_MAP.values()))
    )
    raise RuntimeError(
        f"GPU type '{gpu_type}' is not supported on GKE. Supported types: {supported}"
    )


def resolve_tpu_accelerator_label(tpu_type: str) -> str:
    """Translate a user-supplied TPU type to its GKE accelerator label."""
    tpu_type_raw = tpu_type.lower().strip()
    if tpu_type_raw in GKE_TPU_TYPE_MAP:
        return GKE_TPU_TYPE_MAP[tpu_type_raw]
    if tpu_type_raw in GKE_TPU_TYPE_MAP.values():
        return tpu_type_raw
    supported = ", ".join(
        sorted(set(GKE_TPU_TYPE_MAP.keys()) | set(GKE_TPU_TYPE_MAP.values()))
    )
    raise RuntimeError(
        f"TPU type '{tpu_type}' is not supported on GKE. Supported types: {supported}"
    )


# --- NVIDIA driver injection on GKE ---
#
# Provenance: this is the ``-container-path`` flag default of GKE's device
# plugin (``container-engine-accelerators/cmd/nvidia_gpu/nvidia_gpu.go``), which
# mounts host ``/home/kubernetes/bin/nvidia`` read-only. It is a GKE convention,
# NOT a universal Linux one -- clusters running the NVIDIA GPU Operator inject
# the driver into system paths instead and there is nothing at this location.
#
# Two consequences that the surrounding code depends on:
#  1. The mount lands ONLY in the container holding the ``nvidia.com/gpu``
#     allocation. Device nodes, by contrast, appear in any ``privileged``
#     container regardless of allocation (measured 2026-09-17), so the presence
#     of ``/dev/nvidia*`` is NOT evidence that CUDA will work.
#  2. The plugin sets no environment variables (``Envs()`` returns ``{}`` unless
#     MPS sharing is on), so wiring up the dynamic linker is ours to do.
#
# Verified on GKE 1.35.6 / L4: 15 binaries under bin/, 100 shared objects
# under lib64/.
GKE_NVIDIA_HOST_DIR = "/usr/local/nvidia"
GKE_NVIDIA_LIB_DIR = f"{GKE_NVIDIA_HOST_DIR}/lib64"
GKE_NVIDIA_BIN_DIR = f"{GKE_NVIDIA_HOST_DIR}/bin"

# Register the driver directory with the dynamic linker rather than exporting
# ``LD_LIBRARY_PATH`` on the container spec. Rationale:
#
#  * A container-spec env var REPLACES the image's own ``LD_LIBRARY_PATH``.
#    CUDA base images ship ``LD_LIBRARY_PATH=/usr/local/cuda/lib64``, so setting
#    it would resolve libcuda.so.1 at the cost of breaking libcudart.
#  * Exporting it from the idle shell would not help: processes started later
#    through the Kubernetes exec API (the agent and the verifier) inherit the
#    container's configured environment, not that shell's.
#
# Writing ``ld.so.conf.d`` and running ``ldconfig`` persists in the container
# filesystem, so every subsequent exec resolves the driver. This is what the
# NVIDIA Container Runtime itself does after injecting driver libraries. Every
# step is best-effort so images without ldconfig (musl, distroless) still start.
_GKE_NVIDIA_LDCONFIG_FRAGMENT = (
    f'if [ -d "{GKE_NVIDIA_LIB_DIR}" ]; then '
    "mkdir -p /etc/ld.so.conf.d 2>/dev/null "
    f'&& echo "{GKE_NVIDIA_LIB_DIR}" > /etc/ld.so.conf.d/harbor-nvidia.conf 2>/dev/null '
    "&& ldconfig 2>/dev/null || true; fi;"
)

# The driver's 15 utilities -- `nvidia-smi` above all -- live in a directory no
# image has on its PATH, and `PATH` cannot be fixed the obvious way for exactly
# the reason `LD_LIBRARY_PATH` cannot: a container-spec env var REPLACES the
# image's own value, and an export from the idle shell is not inherited by
# processes started later through the exec API.
#
# Symlinking into /usr/local/bin is the PATH-shaped analogue of the ldconfig
# write: it persists in the container filesystem, so every subsequent exec sees
# it. /usr/local/bin is the first entry of the OCI default PATH
# (`oci/defaults.go`), so a symlink placed there is on the PATH of any image
# that did not override it.
#
# An existing name is never replaced. An image that ships its own `nvidia-smi`
# has made a deliberate choice and Harbor must not shadow it.
_GKE_NVIDIA_BIN_LINK_FRAGMENT = (
    f'if [ -d "{GKE_NVIDIA_BIN_DIR}" ]; then '
    "mkdir -p /usr/local/bin 2>/dev/null; "
    f'for f in "{GKE_NVIDIA_BIN_DIR}"/*; do '
    '[ -f "$f" ] && [ -x "$f" ] || continue; '
    "b=${f##*/}; "
    '[ -e "/usr/local/bin/$b" ] || ln -s "$f" "/usr/local/bin/$b" 2>/dev/null; '
    "done; fi;"
)

# Startup prelude for Pods holding an `nvidia.com/gpu` allocation: make the
# injected driver resolvable by the dynamic linker, then make its utilities
# reachable on PATH. Every step is best-effort so images without `ldconfig`
# (musl, distroless) still start.
GKE_NVIDIA_LDCONFIG_SNIPPET = (
    f"{_GKE_NVIDIA_LDCONFIG_FRAGMENT} {_GKE_NVIDIA_BIN_LINK_FRAGMENT}"
)


# --- Kubernetes Pod Security Standards (PSS) Baseline Capabilities ---
#
# Authoritative allowlist from:
# https://kubernetes.io/docs/concepts/security/pod-security-standards/#baseline
#
# In addition to the 13 PSS Baseline capabilities below, ``NET_RAW`` is in the
# default OCI/containerd capability bounding set (``oci/defaults.go``) on GKE
# Standard nodes and does not expand host kernel attack surface beyond an
# ordinary container. Any capability outside this allowlist exceeds the
# Baseline posture and triggers gVisor sandbox isolation on the Pod.
_PSS_BASELINE_CAPABILITIES: frozenset[str] = frozenset(
    {
        "AUDIT_WRITE",
        "CHOWN",
        "DAC_OVERRIDE",
        "FOWNER",
        "FSETID",
        "KILL",
        "MKNOD",
        "NET_BIND_SERVICE",
        "NET_RAW",
        "SETFCAP",
        "SETGID",
        "SETPCAP",
        "SETUID",
        "SYS_CHROOT",
    }
)


# --- Transport & Proxy Keepalive Configuration ---

_GKE_EXEC_STREAM_PING_INTERVAL_SEC = 10.0
# Upper bounds for short housekeeping execs: launching a decoupled command, and
# killing a timed-out supervised command.
_GKE_DECOUPLED_LAUNCH_TIMEOUT_SEC = 120.0
_GKE_EXEC_KILL_TIMEOUT_SEC = 10.0
# Upper bound for reading dind-engine's cgroup usage at teardown, connection
# included. Diagnostics only: it must never hold up Pod deletion for long.
_GKE_DIND_USAGE_REPORT_TIMEOUT_SEC = 10.0
_GKE_TCP_KEEPALIVE_IDLE_SEC = 10
_GKE_TCP_KEEPALIVE_INTERVAL_SEC = 5
_GKE_TCP_KEEPALIVE_COUNT = 3

# --- Kubernetes Pod Deadline & Timeout Defaults ---
_GKE_DEFAULT_DEADLINE_BUFFER_MINUTES = 15
_GKE_DEFAULT_AGENT_TIMEOUT_MINUTES = 1440
_GKE_DEFAULT_AGENT_SETUP_TIMEOUT_SEC = 360
_GKE_DEFAULT_VERIFIER_TIMEOUT_SEC = 600
_GKE_JOB_POD_SPAWN_TIMEOUT_SEC = 600
# How many times the Job controller replaces a trial Pod lost to infrastructure:
# preemption, eviction or node loss (`DisruptionTarget`), or a kubelet that
# rejected the Pod before any container ran. A container that fails on its own
# fails the Job at once instead. See `pod_builder.build_job`.
_GKE_JOB_BACKOFF_LIMIT = 3
_GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB = 10240
# Autopilot injects this ephemeral-storage request into any container that does
# not declare one. It is a platform figure, not a Harbor choice.
_GKE_AUTOPILOT_DEFAULT_CONTAINER_STORAGE_MB = 1024
_GKE_DEFAULT_COMPOSE_UP_TIMEOUT_SEC = 300

# --- Konnectivity Tunnel & API Server Retry Configuration ---
_GKE_EXEC_CONNECT_MAX_ATTEMPTS = 15
_GKE_EXEC_CONNECT_INITIAL_DELAY_SEC = 2.0
_GKE_EXEC_CONNECT_MAX_DELAY_SEC = 20.0
_GKE_EXEC_CONNECT_EXPONENTIAL_BASE = 2.0
_KUBERNETES_NAME_MAX_LENGTH = 63

# --- High-Concurrency Transport & Handshake Rate-Limiting Configuration ---
_GKE_EXEC_SEMAPHORE_LIMIT = 128

# --- Control-Plane Overload Adaptation ---
# Exec handshakes and Job/Pod creates share one process-wide AIMD limit. It
# starts at (and never exceeds) the exec pool size, so a healthy control plane
# sees exactly the previous static behaviour. Overload responses (HTTP 429/503,
# or a fail-closed admission webhook that could not be called) halve it, at most
# once per cooldown so one burst of simultaneous failures counts once; each
# interval with at least one success adds one slot back.
_GKE_CONTROL_PLANE_LIMIT_MAX = _GKE_EXEC_SEMAPHORE_LIMIT
_GKE_CONTROL_PLANE_LIMIT_MIN = 4
_GKE_CONTROL_PLANE_LIMIT_DECREASE_FACTOR = 0.5
_GKE_CONTROL_PLANE_LIMIT_DECREASE_COOLDOWN_SEC = 5.0
_GKE_CONTROL_PLANE_LIMIT_INCREASE_INTERVAL_SEC = 1.0
# Relative spread applied to every retry delay (uniform, mean-preserving), so
# clients that failed together do not retry together.
_GKE_RETRY_JITTER_RATIO = 0.2
# Job/Pod create attempts on overload, using the exec backoff schedule
# (2 s doubling to a 20 s cap): about 130 s of retrying on average.
_GKE_CONTROL_PLANE_WRITE_MAX_ATTEMPTS = 10
# Substring of the API server's message when a fail-closed admission webhook
# could not be reached or timed out ("failed calling webhook ..."). This is a
# call failure, distinct from a webhook denial ("admission webhook ... denied").
_GKE_WEBHOOK_CALL_FAILURE_MARKER = "failed calling webhook"
# Konnectivity tunnel between the control plane and a node is down. Transient,
# but a node-path problem, not control-plane load, so it must not shrink the limit.
_GKE_KONNECTIVITY_NO_AGENT_MARKER = "no agent available"

# --- Kubernetes API Client Transport Timeout Defaults ---
_GKE_API_CONNECT_TIMEOUT_SEC = 15.0
_GKE_API_READ_TIMEOUT_SEC = 60.0
_GKE_EXEC_HANDSHAKE_TIMEOUT_SEC = 30.0


class GKEExecStreamClosedError(RuntimeError):
    """Raised when an exec stream terminates before receiving a normal exit packet."""


class TrialContainerLostError(RuntimeError):
    """Raised when the trial's container or Pod dies or disappears mid-trial.

    Examples: the Pod was deleted or evicted, the node was lost, or the container
    exited (for instance, OOM-killed along with its keepalive process). The
    command's outcome and the trial's filesystem state are unknown, so the trial
    produced no valid result. This is an infrastructure failure, not a task
    failure, and it is safe to retry. Harbor's retry filter matches exception
    class names, and this name is not in its default exclude list, so
    ``--max-retries N`` retries it without further configuration.

    A command that is killed inside a container that stays alive (for example,
    the OOM killer picking a compiler process) is *not* this error. That case is
    reported as the command's non-zero exit code, as it is under Docker.
    """


class UnsatisfiableMachineTypeError(RuntimeError):
    """Raised when no node the cluster can offer satisfies a task's machine type pin.

    A machine type pin requires its family and at least its vCPU count. This is
    raised only when the cluster's node pools are known, none of them qualifies,
    and the cluster cannot create new node shapes (no node auto-provisioning).
    Distinct from a generic scheduling failure on purpose: it is a configuration
    mismatch that no amount of waiting will resolve.
    """


class PlacementConflictError(ValueError):
    """Raised when a task's placement settings contradict each other.

    Examples: a job-wide node pool combined with any ComputeClass setting, a
    node pool and a ComputeClass mapped to the same task, or a machine type
    combined with a ComputeClass for the same task. Each task must resolve to
    exactly one placement mechanism; Harbor refuses to guess which one wins.
    """


class EphemeralStorageUnschedulableError(RuntimeError):
    """Raised when a Pod's ephemeral-storage request exceeds every node's capacity.

    Like ``UnsatisfiableMachineTypeError``, waiting cannot help: no node will ever
    grow to fit the request, so the Pod would pend until the trial timed out.
    """


class RemoteFileNotFoundError(FileNotFoundError, RuntimeError):
    """Raised when tar reports a missing path during download_file or download_dir.

    Subclasses both ``FileNotFoundError`` (so retry policies skip retrying deterministic
    missing-file probes such as ``reward.txt`` vs ``reward.json``) and ``RuntimeError``
    for backward compatibility with callers catching ``RuntimeError``.
    """


def _sanitize_kubernetes_resource_name(name: str) -> str:
    """Return a deterministic RFC-1123 label suitable for GKE resources."""
    sanitized = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if sanitized and len(sanitized) <= _KUBERNETES_NAME_MAX_LENGTH:
        return sanitized
    if not sanitized:
        sanitized = "harbor"
    digest = hashlib.sha256(name.encode()).hexdigest()[:8]
    prefix_length = _KUBERNETES_NAME_MAX_LENGTH - len(digest) - 1
    prefix = sanitized[:prefix_length].rstrip("-")
    return f"{prefix}-{digest}"


def _parse_bool(value: object, default: bool = False) -> bool:
    """Safely parse a boolean from boolean, integer, or string representations."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "t", "y", "on")
    return bool(value)
