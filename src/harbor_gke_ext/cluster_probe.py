"""GKE cluster capability probe and per-cluster admission control.

Why this module exists
----------------------
1. Harbor must NEVER apply or ship cluster-wide policy manifests (such as
   ``WorkloadAllowlist``). Cluster administration belongs to the cluster operator.
2. Harbor reads ``gcloud container clusters describe`` output to decide what a
   trial can request before it creates a Pod:
   - Autopilot status and privileged DinD admissibility (``autopilot.enabled``,
     ``currentMasterVersion``,
     ``autopilot.privilegedAdmissionConfig.allowlistPaths``,
     ``autopilot.workloadPolicyConfig.allowNetAdmin``).
   - Schedulable CPU (overall and on gVisor-capable pools), the machine-type
     inventory, and node auto-provisioning status.
   - An estimate of the largest node's allocatable ephemeral storage.
3. Kubernetes API probes complement the describe output: the largest live
   node's allocatable ephemeral storage, and a dry run that detects support for
   Pod-level resources.
4. ``ClusterAdmissionController`` queues trials before Pod creation when
   in-flight Pods would exceed the cluster's schedulable CPU budget (overall or
   gVisor) or the optional ``max_concurrent_pods`` limit.
"""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from harbor.utils.logger import logger

_VERSION_RE = re.compile(r"^v?(\d+)\.(\d+)")
_MACHINE_VCPU_RE = re.compile(r"-(\d+)(?:-[a-z0-9]+)?$")
_FIXED_MACHINE_VCPUS: dict[str, int] = {
    "e2-micro": 2,
    "e2-small": 2,
    "e2-medium": 2,
    "f1-micro": 1,
    "g1-small": 1,
}


class DindAvailability(StrEnum):
    """Availability state of privileged DinD execution on the target GKE cluster."""

    DIND_AVAILABLE = "available"
    DIND_UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class ClusterCapabilities:
    """Evaluated GKE cluster capabilities for compose placement and Pod synthesis."""

    is_autopilot: bool = False
    gke_version: str | None = None
    allow_net_admin: bool = False
    allowlist_paths: tuple[str, ...] = ()
    dind_availability: DindAvailability = DindAvailability.DIND_AVAILABLE
    dind_reason: str = "Standard GKE cluster"
    max_schedulable_cpu_cores: int | None = None
    max_gvisor_cpu_cores: int | None = None

    # --- Machine-type inventory (used to validate machine-type pins before submission) ---
    # Empty tuples mean "not probed", which callers must treat as "unknown, do not
    # block" -- never as "the cluster offers nothing". On Autopilot there are no
    # node pools to enumerate, so these stay empty there by construction.
    available_machine_types: tuple[str, ...] = ()
    available_machine_families: tuple[str, ...] = ()
    node_auto_provisioning_enabled: bool = False
    # (pool name, machine type) for every node pool, tainted or not. Used to check
    # that a machine type pin does not contradict an explicitly selected pool.
    node_pool_machine_types: tuple[tuple[str, str], ...] = ()

    # --- Pod-level `spec.resources` (KEP-2837, beta and on by default in 1.34+) ---
    # Tri-state on purpose. `None` means "not probed"; `False` means "probed and
    # the API server pruned the field". Collapsing those into one boolean would
    # make an unprobed cluster indistinguishable from a broken one, and the two
    # warrant opposite responses.
    supports_pod_level_resources: bool | None = None

    # --- Largest single node's allocatable ephemeral storage, in MiB ---
    # A Pod is scheduled onto one node, so the maximum across pools -- not the sum,
    # and not the currently-active node -- is what decides whether a reservation is
    # satisfiable at all. `None` means unknown and must never block submission.
    max_node_allocatable_ephemeral_storage_mb: int | None = None

    network_policy_enforced: bool | None = None


_TOLERATED_TAINT_KEYS: frozenset[str] = frozenset(
    {
        "nvidia.com/gpu",
        "google.com/tpu",
        "sandbox.gke.io/runtime",
        "cloud.google.com/gke-spot",
        "cloud.google.com/gke-preemptible",
    }
)


def _has_blocking_taint(taints: Sequence[Any] | None) -> bool:
    """Return True if any taint has a NoSchedule/NoExecute effect not tolerated by Harbor Pods."""
    if not taints:
        return False
    for taint in taints:
        if isinstance(taint, dict):
            key = str(taint.get("key") or "").strip()
            effect = str(taint.get("effect") or "").strip()
        else:
            key = str(getattr(taint, "key", "") or "").strip()
            effect = str(getattr(taint, "effect", "") or "").strip()
        normalized_effect = effect.upper().replace("_", "")
        if (
            normalized_effect in ("NOSCHEDULE", "NOEXECUTE")
            and key not in _TOLERATED_TAINT_KEYS
        ):
            return True
    return False


# GKE Local SSD ephemeral storage: each SSD is 375 GiB, and the system
# reservation is 50 / 75 / 100 GiB for 1 / 2 / 3+ SSDs (plan-node-sizes).
_LOCAL_SSD_GIB = 375.0
_LOCAL_SSD_SYSTEM_RESERVATION_GIB = (50.0, 75.0, 100.0)

# COS_CONTAINERD boot-disk layout: Compute Engine diskSizeGb is in GiB (2^30 B).
# COS reserves ~4.184 GiB for fixed OS partitions (rootA, rootB, OEM, EFI) and
# formats the stateful partition as ext4 (~1/64 = 1.5625% inode/block metadata).
_COS_STATEFUL_EXT4_RATIO = 63.0 / 64.0
_COS_SYSTEM_PARTITIONS_GIB = 4.184


def estimate_gke_allocatable_ephemeral_storage_mb(
    disk_size_gb: int | None,
    *,
    local_ssd_count: int = 0,
) -> int | None:
    """Estimate GKE node allocatable ``ephemeral-storage`` (in MiB) from node pool config.

    Follows GKE's local ephemeral storage reservation
    (https://docs.cloud.google.com/kubernetes-engine/docs/concepts/plan-node-sizes#local_ephemeral_storage_reservation):
    allocatable = capacity - system reservation - eviction threshold.

    Boot-disk-backed ephemeral storage (``local_ssd_count == 0``):

    In Compute Engine and the GKE API, ``diskSizeGb`` is specified in binary GiB
    (``2^30`` bytes). On ``COS_CONTAINERD`` nodes:

    1. Filesystem capacity (``.status.capacity.ephemeral-storage``):
       ``capacity_gib = max(0, disk_size_gb * (63 / 64) - 4.184 GiB)``
       (accounting for fixed OS partitions and ``ext4`` metadata overhead).
    2. System reservation: ``min(0.50 * disk_size_gb, 0.35 * disk_size_gb + 6 GiB, 100 GiB)``
    3. Eviction threshold: ``0.10 * capacity_gib``

    Local SSD-backed ephemeral storage (``local_ssd_count > 0``): ephemeral
    storage lives only on the Local SSDs, so the boot disk is ignored. Each
    Local SSD is ``375 GiB``:

    1. System reservation: ``50``, ``75`` or ``100 GiB`` for 1, 2, or 3+ SSDs
    2. Eviction threshold: ``0.10 * local_ssd_count * 375 GiB``

    GKE computes the Local SSD values before the drives are formatted, so a live
    node's allocatable value is a few percent lower than this estimate.
    """
    if local_ssd_count > 0:
        ssd_raw_gib = float(local_ssd_count) * _LOCAL_SSD_GIB
        ssd_sys_res_gib = _LOCAL_SSD_SYSTEM_RESERVATION_GIB[
            min(local_ssd_count, len(_LOCAL_SSD_SYSTEM_RESERVATION_GIB)) - 1
        ]
        ssd_alloc_gib = max(0.0, ssd_raw_gib * 0.90 - ssd_sys_res_gib)
        return int(ssd_alloc_gib * 1024)

    if disk_size_gb is None or disk_size_gb <= 0:
        return None
    disk_gib = float(disk_size_gb)
    capacity_gib = max(
        0.0, disk_gib * _COS_STATEFUL_EXT4_RATIO - _COS_SYSTEM_PARTITIONS_GIB
    )
    sys_res_gib = min(0.50 * disk_gib, 0.35 * disk_gib + 6.0, 100.0)
    eviction_gib = 0.10 * capacity_gib
    alloc_gib = max(0.0, capacity_gib - sys_res_gib - eviction_gib)
    return int(alloc_gib * 1024)


def _pool_max_nodes(pool: dict[str, Any]) -> int:
    """Return the maximum number of nodes a GKE node pool can scale to."""
    locations = pool.get("locations")
    zone_mult = (
        max(1, len(locations)) if isinstance(locations, list) and locations else 1
    )
    autoscaling = pool.get("autoscaling") or {}
    if autoscaling.get("enabled"):
        if "totalMaxNodeCount" in autoscaling:
            return int(autoscaling["totalMaxNodeCount"])
        if "maxNodeCount" in autoscaling:
            return int(autoscaling["maxNodeCount"]) * zone_mult
        return int(pool.get("initialNodeCount", 1)) * zone_mult
    return int(pool.get("initialNodeCount", 0)) * zone_mult


def _parse_cluster_max_ephemeral_storage_mb(data: dict[str, Any]) -> int | None:
    """Derive maximum single-node allocatable ephemeral storage (MiB) from cluster config."""
    best_mb: int | None = None
    node_pools = data.get("nodePools")
    if isinstance(node_pools, list):
        for pool in node_pools:
            if not isinstance(pool, dict):
                continue
            cfg = pool.get("config") or {}
            if _has_blocking_taint(cfg.get("taints")):
                continue
            if _pool_max_nodes(pool) <= 0:
                continue
            disk_gb_raw = cfg.get("diskSizeGb")
            disk_gb = (
                int(disk_gb_raw) if isinstance(disk_gb_raw, (int, float)) else None
            )
            lssd_cfg = (
                cfg.get("ephemeralStorageLocalSsdConfig")
                or cfg.get("ephemeralStorageConfig")
                or {}
            )
            lssd_count_raw = lssd_cfg.get("localSsdCount")
            lssd_count = (
                int(lssd_count_raw) if isinstance(lssd_count_raw, (int, float)) else 0
            )
            est_mb = estimate_gke_allocatable_ephemeral_storage_mb(
                disk_gb, local_ssd_count=lssd_count
            )
            if est_mb is not None and (best_mb is None or est_mb > best_mb):
                best_mb = est_mb

    cluster_autoscaling = data.get("autoscaling") or {}
    if cluster_autoscaling.get("enableNodeAutoprovisioning"):
        nap_defaults = cluster_autoscaling.get("autoprovisioningNodePoolDefaults") or {}
        nap_disk_raw = nap_defaults.get("diskSizeGb")
        nap_disk_gb = (
            int(nap_disk_raw) if isinstance(nap_disk_raw, (int, float)) else 100
        )
        nap_est_mb = estimate_gke_allocatable_ephemeral_storage_mb(nap_disk_gb)
        if nap_est_mb is not None and (best_mb is None or nap_est_mb > best_mb):
            best_mb = nap_est_mb

    return best_mb


def _parse_machine_type_vcpus(machine_type: str | None) -> int:
    """Estimate vCPU count from a GCE machine type string (e.g. ``e2-standard-4`` -> 4)."""
    if not machine_type:
        return 4
    clean = machine_type.strip().lower()
    if clean in _FIXED_MACHINE_VCPUS:
        return _FIXED_MACHINE_VCPUS[clean]
    match = _MACHINE_VCPU_RE.search(clean)
    if match:
        return max(1, int(match.group(1)))
    return 4


def _parse_cluster_schedulable_capacity(
    data: dict[str, Any],
) -> tuple[int | None, int | None]:
    """Derive max standard and gVisor vCPU capacity from ``gcloud container clusters describe``."""
    standard_vcpus = 0
    gvisor_vcpus = 0
    node_pools = data.get("nodePools")
    if isinstance(node_pools, list):
        for pool in node_pools:
            if not isinstance(pool, dict):
                continue
            cfg = pool.get("config") or {}
            if _has_blocking_taint(cfg.get("taints")):
                continue
            vcpus_per_node = _parse_machine_type_vcpus(cfg.get("machineType"))
            max_nodes = _pool_max_nodes(pool)

            pool_vcpus = max(0, max_nodes * vcpus_per_node)
            sandbox_type = (
                str((cfg.get("sandboxConfig") or {}).get("type") or "").strip().upper()
            )
            if sandbox_type == "GVISOR":
                gvisor_vcpus += pool_vcpus
            else:
                standard_vcpus += pool_vcpus

    cluster_autoscaling = data.get("autoscaling") or {}
    nap_enabled = bool(cluster_autoscaling.get("enableNodeAutoprovisioning", False))
    for limit in cluster_autoscaling.get("resourceLimits") or []:
        if isinstance(limit, dict) and limit.get("resourceType") == "cpu":
            try:
                limit_max = int(limit["maximum"])
                if nap_enabled:
                    standard_vcpus = max(standard_vcpus, limit_max)
                elif standard_vcpus == 0 or limit_max < standard_vcpus:
                    standard_vcpus = limit_max
            except (KeyError, TypeError, ValueError):
                pass

    return (
        standard_vcpus if standard_vcpus > 0 else None,
        gvisor_vcpus if gvisor_vcpus > 0 else None,
    )


class ClusterAdmissionController:
    """Cluster-capacity-aware concurrency gate for trial Pod creation (DS2).

    Queues trials before ``_create_pod`` when in-flight Pods saturate the
    cluster's schedulable CPU budget (or gVisor pool budget), preventing
    ``-n 500`` bursts from starting ``pod_ready_timeout`` clocks on hundreds of
    unschedulable ``Pending`` Pods.
    """

    def __init__(
        self,
        *,
        max_cpu_cores: float | None = None,
        max_gvisor_cpu_cores: float | None = None,
        max_concurrent_pods: int | None = None,
    ) -> None:
        self.max_cpu_cores = max_cpu_cores
        self.max_gvisor_cpu_cores = max_gvisor_cpu_cores
        self.max_concurrent_pods = max_concurrent_pods
        self.in_flight_cpu: float = 0.0
        self.in_flight_gvisor_cpu: float = 0.0
        self.in_flight_pods: int = 0
        self._cond: asyncio.Condition | None = None

    def _get_cond(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    def configure(
        self,
        *,
        max_cpu_cores: float | None = None,
        max_gvisor_cpu_cores: float | None = None,
        max_concurrent_pods: int | None = None,
    ) -> None:
        if max_cpu_cores is not None and self.max_cpu_cores is None:
            self.max_cpu_cores = float(max_cpu_cores)
        if max_gvisor_cpu_cores is not None and self.max_gvisor_cpu_cores is None:
            self.max_gvisor_cpu_cores = float(max_gvisor_cpu_cores)
        if max_concurrent_pods is not None:
            self.max_concurrent_pods = int(max_concurrent_pods)

    def _can_admit(self, cpu_cores: float, is_gvisor: bool) -> bool:
        if (
            self.max_concurrent_pods is not None
            and self.in_flight_pods >= self.max_concurrent_pods
            and self.in_flight_pods > 0
        ):
            return False
        if is_gvisor:
            if (
                self.max_gvisor_cpu_cores is not None
                and self.in_flight_gvisor_cpu + cpu_cores > self.max_gvisor_cpu_cores
                and self.in_flight_gvisor_cpu > 0
            ):
                return False
        else:
            if (
                self.max_cpu_cores is not None
                and self.in_flight_cpu + cpu_cores > self.max_cpu_cores
                and self.in_flight_cpu > 0
            ):
                return False
        return True

    async def acquire(
        self, cpu_cores: float, *, is_gvisor: bool = False
    ) -> tuple[float, bool]:
        cond = self._get_cond()
        req_cpu = max(0.25, float(cpu_cores))
        async with cond:
            while not self._can_admit(req_cpu, is_gvisor):
                await cond.wait()
            self.in_flight_pods += 1
            if is_gvisor:
                self.in_flight_gvisor_cpu += req_cpu
            else:
                self.in_flight_cpu += req_cpu
            return (req_cpu, is_gvisor)

    async def release(self, token: tuple[float, bool] | None) -> None:
        if token is None:
            return
        req_cpu, is_gvisor = token
        cond = self._get_cond()
        async with cond:
            self.in_flight_pods = max(0, self.in_flight_pods - 1)
            if is_gvisor:
                self.in_flight_gvisor_cpu = max(
                    0.0, self.in_flight_gvisor_cpu - req_cpu
                )
            else:
                self.in_flight_cpu = max(0.0, self.in_flight_cpu - req_cpu)
            cond.notify_all()


def parse_gke_minor_version(version_str: str | None) -> tuple[int, int] | None:
    """Extract ``(major, minor)`` tuple from a GKE version string like ``1.35.1-gke.100``."""
    if not version_str:
        return None
    match = _VERSION_RE.match(version_str.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def evaluate_autopilot_dind_capability(
    *,
    is_autopilot: bool,
    gke_version: str | None = None,
    allow_net_admin: bool = False,
    allowlist_paths: list[str] | tuple[str, ...] = (),
    max_schedulable_cpu_cores: int | None = None,
    max_gvisor_cpu_cores: int | None = None,
    available_machine_types: Sequence[str] = (),
    available_machine_families: Sequence[str] = (),
    node_auto_provisioning_enabled: bool = False,
    node_pool_machine_types: Sequence[tuple[str, str]] = (),
    max_node_allocatable_ephemeral_storage_mb: int | None = None,
    network_policy_enforced: bool | None = None,
) -> ClusterCapabilities:
    """Evaluate DinD availability and cluster capabilities from GKE control-plane metadata.

    Every branch below differs only in ``dind_availability`` and ``dind_reason``;
    everything else is carried through unchanged. Keeping the pass-through fields
    in one mapping means a new capability cannot be silently dropped on whichever
    branch its author forgot about.
    """
    paths_tuple = tuple(allowlist_paths)
    carried: dict[str, Any] = {
        "gke_version": gke_version,
        "allowlist_paths": paths_tuple,
        "max_schedulable_cpu_cores": max_schedulable_cpu_cores,
        "max_gvisor_cpu_cores": max_gvisor_cpu_cores,
        "available_machine_types": tuple(available_machine_types),
        "available_machine_families": tuple(available_machine_families),
        "node_auto_provisioning_enabled": node_auto_provisioning_enabled,
        "node_pool_machine_types": tuple(
            (str(name), str(mt)) for name, mt in node_pool_machine_types
        ),
        "max_node_allocatable_ephemeral_storage_mb": max_node_allocatable_ephemeral_storage_mb,
        "network_policy_enforced": network_policy_enforced,
    }

    if not is_autopilot:
        # Standard clusters run unrestricted workloads, so `allow_net_admin` is
        # asserted rather than read from Autopilot's workload policy.
        return ClusterCapabilities(
            is_autopilot=False,
            allow_net_admin=True,
            dind_availability=DindAvailability.DIND_AVAILABLE,
            dind_reason="Standard GKE cluster supports privileged DinD containers.",
            **carried,
        )

    # GKE version check (>= 1.35 required for customer WorkloadAllowlist)
    ver = parse_gke_minor_version(gke_version)
    if ver is not None and ver < (1, 35):
        return ClusterCapabilities(
            is_autopilot=True,
            allow_net_admin=allow_net_admin,
            dind_availability=DindAvailability.DIND_UNAVAILABLE,
            dind_reason=(
                f"GKE Autopilot cluster version {gke_version!r} is below 1.35. "
                f"Customer-managed WorkloadAllowlists for privileged DinD require GKE >= 1.35."
            ),
            **carried,
        )

    # Privileged admission allowlist paths check
    if not paths_tuple:
        return ClusterCapabilities(
            is_autopilot=True,
            allow_net_admin=allow_net_admin,
            dind_availability=DindAvailability.DIND_UNAVAILABLE,
            dind_reason=(
                "GKE Autopilot cluster has no privilegedAdmissionConfig.allowlistPaths "
                "configured. Harbor never installs cluster-wide policies. "
                "To enable DinD on Autopilot, authorize an allowlist path in org policy "
                "'container.managed.autopilotPrivilegedAdmission' and pass "
                "'--autopilot-privileged-admission=<path>' when updating the cluster."
            ),
            **carried,
        )

    return ClusterCapabilities(
        is_autopilot=True,
        allow_net_admin=allow_net_admin,
        dind_availability=DindAvailability.DIND_AVAILABLE,
        dind_reason=(
            f"GKE Autopilot cluster (version {gke_version or '>=1.35'}) with configured "
            f"privileged allowlist paths ({', '.join(paths_tuple)})."
        ),
        **carried,
    )


def _parse_machine_type_inventory(
    data: dict[str, Any],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Collect the distinct machine types and families the cluster's pools declare.

    Returns ``((), ())`` when there are no node pools to read, which is the normal
    case on Autopilot. Callers must read that as "unknown", not as "nothing
    available" -- refusing to schedule on an empty inventory would break every
    Autopilot cluster.
    """
    untainted_types: list[str] = []
    all_types: list[str] = []
    node_pools = data.get("nodePools")
    if isinstance(node_pools, list):
        for pool in node_pools:
            if not isinstance(pool, dict):
                continue
            cfg = pool.get("config") or {}
            machine_type = (cfg.get("machineType") or "").strip()
            if not machine_type:
                continue
            if machine_type not in all_types:
                all_types.append(machine_type)
            if not _has_blocking_taint(cfg.get("taints")):
                if machine_type not in untainted_types:
                    untainted_types.append(machine_type)

    types = untainted_types if untainted_types else all_types
    families: list[str] = []
    for machine_type in types:
        family = machine_type.split("-")[0]
        if family and family not in families:
            families.append(family)

    return tuple(types), tuple(families)


def _parse_node_pool_machine_types(
    data: dict[str, Any],
) -> tuple[tuple[str, str], ...]:
    """Map every named node pool to its machine type, including tainted pools.

    Tainted pools are kept on purpose: an operator who selects a pool explicitly
    (for example a GPU pool) is asking for exactly that pool.
    """
    pairs: list[tuple[str, str]] = []
    node_pools = data.get("nodePools")
    if isinstance(node_pools, list):
        for pool in node_pools:
            if not isinstance(pool, dict):
                continue
            name = str(pool.get("name") or "").strip()
            cfg = pool.get("config") or {}
            machine_type = str(cfg.get("machineType") or "").strip()
            if name and machine_type:
                pairs.append((name, machine_type))
    return tuple(pairs)


def parse_gcloud_cluster_describe(data: dict[str, Any]) -> ClusterCapabilities:
    """Parse JSON output of ``gcloud container clusters describe --format=json``."""
    autopilot_cfg = data.get("autopilot") or {}
    is_autopilot = bool(autopilot_cfg.get("enabled", False))
    gke_version = data.get("currentMasterVersion") or data.get("initialClusterVersion")

    workload_policy = autopilot_cfg.get("workloadPolicyConfig") or {}
    allow_net_admin = bool(workload_policy.get("allowNetAdmin", False))

    priv_cfg = autopilot_cfg.get("privilegedAdmissionConfig") or {}
    allowlist_paths = [str(p) for p in (priv_cfg.get("allowlistPaths") or []) if p]

    max_cpu, max_gvisor_cpu = _parse_cluster_schedulable_capacity(data)
    machine_types, machine_families = _parse_machine_type_inventory(data)
    max_storage_mb = _parse_cluster_max_ephemeral_storage_mb(data)

    cluster_autoscaling = data.get("autoscaling") or {}
    nap_enabled = bool(cluster_autoscaling.get("enableNodeAutoprovisioning", False))

    net_pol_cfg = data.get("networkPolicy") or {}
    addons_np = (data.get("addonsConfig") or {}).get("networkPolicyConfig") or {}
    net_cfg = data.get("networkConfig") or {}
    datapath_provider = str(net_cfg.get("datapathProvider") or "").strip().upper()
    calico_enabled = bool(net_pol_cfg.get("enabled", False)) and not bool(
        addons_np.get("disabled", False)
    )
    if is_autopilot or datapath_provider == "ADVANCED_DATAPATH" or calico_enabled:
        network_policy_enforced: bool | None = True
    elif any(
        k in data
        for k in ("networkPolicy", "networkConfig", "addonsConfig", "autopilot")
    ):
        network_policy_enforced = False
    else:
        network_policy_enforced = None

    return evaluate_autopilot_dind_capability(
        is_autopilot=is_autopilot,
        gke_version=str(gke_version) if gke_version else None,
        allow_net_admin=allow_net_admin,
        allowlist_paths=allowlist_paths,
        max_schedulable_cpu_cores=max_cpu,
        max_gvisor_cpu_cores=max_gvisor_cpu,
        available_machine_types=machine_types,
        available_machine_families=machine_families,
        node_auto_provisioning_enabled=nap_enabled,
        node_pool_machine_types=_parse_node_pool_machine_types(data),
        max_node_allocatable_ephemeral_storage_mb=max_storage_mb,
        network_policy_enforced=network_policy_enforced,
    )


def probe_cluster_via_gcloud(
    cluster_name: str,
    project_id: str,
    location: str,
) -> ClusterCapabilities | None:
    """Fetch cluster metadata via ``gcloud`` CLI and evaluate capabilities."""
    cmd = [
        "gcloud",
        "container",
        "clusters",
        "describe",
        cluster_name,
        f"--project={project_id}",
        f"--location={location}",
        "--format=json",
    ]
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if res.returncode != 0:
            logger.debug(
                f"gcloud cluster describe failed (exit {res.returncode}): {res.stderr.strip()}"
            )
            return None
        data = json.loads(res.stdout)
        if not isinstance(data, dict):
            return None
        return parse_gcloud_cluster_describe(data)
    except Exception as exc:
        logger.debug(f"Failed to probe GKE cluster capabilities via gcloud: {exc}")
        return None


def probe_pod_level_resources_support(
    core_api: Any,
    *,
    namespace: str = "default",
) -> bool | None:
    """Ask the API server whether it retains Pod-level ``spec.resources``.

    Submits a minimal Pod with ``dryRun=All`` and checks whether the field
    survives round-tripping. Nothing is created.

    A dry run is used in preference to comparing ``currentMasterVersion`` against
    1.34 because a version check answers the wrong question: it would report
    "supported" on a 1.34+ cluster whose ``PodLevelResources`` feature gate is
    switched off, and the field would then be pruned in silence. Asking the
    server what it will keep is the only answer that is true by construction.

    Returns ``True`` if retained, ``False`` if pruned, and ``None`` if the probe
    itself could not be completed -- which callers must treat as "unknown".
    """
    if core_api is None:
        return None
    try:
        from kubernetes import client as k8s_client

        probe_pod = k8s_client.V1Pod(
            metadata=k8s_client.V1ObjectMeta(
                generate_name="harbor-podlevel-probe-",
                namespace=namespace,
            ),
            spec=k8s_client.V1PodSpec(
                restart_policy="Never",
                resources=k8s_client.V1ResourceRequirements(
                    requests={"cpu": "100m", "memory": "128Mi"},
                    limits={"cpu": "100m", "memory": "128Mi"},
                ),
                containers=[
                    k8s_client.V1Container(
                        name="probe",
                        image="registry.k8s.io/pause:3.10",
                    )
                ],
            ),
        )
        echoed = core_api.create_namespaced_pod(
            namespace=namespace, body=probe_pod, dry_run="All"
        )
    except Exception as exc:
        logger.debug(f"Pod-level resources dry-run probe did not complete: {exc}")
        return None

    returned = getattr(getattr(echoed, "spec", None), "resources", None)
    return returned is not None and bool(
        getattr(returned, "requests", None) or getattr(returned, "limits", None)
    )


_QUANTITY_SUFFIXES_MIB: dict[str, float] = {
    "Ki": 1 / 1024,
    "Mi": 1.0,
    "Gi": 1024.0,
    "Ti": 1024.0 * 1024,
    "Pi": 1024.0 * 1024 * 1024,
    "k": 1000 / (1024 * 1024),
    "M": 1_000_000 / (1024 * 1024),
    "G": 1_000_000_000 / (1024 * 1024),
    "T": 1_000_000_000_000 / (1024 * 1024),
}


def parse_quantity_to_mib(quantity: str | None) -> int | None:
    """Convert a Kubernetes resource quantity to MiB, or ``None`` if unparseable.

    Handles both binary (``Gi``) and decimal (``G``) suffixes, and bare byte
    counts, which is the form node ``allocatable.ephemeral-storage`` usually takes.
    """
    if quantity is None:
        return None
    text = str(quantity).strip()
    if not text:
        return None
    # Longest suffix first so "Mi" is not matched by "M".
    for suffix in sorted(_QUANTITY_SUFFIXES_MIB, key=len, reverse=True):
        if text.endswith(suffix):
            try:
                return int(float(text[: -len(suffix)]) * _QUANTITY_SUFFIXES_MIB[suffix])
            except ValueError:
                return None
    try:
        return int(float(text) / (1024 * 1024))
    except ValueError:
        return None


def probe_max_node_ephemeral_storage_mb(core_api: Any) -> int | None:
    """Return the largest allocatable ``ephemeral-storage`` across schedulable nodes.

    The maximum, not the sum: a Pod runs on exactly one node, so a reservation
    larger than the biggest single node can never be satisfied no matter how many
    nodes exist. Cordoned nodes and nodes with blocking NoSchedule/NoExecute taints
    (such as ``CriticalAddonsOnly``) are excluded because Harbor trial Pods cannot
    schedule onto them.

    Returns ``None`` when nothing could be read. That is "unknown", and callers
    must not treat it as "zero" -- on a cluster whose pools are all scaled to zero
    this is also the honest answer, since autoscaling may yet produce a node.
    """
    if core_api is None:
        return None
    try:
        nodes = core_api.list_node().items
    except Exception as exc:
        logger.debug(f"Could not list nodes for ephemeral-storage preflight: {exc}")
        return None

    best: int | None = None
    for node in nodes:
        spec = getattr(node, "spec", None)
        if getattr(spec, "unschedulable", False):
            continue
        if _has_blocking_taint(getattr(spec, "taints", None)):
            continue
        allocatable = getattr(getattr(node, "status", None), "allocatable", None) or {}
        parsed = parse_quantity_to_mib(allocatable.get("ephemeral-storage"))
        if parsed is not None and (best is None or parsed > best):
            best = parsed
    return best
