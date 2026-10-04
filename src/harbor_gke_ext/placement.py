"""Compose placement classifier and GPU reconciliation for GKE (Commit S2).

Why this module exists
----------------------
This module enforces Invariants 1, 2, 3, and 7 of the refactored GKE
architecture:
- Invariant 1: ``main`` is a native Pod container (``spec.containers``) unless
  it requests a DinD-only feature (such as ``/var/run/docker.sock`` or
  ``privileged: true``). In that case the project uses Shape C: ``main`` and
  every sidecar run inside the in-Pod Docker daemon, and sidecars without their
  own reason are tagged ``CO_LOCATED_WITH_MAIN_DIND``. With
  ``compose_placement="native"``, placement fails fast instead, with a named
  cause (``MAIN_NEEDS_DIND``).
- Invariant 2: Outside Shape C, DinD is a fallback plane for sidecars only.
  Every reason a service was routed to DinD is recorded in trial metadata.
- Invariant 3: Any unclassified Compose service key raises an immediate
  ``UnsupportedComposeFeatureError`` (``UNCLASSIFIED_KEY``) and logs the
  offending service and key.
- Invariant 4 (LIFTED 2026-09-17): GPU-requesting services used to be pinned to
  the native plane. A nested container was measured performing a real CUDA
  allocation on ``l4-pool``, so a GPU service may now be placed in DinD; the
  ``nvidia.com/gpu`` allocation moves to ``dind-engine``. See step 7 below.
- Invariant 7: Placement and GPU configuration are fully decided before Pod
  construction begins.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from harbor_gke_ext.cluster_probe import (
    ClusterCapabilities,
    DindAvailability,
)
from harbor_gke_ext.compose_spec import (
    MAIN_SERVICE_NAME,
    UnsupportedComposeFeatureError,
    is_harbor_synthetic_log_mount,
    resolve_contained_task_path,
)
from harbor_gke_ext.constants import (
    GKE_GPU_TYPE_MAP,
    resolve_gpu_accelerator_label,
)

ComposePlacementMode = Literal["auto", "native", "dind"]

ALWAYS_UNSUPPORTED: dict[str, str] = {
    "mac_address": "MAC_ADDRESS",
    "credential_spec": "WINDOWS_ONLY",
    "isolation": "WINDOWS_ONLY",
    "external_links": "EXTERNAL_LINKS",
    "cgroup": "CGROUP",
    "cgroup_parent": "CGROUP",
    "provider": "PROVIDER",
    "userns_mode": "HOST_NAMESPACE",
    "uts": "HOST_NAMESPACE",
}

DIND_KEYS: dict[str, str] = {
    "ulimits": "ULIMITS",
    "init": "INIT_PID1",
    "blkio_config": "BLKIO",
    "cpuset": "CPUSET",
    "cpu_count": "CPU_LEGACY",
    "cpu_percent": "CPU_LEGACY",
    "cpu_period": "CPU_LEGACY",
    "cpu_quota": "CPU_LEGACY",
    "cpu_rt_runtime": "CPU_LEGACY",
    "cpu_rt_period": "CPU_LEGACY",
    "oom_kill_disable": "OOM",
    "oom_score_adj": "OOM",
    "mem_swappiness": "SWAP",
    "memswap_limit": "SWAP",
    "pids_limit": "PIDS_LIMIT",
    "storage_opt": "STORAGE_OPT",
    "device_cgroup_rules": "DEVICE_CGROUP",
}

NATIVE_KEYS: frozenset[str] = frozenset(
    {
        "annotations",
        "attach",
        "build",
        "cap_add",
        "cap_drop",
        "command",
        "configs",
        "container_name",
        "cpus",
        "depends_on",
        "deploy",
        "develop",
        "dns",
        "dns_opt",
        "dns_search",
        "domainname",
        "entrypoint",
        "env_file",
        "environment",
        "expose",
        "extends",
        "extra_hosts",
        "gpus",
        "group_add",
        "healthcheck",
        "hostname",
        "image",
        "labels",
        "label_file",
        "links",
        "logging",
        "mem_limit",
        "mem_reservation",
        "networks",
        "platform",
        "post_start",
        "pre_stop",
        "privileged",
        "profiles",
        "pull_policy",
        "read_only",
        "restart",
        "runtime",
        "scale",
        "secrets",
        "security_opt",
        "shm_size",
        "stdin_open",
        "stop_grace_period",
        "stop_signal",
        "sysctls",
        "tmpfs",
        "tty",
        "user",
        "volumes",
        "volumes_from",
        "working_dir",
        "devices",
        "network_mode",
        "pid",
        "ipc",
        "ports",
    }
)

SAFE_SYSCTL_PREFIXES: tuple[str, ...] = (
    "kernel.shm",
    "kernel.msg",
    "kernel.sem",
    "fs.mqueue.",
    "net.ipv4.ip_local_port_range",
    "net.ipv4.ip_unprivileged_port_start",
    "net.ipv4.tcp_syncookies",
    "net.ipv4.ping_group_range",
)

DOCKER_SOCK_PATH = "/var/run/docker.sock"


@dataclass(frozen=True)
class ReconciledGpuConfig:
    """Result of reconciling GPU counts and accelerator labels across sources."""

    gpus: int = 0
    accelerator_label: str | None = None
    gpu_services: tuple[str, ...] = ()
    service_gpus: dict[str, int] = field(default_factory=dict)


@dataclass
class PlacementPlan:
    """Complete pre-flight placement decision for a multi-container GKE Pod."""

    shape: Literal["A", "B", "C"]
    native_init_services: list[str] = field(default_factory=list)
    native_sidecar_services: list[str] = field(default_factory=list)
    pre_main_ordered_services: list[str] = field(default_factory=list)
    post_main_sidecars: list[str] = field(default_factory=list)
    dind_sidecars: list[str] = field(default_factory=list)
    dind_reasons: dict[str, list[str]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    gpu_config: ReconciledGpuConfig = field(default_factory=ReconciledGpuConfig)

    @property
    def dind_gpu_total(self) -> int:
        """Total GPUs requested by services placed in the DinD plane.

        This is the count that must be allocated to ``dind-engine``: the GKE
        device plugin mounts ``/usr/local/nvidia`` only into the container
        holding the allocation, and that driver bind -- not the device nodes --
        is what actually gates CUDA. See GKE_NOTES.md section 10.
        """
        return sum(
            self.gpu_config.service_gpus.get(sname, 0) for sname in self.dind_sidecars
        )


def _extract_compose_service_gpus(sspec: dict[str, Any]) -> int | str | None:
    """Return GPU count (int or 'all') declared in a service's deploy reservations."""
    deploy = sspec.get("deploy")
    if isinstance(deploy, dict):
        res = (deploy.get("resources") or {}).get("reservations") or {}
        devices = res.get("devices") or []
        if isinstance(devices, list):
            for dev in devices:
                if not isinstance(dev, dict):
                    continue
                caps = dev.get("capabilities") or []
                driver = str(dev.get("driver") or "").lower()
                if "gpu" in caps or "nvidia" in caps or driver == "nvidia":
                    cnt = dev.get("count")
                    if cnt is None:
                        return 1
                    if str(cnt).lower() == "all":
                        return "all"
                    try:
                        return int(cnt)
                    except (TypeError, ValueError):
                        return "all"
    # Legacy top-level 'gpus' service key
    top_gpus = sspec.get("gpus")
    if top_gpus is not None:
        if str(top_gpus).lower() == "all":
            return "all"
        try:
            return int(top_gpus)
        except (TypeError, ValueError):
            return 1
    return None


def reconcile_gpu_config(
    project: dict[str, Any],
    *,
    toml_gpus: int | None = None,
    toml_gpu_types: list[str] | str | None = None,
    gpu_override: str | None = None,
    default_gpu_type: str | None = None,
    default_gpu_count: int | None = None,
) -> ReconciledGpuConfig:
    """Reconcile GPU declarations across task.toml, compose YAML, and CLI overrides.

    Enforces:
    - Decision Q3: ``count: all`` in compose resolves from ``task.toml`` ``gpus``
      (or ``default_gpu_count`` if provided); if neither is > 0, raises ``GPU_COMPOSE_ONLY``.
    - Decision Q3b / Q10: When effective ``gpus > 0``, resolves the GKE accelerator
      label via precedence:
      ``gpu_override`` (full label only) > ``task.toml gpu_types`` (short/full) >
      ``default_gpu_type`` (short/full) > hard error ``GPU_TYPE_UNRESOLVED``.
    """
    services = project.get("services") or {}
    eff_toml_gpus = (
        int(toml_gpus)
        if toml_gpus is not None and int(toml_gpus) > 0
        else (
            int(default_gpu_count)
            if default_gpu_count is not None and int(default_gpu_count) > 0
            else 0
        )
    )
    causes: list[str] = []

    raw_service_gpus: dict[str, int | str] = {}
    for sname, sspec in services.items():
        if not isinstance(sspec, dict):
            continue
        cnt = _extract_compose_service_gpus(sspec)
        if cnt is not None:
            raw_service_gpus[sname] = cnt

    service_gpus: dict[str, int] = {}
    sidecar_gpu_sum = 0

    for sname, cnt in raw_service_gpus.items():
        if sname == MAIN_SERVICE_NAME:
            continue
        if cnt == "all":
            if eff_toml_gpus <= 0:
                causes.append(
                    f"GPU_COMPOSE_ONLY:{sname}(count=all): compose service '{sname}' "
                    "declares deploy.resources.reservations.devices with count='all', "
                    "which requires an explicit integer 'gpus' count in task.toml [environment]."
                )
            else:
                service_gpus[sname] = eff_toml_gpus
                sidecar_gpu_sum += eff_toml_gpus
        elif isinstance(cnt, int) and cnt > 0:
            service_gpus[sname] = cnt
            sidecar_gpu_sum += cnt

    main_compose_gpu = raw_service_gpus.get(MAIN_SERVICE_NAME)
    if main_compose_gpu == "all":
        if eff_toml_gpus <= 0:
            causes.append(
                "GPU_COMPOSE_ONLY:main(count=all): compose service 'main' declares "
                "deploy.resources.reservations.devices with count='all', which requires "
                "an explicit integer 'gpus' count in task.toml [environment]. "
                "Use the '-modal' dataset variant or set 'gpus = 1' in task.toml."
            )
        else:
            main_gpus = max(0, eff_toml_gpus - sidecar_gpu_sum)
            if main_gpus > 0:
                service_gpus[MAIN_SERVICE_NAME] = main_gpus
    elif isinstance(main_compose_gpu, int):
        if eff_toml_gpus > 0 and eff_toml_gpus != (main_compose_gpu + sidecar_gpu_sum):
            causes.append(
                f"GPU_COUNT_MISMATCH:main: compose declares count={main_compose_gpu} "
                f"(plus {sidecar_gpu_sum} sidecar GPUs) "
                f"but task.toml declares gpus={eff_toml_gpus}."
            )
        if main_compose_gpu > 0:
            service_gpus[MAIN_SERVICE_NAME] = main_compose_gpu
    else:
        # main did not declare GPU in compose
        if sidecar_gpu_sum == 0:
            if eff_toml_gpus > 0:
                service_gpus[MAIN_SERVICE_NAME] = eff_toml_gpus
        else:
            if eff_toml_gpus > sidecar_gpu_sum:
                service_gpus[MAIN_SERVICE_NAME] = eff_toml_gpus - sidecar_gpu_sum
            elif eff_toml_gpus > 0 and eff_toml_gpus < sidecar_gpu_sum:
                causes.append(
                    f"GPU_COUNT_MISMATCH: task.toml declares gpus={eff_toml_gpus} "
                    f"which is less than sidecar GPU declarations ({sidecar_gpu_sum})."
                )

    gpu_services = list(service_gpus.keys())
    effective_gpus = sum(service_gpus.values())

    accelerator_label: str | None = None
    if effective_gpus > 0 or gpu_services:
        # 1. gpu_override (must be full label)
        if gpu_override:
            valid_full_labels = set(GKE_GPU_TYPE_MAP.values())
            if gpu_override not in valid_full_labels:
                raise ValueError(
                    f"Invalid gpu_override '{gpu_override}'. Must be a full GKE "
                    f"accelerator label: {sorted(valid_full_labels)}"
                )
            accelerator_label = gpu_override
        # 2. task.toml gpu_types (accepts short name or full label)
        elif toml_gpu_types:
            raw_type = (
                toml_gpu_types[0]
                if isinstance(toml_gpu_types, list) and toml_gpu_types
                else str(toml_gpu_types)
            )
            accelerator_label = resolve_gpu_accelerator_label(raw_type)
        # 3. default_gpu_type (accepts short name or full label)
        elif default_gpu_type:
            accelerator_label = resolve_gpu_accelerator_label(default_gpu_type)
        else:
            causes.append(
                "GPU_TYPE_UNRESOLVED: task requests GPU(s) but no GPU type is specified "
                "in task.toml 'gpu_types', '--ek gpu_override', or '--ek default_gpu_type'. "
                "GKE Autopilot rejects GPU Pods without an explicit accelerator selector, "
                "and GKE Standard schedules onto arbitrary GPU pools. "
                "Pass '--ek default_gpu_type=<type>' (e.g. 'l4' or 'nvidia-l4')."
            )

    if causes:
        raise UnsupportedComposeFeatureError(causes)

    return ReconciledGpuConfig(
        gpus=effective_gpus,
        accelerator_label=accelerator_label,
        gpu_services=tuple(gpu_services),
        service_gpus=service_gpus,
    )


def _parse_service_depends_on(sspec: dict[str, Any]) -> dict[str, str]:
    """Return ``{dependency_service_name: condition}`` for a service."""
    deps = sspec.get("depends_on")
    if not deps:
        return {}
    if isinstance(deps, list):
        return {str(d): "service_started" for d in deps}
    if isinstance(deps, dict):
        result: dict[str, str] = {}
        for dep_name, dep_cfg in deps.items():
            if isinstance(dep_cfg, dict):
                cond = str(dep_cfg.get("condition") or "service_started")
            else:
                cond = "service_started"
            result[str(dep_name)] = cond
        return result
    return {}


def _topological_sort_services(
    services: list[str],
    deps_map: dict[str, dict[str, str]],
) -> list[str]:
    """Topologically sort ``services`` so dependencies appear before dependents."""
    service_set = set(services)
    in_degree: dict[str, int] = {s: 0 for s in services}
    adj: dict[str, list[str]] = defaultdict(list)

    for s in services:
        for dep in deps_map.get(s, {}):
            if dep in service_set and dep != s:
                adj[dep].append(s)
                in_degree[s] += 1

    queue = sorted([s for s in services if in_degree[s] == 0])
    ordered: list[str] = []

    while queue:
        curr = queue.pop(0)
        ordered.append(curr)
        for nxt in sorted(adj[curr]):
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)
                queue.sort()

    if len(ordered) != len(services):
        cycle_members = sorted(service_set - set(ordered))
        raise UnsupportedComposeFeatureError(
            [f"DEPENDENCY_CYCLE:{','.join(cycle_members)}"]
        )
    return ordered


def classify_compose_placement(
    project: dict[str, Any],
    *,
    task_dir: Path,
    base_dir: Path,
    compose_placement: ComposePlacementMode = "auto",
    cluster_capabilities: ClusterCapabilities | None = None,
    toml_gpus: int | None = None,
    toml_gpu_types: list[str] | str | None = None,
    gpu_override: str | None = None,
    default_gpu_type: str | None = None,
    default_gpu_count: int | None = None,
    logger: Any | None = None,
) -> PlacementPlan:
    """Classify every service in ``project`` into Native (Shape A) or DinD (Shape B)."""
    from harbor.utils.logger import logger as _default_logger

    eff_logger = logger if logger is not None else _default_logger
    caps = cluster_capabilities or ClusterCapabilities()
    gpu_config = reconcile_gpu_config(
        project,
        toml_gpus=toml_gpus,
        toml_gpu_types=toml_gpu_types,
        gpu_override=gpu_override,
        default_gpu_type=default_gpu_type,
        default_gpu_count=default_gpu_count,
    )

    services = project.get("services") or {}
    if not isinstance(services, dict) or MAIN_SERVICE_NAME not in services:
        causes = ["MISSING_MAIN_SERVICE: compose project must define a 'main' service."]
        eff_logger.error(f"Compose placement failed: {causes}")
        raise UnsupportedComposeFeatureError(causes)

    fail_causes: list[str] = []
    dind_reasons: dict[str, list[str]] = defaultdict(list)
    warnings: list[str] = []
    expose_owners: dict[str, list[str]] = defaultdict(list)
    deps_map: dict[str, dict[str, str]] = {}

    resolved_task_dir = task_dir.resolve()
    resolved_base_dir = base_dir.resolve()

    # 1. Project-level volumes check
    external_vol_names: set[str] = set()
    for vname, vcfg in (project.get("volumes") or {}).items():
        if isinstance(vcfg, dict):
            if vcfg.get("external"):
                external_vol_names.add(vname)
            drv = vcfg.get("driver")
            if drv and str(drv) != "local":
                fail_causes.append(f"VOLUME_DRIVER:<top>:{vname}={drv}")
    mounted_external_vols: set[str] = set()

    # 2. Project-level configs & secrets check
    for section in ("configs", "secrets"):
        for cname, ccfg in (project.get(section) or {}).items():
            if isinstance(ccfg, dict) and ccfg.get("external"):
                fail_causes.append(f"EXTERNAL_{section.upper()}:<top>:{cname}")

    # 3. Project-level networks & segmentation check
    top_nets = project.get("networks") or {}
    main_spec = services.get(MAIN_SERVICE_NAME) or {}
    main_nets_raw = main_spec.get("networks")
    if isinstance(main_nets_raw, dict):
        main_net_names = set(main_nets_raw.keys())
    elif isinstance(main_nets_raw, list):
        main_net_names = {str(n) for n in main_nets_raw}
    else:
        main_net_names = {"default"}

    if isinstance(top_nets, dict) and len(top_nets) > 1:
        for sname, sspec in services.items():
            if sname == MAIN_SERVICE_NAME or not isinstance(sspec, dict):
                continue
            snets_raw = sspec.get("networks")
            if isinstance(snets_raw, dict):
                snet_names = set(snets_raw.keys())
            elif isinstance(snets_raw, list):
                snet_names = {str(n) for n in snets_raw}
            else:
                snet_names = {"default"}

            # If a sidecar is isolated on a network that main is NOT attached to
            if snet_names and not (snet_names & main_net_names):
                isolated = ",".join(sorted(snet_names - main_net_names))
                fail_causes.append(
                    f"MAIN_NETWORK_SEGMENTATION:{sname}:isolated on network(s) "
                    f"'{isolated}' unreachable from native 'main'"
                )
            elif len(snet_names) > 1:
                dind_reasons[sname].append(
                    f"MULTI_NETWORK:{','.join(sorted(snet_names))}"
                )

    # 4. Per-service inspection
    for sname, sspec in services.items():
        if not isinstance(sspec, dict):
            continue

        deps_map[sname] = _parse_service_depends_on(sspec)

        # Key classification: unknown keys fail immediately (Invariant 3)
        for key in sspec:
            if key in ALWAYS_UNSUPPORTED:
                fail_causes.append(f"{ALWAYS_UNSUPPORTED[key]}:{sname}:{key}")
            elif key in DIND_KEYS:
                dind_reasons[sname].append(f"{DIND_KEYS[key]}:{key}")
            elif key not in NATIVE_KEYS and not key.startswith("x-"):
                fail_causes.append(f"UNCLASSIFIED_KEY:{sname}:{key}")

        # Non-GPU host devices route sidecars to DinD
        for dev in sspec.get("devices") or []:
            dev_str = str(dev)
            if dev_str.startswith(("/dev/nvidia", "/dev/dri")):
                continue
            dind_reasons[sname].append(f"DEVICE_NON_GPU:{dev}")

        # Scale / replicas > 1
        scale_val = sspec.get("scale")
        if scale_val is not None:
            try:
                if int(scale_val) > 1:
                    fail_causes.append(f"SCALE_GT1:{sname}:{scale_val}")
            except (TypeError, ValueError):
                pass
        deploy_cfg = sspec.get("deploy")
        if isinstance(deploy_cfg, dict):
            rep_val = deploy_cfg.get("replicas")
            if rep_val is not None:
                try:
                    if int(rep_val) > 1:
                        fail_causes.append(f"SCALE_GT1:{sname}:{rep_val}")
                except (TypeError, ValueError):
                    pass

        # Network mode & host namespaces
        nm = sspec.get("network_mode")
        if nm in ("host", "none"):
            fail_causes.append(f"NETWORK_MODE_{str(nm).upper()}:{sname}:{nm}")
        for ns_key in ("pid", "ipc"):
            if sspec.get(ns_key) == "host":
                fail_causes.append(f"HOST_NAMESPACE:{sname}:{ns_key}=host")

        # Stop signal
        stop_sig = sspec.get("stop_signal")
        if stop_sig and str(stop_sig).upper() not in ("SIGTERM", "SIGKILL", "15", "9"):
            fail_causes.append(f"STOP_SIGNAL:{sname}:{stop_sig}")

        # Static IP assignment check
        snets = sspec.get("networks")
        if isinstance(snets, dict):
            for net_name, net_cfg in snets.items():
                if isinstance(net_cfg, dict):
                    for ip_key in ("ipv4_address", "ipv6_address"):
                        if net_cfg.get(ip_key):
                            fail_causes.append(
                                f"STATIC_IP:{sname}:{net_name}.{ip_key}={net_cfg[ip_key]}"
                            )

        # Privileged check
        if sspec.get("privileged") is True:
            dind_reasons[sname].append("PRIVILEGED:privileged=true")

        # User by symbolic name check
        user_val = sspec.get("user")
        if user_val is not None:
            uid_part = str(user_val).split(":", 1)[0].strip()
            if uid_part and not uid_part.isdigit() and uid_part != "root":
                if sname != MAIN_SERVICE_NAME:
                    dind_reasons[sname].append(f"USER_BY_NAME:{user_val}")

        # Sysctls check
        sysctls = sspec.get("sysctls")
        if sysctls:
            items = (
                [f"{k}={v}" for k, v in sysctls.items()]
                if isinstance(sysctls, dict)
                else list(sysctls)
            )
            for item in items:
                skey = str(item).split("=", 1)[0].strip()
                if not skey.startswith(SAFE_SYSCTL_PREFIXES):
                    dind_reasons[sname].append(f"UNSAFE_SYSCTL:{skey}")

        # Volumes check
        for vol in sspec.get("volumes") or []:
            vtype = "volume"
            vsrc = ""
            vtgt = ""
            if isinstance(vol, dict):
                vtype = str(vol.get("type") or "volume")
                vsrc = str(vol.get("source") or "").strip()
                vtgt = str(vol.get("target") or "").strip()
            elif isinstance(vol, str):
                parts = vol.split(":")
                if len(parts) == 1:
                    vtype = "volume"
                    vsrc = ""
                    vtgt = parts[0].strip()
                else:
                    vsrc = parts[0].strip()
                    vtgt = parts[1].strip()
                    vtype = (
                        "bind"
                        if (vsrc.startswith("/") or vsrc.startswith("."))
                        else "volume"
                    )

            if vsrc in (DOCKER_SOCK_PATH, "/run/docker.sock") and vtgt in (
                DOCKER_SOCK_PATH,
                "/run/docker.sock",
            ):
                dind_reasons[sname].append(f"DOCKER_SOCK:{DOCKER_SOCK_PATH}")
                continue

            if vsrc in external_vol_names:
                mounted_external_vols.add(vsrc)
                dind_reasons[sname].append(f"EXTERNAL_VOLUME:{vsrc}")

            if vtype == "bind" and vsrc:
                if is_harbor_synthetic_log_mount(vsrc, vtgt):
                    continue

                try:
                    resolve_contained_task_path(
                        vsrc,
                        base_dir=resolved_base_dir,
                        allowed_roots=(resolved_base_dir, resolved_task_dir),
                        field_name=f"services.{sname}.volumes",
                    )
                except UnsupportedComposeFeatureError:
                    fail_causes.append(f"ABSOLUTE_BIND:{sname}:{vsrc}")

        # Expose / port collision tracking
        for exp in sspec.get("expose") or []:
            port_str = str(exp).split("/", 1)[0].strip()
            if port_str:
                expose_owners[port_str].append(sname)
        for port_entry in sspec.get("ports") or []:
            if isinstance(port_entry, dict):
                target_p = port_entry.get("target")
                if target_p:
                    expose_owners[str(target_p)].append(sname)
            elif isinstance(port_entry, str):
                p_clean = port_entry.split("/", 1)[0]
                target_p = p_clean.rsplit(":", 1)[-1]
                if target_p:
                    expose_owners[target_p].append(sname)

    # Any unmounted top-level external volumes route sidecars to DinD (or main if no sidecars)
    for unmounted_ext in sorted(external_vol_names - mounted_external_vols):
        non_main = [s for s in services if s != MAIN_SERVICE_NAME]
        if non_main:
            for s in non_main:
                dind_reasons[s].append(f"EXTERNAL_VOLUME:{unmounted_ext}")
        else:
            dind_reasons[MAIN_SERVICE_NAME].append(f"EXTERNAL_VOLUME:{unmounted_ext}")

    # 5. Shape C (RFC 0004): when 'main' requires DinD features, route the
    # compose project into the in-Pod DinD plane unless native placement is forced.
    if dind_reasons.get(MAIN_SERVICE_NAME) and compose_placement == "native":
        for r in dind_reasons[MAIN_SERVICE_NAME]:
            fail_causes.append(f"MAIN_NEEDS_DIND:{r}")

    # 6. Port collisions among native candidates route colliding sidecars to DinD
    for port, owners in expose_owners.items():
        unique_owners = sorted(set(owners))
        if len(unique_owners) > 1:
            for owner in unique_owners:
                if owner != MAIN_SERVICE_NAME:
                    dind_reasons[owner].append(
                        f"PORT_COLLISION:{','.join(unique_owners)}:{port}"
                    )

    # 7. Invariant 4 (LIFTED 2026-09-17): a GPU-requesting service MAY run in DinD.
    #
    # This previously raised GPU_DIND_CONFLICT on the assumption that a nested
    # container could not reach the GPU. That assumption was measured false on
    # `l4-pool` (GKE 1.35.6, docker 28.3.3-dind, privileged, overlay2): a nested
    # container performed a real CUDA allocation, not merely `nvidia-smi`.
    #
    # The allocation is held by `dind-engine` -- which is what causes the device
    # plugin to mount `/usr/local/nvidia` -- and the inner container receives the
    # devices and driver libraries by explicit passthrough. See
    # `_build_shape_b_dind_containers` and GKE_NOTES.md section 10.

    if fail_causes:
        eff_logger.error(
            f"Compose placement failed due to unsupported features/requests: {fail_causes}"
        )
        raise UnsupportedComposeFeatureError(fail_causes)

    # 8. Apply compose_placement mode override ("auto" | "native" | "dind")
    sidecar_names = [s for s in services if s != MAIN_SERVICE_NAME]
    dind_sidecars: list[str] = []

    if dind_reasons.get(MAIN_SERVICE_NAME):
        # Shape C: main and all sibling compose services run inside dind-engine
        dind_sidecars.append(MAIN_SERVICE_NAME)
        for s in sidecar_names:
            dind_sidecars.append(s)
            if not dind_reasons.get(s):
                dind_reasons[s].append("CO_LOCATED_WITH_MAIN_DIND")
    elif compose_placement == "native":
        needing_dind = [s for s in sidecar_names if dind_reasons.get(s)]
        if needing_dind:
            details = [
                f"NATIVE_PLACEMENT_REJECTED:{s}:{','.join(dind_reasons[s])}"
                for s in needing_dind
            ]
            eff_logger.error(
                f"Compose placement failed (compose_placement='native' rejected sidecars): {details}"
            )
            raise UnsupportedComposeFeatureError(details)
    elif compose_placement == "dind":
        for s in sidecar_names:
            dind_sidecars.append(s)
            if not dind_reasons.get(s):
                dind_reasons[s].append("EXPLICIT_PLACEMENT_DIND")
    else:
        # "auto"
        for s in sidecar_names:
            if dind_reasons.get(s):
                dind_sidecars.append(s)

    # 9. Check cluster DinD availability if DinD plane is non-empty
    if dind_sidecars and caps.dind_availability != DindAvailability.DIND_AVAILABLE:
        causes = [
            f"AUTOPILOT_{caps.dind_availability.name}: service(s) "
            f"{sorted(dind_sidecars)} require DinD ({dict(dind_reasons)}), "
            f"but DinD is {caps.dind_availability.value} on this cluster: "
            f"{caps.dind_reason}"
        ]
        eff_logger.error(f"Compose placement failed: {causes}")
        raise UnsupportedComposeFeatureError(causes)

    dind_set = set(dind_sidecars)
    native_candidates = [s for s in sidecar_names if s not in dind_set]

    # Surface GPU-in-DinD placement: the nvidia.com/gpu allocation is transferred
    # from the logical service to `dind-engine`, which is non-obvious when reading
    # the Pod spec.
    for gpu_svc in gpu_config.gpu_services:
        if gpu_svc in dind_set:
            warn_msg = (
                f"Compose service {gpu_svc!r} requests "
                f"{gpu_config.service_gpus.get(gpu_svc, 0)} GPU(s) and is placed in "
                f"DinD ({', '.join(dind_reasons.get(gpu_svc, []))}). The "
                f"nvidia.com/gpu allocation is held by 'dind-engine'; the inner "
                f"container receives devices and /usr/local/nvidia by explicit "
                f"passthrough. Per-service GPU isolation is NOT enforced inside "
                f"the DinD plane."
            )
            warnings.append(warn_msg)
            eff_logger.warning(warn_msg)

    # 10. Separate post-main sidecars (Decision Q1: depends_on: main)
    post_main_sidecars: list[str] = []
    pre_main_candidates: list[str] = []

    for s in native_candidates:
        s_deps = deps_map.get(s, {})
        if MAIN_SERVICE_NAME in s_deps:
            post_main_sidecars.append(s)
            warn_msg = (
                f"Compose service {s!r} declares depends_on 'main' ('depends_on: main'). "
                f"Kubernetes native sidecars (initContainers) start before 'main', "
                f"so Harbor is placing {s!r} in spec.containers with a startup gate "
                f"waiting for '/harbor/gates/main-ready' (Decision Q1 workaround). "
                f"See documentation for behavioral differences."
            )
            warnings.append(warn_msg)
            eff_logger.warning(warn_msg)
        else:
            pre_main_candidates.append(s)

    # 11. Topological sort of pre-main native sidecars & one-shot init classification
    sorted_pre_main = _topological_sort_services(pre_main_candidates, deps_map)

    # Identify one-shot init containers: a service is a one-shot init container ONLY if
    # another service depends on it with condition == 'service_completed_successfully'.
    completed_dep_targets: set[str] = set()
    for s_deps in deps_map.values():
        for dep_target, cond in s_deps.items():
            if cond == "service_completed_successfully":
                completed_dep_targets.add(dep_target)

    native_init_services: list[str] = []
    native_sidecar_services: list[str] = []
    for s in sorted_pre_main:
        if s in completed_dep_targets:
            native_init_services.append(s)
        else:
            native_sidecar_services.append(s)

    shape: Literal["A", "B", "C"] = (
        "C" if MAIN_SERVICE_NAME in dind_set else ("B" if dind_sidecars else "A")
    )

    return PlacementPlan(
        shape=shape,
        native_init_services=native_init_services,
        native_sidecar_services=native_sidecar_services,
        pre_main_ordered_services=list(sorted_pre_main),
        post_main_sidecars=post_main_sidecars,
        dind_sidecars=sorted(dind_sidecars),
        dind_reasons=dict(dind_reasons),
        warnings=warnings,
        gpu_config=gpu_config,
    )
