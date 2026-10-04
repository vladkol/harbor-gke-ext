from __future__ import annotations

import asyncio
import datetime
import inspect
import json
import re
import shlex
import threading
import time
import tomllib
import uuid
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, override

import yaml
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from harbor.constants import MAIN_SERVICE_NAME
from harbor.environments.base import (
    BaseEnvironment,
    ExecResult,
    HealthcheckError,
)
from harbor.environments.capabilities import (
    EnvironmentCapabilities,
    EnvironmentResourceCapabilities,
)
from harbor.environments.compose_service_ops import (
    ComposeServiceOpsMixin,
    ComposeServiceTransport,
)
from harbor.environments.definition import (
    require_agent_environment_definition,
    should_use_prebuilt_docker_image,
)
from harbor.models.task.config import (
    EnvironmentConfig,
    HealthcheckConfig,
    NetworkMode,
    NetworkPolicy,
)
from harbor.models.trial.config import ResourceMode
from harbor.models.trial.paths import TrialPaths
from harbor.utils.logger import logger
from harbor.utils.optional_import import MissingExtraError
from harbor_gke_ext.client import (
    KubernetesClientManager,
    _ensure_file_descriptor_limit,
    derive_region,
    ensure_gcloud_ready,
    resolve_gke_target,
)
from harbor_gke_ext.cloud_build import (
    check_image_exists_in_registry,
    resolve_task_image_url,
    submit_cloud_build,
)
from harbor_gke_ext.cluster_probe import (
    ClusterAdmissionController,
    ClusterCapabilities,
    _parse_machine_type_vcpus,
    parse_quantity_to_mib,
    probe_cluster_via_gcloud,
    probe_fqdn_network_policy_support,
    probe_kube_dns_cluster_ip,
    probe_pod_level_resources_support,
)
from harbor_gke_ext.compose_translator import (
    DIND_ENGINE_CONTAINER,
    DIND_STORAGE_FLOOR_MB,
    _GKENativeComposeServiceTransport,
    discover_compose_build_services,
    resolve_compose_infra_env,
    resolve_compose_placement,
    translate_compose,
)
from harbor_gke_ext.constants import (
    _GKE_AUTOPILOT_DEFAULT_CONTAINER_STORAGE_MB,
    _GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB,
    _GKE_CONTROL_PLANE_WRITE_MAX_ATTEMPTS,
    _GKE_DECOUPLED_LAUNCH_TIMEOUT_SEC,
    _GKE_DEFAULT_AGENT_SETUP_TIMEOUT_SEC,
    _GKE_DEFAULT_AGENT_TIMEOUT_MINUTES,
    _GKE_DEFAULT_COMPOSE_UP_TIMEOUT_SEC,
    _GKE_DEFAULT_DEADLINE_BUFFER_MINUTES,
    _GKE_DEFAULT_VERIFIER_TIMEOUT_SEC,
    _GKE_DIND_USAGE_REPORT_TIMEOUT_SEC,
    _GKE_EXEC_CONNECT_MAX_ATTEMPTS,
    _GKE_EXEC_HANDSHAKE_TIMEOUT_SEC,
    _GKE_EXEC_KILL_TIMEOUT_SEC,
    _GKE_JOB_POD_SPAWN_TIMEOUT_SEC,
    _GKE_WEBHOOK_CALL_FAILURE_MARKER,
    GKE_GPU_TYPE_MAP,
    EphemeralStorageUnschedulableError,
    GKEExecStreamClosedError,
    PlacementConflictError,
    TrialContainerLostError,
    UnsatisfiableMachineTypeError,
    _parse_bool,
    _sanitize_kubernetes_resource_name,
    resolve_gpu_accelerator_label,
    resolve_tpu_accelerator_label,
)
from harbor_gke_ext.control_plane import (
    get_control_plane_limiter,
    is_control_plane_overload,
    jittered_backoff_delay,
)
from harbor_gke_ext.exec_engine import (
    build_decoupled_launch_script,
    build_kill_script,
    build_supervised_script,
    check_pod_terminated,
    collect_exec_bytes,
    connect_exec_stream,
    is_transient_exec_status_error,
    poll_decoupled_exec,
    read_exec_output,
    run_best_effort,
    run_exec_command,
)
from harbor_gke_ext.exec_engine import (
    download_dir as ft_download_dir,
)
from harbor_gke_ext.exec_engine import (
    download_file as ft_download_file,
)
from harbor_gke_ext.exec_engine import (
    upload_dir as ft_upload_dir,
)
from harbor_gke_ext.exec_engine import (
    upload_file as ft_upload_file,
)
from harbor_gke_ext.exec_stream import ExecOutputAccumulator, ExecStream
from harbor_gke_ext.image_plan import (
    is_plan_published,
    was_image_planned,
)
from harbor_gke_ext.image_ref import ImageResolver
from harbor_gke_ext.network_policy import (
    apply_network_policy,
    delete_network_policies,
)
from harbor_gke_ext.placement import ComposePlacementMode, PlacementPlan
from harbor_gke_ext.pod_builder import (
    build_direct_pod,
    build_job,
)

_KNOWN_EK_KEYS: frozenset[str] = frozenset(
    inspect.signature(BaseEnvironment.__init__).parameters
) | frozenset(
    {
        "active_deadline_seconds",
        "agent_setup_timeout_multiplier",
        "agent_setup_timeout_sec",
        "agent_timeout_multiplier",
        "agent_timeout_sec",
        "allow_metadata_server",
        "allow_pod_ingress",
        "cloud_build_disk_size_gb",
        "cloud_build_machine_type",
        "cloud_build_timeout_sec",
        "cloud_build_worker_pool",
        "cluster_name",
        "compose_mode",
        "compose_placement",
        "compose_up_timeout_sec",
        "compute_class",
        "cpu_limit_multiplier",
        "deadline_buffer_minutes",
        "decoupled",
        "default_agent_timeout_minutes",
        "default_gpu_count",
        "default_gpu_type",
        "dind_node_pool",
        "dind_storage_mb",
        "dns_egress_extra_cidrs",
        "enable_dind",
        "gpu_override",
        "image_pull_secrets",
        "location",
        "machine_type",
        "max_concurrent_pods",
        "max_storage_request_mb",
        "memory_limit_multiplier",
        "namespace",
        "network_policy_settlement_sec",
        "node_pool",
        "override_entrypoint",
        "pod_ready_timeout",
        "prebuild_fail_on_incomplete",
        "private_pool",
        "project_id",
        "region",
        "registry_location",
        "registry_name",
        "run_id",
        "runtime_class_name",
        "scratch_volume_size",
        "service_account_name",
        "task",
        "task_compute_classes",
        "task_config",
        "task_dind_storage_mb",
        "task_machine_types",
        "task_node_pools",
        "timeout_multiplier",
        "verifier_timeout_multiplier",
        "verifier_timeout_sec",
        "worker_pool",
        "zone",
    }
)

try:
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    _HAS_KUBERNETES = True
except ImportError:
    _HAS_KUBERNETES = False

if TYPE_CHECKING:
    from kubernetes import client as k8s_client

_PATH_KIND_CHECK_TIMEOUT_SEC = 60
_FAILED_CONTAINER_LOG_TAIL_LINES = 80
_INFRA_CONTAINER_LOG_LIMIT_BYTES = 16384
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Provisional default for ResourceMode.AUTO (Option A: capped by default,
# request == limit == declared budget, matching Docker). Switch to
# ResourceMode.REQUEST to restore uncapped-by-default if post-eval gate requires.
_GKE_DEFAULT_RESOURCE_AUTO_MODE: ResourceMode = ResourceMode.GUARANTEE


# Process-wide cluster capabilities and admission control, keyed by
# (project_id, location, cluster_name). Capabilities are probed once per
# process under `_CLUSTER_CAPABILITIES_LOCK`; a failed probe is not cached.
_CLUSTER_CAPABILITIES_CACHE: dict[tuple[str, str, str], ClusterCapabilities] = {}
_CLUSTER_CAPABILITIES_LOCK = threading.Lock()
_CLUSTER_ADMISSION_CONTROLLERS: dict[
    tuple[str, str, str], ClusterAdmissionController
] = {}


# Exec readiness probes before giving up. With `jittered_backoff_delay` the 17
# waits between them average about 290 s in total.
_EXEC_READY_MAX_ATTEMPTS = 18


class GKEEnvironment(ComposeServiceOpsMixin, BaseEnvironment):
    """
    GKE implementation for Harbor sandboxes.

    Supports both Standard GKE and Autopilot clusters.
    """

    _image_build_locks: ClassVar[dict[str, asyncio.Lock]] = {}

    @classmethod
    @override
    def preflight(cls) -> None:
        # Harbor calls this without the job's environment kwargs, so only host
        # prerequisites can be checked here. Cluster identity is resolved per
        # trial, and a missing kubeconfig is not an error: the client fetches
        # credentials with `gcloud container clusters get-credentials`.
        import shutil

        ensure_gcloud_ready()
        if not shutil.which("gke-gcloud-auth-plugin"):
            logger.warning(
                "gke-gcloud-auth-plugin is not found in PATH. Modern GKE clusters (v1.26+) "
                "require this plugin to authenticate. If you experience authentication errors, "
                "install it via 'gcloud components install gke-gcloud-auth-plugin'."
            )

    def __init__(
        self,
        environment_dir: Path,
        environment_name: str,
        session_id: str,
        trial_paths: TrialPaths,
        task_env_config: EnvironmentConfig,
        cluster_name: str | None = None,
        location: str | None = None,
        region: str | None = None,
        zone: str | None = None,
        namespace: str = "default",
        registry_name: str = "harbor-tasks",
        registry_location: str | None = None,
        project_id: str | None = None,
        cpu_limit_multiplier: float | None = None,
        memory_limit_multiplier: float | None = None,
        cloud_build_machine_type: str | None = None,
        cloud_build_disk_size_gb: int | None = None,
        cloud_build_worker_pool: str | None = None,
        private_pool: str | None = None,
        machine_type: str | None = None,
        node_pool: str | None = None,
        pod_ready_timeout: float | None = None,
        service_account_name: str | None = None,
        allow_metadata_server: bool = False,
        allow_pod_ingress: bool = False,
        compose_mode: str = "auto",
        compose_placement: str | None = None,
        gpu_override: str | None = None,
        default_gpu_type: str | None = None,
        compute_class: str | None = None,
        task_compute_classes: str | list[str] | dict[str, str] | None = None,
        task_machine_types: str | list[str] | dict[str, str] | None = None,
        task_node_pools: str | list[str] | dict[str, str] | None = None,
        dind_storage_mb: int | None = None,
        task_dind_storage_mb: str | list[str] | dict[str, str] | None = None,
        image_pull_secrets: str | list[str] | None = None,
        compose_up_timeout_sec: int | None = None,
        max_storage_request_mb: int | None = None,
        **kwargs,
    ):
        if not _HAS_KUBERNETES:
            raise MissingExtraError(package="kubernetes", extra="gke")

        self.kwargs = kwargs
        self._compose_mode = (environment_dir / "docker-compose.yaml").exists()
        self.compose_mode: str = (
            str(kwargs.get("compose_mode", compose_mode)).lower().strip()
        )
        if self.compose_mode not in ("auto", "native", "dind"):
            raise ValueError(
                f"Invalid compose_mode {self.compose_mode!r}. Must be 'auto', 'native', or 'dind'."
            )
        raw_placement = (
            compose_placement
            if compose_placement is not None
            else kwargs.get("compose_placement")
        )
        if raw_placement is not None:
            norm_placement = str(raw_placement).lower().strip()
            if norm_placement == "auto":
                self.compose_placement: ComposePlacementMode = "auto"
            elif norm_placement == "native":
                self.compose_placement = "native"
            elif norm_placement == "dind":
                self.compose_placement = "dind"
            else:
                raise ValueError(
                    f"Invalid compose_placement {raw_placement!r}. Must be 'auto', 'native', or 'dind'."
                )
        elif self.compose_mode == "dind" or _parse_bool(
            kwargs.get("enable_dind"), default=False
        ):
            self.compose_placement = "dind"
        elif self.compose_mode == "native":
            self.compose_placement = "native"
        else:
            self.compose_placement = "auto"

        raw_task_build_timeout = getattr(task_env_config, "build_timeout_sec", None)
        task_build_timeout_sec = (
            int(raw_task_build_timeout)
            if isinstance(raw_task_build_timeout, (int, float))
            else 0
        )
        raw_compose_up_timeout = (
            compose_up_timeout_sec
            if compose_up_timeout_sec is not None
            else kwargs.get("compose_up_timeout_sec")
        )
        self.compose_up_timeout_sec: int = (
            int(raw_compose_up_timeout)
            if raw_compose_up_timeout is not None
            else max(_GKE_DEFAULT_COMPOSE_UP_TIMEOUT_SEC, task_build_timeout_sec // 2)
        )
        raw_allow_ingress = (
            allow_pod_ingress
            if allow_pod_ingress is not False
            else kwargs.get("allow_pod_ingress", False)
        )
        self.allow_pod_ingress: bool = _parse_bool(raw_allow_ingress, default=False)
        self._resolved_image_url: str | None = None
        self._force_build: bool = False
        self._client_manager: KubernetesClientManager | None = None
        self._core_api: k8s_client.CoreV1Api | None = None
        self._networking_api: k8s_client.NetworkingV1Api | None = None
        self._batch_api: k8s_client.BatchV1Api | None = None
        self._custom_api: k8s_client.CustomObjectsApi | None = None
        self._kwargs = kwargs
        raw_max_storage = (
            max_storage_request_mb
            if max_storage_request_mb is not None
            else kwargs.get("max_storage_request_mb")
        )
        self.max_storage_request_mb: int | None = (
            int(raw_max_storage) if raw_max_storage is not None else None
        )
        self.gpu_override: str | None = gpu_override or kwargs.get("gpu_override")
        self.default_gpu_type: str | None = default_gpu_type or kwargs.get(
            "default_gpu_type"
        )
        raw_default_gpu_count = kwargs.get("default_gpu_count")
        self.default_gpu_count: int | None = (
            int(raw_default_gpu_count) if raw_default_gpu_count is not None else None
        )
        raw_scratch_volume_size = kwargs.get("scratch_volume_size")
        self.scratch_volume_size: str | None = (
            str(raw_scratch_volume_size).strip()
            if raw_scratch_volume_size is not None
            and str(raw_scratch_volume_size).strip()
            else None
        )
        raw_dns_extra = kwargs.get("dns_egress_extra_cidrs")
        if isinstance(raw_dns_extra, str):
            self.dns_egress_extra_cidrs: list[str] = [
                c.strip() for c in raw_dns_extra.split(",") if c.strip()
            ]
        elif isinstance(raw_dns_extra, (list, tuple)):
            self.dns_egress_extra_cidrs = [
                str(c).strip() for c in raw_dns_extra if str(c).strip()
            ]
        else:
            self.dns_egress_extra_cidrs = []
        self.compute_class: str | None = compute_class or kwargs.get("compute_class")
        self.task_compute_classes: dict[str, str] = self._parse_task_compute_classes(
            task_compute_classes
            if task_compute_classes is not None
            else kwargs.get("task_compute_classes")
        )
        self._compose_spec_args: dict[str, Any] | None = None
        self.pod: k8s_client.V1Pod | None = None
        self._created_pod: k8s_client.V1Pod | None = None
        # Logical Compose services that run inside `dind-engine` rather than as
        # native Pod containers, so exec/file-transfer must reach them through
        # `docker exec <service>` on `main`. Populated from the translated Pod
        # spec in `_sync_dind_container_routing`.
        self._dind_services: frozenset[str] = frozenset()
        self._pod_ready: bool = False
        self._seed_uploaded: bool = False
        self._dind_netpol_applied: bool = False
        self._main_gate_released: bool = False

        # GKE cluster identity initialized before super().__init__() so preflight
        # capability checks (e.g. FQDNNetworkPolicy probe) target the right cluster.
        self.project_id, self.location, self.cluster_name = resolve_gke_target(
            project_id=project_id,
            location=location,
            region=region,
            zone=zone,
            cluster_name=cluster_name,
        )

        self.region = derive_region(self.location)
        self.registry_location = (
            derive_region(registry_location) if registry_location else self.region
        )
        self.registry_name = registry_name
        # Needed by the cluster capability probe, which Harbor may trigger from
        # `super().__init__()` by reading `capabilities`.
        self.namespace = namespace

        super().__init__(
            environment_dir=environment_dir,
            environment_name=environment_name,
            session_id=session_id,
            trial_paths=trial_paths,
            task_env_config=task_env_config,
            **kwargs,
        )

        _ensure_file_descriptor_limit()

        self._compose_mode = self._compose_mode or bool(self.extra_docker_compose_paths)
        self._validate_gke_accelerator_config()
        parsed_secrets = self._parse_image_pull_secrets(
            image_pull_secrets
            if image_pull_secrets is not None
            else kwargs.get("image_pull_secrets")
        )
        self.image_pull_secrets: list[str] | None = (
            parsed_secrets if parsed_secrets else None
        )
        self.cloud_build_timeout_sec: int = int(
            kwargs.get("cloud_build_timeout_sec", 10800)
        )
        self.machine_type: str | None = machine_type or kwargs.get("machine_type")
        self.task_machine_types: dict[str, str] = self._parse_task_mapping(
            task_machine_types
            if task_machine_types is not None
            else kwargs.get("task_machine_types")
        )
        self.node_pool: str | None = node_pool or kwargs.get("node_pool")
        self.dind_node_pool: str | None = kwargs.get("dind_node_pool")
        self.task_node_pools: dict[str, str] = self._parse_task_mapping(
            task_node_pools
            if task_node_pools is not None
            else kwargs.get("task_node_pools")
        )
        raw_dind_storage = (
            dind_storage_mb
            if dind_storage_mb is not None
            else kwargs.get("dind_storage_mb")
        )
        self.dind_storage_mb: int | None = (
            int(raw_dind_storage) if raw_dind_storage is not None else None
        )
        self.task_dind_storage_mb: dict[str, str] = self._parse_task_mapping(
            task_dind_storage_mb
            if task_dind_storage_mb is not None
            else kwargs.get("task_dind_storage_mb")
        )
        # The Compose placement the Pod is built from. `start()` resolves it
        # (that runs `docker compose config`, which never happens from this
        # constructor). See `_compose_needs_dind`.
        self._compose_placement: PlacementPlan | None = None
        # Resolved once here because _resolve_active_compute_class() consults it:
        # an explicit node pool and a ComputeClass produce mutually exclusive node
        # selectors, so the node pool suppresses the ComputeClass.
        self._active_node_pool: str | None = self._resolve_active_node_pool()
        self._active_machine_type: str | None = self._resolve_active_machine_type()
        raw_ready_timeout = (
            pod_ready_timeout
            if pod_ready_timeout is not None
            else kwargs.get("pod_ready_timeout")
        )
        self.pod_ready_timeout: int = (
            int(raw_ready_timeout)
            if raw_ready_timeout is not None
            else max(1200, task_build_timeout_sec)
        )
        raw_deadline_buffer = kwargs.get("deadline_buffer_minutes")
        self.deadline_buffer_minutes: int = (
            int(raw_deadline_buffer)
            if raw_deadline_buffer is not None
            else _GKE_DEFAULT_DEADLINE_BUFFER_MINUTES
        )
        raw_default_agent_timeout = kwargs.get("default_agent_timeout_minutes")
        self.default_agent_timeout_minutes: int = (
            int(raw_default_agent_timeout)
            if raw_default_agent_timeout is not None
            else _GKE_DEFAULT_AGENT_TIMEOUT_MINUTES
        )
        raw_active_deadline = kwargs.get("active_deadline_seconds")
        self.active_deadline_seconds: int | None = (
            int(raw_active_deadline) if raw_active_deadline is not None else None
        )
        self.run_id: str | None = kwargs.get("run_id")
        self.service_account_name: str | None = (
            service_account_name
            if service_account_name is not None
            else kwargs.get("service_account_name")
        )
        raw_allow_meta = (
            allow_metadata_server
            if allow_metadata_server is not False
            else kwargs.get("allow_metadata_server", False)
        )
        self.allow_metadata_server: bool = _parse_bool(raw_allow_meta, default=False)

        # Decoupled mode flag (--ek decoupled=true)
        self.decoupled: bool = _parse_bool(kwargs.get("decoupled"), default=False)

        # Resource configuration from task_env_config
        cpu_request = self._resource_request_value(
            "cpu", auto_mode=_GKE_DEFAULT_RESOURCE_AUTO_MODE
        )
        cpu_limit = self._resource_limit_value(
            "cpu", auto_mode=_GKE_DEFAULT_RESOURCE_AUTO_MODE
        )
        memory_request = self._resource_request_value(
            "memory", auto_mode=_GKE_DEFAULT_RESOURCE_AUTO_MODE
        )
        memory_limit = self._resource_limit_value(
            "memory", auto_mode=_GKE_DEFAULT_RESOURCE_AUTO_MODE
        )
        self.cpu_request = str(cpu_request) if cpu_request is not None else None
        self.memory_request = (
            f"{memory_request}Mi" if memory_request is not None else None
        )
        self.ephemeral_storage_request = (
            f"{storage_mb}Mi" if (storage_mb := self._effective_storage_mb) else None
        )

        raw_cpu_mult = (
            cpu_limit_multiplier
            if cpu_limit_multiplier is not None
            else kwargs.get("cpu_limit_multiplier")
        )
        resolved_cpu_mult = float(raw_cpu_mult) if raw_cpu_mult is not None else None
        if (
            self._cpu_resource_mode == ResourceMode.AUTO
            and cpu_request is not None
            and resolved_cpu_mult is not None
            and resolved_cpu_mult > 0
        ):
            limit_cpu_m = int(cpu_request * resolved_cpu_mult * 1000)
            self.cpu_limit = f"{limit_cpu_m}m"
        elif cpu_limit is not None:
            self.cpu_limit = str(cpu_limit)
        else:
            self.cpu_limit = None

        raw_mem_mult = (
            memory_limit_multiplier
            if memory_limit_multiplier is not None
            else kwargs.get("memory_limit_multiplier")
        )
        resolved_mem_mult = float(raw_mem_mult) if raw_mem_mult is not None else None
        if (
            self._memory_resource_mode == ResourceMode.AUTO
            and memory_request is not None
            and resolved_mem_mult is not None
            and resolved_mem_mult > 0
        ):
            limit_memory_mb = int(memory_request * resolved_mem_mult)
            self.memory_limit = f"{limit_memory_mb}Mi"
        elif memory_limit is not None:
            self.memory_limit = f"{memory_limit}Mi"
        else:
            self.memory_limit = None

        resolved_worker_pool = (
            cloud_build_worker_pool
            or private_pool
            or kwargs.get("cloud_build_worker_pool")
            or kwargs.get("private_pool")
            or kwargs.get("worker_pool")
        )
        if resolved_worker_pool and cloud_build_machine_type:
            raise ValueError(
                f"Cannot specify cloud_build_machine_type ({cloud_build_machine_type!r}) "
                f"when a private worker_pool ({resolved_worker_pool!r}) is used. "
                "Private pools manage machine sizing within the pool configuration."
            )
        self.cloud_build_worker_pool = resolved_worker_pool
        self.cloud_build_machine_type = cloud_build_machine_type
        self.cloud_build_disk_size_gb = cloud_build_disk_size_gb
        self.job_name = _sanitize_kubernetes_resource_name(session_id)
        self.pod_name = self.job_name
        # UID of the Job this environment created. The name is derived from the
        # session id and is therefore reused across trial retries, so a
        # predecessor's terminating Pods still carry ``job-name=<same name>``;
        # only ``batch.kubernetes.io/controller-uid`` identifies our Job's Pods.
        self._job_uid: str | None = None

        self._active_strategy: str | None = "native" if self._compose_mode else None
        # Depends on Autopilot mode, so `start()` resolves it from the cluster
        # capabilities.
        self._active_compute_class: str | None = None
        raw_max_pods = kwargs.get("max_concurrent_pods")
        self.max_concurrent_pods: int | None = (
            int(raw_max_pods) if raw_max_pods is not None else None
        )
        self._admission_token: tuple[float, bool] | None = None
        self._applied_network_mode: NetworkMode | None = None
        self._image_resolver: ImageResolver = ImageResolver(
            project_id=self.project_id,
            registry_name=self.registry_name,
            registry_location=self.registry_location,
        )

        unknown_kwargs = sorted(
            k for k in kwargs if k not in _KNOWN_EK_KEYS and not k.startswith("_")
        )
        if unknown_kwargs:
            self.logger.warning(
                "Unrecognized GKE environment kwargs (--ek) ignored or forwarded to base: %s",
                ", ".join(unknown_kwargs),
            )

    @property
    def _api(self) -> k8s_client.CoreV1Api:
        """Return the Kubernetes API client, raising if not initialized."""
        if self._core_api is None:
            raise RuntimeError(
                "Kubernetes client not initialized. Call _ensure_client() first."
            )
        return self._core_api

    async def _ensure_client(self):
        """Ensure Kubernetes client is initialized via the singleton manager."""
        if self._client_manager is None:
            self._client_manager = KubernetesClientManager.get_instance()
        if self._core_api is None:
            self._core_api = await self._client_manager.get_client(
                self.cluster_name, self.location, self.project_id
            )
        api_client = getattr(self._core_api, "api_client", None)
        if self._batch_api is None and api_client is not None:
            self._batch_api = k8s_client.BatchV1Api(api_client)
        if self._networking_api is None and api_client is not None:
            self._networking_api = k8s_client.NetworkingV1Api(api_client)
        if self._custom_api is None and api_client is not None:
            self._custom_api = k8s_client.CustomObjectsApi(api_client)

    @property
    def _cluster_key(self) -> tuple[str, str, str]:
        return (self.project_id, self.location, self.cluster_name)

    @staticmethod
    @override
    def type() -> str:
        return "gke"

    @classmethod
    @override
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(
            cpu_limit=True,
            cpu_request=True,
            memory_limit=True,
            memory_request=True,
        )

    @property
    @override
    def capabilities(self) -> EnvironmentCapabilities:
        # Probes the cluster on first use (Harbor may read this from the
        # constructor) and raises if the probe fails.
        caps = self._cluster_capabilities()
        has_netpol = caps.network_policy_enforced is True
        has_fqdn = has_netpol and caps.fqdn_network_policy_supported
        return EnvironmentCapabilities(
            gpus=True,
            tpus=True,
            disable_internet=has_netpol,
            dynamic_network_policy=has_netpol,
            network_allowlist=has_netpol,
            network_allowlist_ipv4_cidrs=has_netpol,
            network_allowlist_ipv4_addresses=has_netpol,
            network_allowlist_hostnames=has_fqdn,
            network_allowlist_wildcard_hostnames=has_fqdn,
            docker_compose=True,
        )

    @property
    @override
    def _uses_compose(self) -> bool:
        return self._compose_mode

    @property
    def _environment_docker_compose_path(self) -> Path:
        return self.environment_dir / "docker-compose.yaml"

    @property
    def _all_compose_paths(self) -> list[Path]:
        paths: list[Path] = []
        if self._environment_docker_compose_path.exists():
            paths.append(self._environment_docker_compose_path)
        for p in self.extra_docker_compose_paths:
            if p.exists() and p not in paths:
                paths.append(p)
        return paths

    @override
    def _validate_definition(self):
        require_agent_environment_definition(
            self.environment_dir,
            docker_image=self.task_env_config.docker_image,
            extra_docker_compose_paths=self.extra_docker_compose_paths,
        )

    @property
    def _effective_gpu_types(self) -> list[str] | None:
        if self._effective_gpus <= 0:
            return None
        if self.gpu_override:
            return [self.gpu_override]
        if self.task_env_config.gpu_types:
            return self.task_env_config.gpu_types
        if self.default_gpu_type:
            return [resolve_gpu_accelerator_label(self.default_gpu_type)]
        return None

    def _validate_gke_accelerator_config(self):
        tpu = self.task_env_config.tpu
        if self._effective_gpus > 0 and tpu is not None:
            raise RuntimeError(
                "GKE pods can only target one accelerator family per pod "
                "via nodeSelector, but the task requests both GPU and TPU."
            )
        if self.gpu_override:
            raw_override = self.gpu_override.lower().strip()
            if (
                raw_override in GKE_GPU_TYPE_MAP
                and raw_override not in GKE_GPU_TYPE_MAP.values()
            ):
                suggested = GKE_GPU_TYPE_MAP[raw_override]
                raise RuntimeError(
                    f"gpu_override must be specified as the full GKE accelerator type "
                    f"(e.g. '{suggested}'), not short name '{self.gpu_override}'."
                )
            resolve_gpu_accelerator_label(self.gpu_override)
        elif self._effective_gpus > 0 and self.task_env_config.gpu_types:
            resolve_gpu_accelerator_label(self.task_env_config.gpu_types[0])

        if self.default_gpu_type:
            resolve_gpu_accelerator_label(self.default_gpu_type)

        if tpu is not None:
            resolve_tpu_accelerator_label(tpu.type)

    def _get_task_artifact_registry_url(self) -> str:
        return resolve_task_image_url(
            digest=self.environment_id,
            project_id=self.project_id,
            registry_name=self.registry_name,
            registry_location=self.registry_location,
            image_name="task",
        )

    def _get_image_url(self) -> str:
        if self._resolved_image_url:
            return self._resolved_image_url
        if self.task_env_config.docker_image and should_use_prebuilt_docker_image(
            self.environment_dir,
            docker_image=self.task_env_config.docker_image,
            force_build=self._force_build,
        ):
            self._resolved_image_url = self.task_env_config.docker_image
            return self._resolved_image_url
        self._resolved_image_url = self._get_task_artifact_registry_url()
        return self._resolved_image_url

    async def _image_exists(self) -> bool:
        if self.task_env_config.docker_image and should_use_prebuilt_docker_image(
            self.environment_dir,
            docker_image=self.task_env_config.docker_image,
            force_build=self._force_build,
        ):
            self._resolved_image_url = self.task_env_config.docker_image
            return True
        for url in (
            self._get_task_artifact_registry_url(),
            resolve_task_image_url(
                digest=self.environment_id,
                project_id=self.project_id,
                registry_name=self.registry_name,
                registry_location=self.registry_location,
                image_name=self.environment_name,
            ),
        ):
            if await check_image_exists_in_registry(url, project_id=self.project_id):
                self._resolved_image_url = url
                return True

        return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        reraise=True,
    )
    async def _build_and_push_image(
        self,
        context_dir: Path | None = None,
        target_image_url: str | None = None,
        dockerfile: str | None = None,
    ):
        shared_image_url = target_image_url or self._get_task_artifact_registry_url()
        build_context = context_dir or self.environment_dir
        if is_plan_published() and not was_image_planned(shared_image_url):
            self.logger.warning(
                "Inline Cloud Build for an image the pre-builder did not plan: "
                f"{shared_image_url} from {build_context}. This build is not "
                "concurrency-gated by the plugin; the planner and the runtime "
                "have diverged for this build context."
            )
        self.logger.debug(
            f"Building and pushing image: {shared_image_url} from {build_context}"
        )

        await submit_cloud_build(
            build_context=build_context,
            image_url=shared_image_url,
            project_id=self.project_id,
            region=self.region,
            timeout_sec=self.cloud_build_timeout_sec,
            dockerfile=dockerfile,
            machine_type=self.cloud_build_machine_type,
            disk_size_gb=self.cloud_build_disk_size_gb,
            worker_pool=self.cloud_build_worker_pool,
            reraise=True,
            logger_instance=self.logger,
        )

    def _get_sidecar_image_url(
        self, context_dir: Path, dockerfile: str | None = None
    ) -> str:
        from harbor_gke_ext.image_plan import compute_context_digest

        content_hash = compute_context_digest(context_dir, dockerfile=dockerfile)
        return resolve_task_image_url(
            digest=content_hash,
            project_id=self.project_id,
            registry_name=self.registry_name,
            registry_location=self.registry_location,
            image_name="task",
        )

    async def _get_cluster_capabilities(self) -> ClusterCapabilities:
        caps = _CLUSTER_CAPABILITIES_CACHE.get(self._cluster_key)
        if caps is not None:
            return caps
        return await asyncio.to_thread(self._cluster_capabilities)

    def _cluster_capabilities(self) -> ClusterCapabilities:
        """Return the cluster's capabilities, probing once per process.

        Synchronous because Harbor reads ``capabilities`` from the environment
        constructor. Blocks while the first probe runs; any failure raises and
        is not cached, so a later call probes again.
        """
        key = self._cluster_key
        caps = _CLUSTER_CAPABILITIES_CACHE.get(key)
        if caps is not None:
            return caps
        with _CLUSTER_CAPABILITIES_LOCK:
            caps = _CLUSTER_CAPABILITIES_CACHE.get(key)
            if caps is None:
                caps = self._probe_cluster_capabilities()
                _CLUSTER_CAPABILITIES_CACHE[key] = caps
        return caps

    def _probe_cluster_capabilities(self) -> ClusterCapabilities:
        base = probe_cluster_via_gcloud(
            self.cluster_name, self.project_id, self.location
        )
        manager = KubernetesClientManager.get_instance()
        with manager.scoped_client(
            self.cluster_name, self.location, self.project_id
        ) as core_api:
            # Pod-level `spec.resources` is the whole basis of Harbor's
            # task-budget model on GKE. On a control plane that prunes it, the
            # field vanishes with no error and the model evaporates in silence.
            supports_pod_level = probe_pod_level_resources_support(
                core_api, namespace=self.namespace
            )
            kube_dns_ip = probe_kube_dns_cluster_ip(core_api)
            fqdn_supported = probe_fqdn_network_policy_support(core_api)

        if not supports_pod_level:
            self.logger.warning(
                "Cluster %r does not retain Pod-level `spec.resources` "
                "(Kubernetes 1.34+ with the PodLevelResources feature gate is "
                "required). Trials whose budget is Pod-level (compose Pods, and "
                "direct Pods unless run with `--cpus guarantee --memory "
                "guarantee`) will run with no ceiling on the Pod and no "
                "ceiling on any container, so a task that exceeds its declared "
                "budget is bounded only by the node -- and the resulting "
                "kubelet eviction may fall on a different trial's Pod rather "
                "than the one at fault.",
                self.cluster_name,
            )
        return replace(
            base,
            supports_pod_level_resources=supports_pod_level,
            kube_dns_cluster_ip=kube_dns_ip,
            fqdn_network_policy_supported=fqdn_supported,
        )

    def _verify_network_enforcement(self, caps: ClusterCapabilities) -> None:
        """Fail closed if NetworkPolicy or metadata server blocking is required on an unenforced cluster."""
        if (
            self.network_policy.network_mode == NetworkMode.PUBLIC
            and self.allow_metadata_server
        ):
            return
        if caps.network_policy_enforced is not True:
            raise RuntimeError(
                f"Cannot start trial for task {self.environment_name!r} on GKE cluster "
                f"{self.cluster_name!r}: active Kubernetes NetworkPolicy enforcement is required "
                f"(network_mode={self.network_policy.network_mode.value!r}, "
                f"allow_metadata_server={self.allow_metadata_server}), but the cluster has neither "
                "GKE Dataplane V2 (`--enable-dataplane-v2`) nor Calico (`--enable-network-policy`) "
                "enabled. Without an active CNI enforcer, Kubernetes accepts NetworkPolicy resources "
                "without filtering any traffic, leaving the GCE/GKE metadata server "
                "(169.254.169.254 / 169.254.169.252) and egress unrestricted. "
                "If this is a public-network task and you explicitly accept unisolated metadata "
                "server access on this cluster, pass `--ek allow_metadata_server=true`."
            )

    @override
    async def start(self, force_build: bool):
        """Start a pod in GKE."""
        self._force_build = force_build
        await self._ensure_client()
        # Capabilities are cached per cluster, so this costs one probe per process.
        caps = await self._get_cluster_capabilities()
        is_autopilot = caps.is_autopilot
        self._verify_network_enforcement(caps)
        # Refuse contradictory placement settings before anything is created.
        self._validate_placement(caps)

        # The ComputeClass (storage sizing), the node pool and the NetworkPolicy
        # handshake below all depend on whether the Pod gets an in-Pod Docker
        # daemon. They read the placement resolved here from exactly the
        # arguments `translate_compose` receives, so they describe the Pod that
        # is built. A project that cannot run fails here, as translation would.
        compose_placement_args: dict[str, Any] = {}
        if self._compose_mode:
            compose_placement_args = {
                "compose_path": self._all_compose_paths,
                "compose_env": resolve_compose_infra_env(
                    self, use_prebuilt=bool(self.task_env_config.docker_image)
                ),
                "task_dir": getattr(self, "task_dir", None)
                or self.environment_dir.parent,
                "compose_placement": self.compose_placement,
                "is_autopilot": is_autopilot,
                "cluster_capabilities": caps,
                "effective_gpus": self._effective_gpus,
                "gpu_types": self._effective_gpu_types,
                "gpu_override": self.gpu_override,
                "default_gpu_type": self.default_gpu_type,
                "default_gpu_count": self.default_gpu_count,
                "logger": self.logger,
            }
            resolved = await asyncio.to_thread(
                resolve_compose_placement, **compose_placement_args
            )
            self._compose_placement = resolved.plan

        self._active_compute_class = self._resolve_active_compute_class(
            is_autopilot=is_autopilot
        )
        if self._active_compute_class:
            self.logger.debug(
                f"Assigning ComputeClass '{self._active_compute_class}' to pod {self.pod_name}"
            )

        # A `nodeSelector` no node matches does not fail -- it pends silently until
        # the trial's clock runs out. Fail fast when the cluster provably cannot
        # satisfy the machine type pin.
        self._assert_machine_type_satisfiable(caps)

        if self._compose_mode:
            await self._ensure_client()
            compose_paths = compose_placement_args["compose_path"]
            compose_env = compose_placement_args["compose_env"]
            self._active_strategy = "native"

            sidecar_builds = discover_compose_build_services(
                compose_paths,
                compose_env,
            )
            sidecar_images: dict[str, str] = {}
            for sname, (s_context, s_dockerfile) in sidecar_builds.items():
                sidecar_images[sname] = self._get_sidecar_image_url(
                    s_context, dockerfile=s_dockerfile
                )

            use_prebuilt = should_use_prebuilt_docker_image(
                self.environment_dir,
                docker_image=self.task_env_config.docker_image,
                force_build=force_build,
            )

            main_image_url = (
                self.task_env_config.docker_image
                if use_prebuilt and self.task_env_config.docker_image
                else self._get_task_artifact_registry_url()
            )

            async def _ensure_main_image() -> None:
                if use_prebuilt:
                    await self._image_exists()
                    return
                lock = self._image_build_locks.setdefault(
                    main_image_url, asyncio.Lock()
                )
                async with lock:
                    if force_build or not await self._image_exists():
                        self.logger.debug(
                            f"Image {main_image_url} not found, building..."
                        )
                        await self._build_and_push_image(
                            target_image_url=main_image_url
                        )
                        self._resolved_image_url = main_image_url
                    else:
                        self.logger.debug(
                            f"Using existing image: {self._get_image_url()}"
                        )

            async def _ensure_sidecar_image(
                url: str, context: Path, dockerfile: str | None
            ) -> None:
                lock = self._image_build_locks.setdefault(url, asyncio.Lock())
                async with lock:
                    if force_build or not await check_image_exists_in_registry(
                        url, project_id=self.project_id
                    ):
                        self.logger.debug(
                            f"Sidecar image {url} not found, building from {context}..."
                        )
                        await self._build_and_push_image(
                            context_dir=context,
                            target_image_url=url,
                            dockerfile=dockerfile,
                        )
                    else:
                        self.logger.debug(f"Using existing sidecar image: {url}")

            async with asyncio.TaskGroup() as tg:
                tg.create_task(_ensure_main_image())
                for sname, (s_context, s_dockerfile) in sidecar_builds.items():
                    tg.create_task(
                        _ensure_sidecar_image(
                            sidecar_images[sname], s_context, s_dockerfile
                        )
                    )

            needs_netpol = (
                self.network_policy.network_mode != NetworkMode.PUBLIC
                or not self.allow_metadata_server
            )
            effective_compose_node_pool = self._resolve_active_node_pool()
            self._compose_spec_args = {
                # The placement inputs, exactly as resolved above.
                **compose_placement_args,
                "pod_name": self.pod_name,
                "namespace": self.namespace,
                "environment_name": self.environment_name,
                "run_id": self.run_id,
                "main_image_url": self._get_image_url(),
                "sidecar_images": sidecar_images,
                "sidecar_image_urls": sidecar_images,
                "startup_env": self._startup_env(),
                "cpu_request": self.cpu_request,
                "cpu_limit": self.cpu_limit,
                "memory_request": self.memory_request,
                "memory_limit": self.memory_limit,
                "ephemeral_storage_request": self.ephemeral_storage_request,
                "scratch_volume_size": self.scratch_volume_size,
                "dind_storage_mb": self._resolve_dind_storage_mb(),
                "compose_up_timeout_sec": self.compose_up_timeout_sec,
                "machine_type": self._active_machine_type,
                "node_pool": effective_compose_node_pool,
                "tpu": self.task_env_config.tpu,
                "main_workdir": self.task_env_config.workdir
                if self.task_env_config
                else None,
                "service_account_name": self.service_account_name,
                "image_pull_secrets": self.image_pull_secrets,
                "runtime_class_name": self.kwargs.get("runtime_class_name"),
                "image_resolver": self._image_resolver,
                "allow_metadata_server": self.allow_metadata_server,
                "wait_for_netpol": bool(needs_netpol and self._compose_needs_dind()),
            }
            pod = self._build_compose_pod()
            self._created_pod = pod
            self._sync_dind_container_routing(pod)
            self._warn_if_unfenced_privileged_dind(is_autopilot=is_autopilot)

            if needs_netpol and not self._dind_services:
                await self._apply_network_policy(self.network_policy)
                await asyncio.sleep(1.0)
            await self._create_pod(pod)

            await self._wait_for_pod_ready(timeout_sec=self.pod_ready_timeout)
            if needs_netpol and self._dind_services and not self._dind_netpol_applied:
                await self._apply_network_policy(self.network_policy)
                self._dind_netpol_applied = True
            await self._wait_for_container_exec_ready(container=MAIN_SERVICE_NAME)

            mkdir_result = await self.ensure_dirs(
                self._mount_targets(writable_only=True)
            )
            if mkdir_result is not None and mkdir_result.return_code != 0:
                raise RuntimeError(
                    f"Failed to create mounted directories in pod {self.pod_name}: "
                    f"stdout={mkdir_result.stdout}, stderr={mkdir_result.stderr}"
                )

            await self._upload_environment_dir_after_start()
            return

        await self._ensure_client()

        use_prebuilt = should_use_prebuilt_docker_image(
            self.environment_dir,
            docker_image=self.task_env_config.docker_image,
            force_build=force_build,
        )

        image_url = (
            self.task_env_config.docker_image
            if use_prebuilt and self.task_env_config.docker_image
            else self._get_task_artifact_registry_url()
        )
        if use_prebuilt:
            await self._image_exists()
        else:
            lock = self._image_build_locks.setdefault(image_url, asyncio.Lock())
            async with lock:
                if force_build or not await self._image_exists():
                    self.logger.debug(f"Image {image_url} not found, building...")
                    await self._build_and_push_image(target_image_url=image_url)
                    self._resolved_image_url = image_url
                else:
                    self.logger.debug(f"Using existing image: {self._get_image_url()}")

        pod = self._build_direct_pod()
        self._created_pod = pod

        if (
            self.network_policy.network_mode != NetworkMode.PUBLIC
            or not self.allow_metadata_server
        ):
            await self._apply_network_policy(self.network_policy)
        await self._create_pod(pod)

        await self._wait_for_pod_ready(timeout_sec=self.pod_ready_timeout)
        await self._wait_for_container_exec_ready()

        mkdir_result = await self.ensure_dirs(self._mount_targets(writable_only=True))
        if mkdir_result is not None and mkdir_result.return_code != 0:
            raise RuntimeError(
                f"Failed to create mounted directories in pod {self.pod_name}: "
                f"stdout={mkdir_result.stdout}, stderr={mkdir_result.stderr}"
            )

        await self._upload_environment_dir_after_start()

    @override
    async def _upload_environment_dir_after_start(self) -> None:
        """Upload task environment/ and any large compose bind-mounts after Pod start."""
        await super()._upload_environment_dir_after_start()
        pod_obj = self._created_pod or self.pod
        if not pod_obj or not pod_obj.metadata or not pod_obj.metadata.annotations:
            return
        # If harbor-seed initContainer already unpacked bind mounts, skip re-uploading
        if pod_obj.spec and any(
            c.name == "harbor-seed" for c in (pod_obj.spec.init_containers or [])
        ):
            return
        bind_mounts_raw = pod_obj.metadata.annotations.get(
            "harbor.dev/compose-bind-mounts"
        )
        if not bind_mounts_raw:
            return
        try:
            bind_mounts = json.loads(bind_mounts_raw)
        except Exception as exc:
            self.logger.warning(
                f"Failed to parse compose-bind-mounts annotation: {exc}"
            )
            return
        for rel_src, target_info in bind_mounts.items():
            if isinstance(target_info, dict):
                container_name = target_info.get("container")
                target_dir = target_info.get("target_dir", "")
            else:
                container_name = None
                target_dir = str(target_info)
            if not target_dir:
                continue
            local_path = (self.environment_dir / rel_src).resolve()
            if not local_path.exists():
                continue
            if local_path.is_file():
                target_file = f"{target_dir.rstrip('/')}/{local_path.name}"
                self.logger.debug(
                    f"Uploading compose bind-mount file {local_path} -> {target_file} (container={container_name})"
                )
                await self.upload_file(
                    local_path, target_file, container=container_name
                )
            elif local_path.is_dir():
                self.logger.debug(
                    f"Uploading compose bind-mount directory {local_path} -> {target_dir} (container={container_name})"
                )
                await self.upload_dir(local_path, target_dir, container=container_name)

    def _resolve_active_deadline_seconds(self) -> int | None:
        """Calculate the Kubernetes spec.activeDeadlineSeconds for the Pod.

        If active_deadline_seconds was explicitly passed, returns it directly.
        Otherwise, calculates the deadline dynamically based on task and environment configuration:
          - Setup budget: 360s (Agent setup allocation)
          - Agent timeout: task.agent.timeout_sec or agent override (default 1440m)
          - Verifier timeout: task.verifier.timeout_sec or 600s (if verifier runs in this pod)
          - Multi-step: sums agent and verifier budgets across all steps
          - Buffer: deadline_buffer_minutes * 60 (default 15 minutes = 900s)
        """
        if self.active_deadline_seconds is not None:
            return self.active_deadline_seconds

        # 1. Resolve task data from kwargs, task wrapper, or disk
        task_data: dict[str, Any] = {}
        task_config_obj = self._kwargs.get("task_config")
        if task_config_obj is None:
            task_obj = self._kwargs.get("task")
            if task_obj is not None:
                task_config_obj = getattr(task_obj, "config", task_obj)

        if isinstance(task_config_obj, dict):
            task_data = task_config_obj
        elif task_config_obj is not None and hasattr(task_config_obj, "model_dump"):
            task_data = task_config_obj.model_dump()
        else:
            candidate_paths = [
                self.environment_dir / "task.toml",
                self.environment_dir.parent / "task.toml",
            ]
            for candidate in candidate_paths:
                if candidate.is_file():
                    try:
                        task_data = tomllib.loads(candidate.read_text())
                        break
                    except Exception:
                        pass

        # 2. Resolve runtime trial configuration (multipliers & overrides)
        trial_data: dict[str, Any] = {}
        trial_paths_obj = getattr(self, "trial_paths", None)
        if (
            trial_paths_obj
            and hasattr(trial_paths_obj, "config_path")
            and trial_paths_obj.config_path.is_file()
        ):
            try:
                trial_data = json.loads(trial_paths_obj.config_path.read_text())
            except Exception:
                pass

        timeout_mult = float(
            self._kwargs.get("timeout_multiplier")
            or trial_data.get("timeout_multiplier")
            or 1.0
        )
        agent_mult = float(
            self._kwargs.get("agent_timeout_multiplier")
            or trial_data.get("agent_timeout_multiplier")
            or timeout_mult
        )
        verifier_mult = float(
            self._kwargs.get("verifier_timeout_multiplier")
            or trial_data.get("verifier_timeout_multiplier")
            or timeout_mult
        )
        setup_mult = float(
            self._kwargs.get("agent_setup_timeout_multiplier")
            or trial_data.get("agent_setup_timeout_multiplier")
            or timeout_mult
        )

        trial_agent = trial_data.get("agent") or {}
        trial_verifier = trial_data.get("verifier") or {}
        agent_override_sec = self._kwargs.get("agent_timeout_sec") or trial_agent.get(
            "override_timeout_sec"
        )
        verifier_override_sec = self._kwargs.get(
            "verifier_timeout_sec"
        ) or trial_verifier.get("override_timeout_sec")
        setup_override_sec = self._kwargs.get(
            "agent_setup_timeout_sec"
        ) or trial_agent.get("override_setup_timeout_sec")

        agent_max_sec = trial_agent.get("max_timeout_sec")
        verifier_max_sec = trial_verifier.get("max_timeout_sec")

        def _resolve_timeout_budget(
            base_sec: float, multiplier: float, max_sec: float | None = None
        ) -> int:
            capped_base = min(base_sec, max_sec) if max_sec is not None else base_sec
            return int(capped_base * multiplier)

        # 3. Check if this is a dedicated separate verifier pod
        is_verifier_pod = "__verifier__" in self.session_id

        # 4. Check whether verifier runs in this pod (shared mode)
        verifier_section = task_data.get("verifier") or {}
        verifier_env = verifier_section.get("environment")
        verifier_env_mode = verifier_section.get("environment_mode")
        is_separate_verifier = verifier_env_mode == "separate" or (
            verifier_env is not None and verifier_env_mode != "shared"
        )

        steps = task_data.get("steps")
        total_setup_sec = 0
        total_agent_sec = 0
        total_verifier_sec = 0

        default_verifier_sec = _resolve_timeout_budget(
            base_sec=_GKE_DEFAULT_VERIFIER_TIMEOUT_SEC,
            multiplier=verifier_mult,
            max_sec=verifier_max_sec,
        )
        setup_budget_sec = _resolve_timeout_budget(
            base_sec=float(setup_override_sec)
            if setup_override_sec is not None
            else _GKE_DEFAULT_AGENT_SETUP_TIMEOUT_SEC,
            multiplier=setup_mult,
        )

        step_list = steps if isinstance(steps, list) and steps else [task_data]
        for step in step_list:
            total_setup_sec += setup_budget_sec
            if not is_verifier_pod:
                step_agent = (step.get("agent") if isinstance(step, dict) else {}) or {}
                a_sec = (
                    agent_override_sec
                    if agent_override_sec is not None
                    else step_agent.get("timeout_sec")
                )
                if a_sec is None:
                    a_sec = self.default_agent_timeout_minutes * 60
                total_agent_sec += _resolve_timeout_budget(
                    float(a_sec), agent_mult, agent_max_sec
                )
            if is_verifier_pod or not is_separate_verifier:
                step_verifier = (
                    step.get("verifier") if isinstance(step, dict) else {}
                ) or {}
                v_sec = (
                    verifier_override_sec
                    if verifier_override_sec is not None
                    else step_verifier.get("timeout_sec")
                )
                total_verifier_sec += (
                    _resolve_timeout_budget(
                        float(v_sec), verifier_mult, verifier_max_sec
                    )
                    if v_sec is not None
                    else default_verifier_sec
                )

        buffer_sec = self.deadline_buffer_minutes * 60
        total_deadline_sec = (
            total_setup_sec + total_agent_sec + total_verifier_sec + buffer_sec
        )

        return max(total_deadline_sec, 60)

    def _build_compose_pod(self) -> k8s_client.V1Pod:
        if self._compose_spec_args is None:
            raise RuntimeError("Compose spec arguments not initialized")
        args = dict(self._compose_spec_args)
        args["compute_class"] = self._active_compute_class
        args["active_deadline_seconds"] = self._resolve_active_deadline_seconds()

        override_val = self.kwargs.get("override_entrypoint")
        args["override_entrypoint"] = _parse_bool(override_val, default=False)

        return translate_compose(**args)

    def _build_direct_pod(self) -> k8s_client.V1Pod:
        return build_direct_pod(
            pod_name=self.pod_name,
            namespace=self.namespace,
            environment_name=self.environment_name,
            run_id=self.run_id,
            image_url=self._get_image_url(),
            startup_env=self._startup_env(),
            cpu_request=self.cpu_request,
            cpu_limit=self.cpu_limit,
            memory_request=self.memory_request,
            memory_limit=self.memory_limit,
            ephemeral_storage_request=self.ephemeral_storage_request,
            machine_type=self._active_machine_type,
            node_pool=self._active_node_pool,
            effective_gpus=self._effective_gpus,
            gpu_types=self._effective_gpu_types,
            tpu=self.task_env_config.tpu,
            active_deadline_seconds=self._resolve_active_deadline_seconds(),
            workdir=self.task_env_config.workdir if self.task_env_config else None,
            service_account_name=self.service_account_name,
            compute_class=self._active_compute_class,
            image_pull_secrets=self.image_pull_secrets,
            override_entrypoint=_parse_bool(
                self.kwargs.get("override_entrypoint"), default=False
            ),
            runtime_class_name=self.kwargs.get("runtime_class_name"),
        )

    @staticmethod
    def _is_gvisor_retryable_error(err_str: str) -> bool:
        err_lower = err_str.lower()
        keywords = (
            "gvisor",
            "runtimeclassname",
            "capabilities",
            "privileged",
            "podsecurity",
            "autopilot",
            "securitycontext",
        )
        return any(kw in err_lower for kw in keywords)

    def _get_admission_controller(self) -> ClusterAdmissionController:
        cluster_key = self._cluster_key
        ctrl = _CLUSTER_ADMISSION_CONTROLLERS.get(cluster_key)
        if ctrl is None:
            ctrl = ClusterAdmissionController()
            _CLUSTER_ADMISSION_CONTROLLERS[cluster_key] = ctrl
        caps = _CLUSTER_CAPABILITIES_CACHE.get(cluster_key)
        ctrl.configure(
            max_cpu_cores=(
                caps.max_schedulable_cpu_cores if caps is not None else None
            ),
            max_gvisor_cpu_cores=(
                caps.max_gvisor_cpu_cores if caps is not None else None
            ),
            max_concurrent_pods=self.max_concurrent_pods,
        )
        return ctrl

    async def _release_admission_token(self) -> None:
        if self._admission_token is not None:
            token = self._admission_token
            self._admission_token = None
            await self._get_admission_controller().release(token)

    def _sync_dind_container_routing(self, pod: Any | None = None) -> None:
        """Record which logical services execute inside ``dind-engine``.

        Read from the translated Pod spec, which carries the set as an
        annotation, and held on the instance. It is deliberately *not* filed in a
        module-level registry keyed by Pod name: under the Job path the Pod name
        is assigned by the controller after this spec is built, so any name-keyed
        entry written here would be unreachable by the exec paths that later look
        it up by the real name. The instance outlives both names.
        """
        target = pod or self._created_pod or self.pod
        meta = getattr(target, "metadata", None)
        annotations = getattr(meta, "annotations", None) or {}
        raw = annotations.get("harbor.dev/dind-delegated-services")
        if not raw:
            return
        try:
            services = json.loads(raw)
        except (ValueError, TypeError) as exc:
            self.logger.warning(
                f"Could not parse harbor.dev/dind-delegated-services={raw!r} on pod "
                f"{self.pod_name}: {exc}. Exec and file transfer for DinD-delegated "
                "services will incorrectly target the native Pod container."
            )
            return
        if isinstance(services, list) and services:
            self._dind_services = frozenset(str(s) for s in services)

    def _peak_ephemeral_storage_request_mb(self, pod: k8s_client.V1Pod) -> int:
        """Peak ``ephemeral-storage`` the Pod will hold at once, in MiB.

        Init containers run one at a time and release nothing until the Pod ends,
        so their reservations do not stack with each other -- but restartable init
        containers (sidecars, ``restartPolicy: Always``) stay alive alongside the
        app containers and do stack with them. This mirrors how the scheduler
        computes a Pod's effective request.
        """
        spec = getattr(pod, "spec", None)
        if spec is None:
            return 0

        def _request_mb(container: Any) -> int:
            resources = getattr(container, "resources", None)
            requests = getattr(resources, "requests", None) or {}
            return parse_quantity_to_mib(requests.get("ephemeral-storage")) or 0

        app_total = sum(_request_mb(c) for c in (spec.containers or []))
        sidecar_total = 0
        one_shot_peak = 0
        for container in spec.init_containers or []:
            amount = _request_mb(container)
            if getattr(container, "restart_policy", None) == "Always":
                sidecar_total += amount
            else:
                one_shot_peak = max(one_shot_peak, amount)
        return max(app_total + sidecar_total + one_shot_peak, one_shot_peak)

    def _assert_ephemeral_storage_schedulable(
        self, pod: k8s_client.V1Pod, caps: ClusterCapabilities
    ) -> None:
        """Refuse a storage reservation no single node in the cluster can hold.

        A Pod requesting more ephemeral storage than any node's allocatable does
        not fail -- it pends until the trial's clock runs out, and the only clue is
        a ``FailedScheduling`` event that expires within the hour. Turning that into
        a message at submission time is the entire point.

        Deliberately conservative in three ways, because a false refusal is worse
        than a slow pend: the ceiling is the *largest* node rather than the current
        one; an unknown ceiling never blocks; and clusters with node
        auto-provisioning are exempt, since NAP may create a larger node than any
        that exists right now.
        """
        ceiling_mb = caps.max_node_allocatable_ephemeral_storage_mb
        if ceiling_mb is None or ceiling_mb <= 0:
            return
        if caps.is_autopilot or caps.node_auto_provisioning_enabled:
            return

        requested_mb = self._peak_ephemeral_storage_request_mb(pod)
        if requested_mb <= ceiling_mb:
            return

        raise EphemeralStorageUnschedulableError(
            f"Task {self.environment_name!r} requests {requested_mb:,} MiB of "
            f"ephemeral storage, but the largest schedulable node in cluster "
            f"{self.cluster_name!r} offers {ceiling_mb:,} MiB -- short by "
            f"{requested_mb - ceiling_mb:,} MiB. The Pod would stay Pending until "
            f"the trial timed out. Add a node pool with a larger boot disk, or cap "
            f"the reservation with `--ek task_dind_storage_mb="
            f"{self.environment_name}=<mb>`."
        )

    async def _create_pod(self, pod: k8s_client.V1Pod) -> None:
        """Create a Job wrapping the Pod, retrying with runtimeClassName='gvisor' if Autopilot rejects capabilities."""
        self._created_pod = pod
        self._sync_dind_container_routing(pod)
        cached_caps = _CLUSTER_CAPABILITIES_CACHE.get(self._cluster_key)
        if cached_caps is not None:
            self._assert_ephemeral_storage_schedulable(pod, cached_caps)
        ctrl = self._get_admission_controller()

        is_gvisor = (
            getattr(getattr(pod, "spec", None), "runtime_class_name", None) == "gvisor"
        )
        cpu_cores = float(self._effective_cpus or 1)
        if self._admission_token is None:
            self._admission_token = await ctrl.acquire(cpu_cores, is_gvisor=is_gvisor)
        try:
            await self._attempt_create_pod(pod)
        except Exception as exc:
            if getattr(
                pod.spec, "runtime_class_name", None
            ) != "gvisor" and self._is_gvisor_retryable_error(str(exc)):
                self.logger.info(
                    f"Pod creation rejected due to security/capability constraints ({exc}); "
                    "retrying with runtimeClassName='gvisor'..."
                )
                await self._release_admission_token()
                pod.spec.runtime_class_name = "gvisor"
                self._admission_token = await ctrl.acquire(cpu_cores, is_gvisor=True)
                if self._batch_api is not None:
                    try:
                        await self._call_control_plane_write(
                            f"delete Job {self.job_name}",
                            self._batch_api.delete_namespaced_job,
                            name=self.job_name,
                            namespace=self.namespace,
                            propagation_policy="Background",
                        )
                        await asyncio.sleep(1.0)
                    except Exception:
                        pass
                try:
                    await self._attempt_create_pod(pod)
                except Exception:
                    await self._release_admission_token()
                    raise
            else:
                await self._release_admission_token()
                raise

    async def _call_control_plane_write(
        self, description: str, fn: Callable[..., Any], /, **kwargs: Any
    ) -> Any:
        """Run a blocking Kubernetes write, retrying while the control plane is overloaded.

        The call is bounded by the process-wide control-plane limiter, which it
        also feeds: a success lets the limit grow, an overload response shrinks
        it. Only errors classified by ``is_control_plane_overload`` are retried.
        An admission webhook that could not be called rejects the request before
        it is persisted, so repeating it is safe; if a retried create finds the
        object already there, the caller's HTTP 409 handling applies. Any other
        ``ApiException``, and the last overload error, propagate unchanged.
        """
        limiter = get_control_plane_limiter()
        for attempt in range(_GKE_CONTROL_PLANE_WRITE_MAX_ATTEMPTS):
            try:
                async with limiter:
                    result = await asyncio.to_thread(fn, **kwargs)
            except ApiException as e:
                if not is_control_plane_overload(e):
                    raise
                limiter.record_overload()
                if attempt >= _GKE_CONTROL_PLANE_WRITE_MAX_ATTEMPTS - 1:
                    raise
                delay = jittered_backoff_delay(attempt)
                self.logger.debug(
                    f"Kubernetes control plane overloaded during {description} "
                    f"(HTTP {e.status}: {e.reason}); retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{_GKE_CONTROL_PLANE_WRITE_MAX_ATTEMPTS})..."
                )
                await asyncio.sleep(delay)
                continue
            limiter.record_success()
            return result
        raise RuntimeError(f"Kubernetes {description} was never attempted")

    @staticmethod
    def _uid_of(obj: Any) -> str | None:
        uid = getattr(getattr(obj, "metadata", None), "uid", None)
        return uid if isinstance(uid, str) and uid else None

    @staticmethod
    def _is_pod_terminating(pod: Any) -> bool:
        deletion_ts = getattr(
            getattr(pod, "metadata", None), "deletion_timestamp", None
        )
        return isinstance(deletion_ts, (datetime.datetime, str))

    def _job_pod_label_selector(self) -> str:
        """Select the Pods of the Job this environment created.

        ``job-name`` is shared with any earlier Job of the same name whose Pods
        are still terminating, so prefer the controller UID once it is known.
        """
        if self._job_uid:
            return f"batch.kubernetes.io/controller-uid={self._job_uid}"
        return f"job-name={self.job_name}"

    def _job_event_field_selector(self) -> str:
        """Select events for the Job this environment created (not a same-named predecessor)."""
        if self._job_uid:
            return f"involvedObject.uid={self._job_uid}"
        return f"involvedObject.name={self.job_name}"

    async def _attempt_create_pod(self, pod: k8s_client.V1Pod) -> None:
        """Execute a single attempt to create a Job or Pod in Kubernetes."""
        if self._batch_api is not None:
            self._job_uid = None
            job = build_job(
                job_name=self.job_name,
                namespace=self.namespace,
                pod_spec=pod.spec,
                labels=pod.metadata.labels or {},
                annotations=pod.metadata.annotations if pod.metadata else None,
                ttl_seconds_after_finished=120,
            )
            try:
                created_job = await self._call_control_plane_write(
                    f"create Job {self.job_name}",
                    self._batch_api.create_namespaced_job,
                    namespace=self.namespace,
                    body=job,
                )
                self._job_uid = self._uid_of(created_job)
                self.logger.debug(f"Job {self.job_name} created successfully")
            except ApiException as e:
                if e.status == 409:
                    self.logger.debug(
                        f"Job {self.job_name} already exists, deleting and recreating..."
                    )
                    try:
                        await self._call_control_plane_write(
                            f"delete Job {self.job_name}",
                            self._batch_api.delete_namespaced_job,
                            name=self.job_name,
                            namespace=self.namespace,
                            propagation_policy="Background",
                        )
                        wait_start = time.monotonic()
                        timeout = 60.0
                        while True:
                            if time.monotonic() - wait_start > timeout:
                                try:
                                    stuck_job = await asyncio.to_thread(
                                        self._batch_api.read_namespaced_job,
                                        name=self.job_name,
                                        namespace=self.namespace,
                                    )
                                    finalizers = getattr(
                                        stuck_job.metadata, "finalizers", None
                                    )
                                except Exception:
                                    finalizers = None
                                raise RuntimeError(
                                    f"Timed out waiting for existing Job {self.job_name} in namespace "
                                    f"{self.namespace} to be deleted after {timeout}s (finalizers={finalizers})"
                                )
                            try:
                                await asyncio.to_thread(
                                    self._batch_api.read_namespaced_job,
                                    name=self.job_name,
                                    namespace=self.namespace,
                                )
                                await asyncio.sleep(0.5)
                            except ApiException as read_err:
                                if read_err.status == 404:
                                    break
                                raise
                        created_job = await self._call_control_plane_write(
                            f"recreate Job {self.job_name}",
                            self._batch_api.create_namespaced_job,
                            namespace=self.namespace,
                            body=job,
                        )
                        self._job_uid = self._uid_of(created_job)
                        self.logger.debug(f"Job {self.job_name} recreated successfully")
                    except ApiException as delete_err:
                        raise RuntimeError(
                            f"Failed to recreate existing job {self.job_name}: {delete_err}"
                        )
                else:
                    detail = f": {e.body}" if getattr(e, "body", None) else ""
                    raise RuntimeError(
                        f"Failed to create job {self.job_name}: {e.status} - {e.reason}{detail}"
                    )

            # Resolve pod spawned by Job controller
            start_time = time.monotonic()
            timeout_sec = _GKE_JOB_POD_SPAWN_TIMEOUT_SEC
            poll_interval = 1.0
            seen_overload_events: set[tuple[str, int]] = set()

            while time.monotonic() - start_time < timeout_sec:
                pod_list = await asyncio.to_thread(
                    self._api.list_namespaced_pod,
                    namespace=self.namespace,
                    label_selector=self._job_pod_label_selector(),
                )
                live_pods = [
                    p
                    for p in (getattr(pod_list, "items", None) or [])
                    if not self._is_pod_terminating(p)
                ]
                if live_pods:
                    self.pod = live_pods[0]
                    self.pod_name = self.pod.metadata.name
                    break

                try:
                    events = await asyncio.to_thread(
                        self._api.list_namespaced_event,
                        namespace=self.namespace,
                        field_selector=self._job_event_field_selector(),
                    )
                    for ev in getattr(events, "items", []):
                        if ev.type != "Warning" or ev.reason != "FailedCreate":
                            continue
                        message = str(ev.message or "")
                        if _GKE_WEBHOOK_CALL_FAILURE_MARKER in message.lower():
                            # The API server could not reach a fail-closed admission
                            # webhook (Autopilot's Warden under load). The Job
                            # controller retries Pod creation with its own backoff,
                            # so keep polling until the spawn timeout.
                            event_key = (
                                str(getattr(ev.metadata, "name", "") or ""),
                                int(getattr(ev, "count", 0) or 0),
                            )
                            if event_key not in seen_overload_events:
                                seen_overload_events.add(event_key)
                                get_control_plane_limiter().record_overload()
                                self.logger.debug(
                                    f"Job {self.job_name} Pod creation hit a control-plane "
                                    f"admission webhook failure; waiting for the Job "
                                    f"controller to retry: {message}"
                                )
                            continue
                        raise RuntimeError(
                            f"Kubernetes Job {self.job_name} failed to create Pod: {message}"
                        )
                except ApiException:
                    pass

                await asyncio.sleep(poll_interval)
            else:
                warning_messages: list[str] = []
                try:
                    events = await asyncio.to_thread(
                        self._api.list_namespaced_event,
                        namespace=self.namespace,
                        field_selector=self._job_event_field_selector(),
                    )
                    for ev in getattr(events, "items", []):
                        if ev.type == "Warning" and ev.message:
                            warning_messages.append(f"({ev.reason}) {ev.message}")
                except ApiException:
                    pass

                error_detail = (
                    f": {'; '.join(warning_messages)}" if warning_messages else "."
                )
                raise TimeoutError(
                    f"Timed out after {timeout_sec}s waiting for Kubernetes Job controller "
                    f"to spawn Pod for Job '{self.job_name}'{error_detail}"
                )
        else:
            try:
                await self._call_control_plane_write(
                    f"create Pod {pod.metadata.name}",
                    self._api.create_namespaced_pod,
                    namespace=self.namespace,
                    body=pod,
                )
                self.pod = pod
                self.pod_name = pod.metadata.name
                self.logger.debug(f"Pod {self.pod_name} created successfully")
            except ApiException as e:
                if e.status == 409:
                    self.logger.debug(
                        f"Pod {self.pod_name} already exists, deleting and recreating..."
                    )
                    try:
                        await self._call_control_plane_write(
                            f"delete Pod {self.pod_name}",
                            self._api.delete_namespaced_pod,
                            name=self.pod_name,
                            namespace=self.namespace,
                        )
                        wait_start = time.monotonic()
                        while time.monotonic() - wait_start < 60:
                            try:
                                await asyncio.to_thread(
                                    self._api.read_namespaced_pod,
                                    name=self.pod_name,
                                    namespace=self.namespace,
                                )
                                await asyncio.sleep(1)
                            except ApiException as read_err:
                                if read_err.status == 404:
                                    break
                                raise
                        await self._call_control_plane_write(
                            f"recreate Pod {pod.metadata.name}",
                            self._api.create_namespaced_pod,
                            namespace=self.namespace,
                            body=pod,
                        )
                        self.pod = pod
                        self.pod_name = pod.metadata.name
                        self.logger.debug(f"Pod {self.pod_name} recreated successfully")
                    except ApiException as delete_err:
                        raise RuntimeError(
                            f"Failed to recreate existing pod {self.pod_name}: {delete_err}"
                        )
                else:
                    raise RuntimeError(
                        f"Failed to create pod {self.pod_name}: {e.status} - {e.reason}"
                    )

    @override
    async def stop(self, delete: bool):
        await self._delete_pod_and_release(delete)

    async def _delete_pod_and_release(self, delete: bool):
        """Clean up job, pod, and network policies, and release Kubernetes client reference."""
        try:
            await self._report_dind_engine_usage()
            if delete:
                if self._networking_api is not None:
                    await delete_network_policies(
                        networking_api=self._networking_api,
                        custom_api=self._custom_api,
                        namespace=self.namespace,
                        pod_name=self.job_name,
                    )
                if self._batch_api is not None:
                    self.logger.debug(f"Deleting job {self.job_name}...")
                    try:
                        await self._call_control_plane_write(
                            f"delete Job {self.job_name}",
                            self._batch_api.delete_namespaced_job,
                            name=self.job_name,
                            namespace=self.namespace,
                            propagation_policy="Background",
                        )
                        self.logger.debug(f"Job {self.job_name} deleted successfully")
                    except ApiException as e:
                        if e.status != 404:
                            self.logger.warning(
                                f"Failed to delete job {self.job_name}: {e}"
                            )
                if self._core_api is not None:
                    self.logger.debug(f"Deleting pod {self.pod_name}...")
                    try:
                        await self._call_control_plane_write(
                            f"delete Pod {self.pod_name}",
                            self._api.delete_namespaced_pod,
                            name=self.pod_name,
                            namespace=self.namespace,
                            body=k8s_client.V1DeleteOptions(grace_period_seconds=0),
                        )
                        self.logger.debug(f"Pod {self.pod_name} deleted successfully")
                    except ApiException as e:
                        if e.status != 404:
                            self.logger.warning(
                                f"Failed to delete pod {self.pod_name}: {e}"
                            )
        finally:
            await self._release_admission_token()
            if self._client_manager is not None:
                self._client_manager.release_client(self._core_api)
                self._core_api = None
                self._batch_api = None
                self._networking_api = None
                self._custom_api = None

    # Reads cgroup v2 files from inside dind-engine, which shares the node's
    # cgroup namespace. `kubectl exec` lands in the `harbor-daemon` leaf
    # dind-engine moved its processes into, so the suffix is stripped to reach
    # the container's cgroup (``D``), which also holds every nested container
    # (see `_build_shape_b_dind_containers`). Its parent (``P``) is the Pod's
    # cgroup, where `spec.resources` sets the ceiling. `memory.events` is
    # hierarchical, so the Pod's `oom_kill` / `oom_group_kill` count kills in
    # `main`, sidecars, the daemon and every nested container, and outlive a
    # restarted dind-engine; `memory.events.local` counts only the Pod's own
    # ceiling. `D/memory.oom.group` is what the kubelet asked for: 1 unless the
    # node runs with `singleProcessOOMKill: true`.
    _DIND_USAGE_SCRIPT = (
        "S=$(sed -n 's/^0:://p' /proc/self/cgroup); S=${S%/harbor-daemon}; "
        'D="/sys/fs/cgroup$S"; P="${D%/*}"; '
        'echo "engine.memory.current $(cat "$D/memory.current")"; '
        '[ -f "$D/memory.peak" ] && echo "engine.memory.peak $(cat "$D/memory.peak")"; '
        'echo "engine.oom_group $(cat "$D/memory.oom.group")"; '
        'echo "pod.memory.max $(cat "$P/memory.max")"; '
        'awk \'$1=="oom_kill"||$1=="oom_group_kill"{print "pod." $1, $2}\' "$P/memory.events"; '
        'awk \'$1=="oom"{print "pod.ceiling_oom", $2}\' "$P/memory.events.local"; '
        'awk \'$1=="usage_usec"{print "engine.usage_usec", $2}\' "$D/cpu.stat"'
    )

    _SINGLE_PROCESS_OOM_KILL_HINT = (
        "Docker kills single processes instead. Set `singleProcessOOMKill: true` "
        "in the ComputeClass, or `singleProcessOomKill: true` in the node pool's "
        "`--system-config-from-file`, for the nodes that run DinD tasks (see "
        "docs/cluster-setup.md)."
    )

    async def _report_dind_engine_usage(self) -> None:
        """Log what the DinD Pod used and which memory ceiling, if any, it hit.

        A DinD Pod is the task's Docker host, bounded by the Pod ceiling (see
        ``build_pod_level_resources``). Containers a task starts through the
        Docker socket are invisible to Kubernetes, so what happened to them
        shows up only in the Pod's cgroup. An INFO line reports usage; each
        kind of OOM kill gets its own WARNING, because each has a different
        owner:

        - the Pod ceiling was reached: the task used more memory than it
          declares (``memory_mb`` / ``--override-memory-mb``);
        - a container reached its own memory limit: the limit is declared by
          the task's Compose files and applies on Docker too;
        - the kernel killed whole containers: the kubelet set
          ``memory.oom.group=1`` (``singleProcessOOMKill: false``, the cgroup v2
          default), which can take down the Docker daemon with everything it
          runs. That is node configuration.

        When the cgroup cannot be read (``dind-engine`` has exited, or exec
        into it hangs), the Pod status is the remaining evidence; see
        ``_report_oom_killed_containers``. An unreadable report is always a
        WARNING.

        Diagnostics only: bounded by ``_GKE_DIND_USAGE_REPORT_TIMEOUT_SEC`` and
        never raises, so it cannot hold up deletion. gVisor is skipped because
        its sandbox reports its own cgroup view, not the node's.
        """
        spec = getattr(self._created_pod, "spec", None)
        if spec is None or self._core_api is None:
            return
        if getattr(spec, "runtime_class_name", None) == "gvisor":
            return
        if not any(
            c.name == DIND_ENGINE_CONTAINER
            for c in [*(spec.init_containers or []), *(spec.containers or [])]
        ):
            return
        try:
            async with asyncio.timeout(_GKE_DIND_USAGE_REPORT_TIMEOUT_SEC):
                stdout, stderr, rc = await run_exec_command(
                    self._connect_exec_stream,
                    ["sh", "-c", self._DIND_USAGE_SCRIPT],
                    container=DIND_ENGINE_CONTAINER,
                    timeout_sec=_GKE_DIND_USAGE_REPORT_TIMEOUT_SEC,
                )
        except Exception as exc:  # noqa: BLE001 - diagnostics must never block teardown
            await self._report_oom_killed_containers(
                read_error=f"{type(exc).__name__}: {exc}"
            )
            return

        stats: dict[str, int] = {}
        for line in stdout.decode("utf-8", "replace").splitlines():
            key, _, value = line.strip().partition(" ")
            if value.strip().isdigit():
                stats[key] = int(value)
        if rc != 0 or "engine.memory.current" not in stats:
            await self._report_oom_killed_containers(
                read_error=f"exit {rc}: {stderr.decode('utf-8', 'replace').strip()}"
            )
            return

        mib = 1024 * 1024

        def _mib(key: str) -> str:
            return f"{stats[key] // mib} MiB" if key in stats else "n/a"

        cpu_sec = (
            f"{stats['engine.usage_usec'] / 1_000_000:.1f} s"
            if "engine.usage_usec" in stats
            else "n/a"
        )
        oom_kills = stats.get("pod.oom_kill", 0)
        group_kills = stats.get("pod.oom_group_kill", 0)
        ceiling_ooms = stats.get("pod.ceiling_oom", 0)
        oom_mode = (
            "whole container"
            if stats.get("engine.oom_group") == 1
            else "per process"
            if "engine.oom_group" in stats
            else "n/a"
        )
        self.logger.info(
            f"DinD usage for pod {self.pod_name}: Docker host memory "
            f"{_mib('engine.memory.current')} (peak {_mib('engine.memory.peak')}), "
            f"CPU time {cpu_sec}; Pod memory ceiling {_mib('pod.memory.max')}, "
            f"OOM at the ceiling {ceiling_ooms} time(s), OOM kills {oom_kills} "
            f"(group kills {group_kills}); OOM kill mode: {oom_mode}."
        )
        if ceiling_ooms:
            self.logger.warning(
                f"Pod {self.pod_name} ran out of memory: its memory ceiling of "
                f"{_mib('pod.memory.max')} was reached {ceiling_ooms} time(s), "
                f"with {oom_kills} OOM kill(s) in the Pod. The task used more "
                "memory than it declares. The ceiling is the task's memory "
                "budget plus what its Compose services declare and the Docker "
                "daemon baseline; containers the task starts through the Docker "
                "socket count against it. Raise `memory_mb` in task.toml, or "
                "re-size the run with `--override-memory-mb`."
            )
        elif oom_kills:
            self.logger.warning(
                f"{oom_kills} process(es) in pod {self.pod_name} were OOM-killed "
                "because a container reached its own memory limit; the Pod "
                "ceiling was not reached. That limit comes from the task's "
                "Compose files and applies the same way on a Docker host."
            )
        if group_kills:
            self.logger.warning(
                f"The kernel OOM-killed whole containers {group_kills} time(s) in "
                f"pod {self.pod_name}: the kubelet set `memory.oom.group=1`, so one "
                "OOM kill took every process in the container with it -- for "
                "`dind-engine`, the Docker daemon and everything it ran. "
                f"{self._SINGLE_PROCESS_OOM_KILL_HINT}"
            )

    _SIGKILL_EXIT_CODE = 137

    async def _report_oom_killed_containers(self, *, read_error: str) -> None:
        """Explain an unreadable DinD usage report from the Pod status.

        Exec into ``dind-engine`` fails when the container has exited and can
        hang when it is stalled for memory, so the cgroup report is lost. The
        Pod status is the remaining evidence. containerd sets the reason
        ``OOMKilled`` when any process in a container was OOM-killed (its
        ``TaskOOM`` event handler), so the reason alone does not mean the
        container was killed: only an exit by SIGKILL (137) does. For
        ``dind-engine`` that is the Docker daemon itself. Best effort, like
        the usage report.
        """
        unreadable = (
            f"The DinD usage report for pod {self.pod_name} could not be read "
            f"({read_error}), so what the Docker host used is unknown."
        )
        try:
            async with asyncio.timeout(_GKE_DIND_USAGE_REPORT_TIMEOUT_SEC):
                pod = await asyncio.to_thread(
                    self._api.read_namespaced_pod,
                    name=self.pod_name,
                    namespace=self.namespace,
                )
        except Exception as exc:  # noqa: BLE001 - diagnostics must never block teardown
            self.logger.warning(
                f"{unreadable} The Pod status is unavailable too "
                f"({type(exc).__name__}: {exc})."
            )
            return
        status = getattr(pod, "status", None)
        oom_marked: dict[str, int | None] = {}
        for cs in [
            *(getattr(status, "init_container_statuses", None) or []),
            *(getattr(status, "container_statuses", None) or []),
        ]:
            for state in (cs.state, cs.last_state):
                terminated = getattr(state, "terminated", None)
                if getattr(terminated, "reason", None) == "OOMKilled":
                    exit_code = getattr(terminated, "exit_code", None)
                    if oom_marked.get(cs.name) != self._SIGKILL_EXIT_CODE:
                        oom_marked[cs.name] = exit_code
        if not oom_marked:
            self.logger.warning(
                f"{unreadable} Exec into `{DIND_ENGINE_CONTAINER}` fails when the "
                "container has exited, and can hang when it is out of memory. "
                "No container in the Pod reports an OOM kill."
            )
            return
        sizing = (
            "Check that the task's `memory_mb` covers its whole environment, "
            "including containers it starts at runtime (`--override-memory-mb` "
            "re-sizes a run), and the memory limits its Compose files declare."
        )
        if oom_marked.get(DIND_ENGINE_CONTAINER) == self._SIGKILL_EXIT_CODE:
            self.logger.warning(
                f"{unreadable} The Docker daemon in pod {self.pod_name} was killed "
                f"(`{DIND_ENGINE_CONTAINER}` OOMKilled, exit code 137), taking every "
                f"container it ran with it. {sizing} On a node with "
                "`singleProcessOOMKill: false` one OOM kill anywhere in "
                f"`{DIND_ENGINE_CONTAINER}` kills the daemon too. "
                f"{self._SINGLE_PROCESS_OOM_KILL_HINT}"
            )
        killed = sorted(
            name
            for name, code in oom_marked.items()
            if code == self._SIGKILL_EXIT_CODE and name != DIND_ENGINE_CONTAINER
        )
        if killed:
            self.logger.warning(
                f"{unreadable} Container(s) {', '.join(killed)} in pod "
                f"{self.pod_name} were OOM-killed (exit code 137). {sizing}"
            )
        survived = sorted(
            f"{name} (exit code {code})"
            for name, code in oom_marked.items()
            if code != self._SIGKILL_EXIT_CODE
        )
        if survived:
            self.logger.warning(
                f"{unreadable} Processes were OOM-killed inside container(s) "
                f"{', '.join(survived)} of pod {self.pod_name}: Kubernetes marks a "
                "container `OOMKilled` when any process in it was OOM-killed, but "
                f"these containers were not killed themselves. {sizing}"
            )

    async def _connect_exec_stream(
        self,
        command: list[str],
        *,
        container: str | None = None,
        stderr: bool = True,
        stdin: bool = False,
        stdout: bool = True,
        tty: bool = False,
        max_attempts: int = _GKE_EXEC_CONNECT_MAX_ATTEMPTS,
    ) -> ExecStream:
        await self._ensure_client()
        if self._api is None:
            raise RuntimeError("Kubernetes CoreV1Api client is not initialized")
        # Always the Pod `start()` brought up. The Job may replace a disrupted
        # Pod, but the trial's state died with the old one, so a 404 propagates.
        return await connect_exec_stream(
            self._api,
            self.pod_name,
            self.namespace,
            command,
            container=container,
            stderr=stderr,
            stdin=stdin,
            stdout=stdout,
            tty=tty,
            dind_services=self._dind_services,
            max_attempts=max_attempts,
        )

    async def _terminate_lingering_phase_connections(self) -> None:
        """Best-effort teardown of active outbound sockets/conntrack flows across phase narrowing."""
        try:
            await self.exec(
                "ss -K 2>/dev/null || conntrack -F 2>/dev/null || true",
                timeout_sec=5,
                supervised=False,
            )
        except Exception:
            pass
        if self._dind_services:
            try:
                await self.exec(
                    "conntrack -F 2>/dev/null || ss -K 2>/dev/null || true",
                    container="dind-engine",
                    timeout_sec=5,
                    supervised=False,
                )
            except Exception:
                pass

    async def _verify_no_network_convergence(self) -> None:
        """Best-effort in-Pod probe verifying metadata egress is blocked after transitioning to NO_NETWORK."""
        try:
            await self.exec(
                "sh -c '(! nc -z -w 1 169.254.169.254 80) 2>/dev/null || true'",
                timeout_sec=5,
                supervised=False,
            )
        except Exception:
            pass

    @override
    async def _apply_network_policy(self, network_policy: NetworkPolicy) -> None:
        await self._ensure_client()
        if self._networking_api is None:
            raise RuntimeError(
                "Cannot apply NetworkPolicy: Kubernetes NetworkingV1Api client is not initialized."
            )
        prev_mode = self._applied_network_mode
        target_pod = self.pod or self._created_pod
        pod_uid = self.pod.metadata.uid if (self.pod and self.pod.metadata) else None
        pod_labels = (
            target_pod.metadata.labels if (target_pod and target_pod.metadata) else None
        )
        caps = await self._get_cluster_capabilities()
        await apply_network_policy(
            networking_api=self._networking_api,
            custom_api=self._custom_api,
            namespace=self.namespace,
            pod_name=self.pod_name,
            session_id=self.session_id,
            network_policy=network_policy,
            fqdn_supported=caps.fqdn_network_policy_supported,
            allow_metadata_server=self.allow_metadata_server,
            pod_uid=pod_uid,
            pod_labels=pod_labels,
            policy_key=self.job_name,
            dns_egress_extra_cidrs=self.dns_egress_extra_cidrs,
            allow_pod_ingress=self.allow_pod_ingress,
            kube_dns_cluster_ip=caps.kube_dns_cluster_ip,
        )
        self._applied_network_mode = network_policy.network_mode
        if self._pod_ready:
            is_narrowing = prev_mode in (
                NetworkMode.PUBLIC,
                NetworkMode.ALLOWLIST,
            ) and (
                network_policy.network_mode == NetworkMode.NO_NETWORK
                or (
                    prev_mode == NetworkMode.PUBLIC
                    and network_policy.network_mode == NetworkMode.ALLOWLIST
                )
            )
            if is_narrowing:
                await self._terminate_lingering_phase_connections()
            settlement_sec = float(
                self.kwargs.get("network_policy_settlement_sec", 2.0)
            )
            if settlement_sec > 0:
                await asyncio.sleep(settlement_sec)
            if is_narrowing and network_policy.network_mode == NetworkMode.NO_NETWORK:
                await self._verify_no_network_convergence()

    @override
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
        supervised: bool = True,
        container: str | None = None,
    ) -> ExecResult:
        """Execute command in pod using kubectl exec equivalent."""
        container = container or (MAIN_SERVICE_NAME if self._compose_mode else None)
        await self._ensure_client()

        user = self._resolve_user(user)
        env = self._merge_env(env)
        if env:
            for k in env:
                if not _ENV_VAR_NAME_RE.fullmatch(k):
                    raise ValueError(f"Invalid environment variable name: {k!r}")

        # Sidecars are arbitrary third-party images (such as Alpine variants) where
        # bash is frequently absent, whereas POSIX sh is universal.
        # The main container is a harbor-built image that always ships bash.
        is_sidecar = bool(
            self._compose_mode
            and container is not None
            and container != MAIN_SERVICE_NAME
        )
        shell_name = "sh" if is_sidecar else "bash"
        shell_path = "/bin/sh" if is_sidecar else "/bin/bash"

        full_command = f"{shell_name} -c {shlex.quote(command)}"
        if env:
            for key, value in env.items():
                full_command = f"{key}={shlex.quote(value)} {full_command}"

        effective_cwd = cwd or (None if is_sidecar else self.task_env_config.workdir)
        if effective_cwd:
            full_command = f"cd {shlex.quote(str(effective_cwd))} && {full_command}"

        if user is not None:
            if isinstance(user, int):
                user_arg = f"$(getent passwd {user} | cut -d: -f1)"
            else:
                user_arg = shlex.quote(user)
            full_command = (
                f"su {user_arg} -s {shell_path} -c {shlex.quote(full_command)}"
            )

        callback = self._output_callback()
        start_time = time.monotonic()
        workdir = f"/tmp/harbor_{uuid.uuid4().hex[:10]}"

        if supervised:
            exec_command = ["sh", "-c", build_supervised_script(workdir, full_command)]
        else:
            exec_command = [
                "sh",
                "-c",
                full_command,
            ]

        output = ExecOutputAccumulator(callback)
        stream: ExecStream | None = None
        # Set once the command's status is followed through its workdir, which
        # already handles lost streams itself.
        polling = False
        # Set when Kubernetes returned an explicit ERROR_CHANNEL Failure status
        # without an ExitCode cause (the process never started on the kubelet).
        status_failed = False

        try:
            if supervised and self.decoupled:
                detach_script = build_decoupled_launch_script(workdir, full_command)
                _, launch_err, launch_rc = await run_exec_command(
                    self._connect_exec_stream,
                    ["sh", "-c", detach_script],
                    container=container,
                    timeout_sec=_GKE_DECOUPLED_LAUNCH_TIMEOUT_SEC,
                    max_attempts=_GKE_EXEC_CONNECT_MAX_ATTEMPTS,
                )
                if launch_rc != 0:
                    raise RuntimeError(
                        f"Failed to launch decoupled command on pod {self.pod_name} "
                        f"(exit {launch_rc}): "
                        f"{launch_err.decode('utf-8', 'replace').strip()}"
                    )
                polling = True
                return await self._poll_decoupled_exec(
                    workdir=workdir,
                    output=output,
                    timeout_sec=timeout_sec,
                    start_time=start_time,
                    container=container,
                )

            # Direct streaming mode (Mode A)
            for attempt in range(_GKE_EXEC_CONNECT_MAX_ATTEMPTS):
                stream = await self._connect_exec_stream(
                    exec_command,
                    container=container,
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                )
                # The clock starts once the command runs and its output is being read.
                deadline = asyncio.timeout(timeout_sec if timeout_sec else None)
                try:
                    async with deadline:
                        await read_exec_output(stream, output)
                except TimeoutError:
                    if not deadline.expired():
                        raise
                    return await self._handle_exec_timeout(
                        stream=stream,
                        output=output,
                        supervised=supervised,
                        workdir=workdir,
                        timeout_sec=timeout_sec,
                        container=container,
                    )

                try:
                    return_code = stream.returncode()
                except GKEExecStreamClosedError as status_exc:
                    status_failed = True
                    stream.close()
                    stream = None
                    await self._raise_if_container_lost(container)
                    if (
                        is_transient_exec_status_error(status_exc)
                        and output.stdout_bytes == 0
                        and output.stderr_bytes == 0
                        and attempt < _GKE_EXEC_CONNECT_MAX_ATTEMPTS - 1
                    ):
                        status_failed = False
                        wait_time = jittered_backoff_delay(attempt)
                        self.logger.debug(
                            f"Transient exec proxy error on pod {self.pod_name} "
                            f"({status_exc}), retrying in {wait_time:.1f}s "
                            f"(attempt {attempt + 1}/{_GKE_EXEC_CONNECT_MAX_ATTEMPTS})..."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    raise

                if return_code != 0:
                    await self._raise_if_container_lost(container)
                return ExecResult(
                    stdout=output.stdout, stderr=output.stderr, return_code=return_code
                )
            raise RuntimeError(f"Exec on pod {self.pod_name} made no attempts")

        except GKEExecStreamClosedError as exc:
            if supervised and not polling and not status_failed:
                self.logger.warning(
                    f"Exec stream disconnected prematurely on pod {self.pod_name}: "
                    f"{type(exc).__name__}: {exc}. Recovering output and exit code from "
                    f"{workdir} (received {output.stdout_bytes} stdout / "
                    f"{output.stderr_bytes} stderr bytes so far)..."
                )
                return await self._poll_decoupled_exec(
                    workdir=workdir,
                    output=output,
                    timeout_sec=timeout_sec,
                    start_time=start_time,
                    container=container,
                )
            raise

        finally:
            if stream is not None:
                stream.close()

    async def _handle_exec_timeout(
        self,
        *,
        stream: ExecStream,
        output: ExecOutputAccumulator,
        supervised: bool,
        workdir: str,
        timeout_sec: int | None,
        container: str | None,
    ) -> ExecResult:
        """Close a timed-out command's stream, kill it if supervised, and return 124."""
        stream.close()
        if supervised:
            await run_best_effort(
                self._connect_exec_stream,
                build_kill_script(workdir),
                container=container,
                timeout_sec=_GKE_EXEC_KILL_TIMEOUT_SEC,
                pod_name=self.pod_name,
                purpose="kill of timed-out supervised command",
            )
        await output.finish()
        return ExecResult(
            stdout=output.stdout or None,
            stderr=f"{output.stderr}\nCommand timed out after {timeout_sec} seconds".strip(),
            return_code=124,
        )

    async def _poll_decoupled_exec(
        self,
        workdir: str,
        output: ExecOutputAccumulator,
        timeout_sec: int | None,
        start_time: float,
        container: str | None = None,
    ) -> ExecResult:
        await self._ensure_client()
        return await poll_decoupled_exec(
            api=self._api,
            pod_name=self.pod_name,
            namespace=self.namespace,
            workdir=workdir,
            output=output,
            timeout_sec=timeout_sec,
            start_time=start_time,
            # Raw execs, exactly like the supervisor: no su/cd/env wrapping.
            connect=self._connect_exec_stream,
            container=container,
        )

    async def _wait_for_container_exec_ready(self, container: str | None = None) -> None:
        await self._ensure_client()
        attempts = _EXEC_READY_MAX_ATTEMPTS
        for attempt in range(attempts):
            if self._api is not None:
                await check_pod_terminated(
                    self._api, self.pod_name, self.namespace, target_container=container
                )
            resp: ExecStream | None = None
            try:
                resp = await self._connect_exec_stream(
                    command=["true"],
                    container=container,
                    stderr=False,
                    stdin=False,
                    stdout=True,
                    tty=False,
                    max_attempts=1,
                )
                if type(resp) is ExecStream:
                    async with asyncio.timeout(_GKE_EXEC_HANDSHAKE_TIMEOUT_SEC):
                        await collect_exec_bytes(resp)
                    resp.returncode()
                return
            except TrialContainerLostError:
                raise
            except (
                GKEExecStreamClosedError,
                ApiException,
                OSError,
                TimeoutError,
            ) as exc:
                if self._api is not None:
                    await check_pod_terminated(
                        self._api,
                        self.pod_name,
                        self.namespace,
                        target_container=container,
                    )
                if attempt < attempts - 1:
                    wait_time = jittered_backoff_delay(attempt)
                    self.logger.debug(
                        f"Container exec readiness probe on pod {self.pod_name} failed "
                        f"({type(exc).__name__}: {exc}), retrying in {wait_time:.1f}s "
                        f"(attempt {attempt + 1}/{attempts})..."
                    )
                    await asyncio.sleep(wait_time)
                    continue
                raise
            finally:
                if resp is not None:
                    try:
                        resp.close()
                    except Exception:
                        pass

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_not_exception_type(TrialContainerLostError),
        reraise=True,
    )
    @override
    async def upload_file(
        self,
        source_path: Path | str,
        target_path: str,
        container: str | None = None,
    ):
        target_container = container or (
            MAIN_SERVICE_NAME if self._compose_mode else None
        )
        await self._ensure_client()
        await self._wait_for_container_exec_ready(container=target_container)
        await ft_upload_file(
            self._api,
            self.pod_name,
            self.namespace,
            source_path,
            target_path,
            container=target_container,
            dind_services=self._dind_services,
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_not_exception_type(TrialContainerLostError),
        reraise=True,
    )
    @override
    async def upload_dir(
        self,
        source_dir: Path | str,
        target_dir: str,
        container: str | None = None,
    ):
        target_container = container or (
            MAIN_SERVICE_NAME if self._compose_mode else None
        )
        await self._ensure_client()
        await self._wait_for_container_exec_ready(container=target_container)
        await ft_upload_dir(
            self._api,
            self.pod_name,
            self.namespace,
            source_dir,
            target_dir,
            container=target_container,
            dind_services=self._dind_services,
        )

    async def _raise_if_container_lost(self, container: str | None) -> None:
        """Raise ``TrialContainerLostError`` if the trial Pod or container is gone.

        Used after an operation failed to tell a lost container (an infrastructure
        failure that invalidates the trial) apart from an ordinary failure inside a
        live container. Returns normally if the container is alive or its state
        cannot be read due to a transient API error.
        """
        await self._ensure_client()
        if self._api is None:
            return
        await check_pod_terminated(
            self._api, self.pod_name, self.namespace, target_container=container
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_not_exception_type((TrialContainerLostError, FileNotFoundError)),
        reraise=True,
    )
    @override
    async def download_file(
        self,
        source_path: str,
        target_path: Path | str,
        container: str | None = None,
    ):
        target_container = container or (
            MAIN_SERVICE_NAME if self._compose_mode else None
        )
        await self._ensure_client()
        try:
            await ft_download_file(
                self._api,
                self.pod_name,
                self.namespace,
                source_path,
                target_path,
                container=target_container,
                dind_services=self._dind_services,
            )
        except (TrialContainerLostError, FileNotFoundError):
            raise
        except (ApiException, RuntimeError):
            # Never swallow this: a silent return here turns a dead container into
            # a missing reward file, which reads as a task failure.
            await self._raise_if_container_lost(target_container)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_not_exception_type((TrialContainerLostError, FileNotFoundError)),
        reraise=True,
    )
    @override
    async def download_dir(
        self,
        source_dir: str,
        target_dir: Path | str,
        container: str | None = None,
    ):
        target_container = container or (
            MAIN_SERVICE_NAME if self._compose_mode else None
        )
        await self._ensure_client()
        try:
            await ft_download_dir(
                self._api,
                self.pod_name,
                self.namespace,
                source_dir,
                target_dir,
                container=target_container,
                dind_services=self._dind_services,
            )
        except (TrialContainerLostError, FileNotFoundError):
            raise
        except (ApiException, RuntimeError):
            # See download_file: a dead container must not look like missing output.
            await self._raise_if_container_lost(target_container)
            raise

    @override
    async def is_dir(self, path: str, user: str | int | None = None) -> bool:
        """Check if a remote path is a directory using fast direct exec without supervision overhead."""
        return await self._check_path_kind(path, require_dir=True, user=user)

    @override
    async def is_file(self, path: str, user: str | int | None = None) -> bool:
        """Check if a remote path is a regular file using fast direct exec without supervision overhead."""
        return await self._check_path_kind(path, require_dir=False, user=user)

    @override
    async def service_is_dir(
        self,
        path: str,
        *,
        service: str | None = None,
        user: str | int | None = None,
    ) -> bool:
        """Check whether a path inside a compose service is a directory."""
        if self.is_main_service(service):
            return await self.is_dir(path, user=user)
        return await self._check_path_kind(
            path, require_dir=True, service=service, user=user
        )

    async def _check_path_kind(
        self,
        path: str,
        *,
        require_dir: bool,
        service: str | None = None,
        user: str | int | None = None,
    ) -> bool:
        """Execute a direct test -d or test -f in the target container without supervision overhead."""
        command = self._path_kind_check_command(path, require_dir=require_dir)
        container = (
            _sanitize_kubernetes_resource_name(service)
            if service is not None and not self.is_main_service(service)
            else None
        )
        try:
            result = None
            for attempt in range(2):
                try:
                    if container is not None:
                        result = await self.exec(
                            command,
                            timeout_sec=_PATH_KIND_CHECK_TIMEOUT_SEC,
                            user=user,
                            supervised=False,
                            container=container,
                        )
                    else:
                        result = await self.exec(
                            command,
                            timeout_sec=_PATH_KIND_CHECK_TIMEOUT_SEC,
                            user=user,
                            supervised=False,
                        )
                    break
                except GKEExecStreamClosedError:
                    if attempt == 0:
                        await asyncio.sleep(0.5)
                        continue
                    raise
            if result is None:
                raise RuntimeError(f"Path kind check for {path!r} produced no result")
            if result.return_code == 124:
                raise TimeoutError(
                    f"Path kind check for {path!r} timed out after {_PATH_KIND_CHECK_TIMEOUT_SEC}s"
                )
            return result.return_code == 0
        except TimeoutError:
            self.logger.warning(
                f"Path kind check timed out for {path!r}; raising to trigger extension fallback"
            )
            raise
        except Exception as exc:
            self.logger.debug(f"Path kind check failed for {path!r}: {exc}")
            raise

    @override
    async def run_healthcheck(
        self, healthcheck: HealthcheckConfig | None = None
    ) -> None:
        """Run a healthcheck using lightweight direct exec without supervision overhead."""
        hc = (
            healthcheck if healthcheck is not None else self.task_env_config.healthcheck
        )
        if hc is None:
            return

        self.logger.debug(f"Running healthcheck: {hc.command}")

        start_time = time.monotonic()
        start_period_end = start_time + hc.start_period_sec
        consecutive_failures = 0

        while True:
            now = time.monotonic()
            in_start_period = now < start_period_end

            result = await self.exec(
                hc.command,
                timeout_sec=int(hc.timeout_sec),
                supervised=False,
            )

            if result.return_code == 0:
                self.logger.debug("Healthcheck passed")
                return

            self.logger.debug(
                f"Healthcheck failed (rc={result.return_code}, "
                f"in_start_period={in_start_period})"
            )

            if in_start_period:
                await asyncio.sleep(hc.start_interval_sec)
            else:
                consecutive_failures += 1
                if consecutive_failures >= hc.retries:
                    raise HealthcheckError(
                        f"Healthcheck failed after {hc.retries} consecutive "
                        f"retries: {hc.command}"
                    )
                await asyncio.sleep(hc.interval_sec)

    @override
    def _compose_service_transport(
        self, service: str | None
    ) -> ComposeServiceTransport:
        if not self._compose_mode and service not in ("main", MAIN_SERVICE_NAME):
            raise self._compose_unsupported(service)
        return _GKENativeComposeServiceTransport(self)

    @override
    async def stop_service(self, service: str) -> None:
        """Stop a container/service in the Pod using Zero-Entrypoint-Intrusion Lifecycle Control."""
        if self._compose_mode or service in ("main", MAIN_SERVICE_NAME):
            transport = _GKENativeComposeServiceTransport(self)
            await transport.stop_service(service)
        else:
            await super().stop_service(service)

    @staticmethod
    def _parse_task_mapping(
        raw: str | list[str] | dict[str, str] | None,
    ) -> dict[str, str]:
        """Parse a task-keyed option into a dict of task_name -> value.

        Shared by ``task_compute_classes`` and ``task_node_pools`` so both accept
        identical syntax.

        Accepts:
          - "task1=value1, task2=value2"
          - ["task1=value1", "task2=value2"]
          - {"task1": "value1", "task2": "value2"}
        """
        if not raw:
            return {}
        if isinstance(raw, dict):
            return {
                str(k).strip(): str(v).strip()
                for k, v in raw.items()
                if str(k).strip() and str(v).strip()
            }
        mapping: dict[str, str] = {}
        items: list[str] = []
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    items.extend(item.split(","))
        elif isinstance(raw, str):
            items = raw.split(",")

        for entry in items:
            entry = entry.strip()
            if not entry:
                continue
            for separator in ("=", ":"):
                if separator in entry:
                    task, value = entry.split(separator, 1)
                    if task.strip() and value.strip():
                        mapping[task.strip()] = value.strip()
                    break
        return mapping

    @staticmethod
    def _parse_task_compute_classes(
        raw: str | list[str] | dict[str, str] | None,
    ) -> dict[str, str]:
        """Parse task_compute_classes option into a dict of task_name -> compute_class."""
        return GKEEnvironment._parse_task_mapping(raw)

    @staticmethod
    def _parse_image_pull_secrets(raw: Any) -> list[str]:
        """Parse image_pull_secrets option into a list of Kubernetes Secret names."""
        if not raw:
            return []
        if isinstance(raw, str):
            trimmed = raw.strip()
            if trimmed.startswith("[") and trimmed.endswith("]"):
                try:
                    parsed = json.loads(trimmed)
                    if isinstance(parsed, list):
                        return [str(s).strip() for s in parsed if str(s).strip()]
                except Exception:
                    pass
            return [s.strip() for s in raw.split(",") if s.strip()]
        if isinstance(raw, (list, tuple, set)):
            return [str(s).strip() for s in raw if str(s).strip()]
        return []

    def _has_task_specific_compute_class(self) -> bool:
        task_key = self.environment_name
        task_base = self.environment_name.split("/")[-1]
        return bool(
            self.task_compute_classes.get(task_key)
            or self.task_compute_classes.get(task_base)
        )

    @property
    @override
    def _effective_storage_mb(self) -> int | None:
        storage_mb = super()._effective_storage_mb
        if (
            storage_mb is not None
            and self.max_storage_request_mb is not None
            and not self._has_task_specific_compute_class()
        ):
            return min(storage_mb, self.max_storage_request_mb)
        return storage_mb

    def _compose_needs_dind(self) -> bool:
        """Whether this Compose task's Pod runs an in-Pod Docker daemon (Shape B or C).

        Read from the placement ``start()`` resolves with the same arguments it
        passes to ``translate_compose``, so the answer always describes the Pod
        that is built. Before ``start()`` (the constructor's ComputeClass, which
        ``start()`` resolves again) and for non-Compose tasks it is ``False``.
        """
        plan = self._compose_placement
        return plan is not None and plan.shape in ("B", "C")

    def _effective_total_ephemeral_storage_mb(self) -> int:
        """Estimate the Pod's total ephemeral-storage request, in MiB.

        This exists to answer one question: does the Pod clear GKE Autopilot's
        10 GiB general-purpose ceiling, and therefore need the Performance
        compute class?

        Autopilot injects a 1024 MiB ephemeral-storage request into every
        container that does not declare one. Harbor declares one for exactly two
        containers -- ``main`` (the task's storage budget) and ``dind-engine``
        (``/var/lib/docker``) -- so every other container in the Pod contributes
        the injected default. This is each Compose sidecar.

        Only containers that add together are counted. Kubernetes derives a
        Pod's effective request as the higher of the sum over app containers
        (``restartPolicy: Always`` init containers included) and the maximum
        over regular init containers, per the resource-sharing rules in the
        Kubernetes init-container documentation. Harbor's regular init
        containers -- ``harbor-seed``, one-shot Compose services,
        ``dind-cache-*`` and ``compose-up-gate`` -- declare no ephemeral
        storage, so each contributes at most the injected default to a maximum
        that the sum already exceeds. Compose services that placement turns into
        one-shot init containers are nevertheless counted below as if they added
        together; that over-estimates by 1024 MiB each and biases towards
        Performance, which is the safe direction.

        For Compose tasks needing an in-Pod Docker daemon only ``dind-engine``'s
        *floor* is added, not the full estimate: the real figure depends on the
        sizes of the images the DinD services declare, and those references are
        not resolved to registry URLs until later in ``start()``. The floor is
        sufficient, because it already equals the ceiling on its own.
        """
        main_storage = self._effective_storage_mb or 0
        if not self._compose_mode:
            return main_storage

        defaulted_containers = 0
        try:
            all_compose_paths = []
            if self._environment_docker_compose_path.exists():
                all_compose_paths.append(self._environment_docker_compose_path)
            all_compose_paths.extend(self.extra_docker_compose_paths)
            found_services: set[str] = set()
            for cpath in all_compose_paths:
                if cpath.exists():
                    data = yaml.safe_load(cpath.read_text()) or {}
                    services = data.get("services", {})
                    if isinstance(services, dict):
                        found_services.update(services.keys())
            defaulted_containers = sum(
                1 for name in found_services if name != MAIN_SERVICE_NAME
            )
        except (OSError, yaml.YAMLError) as exc:
            # Guessing here picks the compute class for the whole trial, so it
            # must not be silent. One sidecar is the smallest non-zero guess.
            self.logger.warning(
                "Could not parse compose services for Autopilot storage sizing "
                "(%s). Assuming one sidecar; the Pod may be placed on the "
                "general-purpose class when it needs Performance.",
                exc,
            )
            defaulted_containers = 1

        total = main_storage + (
            defaulted_containers * _GKE_AUTOPILOT_DEFAULT_CONTAINER_STORAGE_MB
        )
        if self._compose_needs_dind():
            total += DIND_STORAGE_FLOOR_MB
        return total

    def _resolve_active_node_pool(self) -> str | None:
        """Resolve the GKE node pool for the current task.

        - Priority 1: ``task_node_pools`` entry matching ``self.environment_name``
          or its basename. An explicit per-task mapping always wins.
        - Priority 2: ``dind_node_pool`` when the task uses Docker-in-Docker.
        - Priority 3: the global ``node_pool`` option.
        - Otherwise ``None``, leaving placement to the scheduler or a ComputeClass.
        """
        task_key = self.environment_name
        task_base = self.environment_name.split("/")[-1]
        task_specific_pool = self.task_node_pools.get(
            task_key
        ) or self.task_node_pools.get(task_base)
        if task_specific_pool:
            return task_specific_pool
        if getattr(self, "dind_node_pool", None) and (
            self._compose_needs_dind() or bool(self._dind_services)
        ):
            return self.dind_node_pool
        return self.node_pool

    def _warn_if_unfenced_privileged_dind(self, *, is_autopilot: bool) -> None:
        """Emit a warning when a DinD Pod runs privileged on GKE Standard without a dedicated node pool or gVisor."""
        if is_autopilot or not self._dind_services:
            return
        rc = self.kwargs.get("runtime_class_name")
        if (
            rc == "gvisor"
            or getattr(self, "dind_node_pool", None)
            or self._resolve_active_node_pool()
        ):
            return
        self.logger.warning(
            "Task %r uses privileged Docker-in-Docker (dind-engine) on GKE Standard cluster %r "
            "without a dedicated node pool (`--ek dind_node_pool=<pool>`) or gVisor "
            "(`--ek runtime_class_name=gvisor`). Privileged containers share the host kernel and "
            "devices; schedule DinD workloads onto a dedicated node pool with a least-privilege "
            "GCE service account for multi-tenant isolation.",
            self.environment_name,
            self.cluster_name,
        )

    def _resolve_active_machine_type_with_source(
        self,
    ) -> tuple[str | None, str | None]:
        """Resolve the machine type and record where the answer came from."""
        task_key = self.environment_name
        task_base = self.environment_name.split("/")[-1]
        task_specific = self.task_machine_types.get(
            task_key
        ) or self.task_machine_types.get(task_base)
        if task_specific:
            return task_specific, "task_machine_types"
        if self.machine_type:
            return self.machine_type, "machine_type"
        return None, None

    def _resolve_active_machine_type(self) -> str | None:
        """Resolve the GCE machine type for the current task.

        - Priority 1: ``task_machine_types`` entry matching ``self.environment_name``
          or its basename.
        - Priority 2: the global ``machine_type`` option.
        """
        machine_type, _ = self._resolve_active_machine_type_with_source()
        return machine_type

    def _validate_placement(self, caps: ClusterCapabilities) -> None:
        """Reject contradictory placement settings for the current task.

        Each task must resolve to one placement mechanism. Silent precedence is
        permitted only when a job-wide ``compute_class`` is overridden for one
        specific task via ``task_node_pools`` (allowing a ComputeClass job to
        route an outlier task onto a dedicated pool). Every other combination of
        pool, ComputeClass, and machine type is rejected up front.
        """
        task_key = self.environment_name
        task_base = self.environment_name.split("/")[-1]
        task_pool = self.task_node_pools.get(task_key) or self.task_node_pools.get(
            task_base
        )
        task_cc = self.task_compute_classes.get(
            task_key
        ) or self.task_compute_classes.get(task_base)
        active_pool = task_pool or self.node_pool
        active_mt, mt_source = self._resolve_active_machine_type_with_source()

        # 1. Autopilot has no user node pools.
        if caps.is_autopilot and active_pool:
            raise PlacementConflictError(
                f"Task {self.environment_name!r} specifies node pool "
                f"{active_pool!r}, but cluster {self.cluster_name!r} is a GKE "
                f"Autopilot cluster (which has no user-managed node pools). Use "
                f"`compute_class` / `task_compute_classes` or `machine_type` / "
                f"`task_machine_types` instead."
            )

        # 2. Job-wide node_pool + any ComputeClass (job-wide or per-task).
        if self.node_pool and (self.compute_class or self.task_compute_classes):
            raise PlacementConflictError(
                f"Task {self.environment_name!r} combines a job-wide "
                f"`node_pool={self.node_pool!r}` with ComputeClass settings "
                f"(`compute_class` or `task_compute_classes`). Use `compute_class` "
                f"for the job and `task_node_pools` only for per-task exceptions, "
                f"or remove the ComputeClass settings."
            )

        # 3. Same task mapped in both task_node_pools and task_compute_classes.
        if task_pool and task_cc:
            raise PlacementConflictError(
                f"Task {self.environment_name!r} is mapped in both "
                f"`task_node_pools` ({task_pool!r}) and `task_compute_classes` "
                f"({task_cc!r}). Specify only one placement target for the task."
            )

        # 4. Machine type + ComputeClass on the same task.
        effective_explicit_cc = task_cc or (
            self.compute_class if not active_pool else None
        )
        if active_mt and effective_explicit_cc:
            cc_source = "task_compute_classes" if task_cc else "compute_class"
            raise PlacementConflictError(
                f"Task {self.environment_name!r} combines `{mt_source}={active_mt!r}` "
                f"with `{cc_source}={effective_explicit_cc!r}`. A ComputeClass "
                f"selects its own machine family via its nodePoolAutoCreation "
                f"rules; remove `{mt_source}` or `{cc_source}` for this task."
            )

        # 5. Machine type + node pool on the same task: if the pool's machine
        #    type is known and belongs to a different family, the Pod would emit
        #    two mutually impossible nodeSelectors and pend forever.
        if active_mt and active_pool and caps.node_pool_machine_types:
            req_family = active_mt.split("-")[0]
            pool_mt = dict(caps.node_pool_machine_types).get(active_pool)
            if pool_mt and req_family:
                pool_family = pool_mt.split("-")[0]
                if pool_family and pool_family != req_family:
                    raise PlacementConflictError(
                        f"Task {self.environment_name!r} targets node pool "
                        f"{active_pool!r} (machine type {pool_mt!r}, family "
                        f"{pool_family!r}) and also requires `{mt_source}="
                        f"{active_mt!r}` (family {req_family!r}). No node in "
                        f"{active_pool!r} can match both selectors."
                    )

    def _assert_machine_type_satisfiable(self, caps: ClusterCapabilities) -> None:
        """Confirm the cluster can satisfy a task's machine type pin.

        A ``machine_type`` pin (for example ``n2-standard-4`` or ``n2``) selects
        the machine family via ``cloud.google.com/machine-family`` and requires
        at least the vCPU count encoded in the machine type name (when a size
        suffix is present). On a Standard cluster with no matching pool of at
        least that size and no node auto-provisioning, the Pod would pend until
        the trial times out.

        Deliberately permissive: this raises only when the inventory is known,
        non-empty, and lacks a qualifying pool while neither Autopilot nor node
        auto-provisioning can create one.
        """
        machine_type, _ = self._resolve_active_machine_type_with_source()
        if not machine_type:
            return
        if not caps.available_machine_types:
            # Inventory unknown (probe failed, or Autopilot). Never block on
            # absence of evidence.
            return
        if caps.is_autopilot or caps.node_auto_provisioning_enabled:
            return

        family = machine_type.split("-")[0]
        has_explicit_size = "-" in machine_type
        min_vcpus = _parse_machine_type_vcpus(machine_type) if has_explicit_size else 1

        family_matches = [
            t for t in caps.available_machine_types if t.split("-")[0] == family
        ]
        qualifying = [
            t for t in family_matches if _parse_machine_type_vcpus(t) >= min_vcpus
        ]
        if qualifying:
            return

        if family_matches:
            raise UnsatisfiableMachineTypeError(
                f"Task {self.environment_name!r} requires machine type "
                f"{machine_type!r} (family {family!r} with at least {min_vcpus} "
                f"vCPUs), but cluster {self.cluster_name!r} only offers smaller "
                f"{family!r} pools [{', '.join(family_matches)}] and has no node "
                f"auto-provisioning. Add a larger {family!r} node pool, or "
                f"override with `--ek task_machine_types={self.environment_name}=<type>`."
            )

        raise UnsatisfiableMachineTypeError(
            f"Task {self.environment_name!r} requires machine type "
            f"{machine_type!r} (family {family!r}), but cluster "
            f"{self.cluster_name!r} offers "
            f"[{', '.join(caps.available_machine_types)}] and has no node "
            f"auto-provisioning. Add a matching node pool, or override with "
            f"`--ek task_machine_types={self.environment_name}=<type>`."
        )

    def _resolve_dind_storage_mb(self) -> int | None:
        """Resolve an explicit ``/var/lib/docker`` reservation for the current task.

        - Priority 1: ``task_dind_storage_mb`` entry matching
          ``self.environment_name`` or its basename.
        - Priority 2: the global ``dind_storage_mb`` option.
        - Otherwise ``None``, which leaves the translator to estimate the size
          from the images the task's DinD services declare.
        """
        task_key = self.environment_name
        task_base = self.environment_name.split("/")[-1]
        raw = self.task_dind_storage_mb.get(task_key) or self.task_dind_storage_mb.get(
            task_base
        )
        if raw is not None:
            try:
                return int(raw)
            except ValueError:
                self.logger.warning(
                    f"Ignoring non-integer task_dind_storage_mb value {raw!r} "
                    f"for task {task_key!r}; falling back to the estimate."
                )
        return self.dind_storage_mb

    def _resolve_active_compute_class(self, *, is_autopilot: bool) -> str | None:
        """Resolve the ComputeClass for the current task.

        A node pool and a ComputeClass emit mutually exclusive node selectors
        (``cloud.google.com/gke-nodepool`` and ``cloud.google.com/compute-class``),
        so whenever an effective node pool is resolved the ComputeClass is dropped.

        On GKE Standard clusters:
        - Checks the effective node pool: if set, returns None.
        - Checks task_compute_classes matching self.environment_name (or basename).
        - Falls back to self.compute_class if specified.

        On GKE Autopilot clusters:
        - Checks the effective node pool: if set, returns None.
        - Priority 1: task_compute_classes matching self.environment_name (or basename).
          Explicit task-level mapping always takes precedence.
        - Priority 2: Accelerators (GPU/TPU). Pods requesting GPUs or TPUs use native
          accelerator node selectors (cloud.google.com/gke-accelerator or
          cloud.google.com/gke-tpu-accelerator) and must not have a CPU ComputeClass assigned.
        - Priority 3: Explicit global --compute-class applied to all remaining tasks.
        - Priority 4: If an effective machine type is resolved (global
          ``machine_type`` or ``task_machine_types``), leave unset so the
          machine-family node selector is not overridden.
        - Priority 5: If aggregate ephemeral storage request > 10 GiB (10,240 MiB),
          including multi-container workloads, automatically select "Performance".
        - Priority 6: Otherwise, leave unset (standard general-purpose Autopilot).
        """
        if self._resolve_active_node_pool():
            return None

        task_key = self.environment_name
        task_base = self.environment_name.split("/")[-1]
        task_specific_class = self.task_compute_classes.get(
            task_key
        ) or self.task_compute_classes.get(task_base)

        if not is_autopilot:
            if task_specific_class:
                return task_specific_class
            return self.compute_class

        # --- GKE Autopilot Resolution Hierarchy ---
        # Priority 1: Explicit per-task ComputeClass mapping
        if task_specific_class:
            return task_specific_class

        # Priority 2: Accelerators (GPU / TPU) use native GKE accelerator selectors
        if self._effective_gpus > 0 or (
            self.task_env_config and self.task_env_config.tpu is not None
        ):
            return None

        # Priority 3: Explicit global --compute-class for non-accelerator tasks
        if self.compute_class:
            return self.compute_class

        # Priority 4: An explicit or per-task machine_type selector takes
        # precedence over storage-driven Performance auto-promotion so that
        # machine-family pins are not suppressed.
        if self._resolve_active_machine_type():
            return None

        # Priority 5: Auto-promote to "Performance" if ephemeral storage > 10 GiB
        effective_storage = self._effective_total_ephemeral_storage_mb()
        if effective_storage > _GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB:
            return "Performance"

        # Priority 6: Default general-purpose Autopilot (unset)
        return None

    async def _upload_seed_bind_mounts(self, annotations: dict[str, str]) -> None:
        """Stream large compose bind mounts into harbor-seed container and signal .seed-ready."""
        from harbor_gke_ext.compose_spec import resolve_contained_task_path

        raw_binds = annotations.get("harbor.dev/compose-bind-mounts")
        if not raw_binds:
            return
        try:
            bind_mounts = json.loads(raw_binds)
        except Exception as exc:
            self.logger.warning(
                f"Failed to parse compose-bind-mounts annotation: {exc}"
            )
            return

        env_dir = self.environment_dir.resolve()
        task_dir = Path(
            getattr(self, "task_dir", None) or self.environment_dir.parent
        ).resolve()

        for rel_src, target_info in bind_mounts.items():
            if isinstance(target_info, dict):
                container_name = target_info.get("container") or "harbor-seed"
                target_dir = target_info.get("target_dir", "")
                local_path_str = target_info.get("local_path")
            else:
                container_name = "harbor-seed"
                target_dir = str(target_info)
                local_path_str = None
            if not target_dir:
                continue
            raw_candidate = local_path_str if local_path_str else str(rel_src)
            local_path = resolve_contained_task_path(
                raw_candidate,
                base_dir=env_dir,
                allowed_roots=(env_dir, task_dir),
                field_name="seed-bind-mount",
            )
            if not local_path.exists():
                continue
            if local_path.is_file():
                target_file = f"{target_dir.rstrip('/')}/{local_path.name}"
                await self.upload_file(
                    local_path, target_file, container=container_name
                )
            elif local_path.is_dir():
                await self.upload_dir(local_path, target_dir, container=container_name)

        await self.exec(
            "touch /harbor/compose-binds/.seed-ready",
            container="harbor-seed",
            timeout_sec=15,
        )

    async def _wait_for_pod_ready(self, timeout_sec: int = 1200):
        self.logger.debug(f"Waiting for pod {self.pod_name} to be ready...")
        poll_interval = 3.0
        start_time = time.monotonic()
        last_logged = 0
        seen_warning_events: set[tuple[str, str]] = set()

        while time.monotonic() - start_time < timeout_sec:
            elapsed = int(time.monotonic() - start_time)
            try:
                pod = await asyncio.to_thread(
                    self._api.read_namespaced_pod,
                    name=self.pod_name,
                    namespace=self.namespace,
                )
                if self._is_pod_lost(pod):
                    await self._follow_job_replacement_pod()
                    await asyncio.sleep(poll_interval)
                    continue

                try:
                    events = await asyncio.to_thread(
                        self._api.list_namespaced_event,
                        namespace=self.namespace,
                        field_selector=f"involvedObject.name={self.pod_name}",
                    )
                except ApiException as ev_err:
                    self.logger.debug(f"Could not retrieve pod events: {ev_err}")
                    events = None

                for ev in getattr(events, "items", None) or []:
                    if ev.type != "Warning" or not ev.message:
                        continue
                    event_key = (ev.reason or "", ev.message or "")
                    if event_key in seen_warning_events:
                        continue
                    seen_warning_events.add(event_key)
                    self.logger.debug(f"[Kubernetes Event] {ev.reason}: {ev.message}")
                    if ev.reason == "NotTriggerScaleUp":
                        # The cluster autoscaler has decided no node group can
                        # be grown to fit this Pod. Nothing changes by waiting,
                        # and its message names the shortfall.
                        raise RuntimeError(
                            "Pod cannot be scheduled and the cluster autoscaler "
                            f"will not scale up for it: {ev.message}. "
                            f"Harbor requested {self._pod_request_summary()}."
                        )

                annotations = (
                    (
                        self._created_pod.metadata.annotations
                        if self._created_pod and self._created_pod.metadata
                        else None
                    )
                    or (pod.metadata.annotations if pod.metadata else None)
                    or {}
                )
                if (
                    annotations.get("harbor.dev/seed-required") == "true"
                    and not self._seed_uploaded
                ):
                    init_statuses = list(pod.status.init_container_statuses or [])
                    seed_running = any(
                        c.name == "harbor-seed" and c.state and c.state.running
                        for c in init_statuses
                    )
                    if seed_running:
                        await self._upload_seed_bind_mounts(annotations)
                        self._seed_uploaded = True

                if self._dind_services and not self._dind_netpol_applied:
                    init_statuses = list(pod.status.init_container_statuses or [])
                    dind_pull_running = any(
                        c.name == "dind-pull" and c.state and c.state.running
                        for c in init_statuses
                    )
                    if dind_pull_running:
                        try:
                            probe_res = await self.exec(
                                "test -f /harbor/dind-images/.ready-for-netpol",
                                container="dind-engine",
                                supervised=False,
                                timeout_sec=10,
                            )
                            if probe_res.return_code == 0:
                                needs_netpol = (
                                    self.network_policy.network_mode
                                    != NetworkMode.PUBLIC
                                    or not self.allow_metadata_server
                                )
                                if needs_netpol:
                                    await self._apply_network_policy(
                                        self.network_policy
                                    )
                                    settlement_sec = float(
                                        self.kwargs.get(
                                            "network_policy_settlement_sec", 1.0
                                        )
                                    )
                                    if settlement_sec > 0:
                                        await asyncio.sleep(settlement_sec)
                                await self.exec(
                                    "touch /harbor/dind-images/.netpol-applied",
                                    container="dind-engine",
                                    supervised=False,
                                    timeout_sec=10,
                                )
                                self._dind_netpol_applied = True
                                self.logger.debug(
                                    f"Applied restrictive NetworkPolicy and released dind-pull handshake for pod {self.pod_name}"
                                )
                        except Exception as dind_np_err:
                            self.logger.debug(
                                f"Waiting for dind-pull .ready-for-netpol handshake: {dind_np_err}"
                            )

                if (
                    annotations.get("harbor.dev/post-main-sidecars")
                    and not self._main_gate_released
                ):
                    c_statuses = list(pod.status.container_statuses or [])
                    main_ready = any(
                        c.name == MAIN_SERVICE_NAME and bool(c.ready)
                        for c in c_statuses
                    )
                    if main_ready:
                        try:
                            await self.exec(
                                "mkdir -p /harbor/gates && touch /harbor/gates/main-ready",
                                container=MAIN_SERVICE_NAME,
                                timeout_sec=15,
                            )
                            self._main_gate_released = True
                            self.logger.debug(
                                f"Released '/harbor/gates/main-ready' gate for post-main sidecars: "
                                f"{annotations.get('harbor.dev/post-main-sidecars')}"
                            )
                        except Exception as gate_err:
                            self.logger.debug(
                                f"Waiting to release main-ready gate: {gate_err}"
                            )

                all_statuses = list(pod.status.container_statuses or []) + list(
                    pod.status.init_container_statuses or []
                )

                if pod.status.phase == "Running":
                    if pod.status.container_statuses and all(
                        c.ready for c in pod.status.container_statuses
                    ):
                        self._pod_ready = True
                        self.logger.debug(f"Pod {self.pod_name} is ready!")
                        # Must happen before the Pod is torn down: container
                        # logs are destroyed with the Pod object, and this is
                        # the only record of what the DinD plane actually did.
                        # Never let a diagnostics failure fail a healthy trial.
                        try:
                            infra_logs = await self._collect_infra_container_logs(pod)
                            if infra_logs:
                                self.logger.debug(
                                    f"DinD infrastructure diagnostics:\n{infra_logs}"
                                )
                        except Exception as diag_err:
                            self.logger.debug(
                                f"Could not collect DinD infrastructure logs: {diag_err}"
                            )
                        return

                elif pod.status.phase in ["Failed", "Unknown", "Error"]:
                    for c in all_statuses:
                        await self._check_container_port_collision(c.name)
                    error_details = self._get_pod_failure_summary(pod)
                    failed_logs = await self._collect_failed_container_logs(pod)
                    if failed_logs:
                        error_details = f"{error_details}\n{failed_logs}"
                    raise RuntimeError(f"Pod failed to start: {error_details}")

                elif pod.status.phase == "Pending":
                    for c in all_statuses:
                        if c.state and c.state.waiting and c.state.waiting.reason:
                            if (
                                "ImagePullBackOff" in c.state.waiting.reason
                                or "ErrImagePull" in c.state.waiting.reason
                            ):
                                raise RuntimeError(
                                    f"Failed to pull image: {c.state.waiting.message or c.state.waiting.reason}"
                                )
                            if c.state.waiting.reason == "CrashLoopBackOff":
                                await self._check_container_port_collision(c.name)
                        elif (
                            c.state
                            and c.state.terminated
                            and c.state.terminated.exit_code != 0
                        ):
                            await self._check_container_port_collision(c.name)

                if elapsed - last_logged >= 10:
                    self.logger.debug(
                        f"Pod status: {pod.status.phase} ({elapsed}s elapsed)"
                    )
                    last_logged = elapsed

            except ApiException as e:
                if e.status != 404:
                    raise RuntimeError(f"Kubernetes API error: {e.status} - {e.reason}")
                # The Pod is gone, for example force-deleted after its node was lost.
                await self._follow_job_replacement_pod()

            await asyncio.sleep(poll_interval)

        raise RuntimeError(f"Pod not ready after {timeout_sec} seconds")

    @staticmethod
    def _is_pod_lost(pod: Any) -> bool:
        """Whether the Pod is being deleted or was disrupted into ``Failed``.

        Node-pressure eviction leaves a ``Failed`` Pod with ``DisruptionTarget``
        and no deletion timestamp. The condition alone is not enough: Kubernetes
        may set it and then not delete the Pod.
        """
        if GKEEnvironment._is_pod_terminating(pod):
            return True
        status = getattr(pod, "status", None)
        return getattr(status, "phase", None) == "Failed" and any(
            c.type == "DisruptionTarget" and c.status == "True"
            for c in (getattr(status, "conditions", None) or [])
        )

    async def _follow_job_replacement_pod(self) -> None:
        """Switch to the Pod the Job created to replace a lost one.

        The Job replaces a disrupted Pod (see `build_job`), but only after the
        lost Pod is terminal, so finding no replacement yet is normal. A terminal
        Job creates no more Pods, so that fails at once. Only the bring-up
        follows a replacement: once `start()` returns, the trial's state lives
        in its Pod and a lost Pod is a lost trial.
        """
        pods = await asyncio.to_thread(
            self._api.list_namespaced_pod,
            namespace=self.namespace,
            label_selector=self._job_pod_label_selector(),
        )
        for candidate in getattr(pods, "items", None) or []:
            name = candidate.metadata.name
            if (
                name != self.pod_name
                and candidate.status.phase in ("Pending", "Running")
                and not self._is_pod_lost(candidate)
            ):
                self.logger.warning(
                    f"Pod {self.pod_name} was lost before the trial started; "
                    f"continuing with replacement Pod {name} from Job {self.job_name}."
                )
                self.pod = candidate
                self.pod_name = name
                # Earned by the lost Pod; its replacement starts from scratch.
                self._seed_uploaded = False
                self._dind_netpol_applied = False
                self._main_gate_released = False
                return

        job = await asyncio.to_thread(
            self._batch_api.read_namespaced_job,
            name=self.job_name,
            namespace=self.namespace,
        )
        for condition in getattr(job.status, "conditions", None) or []:
            if condition.type in ("Failed", "Complete") and condition.status == "True":
                raise TrialContainerLostError(
                    f"Pod {self.pod_name} was lost before the trial started and "
                    f"Job {self.job_name} will not replace it "
                    f"({condition.type}: {condition.reason}: {condition.message})."
                )

    def _pod_request_summary(self) -> str:
        """Describe what the created Pod asked the scheduler for.

        Only reservations matter for scheduling, so limits are omitted. The
        Pod-level block usually carries the task's CPU/memory budget; Guaranteed
        direct Pods carry it on the container instead. Ephemeral storage has no
        Pod-level equivalent and is always reported per container.
        """
        pod = self._created_pod
        spec = getattr(pod, "spec", None) if pod else None
        if spec is None:
            return "an unknown reservation (Pod spec unavailable)"

        parts: list[str] = []
        pod_resources = getattr(spec, "resources", None)
        pod_requests = (
            getattr(pod_resources, "requests", None) if pod_resources else None
        )
        if pod_requests:
            parts.append(
                "pod-level "
                + ", ".join(f"{k}={v}" for k, v in sorted(pod_requests.items()))
            )

        for container in list(spec.init_containers or []) + list(spec.containers or []):
            reqs = getattr(container.resources, "requests", None) or {}
            # Guaranteed direct Pods carry their CPU/memory budget on the
            # container (see build_direct_pod), so report it where it lives.
            for key in ("cpu", "memory", "ephemeral-storage"):
                value = reqs.get(key)
                if value:
                    parts.append(f"{container.name} {key}={value}")

        return "; ".join(parts) if parts else "no explicit reservation"

    def _get_pod_failure_summary(self, pod) -> str:
        reasons = []
        if pod.status.reason:
            reasons.append(f"Reason: {pod.status.reason}")
        if pod.status.message:
            reasons.append(f"Message: {pod.status.message}")
        if pod.status.reason == "DeadlineExceeded":
            deadline = (
                getattr(pod.spec, "active_deadline_seconds", None) if pod.spec else None
            )
            reasons.append(
                f"Diagnostic: Pod exceeded its active deadline (spec.activeDeadlineSeconds={deadline}s) "
                "and was terminated by the Kubernetes Kubelet."
            )
        all_statuses = list(pod.status.container_statuses or []) + list(
            pod.status.init_container_statuses or []
        )
        if all_statuses:
            for c in all_statuses:
                if c.state.waiting:
                    reasons.append(
                        f"Container {c.name} waiting: {c.state.waiting.reason}"
                    )
                elif c.state.terminated:
                    reasons.append(
                        f"Container {c.name} terminated: {c.state.terminated.reason} "
                        f"(exit code {c.state.terminated.exit_code})"
                    )
                    term_msg = getattr(c.state.terminated, "message", "") or ""
                    if c.state.terminated.exit_code == 127 or "/bin/sh" in term_msg:
                        reasons.append(
                            "Diagnostic: Container failed with exit code 127 (/bin/sh not found)."
                        )
        return "; ".join(reasons) if reasons else "No failure details available"

    async def _collect_failed_container_logs(self, pod) -> str:
        """Collect the tail of logs for every container that terminated with a non-zero exit code.

        Container logs are destroyed once the Pod object is deleted, so they must be
        captured while the failed Pod still exists; otherwise failures such as a
        Shape B `compose-up-gate` error surface only as a bare exit code.
        """
        all_statuses = list(pod.status.container_statuses or []) + list(
            pod.status.init_container_statuses or []
        )
        sections: list[str] = []
        for c in all_statuses:
            terminated = c.state.terminated if c.state else None
            if terminated is None or terminated.exit_code == 0:
                continue
            try:
                log_text = await asyncio.to_thread(
                    self._api.read_namespaced_pod_log,
                    name=self.pod_name,
                    namespace=self.namespace,
                    container=c.name,
                    tail_lines=_FAILED_CONTAINER_LOG_TAIL_LINES,
                )
            except Exception as log_err:
                sections.append(
                    f"--- logs for failed container '{c.name}' unavailable: {log_err} ---"
                )
                continue
            body = (log_text or "").strip()
            sections.append(
                f"--- last {_FAILED_CONTAINER_LOG_TAIL_LINES} log lines of failed container '{c.name}' "
                f"(exit code {terminated.exit_code}) ---\n"
                f"{body or '(no output)'}"
            )
        return "\n".join(sections)

    async def _collect_infra_container_logs(self, pod) -> str:
        """Capture the logs of Harbor's DinD infrastructure containers on success.

        ``_collect_failed_container_logs`` only fires when a container exits
        non-zero, so a *successful* Shape B start-up previously left no record
        at all. That is the wrong default: the DinD plane is where images are
        reconstructed and where the GPU is wired up, and both are things you
        need evidence for even -- especially -- when the trial passes. Without
        this, a passing GPU trial cannot tell you which device it actually ran
        on, and a silently metadata-lossy image import looks identical to a
        clean one.

        Only Harbor's own containers are captured, never task services: agent
        and task output already has dedicated destinations, and duplicating it
        here would bury the infrastructure signal.

        ``_INFRA_CONTAINER_LOG_LIMIT_BYTES`` reads from the START of the stream rather than using
        ``tail_lines``. ``dind-engine`` prints the GPU probe result and then
        hands off to a very chatty ``dockerd``, so a tail would reliably
        discard exactly the lines worth keeping.
        """
        from harbor_gke_ext.compose_translator import (
            COMPOSE_UP_GATE_CONTAINER,
            DIND_ENGINE_CONTAINER,
            DIND_PULL_CONTAINER,
        )

        def _is_infra(name: str) -> bool:
            return name in (
                DIND_ENGINE_CONTAINER,
                DIND_PULL_CONTAINER,
                COMPOSE_UP_GATE_CONTAINER,
            )

        spec = pod.spec
        if spec is None:
            return ""
        names = [
            c.name
            for c in list(spec.init_containers or []) + list(spec.containers or [])
            if _is_infra(c.name)
        ]
        if not names:
            return ""

        sections: list[str] = []
        for name in names:
            try:
                log_text = await asyncio.to_thread(
                    self._api.read_namespaced_pod_log,
                    name=self.pod_name,
                    namespace=self.namespace,
                    container=name,
                    limit_bytes=_INFRA_CONTAINER_LOG_LIMIT_BYTES,
                )
            except Exception as log_err:
                sections.append(f"--- '{name}' logs unavailable: {log_err} ---")
                continue
            body = (log_text or "").strip()
            sections.append(
                f"--- first {_INFRA_CONTAINER_LOG_LIMIT_BYTES}B of DinD infrastructure container "
                f"'{name}' ---\n{body or '(no output)'}"
            )
        return "\n".join(sections)

    async def _check_container_port_collision(self, container_name: str) -> None:
        try:
            log_text = await asyncio.to_thread(
                self._api.read_namespaced_pod_log,
                name=self.pod_name,
                namespace=self.namespace,
                container=container_name,
                tail_lines=100,
            )
            if log_text:
                collision_signatures = [
                    "address already in use",
                    "eaddrinuse",
                    "failed to bind to port",
                ]
                if any(sig in log_text.lower() for sig in collision_signatures):
                    raise RuntimeError(
                        f"Port collision detected in service '{container_name}': {log_text.strip()}\n"
                        "In native compose mode, all sibling containers share localhost (127.0.0.1).\n"
                        "To isolate container network namespaces, rerun with: --ek compose_mode=dind"
                    )
        except ApiException:
            pass
