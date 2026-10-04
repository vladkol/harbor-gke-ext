from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import contextlib
import functools
import os
import re
import shutil
import subprocess
import threading
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, override

from harbor_gke_ext.constants import (
    _GKE_API_CONNECT_TIMEOUT_SEC,
    _GKE_API_READ_TIMEOUT_SEC,
    _GKE_EXEC_SEMAPHORE_LIMIT,
)
from harbor_gke_ext.exec_stream import shutdown_exec_reactor
from harbor.utils.logger import logger
from harbor.utils.optional_import import MissingExtraError

try:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.client.rest import ApiException

    _HAS_KUBERNETES = True
    _ApiClientBase = k8s_client.ApiClient
except ImportError:
    _HAS_KUBERNETES = False
    _ApiClientBase = object  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.client.rest import ApiException

    _ApiClientBase = k8s_client.ApiClient

_GKE_EXEC_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None


def _get_exec_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Return the global process-level thread pool for blocking exec handshakes.

    Only the WebSocket handshake (TLS and HTTP upgrade) runs here. Established
    streams are served by the exec reactor thread (see ``exec_stream``), so this
    pool no longer bounds how many commands can run at once.
    """
    global _GKE_EXEC_EXECUTOR
    if _GKE_EXEC_EXECUTOR is None:
        _GKE_EXEC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
            max_workers=_GKE_EXEC_SEMAPHORE_LIMIT,
            thread_name_prefix="gke-exec",
        )
    return _GKE_EXEC_EXECUTOR


def _shutdown_exec_executor() -> None:
    """Terminate the exec handshake pool and the exec reactor to prevent exit hangs."""
    global _GKE_EXEC_EXECUTOR
    shutdown_exec_reactor()
    if _GKE_EXEC_EXECUTOR is not None:
        try:
            _GKE_EXEC_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        _GKE_EXEC_EXECUTOR = None


def _ensure_file_descriptor_limit() -> None:
    """Ensure RLIMIT_NOFILE soft limit has sufficient headroom for high concurrency.

    At -n 1000 with the exec handshake limit at its 128 maximum, Harbor requires file descriptors for:
    - 1,000 active trial.log files (1 per active trial)
    - Up to 128 active socket descriptors during exec launch bursts
    - Runtime handles and standard streams (~50)
    Total minimum needed is ~1,200. The target is 65,536 (or the system hard limit).

    Invariants:
    - Never lowers the limit if already higher.
    - Caps at the system hard limit.
    - Swallows exceptions with a warning log so failure never blocks execution.
    """
    target = 65536
    try:
        import resource
    except ImportError:
        return

    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft >= target:
            return

        # Never lower the limit, and cap at hard limit
        new_soft = min(max(soft, target), hard)
        if new_soft > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
            logger.debug(
                f"Raised RLIMIT_NOFILE soft limit from {soft} to {new_soft} (hard limit: {hard})"
            )
    except Exception as e:
        logger.warning(f"Could not auto-raise RLIMIT_NOFILE soft limit: {e}")


def _extract_api_status_code(e: ApiException) -> int:
    """Extract true HTTP status code from ApiException, handling WebSocket status erasure."""
    if e.status and e.status != 0:
        return e.status
    text = f"{e.reason or ''} {e.body or ''} {str(e)}"
    match = re.search(r"(?:Handshake status|\"code\":\s*)\s*(\d{3})", text)
    return int(match.group(1)) if match else 0


def is_gcp_zone(location: str) -> bool:
    """Check if location string has a zonal suffix (e.g. 'us-central1-a')."""
    return bool(re.match(r"^[a-z]+-[a-z0-9]+-[a-z]$", location.strip().lower()))


def derive_region(location: str) -> str:
    """Extract the parent region from a zone, or return the region as-is."""
    return location.rsplit("-", 1)[0] if is_gcp_zone(location) else location


@functools.cache
def ensure_gcloud_ready() -> None:
    """Verify once per process that gcloud CLI is installed and authenticated.

    A failed check raises, and ``functools.cache`` does not store raised calls,
    so a later call checks again.
    """
    if not shutil.which("gcloud"):
        raise SystemExit(
            "GKE requires the gcloud CLI to be installed. "
            "See https://docs.cloud.google.com/sdk/docs/install-sdk"
        )
    adc_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if adc_path and os.path.isfile(adc_path):
        return
    try:
        res = subprocess.run(
            [
                "gcloud",
                "auth",
                "list",
                "--filter=status:ACTIVE",
                "--format=value(account)",
                "--quiet",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SystemExit(
            f"Failed to verify gcloud authentication status: {exc}"
        ) from exc
    if res.returncode != 0 or not res.stdout.strip():
        raise SystemExit(
            "GKE requires an active authenticated gcloud account. "
            "Run 'gcloud auth login' or 'gcloud auth activate-service-account' to authenticate."
        )


def parse_gke_context_identifier(identifier: str) -> tuple[str, str, str] | None:
    """Parse canonical GKE context/cluster string 'gke_<project>_<location>_<cluster>'."""
    if not identifier or not identifier.startswith("gke_"):
        return None
    parts = identifier.split("_", 3)
    if len(parts) == 4 and parts[0] == "gke" and parts[1] and parts[2] and parts[3]:
        return parts[1], parts[2], parts[3]
    return None


def get_active_gke_context() -> tuple[str, str, str] | None:
    """Return (project_id, location, cluster_name) from the active kubectl context if it is a GKE context."""
    if not _HAS_KUBERNETES:
        return None
    try:
        _, active = k8s_config.list_kube_config_contexts(
            config_file=os.environ.get("KUBECONFIG") or None
        )
    except Exception:
        return None
    if not active or not isinstance(active, dict):
        return None
    ctx_cluster = str((active.get("context") or {}).get("cluster") or "")
    ctx_name = str(active.get("name") or "")
    for candidate in (ctx_cluster, ctx_name):
        parsed = parse_gke_context_identifier(candidate)
        if parsed is not None:
            return parsed
    return None


def resolve_default_project_id() -> str:
    """Resolve default GCP project from env vars or once-per-process gcloud config."""
    env_proj = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("CLOUDSDK_CORE_PROJECT")
        or os.environ.get("GCP_PROJECT")
    )
    if env_proj and env_proj.strip():
        return env_proj.strip()
    return _gcloud_default_project_id()


@functools.cache
def _gcloud_default_project_id() -> str:
    """Read the gcloud default project. Raises (uncached) when none is set."""
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project", "-q"],
            capture_output=True,
            text=True,
            check=True,
        )
        project = result.stdout.strip()
        if project and project != "(unset)":
            return project
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    raise ValueError(
        "No GCP project specified. Set project_id parameter, "
        "GOOGLE_CLOUD_PROJECT / GCP_PROJECT environment variable, "
        "an active GKE kubectl context, or configure gcloud default project."
    )


def resolve_gke_target(
    *,
    project_id: str | None = None,
    location: str | None = None,
    region: str | None = None,
    zone: str | None = None,
    cluster_name: str | None = None,
) -> tuple[str, str, str]:
    """Resolve (project_id, location, cluster_name), using active GKE kubectl context when omitted."""
    eff_project = (
        project_id.strip()
        if isinstance(project_id, str) and project_id.strip()
        else None
    )
    raw_loc = location or zone or region
    eff_location = (
        raw_loc.strip() if isinstance(raw_loc, str) and raw_loc.strip() else None
    )
    eff_cluster = (
        cluster_name.strip()
        if isinstance(cluster_name, str) and cluster_name.strip()
        else None
    )

    if not eff_project or not eff_location or not eff_cluster:
        active_ctx = get_active_gke_context()
        if active_ctx is not None:
            ctx_proj, ctx_loc, ctx_cluster = active_ctx
            proj_ok = not eff_project or eff_project == ctx_proj
            loc_ok = (
                not eff_location
                or eff_location == ctx_loc
                or derive_region(eff_location) == derive_region(ctx_loc)
            )
            cluster_ok = not eff_cluster or eff_cluster == ctx_cluster
            if proj_ok and loc_ok and cluster_ok:
                eff_project = eff_project or ctx_proj
                eff_location = eff_location or ctx_loc
                eff_cluster = eff_cluster or ctx_cluster

    if not eff_cluster:
        raise ValueError(
            "cluster_name must be specified for GKE cluster (or an active GKE kubectl context must be configured)."
        )
    if not eff_location:
        raise ValueError(
            "Either location, region or zone must be specified for GKE cluster."
        )
    if not eff_project:
        eff_project = resolve_default_project_id()

    return eff_project, eff_location, eff_cluster


def _is_matching_cluster(
    context_entry: dict[str, Any],
    cluster_name: str,
    project_id: str | None = None,
    region: str | None = None,
) -> bool:
    """Check whether a kubeconfig context entry matches the requested cluster identity."""
    ctx_cluster = context_entry.get("context", {}).get("cluster", "")
    ctx_name = context_entry.get("name", "")

    # 1. Exact match on cluster field or context name
    if ctx_cluster == cluster_name or ctx_name == cluster_name:
        return True

    # 2. Parse GKE context format: gke_{project}_{location}_{cluster}
    for identifier in (ctx_cluster, ctx_name):
        parsed = parse_gke_context_identifier(identifier)
        if parsed is not None:
            ctx_proj, ctx_loc, ctx_cl = parsed
            if ctx_cl != cluster_name:
                continue
            if project_id and ctx_proj != project_id:
                continue
            if region:
                effective_ctx_region = derive_region(ctx_loc)
                effective_target_region = derive_region(region)
                if (
                    effective_ctx_region != effective_target_region
                    and ctx_loc != region
                ):
                    continue
            return True

    # 3. Canonical GKE context format: gke_{project}_{location}_{cluster}
    if project_id and region:
        canonical = f"gke_{project_id}_{region}_{cluster_name}"
        if ctx_cluster == canonical or ctx_name == canonical:
            return True

    # 4. Fallback: Loose suffix match ONLY when project_id is not specified
    if not project_id:
        if ctx_cluster.endswith(f"_{cluster_name}") or ctx_name.endswith(
            f"_{cluster_name}"
        ):
            return True

    return False


class TimeoutApiClient(_ApiClientBase):
    """Custom ApiClient that enforces finite default connect and read socket timeouts.

    The upstream kubernetes-client defaults to None (indefinite socket blocking). Under
    high concurrency, stalled or dropped TCP connections block worker threads inside urllib3
    indefinitely, causing thread pool exhaustion and preventing Python from shutting down.
    """

    _DEFAULT_TIMEOUT = (_GKE_API_CONNECT_TIMEOUT_SEC, _GKE_API_READ_TIMEOUT_SEC)

    @override
    def call_api(self, *args: Any, **kwargs: Any) -> Any:
        """Inject default socket timeouts unless explicitly overridden by the caller.

        Also unmasks websocket handshake failures on kubernetes>=36. There,
        ``ApiClient.__call_api`` runs ``e.body.decode(...)`` on every ``ApiException``,
        but ``ws_client.websocket_call`` raises ``ApiException(status=0)`` with
        ``body=None`` (for example, when an admission webhook times out on a
        ``pods/exec`` CONNECT). The resulting ``AttributeError`` would hide the
        ``ApiException`` from overload classification and every retry path, so the
        original exception is re-raised instead.
        """
        if kwargs.get("_request_timeout") is None:
            kwargs["_request_timeout"] = self._DEFAULT_TIMEOUT
        try:
            return super().call_api(*args, **kwargs)
        except AttributeError as exc:
            original = exc.__context__
            if isinstance(original, ApiException) and original.body is None:
                raise original from None
            raise

    @override
    def close(self) -> None:
        """Close connection pools and worker thread pool."""
        super().close()
        if hasattr(self, "rest_client") and hasattr(self.rest_client, "pool_manager"):
            try:
                self.rest_client.pool_manager.clear()
            except Exception:
                pass


class KubernetesClientManager:
    """
    Process-wide manager for the Kubernetes client configuration.

    Handles kubeconfig loading, credential setup, and reference counting.
    Each caller of ``get_client()`` receives its own ``CoreV1Api`` backed by a
    dedicated ``ApiClient`` instance. This is necessary because the kubernetes
    ``stream()`` function (used for exec/attach) temporarily monkey-patches
    ``ApiClient.request`` with a WebSocket handler, which is not thread-safe
    when multiple environments share the same ``ApiClient``.

    Configuration is synchronous and guarded by a ``threading.Lock`` so it can
    be reached both from the event loop (through ``asyncio.to_thread``) and
    from synchronous code such as ``GKEEnvironment.capabilities``, which Harbor
    reads from the environment constructor.
    """

    _instance: KubernetesClientManager | None = None
    _instance_lock = threading.Lock()

    @staticmethod
    def _find_matching_kube_context(
        cluster_name: str, project_id: str, region: str
    ) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None, str | None]:
        """Return ``(contexts, active_context, matched_context_name)`` from kubeconfig."""
        try:
            contexts, active = k8s_config.list_kube_config_contexts()
        except Exception:
            return None, None, None
        if not contexts:
            return contexts, active, None
        if active and _is_matching_cluster(active, cluster_name, project_id, region):
            return contexts, active, active.get("name")
        matched_ctx = next(
            (
                c["name"]
                for c in contexts
                if _is_matching_cluster(c, cluster_name, project_id, region)
            ),
            None,
        )
        return contexts, active, matched_ctx

    @staticmethod
    def _fetch_gke_kube_credentials(
        cluster_name: str, region: str, project_id: str
    ) -> subprocess.CompletedProcess[str]:
        """Fetch GKE cluster credentials via ``gcloud container clusters get-credentials``."""
        loc_flag = "--zone" if is_gcp_zone(region) else "--region"
        get_creds_cmd = [
            "gcloud",
            "container",
            "clusters",
            "get-credentials",
            cluster_name,
            loc_flag,
            region,
            "--project",
            project_id,
            "--quiet",
        ]
        return subprocess.run(get_creds_cmd, capture_output=True, text=True)

    def __init__(self):
        if not _HAS_KUBERNETES:
            raise MissingExtraError(package="kubernetes", extra="gke")
        self._core_api = None
        self._reference_count = 0
        self._lock = threading.Lock()
        self._initialized = False
        self._cleanup_registered = False
        self._logger = logger.getChild(__name__)
        # Store cluster config to validate consistency across calls
        self._cluster_name: str | None = None
        self._region: str | None = None
        self._project_id: str | None = None

    @classmethod
    def get_instance(cls) -> KubernetesClientManager:
        """Get or create the singleton instance."""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _init_client(self, cluster_name: str, region: str, project_id: str):
        """Initialize the Kubernetes client from kubeconfig or GKE credentials."""
        if self._initialized:
            return

        try:
            contexts, active, matched_ctx = self._find_matching_kube_context(
                cluster_name, project_id, region
            )
            if contexts and active:
                if matched_ctx:
                    if matched_ctx == active.get("name"):
                        k8s_config.load_kube_config()
                    else:
                        k8s_config.load_kube_config(context=matched_ctx)
                    self._core_api = k8s_client.CoreV1Api()
                    self._initialized = True
                    self._cluster_name = cluster_name
                    self._region = region
                    self._project_id = project_id
                    return

                raise k8s_config.ConfigException(
                    f"Cluster '{cluster_name}' (project='{project_id}', region='{region}') "
                    "not found in active or available kubeconfig contexts."
                )

            k8s_config.load_kube_config()
            self._core_api = k8s_client.CoreV1Api()
            self._initialized = True
        except k8s_config.ConfigException:
            result = self._fetch_gke_kube_credentials(cluster_name, region, project_id)
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to get GKE credentials: {result.stderr}\n"
                    f"Ensure cluster {cluster_name} exists in {region}"
                )

            k8s_config.load_kube_config()
            self._core_api = k8s_client.CoreV1Api()
            self._initialized = True

        # Store cluster config for validation
        self._cluster_name = cluster_name
        self._region = region
        self._project_id = project_id

    def configure(self, cluster_name: str, region: str, project_id: str) -> None:
        """Load the client configuration for a cluster, once per process.

        Blocking: may read kubeconfig and run ``gcloud container clusters
        get-credentials``. Later calls only verify that the same cluster is
        requested, because the kubernetes client configuration is global.
        """
        with self._lock:
            if not self._initialized:
                self._logger.debug("Creating new Kubernetes client")
                self._init_client(cluster_name, region, project_id)
                if not self._cleanup_registered:
                    atexit.register(self._cleanup_sync)
                    self._cleanup_registered = True
            elif (
                self._cluster_name != cluster_name
                or self._region != region
                or self._project_id != project_id
            ):
                raise ValueError(
                    f"KubernetesClientManager already initialized for cluster "
                    f"'{self._cluster_name}' in {self._region} (project: {self._project_id}). "
                    f"Cannot connect to cluster '{cluster_name}' in {region} "
                    f"(project: {project_id}). Use separate processes for different clusters."
                )

    @staticmethod
    def _new_core_api() -> k8s_client.CoreV1Api:
        # A dedicated TimeoutApiClient per caller avoids the stream()
        # monkey-patching race and enforces socket timeouts.
        return k8s_client.CoreV1Api(TimeoutApiClient())

    @staticmethod
    def _close_core_api(api: k8s_client.CoreV1Api | None) -> None:
        api_client = getattr(api, "api_client", None)
        if api_client is None:
            return
        try:
            api_client.close()
        except Exception:
            pass

    @contextlib.contextmanager
    def scoped_client(
        self, cluster_name: str, region: str, project_id: str
    ) -> Iterator[k8s_client.CoreV1Api]:
        """Yield a short-lived ``CoreV1Api`` for synchronous, one-off calls."""
        self.configure(cluster_name, region, project_id)
        api = self._new_core_api()
        try:
            yield api
        finally:
            self._close_core_api(api)

    async def get_client(self, cluster_name: str, region: str, project_id: str):
        """
        Get a Kubernetes CoreV1Api client, creating the shared config if necessary.
        Also increments the reference count.

        Each caller receives its own CoreV1Api backed by a dedicated ApiClient
        to avoid a thread-safety race condition: the kubernetes stream() function
        temporarily monkey-patches ApiClient.request with a WebSocket handler,
        which is not safe when multiple threads share one ApiClient.
        """
        await asyncio.to_thread(self.configure, cluster_name, region, project_id)
        with self._lock:
            self._reference_count += 1
            self._logger.debug(
                f"Kubernetes client reference count incremented to {self._reference_count}"
            )
        return self._new_core_api()

    def release_client(self, api: k8s_client.CoreV1Api | None = None) -> None:
        """Decrement the reference count for the client and clean up connections."""
        self._close_core_api(api)
        with self._lock:
            if self._reference_count > 0:
                self._reference_count -= 1
                self._logger.debug(
                    f"Kubernetes client reference count decremented to {self._reference_count}"
                )

    def _cleanup_sync(self) -> None:
        """Clean up the Kubernetes client and background thread pools at exit."""
        try:
            _shutdown_exec_executor()
            with self._lock:
                if self._initialized:
                    self._logger.debug("Cleaning up Kubernetes client at program exit")
                    self._core_api = None
                    self._initialized = False
        except Exception as e:
            self._logger.error(f"Error during Kubernetes client cleanup: {e}")


# Auto-raise FD limit on module load
_ensure_file_descriptor_limit()
