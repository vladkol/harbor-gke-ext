from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import inspect
import os
import re
import shutil
import subprocess
import threading
from typing import TYPE_CHECKING, Any, ClassVar, override

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


def _ensure_file_descriptor_limit(target: int = 65536) -> None:
    """Ensure RLIMIT_NOFILE soft limit has sufficient headroom for high concurrency.

    At -n 1000 with the exec handshake limit at its 128 maximum, Harbor requires file descriptors for:
    - 1,000 active trial.log files (1 per active trial)
    - Up to 128 active socket descriptors during exec launch bursts
    - Runtime handles and standard streams (~50)
    Total minimum needed is ~1,200. Target defaults to 65,536 (or system hard limit).

    Invariants:
    - Never lowers the limit if already higher.
    - Caps at the system hard limit.
    - Swallows exceptions with a warning log so failure never blocks execution.
    """
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


_GCLOUD_CHECK_LOCK = threading.Lock()
_GCLOUD_VERIFIED: bool = False
_PROJECT_ID_LOCK = threading.Lock()
_CACHED_DEFAULT_PROJECT_ID: str | None = None


def reset_gcloud_cache() -> None:
    """Reset process-wide gcloud verification and default project caches."""
    global _GCLOUD_VERIFIED, _CACHED_DEFAULT_PROJECT_ID
    with _GCLOUD_CHECK_LOCK:
        _GCLOUD_VERIFIED = False
    with _PROJECT_ID_LOCK:
        _CACHED_DEFAULT_PROJECT_ID = None


def ensure_gcloud_ready(*, force_probe: bool = False) -> None:
    """Verify once per process that gcloud CLI is installed and authenticated."""
    global _GCLOUD_VERIFIED
    if not shutil.which("gcloud"):
        raise SystemExit(
            "GKE requires the gcloud CLI to be installed. "
            "See https://docs.cloud.google.com/sdk/docs/install-sdk"
        )
    if _GCLOUD_VERIFIED and not force_probe:
        return
    with _GCLOUD_CHECK_LOCK:
        if _GCLOUD_VERIFIED and not force_probe:
            return
        adc_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if adc_path and os.path.isfile(adc_path):
            _GCLOUD_VERIFIED = True
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
            if res.returncode != 0 or not res.stdout.strip():
                raise SystemExit(
                    "GKE requires an active authenticated gcloud account. "
                    "Run 'gcloud auth login' or 'gcloud auth activate-service-account' to authenticate."
                )
        except (OSError, subprocess.SubprocessError) as exc:
            raise SystemExit(
                f"Failed to verify gcloud authentication status: {exc}"
            ) from exc
        _GCLOUD_VERIFIED = True


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
    global _CACHED_DEFAULT_PROJECT_ID
    env_proj = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("CLOUDSDK_CORE_PROJECT")
        or os.environ.get("GCP_PROJECT")
    )
    if env_proj and env_proj.strip():
        return env_proj.strip()

    if _CACHED_DEFAULT_PROJECT_ID:
        return _CACHED_DEFAULT_PROJECT_ID

    with _PROJECT_ID_LOCK:
        if _CACHED_DEFAULT_PROJECT_ID:
            return _CACHED_DEFAULT_PROJECT_ID
        try:
            result = subprocess.run(
                ["gcloud", "config", "get-value", "project", "-q"],
                capture_output=True,
                text=True,
                check=True,
            )
            project = result.stdout.strip()
            if project and project != "(unset)":
                _CACHED_DEFAULT_PROJECT_ID = project
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

    def __init__(
        self,
        *args: Any,
        default_timeout: tuple[float, float] = (
            _GKE_API_CONNECT_TIMEOUT_SEC,
            _GKE_API_READ_TIMEOUT_SEC,
        ),
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.default_timeout = default_timeout

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
            kwargs["_request_timeout"] = self.default_timeout
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
    Singleton manager for the Kubernetes client configuration.

    Handles kubeconfig loading, credential setup, and reference counting.
    Each caller of ``get_client()`` receives its own ``CoreV1Api`` backed by a
    dedicated ``ApiClient`` instance. This is necessary because the kubernetes
    ``stream()`` function (used for exec/attach) temporarily monkey-patches
    ``ApiClient.request`` with a WebSocket handler, which is not thread-safe
    when multiple environments share the same ``ApiClient``.
    """

    _instance: KubernetesClientManager | None = None
    _lock: asyncio.Lock | None = None
    _fqdn_supported: ClassVar[bool | None] = None
    _fqdn_supported_by_cluster: ClassVar[dict[tuple[str, str, str], bool]] = {}

    @classmethod
    def reset_fqdn_cache(cls) -> None:
        """Reset the cached FQDN network policy support status."""
        cls._fqdn_supported = None
        cls._fqdn_supported_by_cluster.clear()

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

    @classmethod
    def _resolve_cluster_api_client(
        cls,
        cluster_name: str | None = None,
        region: str | None = None,
        project_id: str | None = None,
    ) -> k8s_client.ApiClient:
        """Resolve an ApiClient for a specific GKE cluster without mutating global kubeconfig state."""
        if cluster_name and region and project_id:
            try:
                _, _, matched_ctx = cls._find_matching_kube_context(
                    cluster_name, project_id, region
                )
                if matched_ctx:
                    return k8s_config.new_client_from_config(context=matched_ctx)
            except Exception:
                pass

            # Context not in kubeconfig yet; fetch credentials for target cluster
            try:
                cls._fetch_gke_kube_credentials(cluster_name, region, project_id)
                _, _, matched_ctx = cls._find_matching_kube_context(
                    cluster_name, project_id, region
                )
                if matched_ctx:
                    return k8s_config.new_client_from_config(context=matched_ctx)
            except Exception:
                pass

        try:
            k8s_config.load_kube_config()
        except Exception:
            pass
        return k8s_client.ApiClient()

    @classmethod
    def is_fqdn_network_policy_supported(
        cls,
        api_client: k8s_client.ApiClient | None = None,
        *,
        cluster_name: str | None = None,
        region: str | None = None,
        project_id: str | None = None,
    ) -> bool:
        """Check if FQDNNetworkPolicy CRD exists on the target cluster."""
        cluster_key = (project_id or "", region or "", cluster_name or "")
        has_cluster_identity = bool(cluster_name and region and project_id)

        if has_cluster_identity and cluster_key in cls._fqdn_supported_by_cluster:
            return cls._fqdn_supported_by_cluster[cluster_key]
        if cls._fqdn_supported is not None and not has_cluster_identity:
            return cls._fqdn_supported
        if (
            cls._fqdn_supported is not None
            and has_cluster_identity
            and not cls._fqdn_supported_by_cluster
        ):
            return cls._fqdn_supported

        created_temp_client = False
        try:
            if api_client is None:
                api_client = cls._resolve_cluster_api_client(
                    cluster_name=cluster_name,
                    region=region,
                    project_id=project_id,
                )
                created_temp_client = True
            sig = inspect.signature(api_client.call_api).parameters
            call_kwargs: dict[str, Any] = {"auth_settings": ["BearerToken"]}
            if "response_types_map" in sig:
                call_kwargs["response_types_map"] = {200: "object"}
            else:
                call_kwargs["response_type"] = "object"
            resp, _, _ = api_client.call_api(
                "/apis/networking.gke.io/v1alpha1",
                "GET",
                **call_kwargs,
            )
            if isinstance(resp, dict) and "resources" in resp:
                supported = any(
                    isinstance(r, dict) and r.get("name") == "fqdnnetworkpolicies"
                    for r in resp["resources"]
                )
            else:
                supported = False
        except Exception as exc:
            logger.debug("Failed to probe GKE FQDNNetworkPolicy CRD support: %s", exc)
            supported = False
        finally:
            if created_temp_client and api_client is not None:
                try:
                    api_client.close()
                except Exception:
                    pass

        cls._fqdn_supported = supported
        if has_cluster_identity:
            cls._fqdn_supported_by_cluster[cluster_key] = supported
        return supported

    def __init__(self):
        if not _HAS_KUBERNETES:
            raise MissingExtraError(package="kubernetes", extra="gke")
        self._core_api = None
        self._reference_count = 0
        self._client_lock = asyncio.Lock()
        self._initialized = False
        self._cleanup_registered = False
        self._logger = logger.getChild(__name__)
        # Store cluster config to validate consistency across calls
        self._cluster_name: str | None = None
        self._region: str | None = None
        self._project_id: str | None = None

    @classmethod
    async def get_instance(cls) -> "KubernetesClientManager":
        """Get or create the singleton instance."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()

        if cls._instance is None:
            raise RuntimeError("Failed to create KubernetesClientManager instance")

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

    async def get_client(self, cluster_name: str, region: str, project_id: str):
        """
        Get a Kubernetes CoreV1Api client, creating the shared config if necessary.
        Also increments the reference count.

        Each caller receives its own CoreV1Api backed by a dedicated ApiClient
        to avoid a thread-safety race condition: the kubernetes stream() function
        temporarily monkey-patches ApiClient.request with a WebSocket handler,
        which is not safe when multiple threads share one ApiClient.
        """
        async with self._client_lock:
            if not self._initialized:
                self._logger.debug("Creating new Kubernetes client")
                await asyncio.to_thread(
                    self._init_client, cluster_name, region, project_id
                )

                if not self._cleanup_registered:
                    atexit.register(self._cleanup_sync)
                    self._cleanup_registered = True
            else:
                # Validate cluster config matches
                if (
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

            self._reference_count += 1
            self._logger.debug(
                f"Kubernetes client reference count incremented to {self._reference_count}"
            )

            # Return a per-caller CoreV1Api with its own TimeoutApiClient to avoid
            # the stream() monkey-patching race condition and enforce socket timeouts.
            api_client = (
                TimeoutApiClient() if _HAS_KUBERNETES else k8s_client.ApiClient()
            )
            return k8s_client.CoreV1Api(api_client)

    async def release_client(self, api: k8s_client.CoreV1Api | None = None):
        """Decrement the reference count for the client and clean up connections."""
        if (
            api is not None
            and hasattr(api, "api_client")
            and api.api_client is not None
        ):
            try:
                api.api_client.close()
            except Exception:
                pass
        async with self._client_lock:
            if self._reference_count > 0:
                self._reference_count -= 1
                self._logger.debug(
                    f"Kubernetes client reference count decremented to {self._reference_count}"
                )

    def _cleanup_sync(self):
        """Synchronous cleanup wrapper for atexit."""
        try:
            asyncio.run(self._cleanup())
        except Exception as e:
            self._logger.error(f"Error during Kubernetes client cleanup: {e}")

    async def _cleanup(self):
        """Clean up the Kubernetes client and background thread pools if they exist."""
        _shutdown_exec_executor()
        async with self._client_lock:
            if self._initialized:
                try:
                    self._logger.debug("Cleaning up Kubernetes client at program exit")
                    self._core_api = None
                    self._initialized = False
                    self._logger.debug("Kubernetes client cleaned up successfully")
                except Exception as e:
                    self._logger.error(f"Error cleaning up Kubernetes client: {e}")


# Auto-raise FD limit on module load
_ensure_file_descriptor_limit()
