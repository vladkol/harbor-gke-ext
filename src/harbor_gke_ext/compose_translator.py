"""Multi-container Docker Compose to Kubernetes Unified Native Pod translator for GKE.

Architecture Overview (Commits S4 & S5)
---------------------------------------
Translates a multi-service Docker Compose project into a single Kubernetes Pod
(Unified Native Pod) across three placement shapes:

- **Shape A (All Native)**:
  Every sidecar service runs as a native Kubernetes 1.28+ sidecar container
  (``initContainers`` with ``restartPolicy: Always``), topologically sorted by
  ``depends_on``. Compose ``healthcheck`` definitions translate directly to
  Kubernetes ``startupProbe`` specifications so kubelet guarantees dependency
  health before starting downstream containers and ``main``.
  One-shot setup services (``condition: service_completed_successfully``) run
  as plain ``initContainers`` (``restartPolicy: None``).
  Services declaring ``depends_on: main`` (Decision Q1)
  are placed in ``spec.containers`` alongside ``main`` with a startup gate
  waiting on ``/harbor/gates/main-ready``.

- **Shape B (Hybrid Native + DinD Fallback Plane)**:
  When one or more sidecars require features incompatible with native Pod
  containers (such as port collisions, ``privileged: true``, ``/var/run/docker.sock``,
  or multiple isolated compose networks among sidecars), those sidecars are
  delegated to an in-Pod ``dind-engine`` native sidecar container.
  - ``main`` remains a native Pod container in ``spec.containers``, preserving
    GKE Image Streaming, GPU/TPU attachment, and Pod-level egress network
    policies.
  - ``dind-engine`` listens exclusively on a Unix domain socket
    (``/var/run/harbor-dind/docker.sock``) mounted only into ``dind-engine``,
    ``dind-cache-*``, and ``compose-up-gate`` — never on TCP and never mounted
    into ``main`` in Shape B.
  - ``dind-engine`` never passes ``--dns=8.8.8.8``, preserving cluster DNS so
    GKE ``FQDNNetworkPolicy`` egress rules function properly.
  - ``compose-up-gate`` (plain ``initContainer``) runs
    ``docker compose -f /harbor/dind-compose.yaml up -d --wait`` so ``main``
    starts only after all DinD sidecars are up and healthy.

- **Shape C (Full DinD)**:
  When ``main`` itself requires DinD features (or ``force_dind=True``), ``main``
  and all sidecars run inside ``dind-engine`` via ``compose-up-gate``, while
  the Pod's ``main`` container acts as a lightweight CLI proxy (``docker wait``
  and ``docker exec`` over ``/var/run/harbor-dind/docker.sock``).

All container image strings across all Pod containers are issued and verified
by :class:`harbor_gke_ext.image_ref.ImageResolver`.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import math
import os
import posixpath
import re
import shlex
import subprocess
import tarfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast, override

import yaml
from kubernetes import client as k8s_client

from harbor.constants import MAIN_SERVICE_NAME
from harbor.environments.base import ExecResult
from harbor.environments.compose_service_ops import ComposeServiceTransport
from harbor.environments.docker.compose_env import (
    ComposeInfraEnvVars,
    legacy_log_mount_env_vars,
    merge_compose_env,
)
from harbor.utils.env import resolve_env_vars
from harbor.utils.logger import logger
from harbor_gke_ext.cluster_probe import ClusterCapabilities
from harbor_gke_ext.compose_spec import (
    discover_compose_build_services,
    is_harbor_synthetic_log_mount,
    normalize_compose_project,
    parse_duration_seconds,
    resolve_contained_task_path,
)
from harbor_gke_ext.constants import (
    _GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB,
    _GKE_DEFAULT_COMPOSE_UP_TIMEOUT_SEC,
    _PSS_BASELINE_CAPABILITIES,
    GKE_NVIDIA_HOST_DIR,
    GKE_NVIDIA_LIB_DIR,
    _sanitize_kubernetes_resource_name,
    resolve_tpu_accelerator_label,
)
from harbor_gke_ext.image_ref import (
    _REGISTRY_HOST_RE,
    ImageOrigin,
    ImageResolver,
    is_google_registry_host,
    parse_image_ref,
)
from harbor_gke_ext.placement import (
    ComposePlacementMode,
    PlacementPlan,
    classify_compose_placement,
)

if TYPE_CHECKING:
    from harbor.models.task.config import TpuSpec
    from harbor.models.trial.config import ServiceVolumeConfig
    from harbor_gke_ext.environment import GKEEnvironment

_HARBOR_SHARED_LOG_PATHS: dict[str, str] = {
    "/logs/verifier": "verifier-logs",
    "/logs/agent": "agent-logs",
    "/logs/artifacts": "artifacts",
}

_DIND_ENGINE_IMAGE = "docker:28.3.3-dind"

# Names of the containers Harbor synthesizes to run the DinD plane. These are
# Harbor's own machinery, not task services; `GKEEnvironment` uses them to
# identify whose logs are infrastructure diagnostics worth preserving.
DIND_ENGINE_CONTAINER = "dind-engine"
DIND_MAIN_ROOTFS_CONTAINER = "dind-main-rootfs"
DIND_PULL_CONTAINER = "dind-pull"
COMPOSE_UP_GATE_CONTAINER = "compose-up-gate"

# Linux caps a SINGLE execve() argv string at MAX_ARG_STRLEN = 32 * PAGE_SIZE
# (fs/exec.c). With the usual 4 KiB page that is 128 KiB. The `harbor-seed`
# inline payload is passed as one `sh -c <script>` argument, so the whole
# script -- base64 blob included -- must stay under this or the kernel rejects
# the exec with E2BIG ("argument list too long") before the container starts.
_LINUX_MAX_ARG_STRLEN = 32 * 4096

# Cap the inline base64 blob well below the hard limit to leave room for the
# surrounding script text and shlex quoting. Anything larger takes the
# streaming path, which has no size ceiling.
_SEED_INLINE_MAX_B64_BYTES = 64 * 1024


# Where the GKE GPU device plugin mounts the host NVIDIA driver.
#
# Defined once in ``constants`` and aliased here: the native GPU path
# (``pod_builder``) and this DinD path must never disagree about where the
# driver lives. See ``constants.GKE_NVIDIA_HOST_DIR`` for full provenance.
_NVIDIA_HOST_DIR = GKE_NVIDIA_HOST_DIR
_NVIDIA_LIB_DIR = GKE_NVIDIA_LIB_DIR

# Scratch dir on a shared emptyDir where ``dind-engine`` records what it can
# actually see of the GPU, for ``compose-up-gate`` to read back. See
# ``_build_shape_b_dind_containers``.
_DIND_GPU_FACTS_DIR = "/harbor/gpu"

_OCI_CONFIG_CACHE: dict[str, dict[str, Any]] = {}

# Sum of the compressed ``layers[].size`` values from an image's OCI manifest,
# keyed by image reference. ``None`` records a resolution failure so a broken or
# unpublished reference is not retried once per Pod build.
_OCI_COMPRESSED_SIZE_CACHE: dict[str, int | None] = {}

# Ratio between an image's on-disk footprint under overlay2 and the sum of its
# compressed layer sizes:
#
#     ubuntu:24.04                 28.4 MiB -> 81.0 MiB   2.85x
#     python:3.11-slim-bookworm    45.6 MiB -> 137.4 MiB  3.02x
#
# Two effects compose here: decompression (2.09-3.02x, lower for large images
# because data blobs compress worse than text) and 4 KiB block rounding
# (a consistent 1.05-1.065x). The high end is chosen deliberately. Under-
# reserving evicts the Pod mid-trial after the work is already done; over-
# reserving fails at admission, immediately and legibly.
DIND_IMAGE_EXPANSION_RATIO = 3.0

# Floor for the DinD ephemeral-storage reservation. dockerd needs room for its
# own metadata, containerd state, and inner container writable layers even when
# every image is tiny.
DIND_STORAGE_FLOOR_MB = 10240


def _parse_registry_image_ref(image_ref: str) -> tuple[str, str, str]:
    """Split ``image_ref`` into ``(registry_host, repository, tag_or_digest)``."""
    parsed = parse_image_ref(image_ref)
    host = (
        "registry-1.docker.io"
        if parsed.registry in ("docker.io", "index.docker.io")
        else parsed.registry
    )
    ref = parsed.digest or parsed.tag or "latest"
    return host, parsed.repository, ref


def _validate_public_https_url(raw_url: str) -> str:
    """Validate that ``raw_url`` is a public HTTPS URL without userinfo, fragments, or private/IP hosts."""
    import ipaddress
    import urllib.parse

    if not raw_url or any(ch.isspace() for ch in raw_url):
        raise ValueError(f"Invalid registry or auth realm URL: {raw_url!r}")
    parts = urllib.parse.urlsplit(raw_url)
    if parts.scheme != "https":
        raise ValueError(
            f"Registry/realm URL must use https:// scheme, got: {raw_url!r}"
        )
    if parts.username is not None or parts.password is not None:
        raise ValueError(
            f"Registry/realm URL must not contain userinfo credentials: {raw_url!r}"
        )
    if parts.fragment:
        raise ValueError(
            f"Registry/realm URL must not contain a fragment: {raw_url!r}"
        )
    hostname = (parts.hostname or "").strip().lower()
    if not hostname or hostname == "localhost" or hostname.endswith((".localhost", ".internal")):
        raise ValueError(
            f"Registry/realm URL must not target localhost or internal hosts: {raw_url!r}"
        )
    try:
        ipaddress.ip_address(hostname)
        is_ip = True
    except ValueError:
        is_ip = False
    if is_ip:
        raise ValueError(
            f"Registry/realm URL must use a public DNS hostname, not an IP literal: {raw_url!r}"
        )
    netloc = parts.netloc.lower()
    if not _REGISTRY_HOST_RE.fullmatch(netloc):
        raise ValueError(f"Invalid registry/realm host {netloc!r} in URL: {raw_url!r}")
    if parts.port is not None and not (1 <= parts.port <= 65535):
        raise ValueError(f"Invalid port in registry/realm URL: {raw_url!r}")
    return raw_url


def _build_safe_https_opener() -> Any:
    """Construct an HTTPS-only urllib OpenerDirector that validates redirects and strips cross-host Authorization."""
    import urllib.parse
    import urllib.request

    class _HTTPSRedirectStripAuthHandler(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            _validate_public_https_url(newurl)
            new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
            if new_req is not None:
                orig_host = urllib.parse.urlsplit(req.full_url).netloc.lower()
                dest_host = urllib.parse.urlsplit(new_req.full_url).netloc.lower()
                if orig_host != dest_host:
                    new_req.headers.pop("Authorization", None)
                    new_req.unredirected_hdrs.pop("Authorization", None)
            return new_req

    opener = urllib.request.OpenerDirector()
    opener.add_handler(urllib.request.UnknownHandler())
    opener.add_handler(urllib.request.HTTPSHandler())
    opener.add_handler(_HTTPSRedirectStripAuthHandler())
    opener.add_handler(urllib.request.HTTPDefaultErrorHandler())
    opener.add_handler(urllib.request.HTTPErrorProcessor())
    return opener


def _fetch_oci_config_from_registry(
    image_ref: str, *, timeout_sec: float = 5.0
) -> tuple[dict[str, Any], int | None]:
    """Fetch an image's OCI ``config`` dict and compressed size via Registry API v2.

    Returns ``(config, total_compressed_bytes)``. The size is the sum of the
    manifest's ``layers[].size`` fields, which the registry reports in bytes for
    the *compressed* blobs. It comes free with the manifest request the config
    lookup already makes, so callers that only want the size pay no extra round
    trip. ``None`` means the manifest carried no usable layer sizes.
    """
    import urllib.error
    import urllib.parse
    import urllib.request

    host, repo, ref = _parse_registry_image_ref(image_ref)
    accept = ", ".join(
        [
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json",
        ]
    )
    is_google = is_google_registry_host(host)
    token: str | None = None
    if is_google:
        try:
            proc = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True,
                text=True,
                timeout=3.0,
                check=False,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                token = proc.stdout.strip()
        except Exception:
            token = None

    opener = _build_safe_https_opener()

    def _http_get_json(
        url: str, extra_headers: dict[str, str] | None = None
    ) -> dict[str, Any]:
        nonlocal token
        _validate_public_https_url(url)
        headers = dict(extra_headers or {})
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with opener.open(req, timeout=timeout_sec) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            if err.code == 401 and not token and not is_google:
                auth_hdr = err.headers.get("WWW-Authenticate", "")
                realm_m = re.search(r'realm="([^"]+)"', auth_hdr)
                if realm_m:
                    realm = _validate_public_https_url(realm_m.group(1))
                    query_params: list[tuple[str, str]] = []
                    svc_m = re.search(r'service="([^"]+)"', auth_hdr)
                    if svc_m:
                        query_params.append(("service", svc_m.group(1)))
                    scope_m = re.search(r'scope="([^"]+)"', auth_hdr)
                    if scope_m:
                        query_params.append(("scope", scope_m.group(1)))
                    sep = "&" if "?" in realm else "?"
                    tok_url = (
                        f"{realm}{sep}{urllib.parse.urlencode(query_params)}"
                        if query_params
                        else realm
                    )
                    _validate_public_https_url(tok_url)
                    with opener.open(
                        urllib.request.Request(tok_url), timeout=timeout_sec
                    ) as tok_resp:
                        tok_data = json.loads(tok_resp.read().decode("utf-8"))
                        token = tok_data.get("token") or tok_data.get("access_token")
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
                        req2 = urllib.request.Request(url, headers=headers)
                        with opener.open(req2, timeout=timeout_sec) as resp2:
                            return json.loads(resp2.read().decode("utf-8"))
            raise

    manifest_url = f"https://{host}/v2/{repo}/manifests/{ref}"
    manifest = _http_get_json(manifest_url, {"Accept": accept})
    if "manifests" in manifest and isinstance(manifest["manifests"], list):
        chosen_digest: str | None = None
        for entry in manifest["manifests"]:
            plat = entry.get("platform") or {}
            if plat.get("os") == "linux" and plat.get("architecture") == "amd64":
                chosen_digest = entry.get("digest")
                break
        if not chosen_digest and manifest["manifests"]:
            chosen_digest = manifest["manifests"][0].get("digest")
        if not chosen_digest:
            return {}, None
        manifest = _http_get_json(
            f"https://{host}/v2/{repo}/manifests/{chosen_digest}",
            {"Accept": accept},
        )

    # Layer sizes come from the manifest, not the config blob, so they are known
    # even when the config fetch below fails or returns nothing useful.
    compressed_bytes: int | None = None
    layers = manifest.get("layers")
    if isinstance(layers, list):
        total = 0
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            size = layer.get("size")
            if isinstance(size, int) and size > 0:
                total += size
        compressed_bytes = total or None

    config_desc = manifest.get("config") or {}
    config_digest = config_desc.get("digest")
    if not config_digest:
        return {}, compressed_bytes
    blob = _http_get_json(f"https://{host}/v2/{repo}/blobs/{config_digest}")
    cfg = blob.get("config")
    return (dict(cfg) if isinstance(cfg, dict) else {}), compressed_bytes


def _resolve_oci_manifest_facts(image_ref: str) -> tuple[dict[str, Any], int | None]:
    """Resolve and cache both the OCI ``config`` dict and the compressed size.

    One registry round trip serves both consumers, so a Pod that needs the size
    for storage sizing and the config for ``--change`` directives pays for a
    single manifest fetch per distinct image per process.

    Only successful fetches are cached. Caching a failure would make one
    transient registry error permanent for the rest of the process, silently
    degrading every later trial that happens to use the same image.
    """
    if image_ref in _OCI_CONFIG_CACHE:
        return (
            dict(_OCI_CONFIG_CACHE[image_ref]),
            _OCI_COMPRESSED_SIZE_CACHE.get(image_ref),
        )
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return {}, None
    try:
        cfg, compressed = _fetch_oci_config_from_registry(image_ref)
    except Exception as exc:
        # Warning, not debug: both consumers degrade silently and in ways that
        # surface far from here. On the `docker import` fallback path (used when
        # `docker pull` fails), the `--change` set loses Entrypoint/Cmd; and the
        # storage estimate loses this image entirely, so `dind-engine`
        # under-reserves and the Pod can be evicted mid-trial for exceeding node
        # storage.
        logger.warning(
            "Could not read the OCI manifest for image %s (%s). Its entrypoint "
            "and command cannot be preserved on the `docker import` fallback "
            "path if `docker pull` fails, and its size is excluded from the "
            "dind-engine ephemeral-storage reservation. Set `dind_storage_mb` "
            "for this task if the Pod is evicted for exceeding node storage.",
            image_ref,
            exc,
        )
        return {}, None
    _OCI_CONFIG_CACHE[image_ref] = cfg
    _OCI_COMPRESSED_SIZE_CACHE[image_ref] = compressed
    return dict(cfg), compressed


def _resolve_oci_image_config(image_ref: str) -> dict[str, Any]:
    """Resolve and cache the OCI image ``config`` dict for ``image_ref``.

    Used at Compose translation time so ``dind-pull`` / ``dind-cache-<svc>`` can
    pass ``--change`` directives for ``Entrypoint``, ``Cmd``, ``Env``,
    ``WorkingDir``, ``User``, ``ExposedPorts``, ``Labels``, and ``StopSignal``
    without network access inside the Pod.
    """
    cfg, _ = _resolve_oci_manifest_facts(image_ref)
    return cfg


def _format_oci_import_changes(oci_cfg: Mapping[str, Any]) -> str:
    """Format an OCI ``config`` dict into shell-escaped ``--change`` flags for ``docker import``."""
    if not oci_cfg:
        return ""
    directives: list[str] = []

    raw_env = oci_cfg.get("Env")
    if isinstance(raw_env, list):
        for entry in raw_env:
            entry_str = str(entry)
            if "=" in entry_str:
                k, v = entry_str.split("=", 1)
                if k.strip():
                    directives.append(f"ENV {k.strip()}={json.dumps(v)}")

    ep = oci_cfg.get("Entrypoint")
    if isinstance(ep, list) and ep:
        directives.append(f"ENTRYPOINT {json.dumps([str(x) for x in ep])}")
    elif isinstance(ep, str) and ep.strip():
        directives.append(f"ENTRYPOINT {ep.strip()}")

    cmd = oci_cfg.get("Cmd")
    if isinstance(cmd, list) and cmd:
        directives.append(f"CMD {json.dumps([str(x) for x in cmd])}")
    elif isinstance(cmd, str) and cmd.strip():
        directives.append(f"CMD {cmd.strip()}")

    workdir = oci_cfg.get("WorkingDir")
    if isinstance(workdir, str) and workdir.strip():
        directives.append(f"WORKDIR {workdir.strip()}")

    user = oci_cfg.get("User")
    if isinstance(user, str) and user.strip():
        directives.append(f"USER {user.strip()}")

    exposed = oci_cfg.get("ExposedPorts")
    if isinstance(exposed, dict):
        for port in sorted(exposed):
            if str(port).strip():
                directives.append(f"EXPOSE {str(port).strip()}")

    labels = oci_cfg.get("Labels")
    if isinstance(labels, dict):
        for lk, lv in sorted(labels.items()):
            if str(lk).strip():
                directives.append(
                    f"LABEL {json.dumps(str(lk))}={json.dumps(str(lv))}"
                )

    stopsig = oci_cfg.get("StopSignal")
    if isinstance(stopsig, str) and stopsig.strip():
        directives.append(f"STOPSIGNAL {stopsig.strip()}")

    return " ".join(f"--change {shlex.quote(d)}" for d in directives)


def _resolve_oci_compressed_size_bytes(image_ref: str) -> int | None:
    """Return the summed compressed layer size for ``image_ref``, or ``None``.

    ``None`` means the size is not knowable: the reference is unpublished (a
    Compose ``build:`` with no ``image:``), the registry is unreachable, or
    credentials are missing. Callers must treat that as "unknown", never as
    zero.
    """
    _, compressed = _resolve_oci_manifest_facts(image_ref)
    return compressed


def compute_dind_storage_mb(
    service_images: Mapping[str, str | None],
    *,
    task_storage_budget_mb: int = 0,
) -> int:
    """Size the ``ephemeral-storage`` reservation for ``dind-engine``, in MiB.

    ``dind-engine`` mounts an emptyDir at ``/var/lib/docker``. Everything the
    inner Docker daemon materialises lands there and is charged to the Pod's
    ephemeral storage, so the reservation has to cover two distinct quantities
    that *add* rather than overlap:

    1. **Image materialisation** -- every image the ``dind-cache-*`` init
       containers import, decompressed and rounded up to 4 KiB blocks. Estimated
       as ``DIND_IMAGE_EXPANSION_RATIO x sum(compressed layer sizes)``.
    2. **The task's own declared storage budget** -- writable scratch for the
       inner containers. This is what the task asked for and is unrelated to how
       large its images happen to be.

    Taking ``max()`` of the two would silently discard whichever is smaller
    (for example, when a task declares a modest scratch budget while its base
    image unpacks to tens of GiB).

    Args:
        service_images: DinD service name -> image reference. A ``None`` or empty
            value marks a service whose image is built in-Pod and therefore has
            no registry manifest to measure.
        task_storage_budget_mb: The task's declared storage budget in MiB.

    Returns:
        The reservation in MiB, never below ``DIND_STORAGE_FLOOR_MB``.
    """
    total_compressed_bytes = 0
    unresolved: list[str] = []

    for sname in sorted(service_images):
        image_ref = service_images[sname]
        if not image_ref:
            unresolved.append(f"{sname} (built in-Pod, no registry manifest)")
            continue
        size = _resolve_oci_compressed_size_bytes(image_ref)
        if size is None:
            unresolved.append(f"{sname} ({image_ref}: manifest unavailable)")
            continue
        total_compressed_bytes += size

    estimated_image_mb = math.ceil(
        DIND_IMAGE_EXPANSION_RATIO * total_compressed_bytes / (1024 * 1024)
    )
    total_mb = max(estimated_image_mb + task_storage_budget_mb, DIND_STORAGE_FLOOR_MB)

    if unresolved:
        logger.warning(
            "Cannot measure the on-disk size of %d DinD image(s): %s. "
            "Reserving %d MiB for /var/lib/docker, which covers only the images "
            "that could be measured plus the task's %d MiB storage budget. If "
            "the unmeasured images are large the Pod may be evicted for "
            "exceeding node ephemeral storage; set `dind_storage_mb` for this "
            "task to reserve an explicit amount.",
            len(unresolved),
            ", ".join(unresolved),
            total_mb,
            task_storage_budget_mb,
        )
    else:
        logger.debug(
            "DinD storage reservation: %d MiB (%d MiB images at %.1fx expansion "
            "of %d MiB compressed, plus %d MiB task storage budget, floor %d MiB)",
            total_mb,
            estimated_image_mb,
            DIND_IMAGE_EXPANSION_RATIO,
            total_compressed_bytes // (1024 * 1024),
            task_storage_budget_mb,
            DIND_STORAGE_FLOOR_MB,
        )

    return total_mb


def _is_google_registry_host(host: str) -> bool:
    """Return True if ``host`` is a Google Container/Artifact Registry domain.

    Used to scope the node service account's OAuth2 access token in
    ``/harbor/dind-images/.docker/config.json`` strictly to Google-hosted
    registries (`gcr.io`, `{us,eu,asia}.gcr.io`, `*-docker.pkg.dev`), never third-party hosts.
    """
    return is_google_registry_host(host)


def _sanitize_docker_image_name(name: str) -> str:
    """Sanitize string into a valid lowercase Docker image repository name."""
    cleaned = re.sub(r"[^a-z0-9_.-]", "_", name.lower()).strip("._-")
    return cleaned or "harbor_task"


def self_bind_mount(mount: ServiceVolumeConfig) -> ServiceVolumeConfig:
    """Map host bind mount source to its target path inside GKE Pods."""
    updated = dict(mount)
    if updated.get("target"):
        updated["source"] = updated["target"]
    return cast("ServiceVolumeConfig", updated)


def resolve_compose_infra_env(
    env: GKEEnvironment, use_prebuilt: bool = False
) -> dict[str, str]:
    """Process infrastructure environment variables for GKE compose translation.

    Sets ``CONTEXT_DIR`` to the resolved host ``env.environment_dir`` path so
    relative task bind mounts like ``${CONTEXT_DIR}/..`` resolve accurately to
    the task directory rather than a synthetic container path.
    """
    eff_context_dir = (
        str(env.environment_dir.resolve().absolute())
        if getattr(env, "environment_dir", None)
        else "/harbor/environment"
    )
    eff_cpus = getattr(env, "_effective_cpus", None) or 1
    eff_mem_mb = getattr(env, "_effective_memory_mb", None) or 2048

    infra = ComposeInfraEnvVars(
        main_image_name=_sanitize_docker_image_name(
            f"hb__{getattr(env, 'environment_name', 'task')}"
        ),
        context_dir=eff_context_dir,
        prebuilt_image_name=(
            env.task_env_config.docker_image
            if use_prebuilt and getattr(env, "task_env_config", None)
            else None
        ),
        cpus=int(eff_cpus),
        memory=f"{int(eff_mem_mb)}M",
    ).to_env_dict()

    volumes = [
        self_bind_mount(m) if m.get("type") == "bind" else m
        for m in getattr(env, "_mounts", [])
    ]
    infra.update(legacy_log_mount_env_vars(volumes, host_value="target"))
    infra.setdefault("ENV_ARTIFACTS_PATH", "/logs/artifacts")
    infra.setdefault("HOST_ARTIFACTS_PATH", "/logs/artifacts")
    infra.setdefault("ENV_VERIFIER_LOGS_PATH", "/logs/verifier")
    infra.setdefault("HOST_VERIFIER_LOGS_PATH", "/logs/verifier")
    infra.setdefault("ENV_AGENT_LOGS_PATH", "/logs/agent")
    infra.setdefault("HOST_AGENT_LOGS_PATH", "/logs/agent")
    infra.setdefault("TEST_DIR", "/tests")

    task_env = (
        resolve_env_vars(env.task_env_config.env)
        if getattr(env, "task_env_config", None) and env.task_env_config.env
        else {}
    )
    if getattr(env, "_persistent_env", None):
        task_env.update(env._persistent_env)
    return merge_compose_env(
        user_env=task_env,
        infra_env=infra,
        logger=getattr(env, "logger", logger),
    )


def _parse_cpu_millicores(val: str | float | None, default_m: int) -> int:
    if val is None:
        return default_m
    raw = str(val).strip()
    if not raw:
        return default_m
    if raw.endswith("m"):
        try:
            return max(10, int(float(raw[:-1])))
        except ValueError:
            return default_m
    try:
        return max(10, int(float(raw) * 1000))
    except ValueError:
        return default_m


# Fallback size for a DinD `tmpfs:` entry that declares none. Matches the
# default already used when converting tmpfs to an emptyDir for native pods.
_DIND_TMPFS_DEFAULT_SIZE_MB = 64


def _normalize_dind_tmpfs_entries(
    tmpfs_list: list[Any],
    *,
    occupied: set[str],
    max_size_mb: int | None,
) -> list[str]:
    """Return ``tmpfs:`` entries safe to emit into the synthesized DinD compose.

    Two invariants are enforced:

    1. A target already mounted by a volume is dropped. Docker refuses a project
       that mounts the same target twice ("target X already mounted as ...").
    2. Every surviving entry carries an explicit ``size=``. An unbounded tmpfs
       defaults to half of the node's RAM, and because tmpfs pages are shmem
       charged to the writing container's cgroup (nested under
       ``dind-engine``, which has no memory limit), an unbounded entry can
       drive the node into memory-pressure eviction of the whole Pod.
    """
    normalized: list[str] = []
    seen: set[str] = set()
    for entry in tmpfs_list:
        entry_str = str(entry).strip()
        if not entry_str:
            continue
        target, _, raw_opts = entry_str.partition(":")
        target = target.strip()
        if not target or target in occupied or target in seen:
            continue
        seen.add(target)

        size_mb = _DIND_TMPFS_DEFAULT_SIZE_MB
        kept_opts: list[str] = []
        for opt in (o.strip() for o in raw_opts.split(",")):
            if not opt:
                continue
            if opt.startswith("size="):
                size_mb = _parse_memory_mb(
                    opt.split("=", 1)[1].strip(), _DIND_TMPFS_DEFAULT_SIZE_MB
                )
                continue
            kept_opts.append(opt)
        if max_size_mb is not None:
            size_mb = min(size_mb, max_size_mb)
        kept_opts.append(f"size={size_mb}m")
        normalized.append(f"{target}:{','.join(kept_opts)}")
    return normalized


def _parse_memory_mb(val: str | float | None, default_mb: int) -> int:
    if val is None:
        return default_mb
    if isinstance(val, (int, float)):
        if val > 1024 * 1024:
            return max(32, int(val / (1024 * 1024)))
        return max(32, int(val))
    raw = str(val).strip()
    if not raw:
        return default_mb
    upper = raw.upper()
    try:
        if upper.endswith("GI") or upper.endswith("G") or upper.endswith("GB"):
            num = float(re.sub(r"[^0-9.]", "", upper))
            return max(32, int(num * 1024))
        if upper.endswith("MI") or upper.endswith("M") or upper.endswith("MB"):
            num = float(re.sub(r"[^0-9.]", "", upper))
            return max(32, int(num))
        if upper.endswith("KI") or upper.endswith("K") or upper.endswith("KB"):
            num = float(re.sub(r"[^0-9.]", "", upper))
            return max(32, int(num / 1024))
        num = float(raw)
        if num > 1024 * 1024:
            return max(32, int(num / (1024 * 1024)))
        return max(32, int(num))
    except ValueError:
        return default_mb


# A sentinel below every clamp the parsers apply (CPU clamps to >= 10, memory to
# >= 32), so it can never collide with a successfully parsed value.
_PARSE_FAILED = -1


def _parse_cpu_millicores_opt(val: str | float | None) -> int | None:
    """Parse a CPU quantity, or ``None`` if absent or unparseable.

    The non-optional form requires a caller-supplied default, which is how
    invented numbers crept into the resource model: an unreadable Compose value
    became a confident ``250m``. This form makes "we do not know" expressible.
    """
    parsed = _parse_cpu_millicores(val, _PARSE_FAILED)
    return None if parsed == _PARSE_FAILED else parsed


def _parse_memory_mb_opt(val: str | float | None) -> int | None:
    """Parse a memory quantity, or ``None`` if absent or unparseable."""
    parsed = _parse_memory_mb(val, _PARSE_FAILED)
    return None if parsed == _PARSE_FAILED else parsed


def _quantity_to_millicores(val: str | None) -> int:
    """Parse a Kubernetes CPU quantity to millicores, treating unset as zero."""
    if val is None or _is_zero_quantity(val):
        return 0
    return _parse_cpu_millicores_opt(val) or 0


def _quantity_to_mib(val: str | None) -> int:
    """Parse a Kubernetes memory quantity to MiB, treating unset as zero."""
    if val is None or _is_zero_quantity(val):
        return 0
    return _parse_memory_mb_opt(val) or 0


def _is_zero_quantity(val: str | float | None) -> bool:
    """True for a quantity that is explicitly zero (``0``, ``0Mi``, ``0m``).

    Needed because the underlying parsers clamp to a floor -- ``max(10, ...)``
    for CPU and ``max(32, ...)`` for memory -- so a deliberate zero would
    otherwise re-enter the aggregate as an invented 10m or 32Mi.
    """
    if val is None:
        return False
    raw = str(val).strip()
    if not raw:
        return False
    digits = re.sub(r"[^0-9.]", "", raw)
    try:
        return float(digits) == 0.0
    except ValueError:
        return False


_POD_LEVEL_PARSERS = {
    "cpu": _quantity_to_millicores,
    "memory": _quantity_to_mib,
}


def _container_resource_amount(
    container: k8s_client.V1Container, key: str, *, prefer_limit: bool
) -> int:
    """Resolve one container's effective CPU/memory figure, in millicores or MiB.

    Kubernetes defaults a container's request to its limit when only the limit
    is declared, so the request side must fall back to the limit or the computed
    aggregate will understate what the API server actually sees. The fallback
    keys on whether the field is *present*, not on whether it is non-zero: a
    container that pins its request to ``0`` has made a statement, and must not
    be treated as though it declared nothing.
    """
    parse = _POD_LEVEL_PARSERS[key]
    res = container.resources
    requests = (res.requests or {}) if res is not None else {}
    limits = (res.limits or {}) if res is not None else {}
    primary, secondary = (limits, requests) if prefer_limit else (requests, limits)
    for source in (primary, secondary):
        raw = source.get(key)
        if raw is not None:
            return parse(raw)
    return 0


def _is_native_sidecar(container: k8s_client.V1Container) -> bool:
    """True for an init container that keeps running (``restartPolicy: Always``)."""
    return getattr(container, "restart_policy", None) == "Always"


def aggregate_pod_resource(
    init_containers: Sequence[k8s_client.V1Container],
    app_containers: Sequence[k8s_client.V1Container],
    key: str,
    *,
    prefer_limit: bool = False,
    overrides: Mapping[str, int] | None = None,
) -> int:
    """Compute the Kubernetes effective Pod request for one resource.

    Mirrors ``resourcehelper.PodRequests``. Native sidecars accumulate and stay
    running, so every later init container is charged on top of them, while
    ordinary init containers run one at a time and contribute via ``max``:

    ``max( peak during the init phase, running sidecars + all app containers )``

    Ordering matters: a native sidecar declared *before* an init container adds
    to that init container's peak, but one declared after does not. This is why
    the aggregate is derived from the assembled container lists rather than
    recomputed from the inputs -- the assembled Pod is the only place the real
    order exists.

    ``overrides`` replaces the figure of the named containers, for a container
    whose cgroup holds more than its own spec shows (see
    ``build_pod_level_resources``).
    """
    by_name = overrides or {}

    def amount_of(container: k8s_client.V1Container) -> int:
        if container.name in by_name:
            return by_name[container.name]
        return _container_resource_amount(container, key, prefer_limit=prefer_limit)

    running_sidecars = 0
    peak = 0
    for container in init_containers:
        amount = amount_of(container)
        if _is_native_sidecar(container):
            running_sidecars += amount
            peak = max(peak, running_sidecars)
        else:
            peak = max(peak, running_sidecars + amount)
    app_total = running_sidecars + sum(amount_of(c) for c in app_containers)
    return max(peak, app_total)


# What a DinD Pod adds on top of the task's own figures for the Docker daemon
# itself. Memory is measured: `dockerd` plus `containerd` held 38.6 MiB of
# anonymous memory once idle in `docker:28.3.3-dind` on GKE 1.35.6
# (2026-10-03). Image-pull page cache is not added: `dind-pull` finishes before
# `main` starts, so the whole budget is free while it runs, and the kernel
# reclaims page cache under the Pod ceiling before it OOM-kills anything. CPU is
# nominal: an idle daemon uses effectively none. Both go into dind-engine's
# request and into its ceiling (see `build_pod_level_resources`), so the task
# keeps its full declared budget for its own containers.
DIND_ENGINE_BASELINE_MEMORY_MB = 64
DIND_ENGINE_BASELINE_CPU_M = 100


# Token requests given to containers that declare nothing on Autopilot. See
# `_apply_autopilot_request_floor`.
_AUTOPILOT_FLOOR_REQUESTS = {"cpu": "1m", "memory": "1Mi"}


def _apply_autopilot_request_floor(
    containers: Sequence[k8s_client.V1Container], key: str
) -> list[str]:
    """Give every container that declares nothing for ``key`` a token request.

    Autopilot's ``autopilot-default-resources-mutator`` injects ``500m`` CPU /
    ``2Gi`` memory into each container that has neither a request nor a limit
    for a resource, and the API server then rejects the Pod when that injected
    aggregate exceeds ``spec.resources.requests``. A token ``1m`` / ``1Mi``
    request is a declaration, so the mutator leaves the container alone and the
    Pod-level budget stays the effective cap.

    Mutates ``containers`` in place and returns the names it floored.
    """
    floored: list[str] = []
    for container in containers:
        res = container.resources
        requests = (res.requests or {}) if res is not None else {}
        limits = (res.limits or {}) if res is not None else {}
        if key in requests or key in limits:
            continue
        if res is None:
            res = k8s_client.V1ResourceRequirements()
            container.resources = res
        res.requests = {**requests, key: _AUTOPILOT_FLOOR_REQUESTS[key]}
        floored.append(container.name)
    return floored


def build_pod_level_resources(
    init_containers: Sequence[k8s_client.V1Container],
    app_containers: Sequence[k8s_client.V1Container],
    *,
    task_cpu_m: int | None,
    task_mem_mb: int | None,
    task_cpu_limit_m: int | None = None,
    task_mem_limit_mb: int | None = None,
    host_ceilings: Mapping[str, Mapping[str, int]] | None = None,
    is_autopilot: bool = False,
    log: Any | None = None,
) -> k8s_client.V1ResourceRequirements | None:
    """Build ``spec.resources``: the Pod is the Docker host for the task.

    Harbor's Docker environment caps `main` at the task budget and leaves every
    other container with what its Compose service declares. Docker documents
    the rest: a container with a limit is capped at it, and one without "can
    use as much of a given resource as the host's kernel scheduler allows". The
    Pod plays the host, so its figures are sums of what runs inside it:

    - request: ``max(task budget, aggregate container requests)``;
    - ceiling: ``max(request, aggregate container ceilings, task limit)``, where
      a container's ceiling is its limit, else its request.

    Containers without a ceiling of their own share whatever the Pod has left.
    The Pod is not a real host, though: its ceiling keeps one task from taking
    memory or CPU that the scheduler gave to its neighbours.

    ``host_ceilings`` maps a container name to ``{"cpu": millicores, "memory":
    MiB}`` and replaces that container's ceiling. It exists for ``dind-engine``,
    whose cgroup holds the Docker daemon and every container the daemon starts
    (see ``cgroup_nesting_cmd`` in ``_build_shape_b_dind_containers``). Its own
    spec carries no limit, so its true ceiling -- daemon baseline plus the
    ceilings of the services it runs -- is only known to the caller.

    A resource the task does not budget gets no Pod-level figure at all: Harbor
    treats an absent ``cpus`` / ``memory_mb`` as unlimited, and Docker leaves an
    undeclared container unconstrained.

    **Requires Kubernetes 1.34+**, where ``spec.resources`` (KEP-2837) is beta
    and on by default. This function does not check: on an older cluster the API
    server silently drops the field. Detection lives one level up, in
    ``probe_pod_level_resources_support``, which issues a server-side dry-run
    once per cluster and warns when the field does not survive. A probe is used
    rather than a version comparison because it also catches an explicitly
    disabled feature gate.

    Three API-server rules shape the output, all measured against GKE 1.35.6:

    1. ``requests[X]`` must be >= the aggregate container requests.
    2. No single container limit may exceed the matching Pod limit. Using the
       limit aggregate as the floor satisfies this, since a sum is never smaller
       than any one of its terms.
    3. Only ``cpu``, ``memory`` and ``hugepages-*`` are accepted, so
       ``ephemeral-storage`` stays a container-level reservation.

    Returns ``None`` when the task declares no budget at all. Harbor's task model
    defaults ``cpus`` and ``memory_mb`` to ``None``, meaning unlimited, so there
    is nothing to cap with and the Pod is admitted as BestEffort.

    **Autopilot request floor (``is_autopilot=True`` only).** Autopilot injects
    ``500m`` / ``2Gi`` into every container that declares nothing, which breaks
    rule 1 for any Pod-level budget below that injected aggregate. Measured on
    GKE Autopilot 1.35.8 with a 7-container Pod and a ``2`` CPU / ``4Gi`` budget:
    the general-purpose, ``Balanced``, ``Scale-Out`` and custom machine-family
    ComputeClasses all reject it, while ``Performance`` and accelerator Pods are
    not defaulted. So for each resource that reaches ``spec.resources``, every
    container (init or app) that declares neither a request nor a limit for it
    gets a token ``1m`` / ``1Mi`` request before the aggregate is computed. The
    Pod-level budget remains the effective cap. Autopilot may still raise a
    container to its own per-container minimum (measured up to ``246m`` /
    ``1020Mi`` on ``Scale-Out``), so a budget below those minimums can still be
    rejected. The floor is strictly Autopilot-gated: floor requests applied on
    Standard were previously measured to collapse a Pod from ``1 CPU / 2 GiB``
    to ``1m / 1Mi``.
    """
    emit = log if log is not None else logger

    if task_cpu_m is None and task_mem_mb is None:
        emit.warning(
            "Task declares neither cpus nor memory_mb, so there is no budget to "
            "cap the Pod with. The Pod is admitted as BestEffort: the scheduler "
            "accounts it as free and the kubelet evicts it first under node "
            "pressure. Declare cpus/memory_mb in task.toml to get a ceiling."
        )
        return None

    requests: dict[str, str] = {}
    limits: dict[str, str] = {}

    for key, budget, budget_limit, unit in (
        ("cpu", task_cpu_m, task_cpu_limit_m, "m"),
        ("memory", task_mem_mb, task_mem_limit_mb, "Mi"),
    ):
        if budget is None:
            continue
        if is_autopilot:
            floored = _apply_autopilot_request_floor(
                [*init_containers, *app_containers], key
            )
            if floored:
                emit.debug(
                    "Autopilot: added token %s request %s to undeclared "
                    "containers %s so injected defaults cannot exceed the "
                    "Pod-level budget.",
                    key,
                    _AUTOPILOT_FLOOR_REQUESTS[key],
                    floored,
                )
        request_aggregate = aggregate_pod_resource(init_containers, app_containers, key)
        ceiling_aggregate = aggregate_pod_resource(
            init_containers,
            app_containers,
            key,
            prefer_limit=True,
            overrides={
                name: ceiling[key]
                for name, ceiling in (host_ceilings or {}).items()
                if key in ceiling
            },
        )

        pod_request = max(budget, request_aggregate)
        # `budget_limit` is an explicitly declared task limit, which
        # `cpu_limit_multiplier` / `memory_limit_multiplier` set deliberately
        # above the request. It must reach the Pod or the multiplier does
        # nothing.
        pod_limit = max(pod_request, ceiling_aggregate, budget_limit or 0)
        if pod_request <= 0:
            continue

        requests[key] = f"{pod_request}{unit}"
        limits[key] = f"{pod_limit}{unit}"

    if not requests:
        return None

    return k8s_client.V1ResourceRequirements(requests=requests, limits=limits)


@dataclass(frozen=True)
class _MainBudget:
    """The task's CPU/memory budget, which Harbor applies to `main` only.

    ``*_request`` falls back to the limit when only a limit was given, matching
    how Kubernetes defaults an unset request. ``None`` means the task left that
    figure undeclared, so nothing is written for it.
    """

    cpu_request_m: int | None = None
    cpu_limit_m: int | None = None
    memory_request_mb: int | None = None
    memory_limit_mb: int | None = None

    def container_resources(self) -> tuple[dict[str, str], dict[str, str]]:
        """The budget as Kubernetes container ``(requests, limits)``."""
        requests: dict[str, str] = {}
        limits: dict[str, str] = {}
        if self.cpu_request_m is not None:
            requests["cpu"] = f"{self.cpu_request_m}m"
        if self.memory_request_mb is not None:
            requests["memory"] = f"{self.memory_request_mb}Mi"
        if self.cpu_limit_m is not None:
            limits["cpu"] = f"{self.cpu_limit_m}m"
        if self.memory_limit_mb is not None:
            limits["memory"] = f"{self.memory_limit_mb}Mi"
        return requests, limits

    def apply_to(self, container: k8s_client.V1Container) -> None:
        """Layer the budget over a natively placed `main`'s own declaration.

        The budget wins wherever it declares a figure, as Harbor's resources
        override does. A ceiling `main` declares for itself survives where the
        task sets none, and the request is lowered to it when it would exceed
        it: the API server rejects a request above the limit.
        """
        budget_requests, budget_limits = self.container_resources()
        res = container.resources or k8s_client.V1ResourceRequirements()
        requests = {**(res.requests or {}), **budget_requests}
        limits = {**(res.limits or {}), **budget_limits}
        for key, parse in (
            ("cpu", _parse_cpu_millicores_opt),
            ("memory", _parse_memory_mb_opt),
        ):
            requested = parse(requests.get(key))
            ceiling = parse(limits.get(key))
            if requested is not None and ceiling is not None and requested > ceiling:
                logger.warning(
                    "`main` declares its own %s ceiling of %s, below the task "
                    "request of %s; requesting %s instead.",
                    key,
                    limits[key],
                    requests[key],
                    limits[key],
                )
                requests[key] = limits[key]
        res.requests = requests or None
        res.limits = limits or None
        container.resources = res


def _compose_cpus(raw: Any) -> Any:
    """Normalize a Compose CPU figure to decimal cores; keep it if unparseable."""
    millicores = _parse_cpu_millicores_opt(raw)
    return raw if millicores is None else format(millicores / 1000, "g")


def _compose_memory(raw: Any) -> Any:
    """Normalize a Compose memory figure to ``<MiB>M``; keep it if unparseable."""
    mib = _parse_memory_mb_opt(raw)
    return raw if mib is None else f"{mib}M"


def _apply_dind_main_budget(
    sspec: dict[str, Any], budget: _MainBudget
) -> dict[str, Any]:
    """Return a copy of inner `main`'s spec carrying the task budget.

    Mirrors Harbor's Docker environment, whose resources override file sets
    ``services.main`` and wins over whatever the task's own Compose file
    declares. Only the figures the task declares are written, so a ceiling
    `main` sets for itself survives in request mode.

    The legacy top-level keys (``cpus``, ``mem_limit``, ``mem_reservation``) are
    folded into ``deploy.resources`` first: Compose rejects a service that sets
    both forms to different values, and one source of truth is easier to read
    in the inner Compose file.
    """
    out = dict(sspec)
    deploy = dict(out["deploy"]) if isinstance(out.get("deploy"), dict) else {}
    resources = (
        dict(deploy["resources"]) if isinstance(deploy.get("resources"), dict) else {}
    )
    limits = (
        dict(resources["limits"]) if isinstance(resources.get("limits"), dict) else {}
    )
    reservations = (
        dict(resources["reservations"])
        if isinstance(resources.get("reservations"), dict)
        else {}
    )

    if "cpus" in out:
        limits.setdefault("cpus", out.pop("cpus"))
    if "mem_limit" in out:
        limits.setdefault("memory", out.pop("mem_limit"))
    if "mem_reservation" in out:
        reservations.setdefault("memory", out.pop("mem_reservation"))

    # Declared figures first, so whatever survives the budget below reads in the
    # same canonical form as the budget itself.
    for block in (limits, reservations):
        if "cpus" in block:
            block["cpus"] = _compose_cpus(block["cpus"])
        if "memory" in block:
            block["memory"] = _compose_memory(block["memory"])

    for block, cpu_m, memory_mb in (
        (limits, budget.cpu_limit_m, budget.memory_limit_mb),
        (reservations, budget.cpu_request_m, budget.memory_request_mb),
    ):
        if cpu_m is not None:
            block["cpus"] = format(cpu_m / 1000, "g")
        if memory_mb is not None:
            block["memory"] = f"{memory_mb}M"

    # In request mode the task sets no ceiling, so one `main` declares for
    # itself stays in force and can sit below the task's request. Docker refuses
    # a memory reservation above the limit, and reserving what `main` can never
    # use only withholds it from the node, so the reservation is lowered to the
    # ceiling.
    for key, parse in (
        ("cpus", _parse_cpu_millicores_opt),
        ("memory", _parse_memory_mb_opt),
    ):
        reserved = parse(reservations.get(key))
        ceiling = parse(limits.get(key))
        if reserved is not None and ceiling is not None and reserved > ceiling:
            logger.warning(
                "DinD `main` declares its own %s ceiling of %s, below the task "
                "request of %s; reserving %s instead.",
                key,
                limits[key],
                reservations[key],
                limits[key],
            )
            reservations[key] = limits[key]

    if limits:
        resources["limits"] = limits
    if reservations:
        resources["reservations"] = reservations
    if resources:
        deploy["resources"] = resources
        out["deploy"] = deploy
    return out


@dataclass(frozen=True)
class _DindDeclared:
    """CPU (millicores) and memory (MiB) the DinD services declare in total.

    These live in the inner Compose file, where the API server cannot see them,
    yet every container dockerd starts is nested in the Pod's cgroup.
    ``ceilings`` sums each service's limit, else its reservation -- the rule
    ``build_pod_level_resources`` applies to Kubernetes containers.
    """

    reservations: dict[str, int]
    ceilings: dict[str, int]


def _dind_declared_totals(
    dind_services: Mapping[str, dict[str, Any]],
) -> _DindDeclared:
    """Sum what the DinD services declare, in both Compose spellings.

    Reservations come from ``deploy.resources.reservations`` or the legacy
    ``mem_reservation``; limits from ``deploy.resources.limits`` or the legacy
    ``cpus`` / ``mem_limit``. A service that declares nothing adds nothing: on
    Docker it is unconstrained, which here means bounded by the Pod.
    """
    reservations = {"cpu": 0, "memory": 0}
    ceilings = {"cpu": 0, "memory": 0}
    for sspec in dind_services.values():
        deploy = sspec.get("deploy")
        resources = deploy.get("resources") if isinstance(deploy, dict) else None
        resources = resources if isinstance(resources, dict) else {}
        declared: dict[str, dict[str, int]] = {}
        for block, legacy_cpu, legacy_memory in (
            ("reservations", None, "mem_reservation"),
            ("limits", "cpus", "mem_limit"),
        ):
            raw = resources.get(block)
            raw = raw if isinstance(raw, dict) else {}
            cpu = raw.get("cpus", sspec.get(legacy_cpu) if legacy_cpu else None)
            memory = raw.get("memory", sspec.get(legacy_memory))
            declared[block] = {
                "cpu": _quantity_to_millicores(cpu),
                "memory": _quantity_to_mib(memory),
            }
        for key in ("cpu", "memory"):
            reserved = declared["reservations"][key]
            reservations[key] += reserved
            ceilings[key] += declared["limits"][key] or reserved
    return _DindDeclared(reservations=reservations, ceilings=ceilings)


def _extract_service_resources(
    sspec: dict[str, Any],
    default_reqs: dict[str, str],
    *,
    service_name: str = "",
) -> k8s_client.V1ResourceRequirements:
    """Layer a Compose service's declared CPU/memory over the baseline.

    Only values the service actually declares are emitted. A value that cannot
    be parsed is dropped with a warning rather than replaced by a fallback: an
    invented number here is indistinguishable from a deliberate one downstream,
    and silently capping a service at a figure nobody chose is worse than
    leaving it bounded by the Pod.

    There is no limit-side baseline: the task-wide ceiling lives on the Pod, so
    a limit only ever reaches a container by way of that container's own
    Compose declaration.
    """
    reqs = dict(default_reqs)
    lims: dict[str, str] = {}
    label = service_name or "compose service"

    deploy = sspec.get("deploy")
    res_block = (deploy.get("resources") or {}) if isinstance(deploy, dict) else {}
    reservations = res_block.get("reservations") or {}
    limits = res_block.get("limits") or {}

    def _take(
        raw: Any, parse: Any, unit: str, target: dict[str, str], key: str, origin: str
    ) -> int | None:
        parsed = parse(raw)
        if parsed is None:
            logger.warning(
                "Ignoring unparseable %s for %s: %r. The value is dropped rather "
                "than replaced by a default, so this resource stays bounded only "
                "by the Pod.",
                origin,
                label,
                raw,
            )
            return None
        target[key] = f"{parsed}{unit}"
        return parsed

    # Requests / reservations
    if reservations.get("cpus") is not None:
        _take(
            reservations["cpus"],
            _parse_cpu_millicores_opt,
            "m",
            reqs,
            "cpu",
            "deploy.resources.reservations.cpus",
        )
    if reservations.get("memory") is not None:
        _take(
            reservations["memory"],
            _parse_memory_mb_opt,
            "Mi",
            reqs,
            "memory",
            "deploy.resources.reservations.memory",
        )

    # Limits (or legacy top-level cpus / mem_limit)
    lim_cpu = (
        limits.get("cpus") if limits.get("cpus") is not None else sspec.get("cpus")
    )
    lim_mem = (
        limits.get("memory")
        if limits.get("memory") is not None
        else sspec.get("mem_limit")
    )

    # A declared limit is a ceiling, not a reservation. Kubernetes defaults an
    # unset request to the limit, which would turn `mem_limit: 8g` into a demand
    # for 8 GiB of node capacity -- and, because the Pod request must cover the
    # aggregate container requests, would drag the whole Pod's reservation up
    # with it. Pinning the request to 0 blocks that defaulting: the container
    # reserves nothing of its own and draws on the Pod's budget, which is what
    # the task actually asked for. Verified accepted by GKE 1.35.6.
    if lim_cpu is not None:
        if (
            _take(
                lim_cpu,
                _parse_cpu_millicores_opt,
                "m",
                lims,
                "cpu",
                "cpu limit",
            )
            is not None
            and reservations.get("cpus") is None
        ):
            reqs.setdefault("cpu", "0")

    if lim_mem is not None:
        if (
            _take(
                lim_mem,
                _parse_memory_mb_opt,
                "Mi",
                lims,
                "memory",
                "memory limit",
            )
            is not None
            and reservations.get("memory") is None
        ):
            reqs.setdefault("memory", "0")

    return k8s_client.V1ResourceRequirements(requests=reqs or None, limits=lims or None)


def _parse_compose_duration_sec(val: Any, default: int = 5) -> int:
    parsed = parse_duration_seconds(val)
    return parsed if parsed is not None and parsed > 0 else default


def _convert_dict_probe_to_k8s(
    probe_dict: dict[str, Any] | None = None,
    raw_healthcheck: dict[str, Any] | None = None,
) -> k8s_client.V1Probe | None:
    """Convert a Compose healthcheck specification into a Kubernetes V1Probe."""
    hc = raw_healthcheck or {}
    if hc.get("disable") is True:
        return None

    test_cmd = hc.get("test")
    exec_cmd: list[str] | None = None

    if isinstance(test_cmd, list) and test_cmd:
        mode = str(test_cmd[0]).upper()
        if mode == "NONE":
            return None
        if mode == "CMD-SHELL":
            shell_str = " ".join(str(x) for x in test_cmd[1:])
            exec_cmd = ["/bin/sh", "-c", shell_str]
        elif mode == "CMD":
            exec_cmd = [str(x) for x in test_cmd[1:]]
        else:
            exec_cmd = [str(x) for x in test_cmd]
    elif isinstance(test_cmd, str) and test_cmd.strip():
        exec_cmd = ["/bin/sh", "-c", test_cmd.strip()]
    elif probe_dict and isinstance(probe_dict.get("exec"), dict):
        raw_c = probe_dict["exec"].get("command") or []
        if len(raw_c) == 1 and isinstance(raw_c[0], str):
            exec_cmd = ["/bin/sh", "-c", raw_c[0]]
        else:
            exec_cmd = [str(x) for x in raw_c]

    if not exec_cmd:
        return None

    interval = _parse_compose_duration_sec(
        hc.get("interval"),
        default=(probe_dict or {}).get("periodSeconds", 5),
    )
    timeout = _parse_compose_duration_sec(
        hc.get("timeout"),
        default=(probe_dict or {}).get("timeoutSeconds", 5),
    )
    start_period = _parse_compose_duration_sec(
        hc.get("start_period"),
        default=(probe_dict or {}).get("initialDelaySeconds", 0),
    )
    retries_raw = hc.get("retries")
    if retries_raw is not None:
        try:
            retries = max(1, int(retries_raw))
        except (TypeError, ValueError):
            retries = 3
    else:
        retries = int((probe_dict or {}).get("failureThreshold", 3))

    return k8s_client.V1Probe(
        _exec=k8s_client.V1ExecAction(command=exec_cmd),
        initial_delay_seconds=max(0, start_period),
        period_seconds=max(1, interval),
        timeout_seconds=max(1, timeout),
        failure_threshold=max(1, retries),
    )


def _extract_command_and_args(
    sspec: dict[str, Any],
) -> tuple[list[str] | None, list[str] | None]:
    """Extract Kubernetes container ``command`` (entrypoint) and ``args`` (command)."""
    ep = sspec.get("entrypoint")
    cmd = sspec.get("command")

    k8s_cmd: list[str] | None = None
    k8s_args: list[str] | None = None

    if isinstance(ep, list):
        k8s_cmd = [str(x) for x in ep]
    elif isinstance(ep, str) and ep.strip():
        k8s_cmd = ["/bin/sh", "-c", ep]

    if isinstance(cmd, list):
        k8s_args = [str(x) for x in cmd]
    elif isinstance(cmd, str) and cmd.strip():
        if k8s_cmd is None:
            k8s_cmd = ["/bin/sh", "-c", cmd]
        else:
            k8s_args = [cmd]

    return k8s_cmd, k8s_args


def _extract_service_env_list(
    sspec: dict[str, Any],
    *,
    is_main: bool,
    startup_env: dict[str, str] | None,
) -> list[k8s_client.V1EnvVar]:
    """Build sorted list of Kubernetes V1EnvVar for a service container."""
    env_map: dict[str, str] = {}
    raw_env = sspec.get("environment")
    if isinstance(raw_env, dict):
        for k, v in raw_env.items():
            if v is not None:
                env_map[str(k)] = str(v)
    elif isinstance(raw_env, list):
        for item in raw_env:
            item_str = str(item)
            if "=" in item_str:
                k, v = item_str.split("=", 1)
                env_map[k] = v

    if is_main and startup_env:
        for k, v in startup_env.items():
            if v is not None:
                env_map[str(k)] = str(v)

    return [k8s_client.V1EnvVar(name=k, value=v) for k, v in sorted(env_map.items())]


def _build_security_context(
    sspec: dict[str, Any],
    *,
    sname: str = "",
) -> tuple[k8s_client.V1SecurityContext | None, bool]:
    """Build container V1SecurityContext and return (context, needs_gvisor)."""
    cap_add_raw = [str(c).upper() for c in (sspec.get("cap_add") or [])]
    cap_drop_raw = [str(c).upper() for c in (sspec.get("cap_drop") or [])]
    read_only = bool(sspec.get("read_only", False))
    privileged = bool(sspec.get("privileged", False))
    user_raw = sspec.get("user")

    run_as_user: int | None = None
    run_as_group: int | None = None
    if user_raw is not None:
        parts = str(user_raw).split(":")
        u_str = parts[0].strip()
        if u_str == "root":
            run_as_user = 0
        elif u_str.isdigit():
            run_as_user = int(u_str)
        if len(parts) > 1 and parts[1].strip().isdigit():
            run_as_group = int(parts[1].strip())

    needs_gvisor = False
    added_caps: list[str] = []
    for cap in cap_add_raw:
        clean_cap = cap.removeprefix("CAP_")
        if clean_cap not in _PSS_BASELINE_CAPABILITIES:
            needs_gvisor = True
            logger.warning(
                "Compose service %r requests capability %r outside Pod Security "
                "Standards Baseline; selecting gVisor sandbox isolation.",
                sname or "<service>",
                clean_cap,
            )
        added_caps.append(clean_cap)

    dropped_caps = [c.removeprefix("CAP_") for c in cap_drop_raw]
    capabilities = None
    if added_caps or dropped_caps:
        capabilities = k8s_client.V1Capabilities(
            add=added_caps or None,
            drop=dropped_caps or None,
        )

    if (
        not privileged
        and not read_only
        and run_as_user is None
        and run_as_group is None
        and capabilities is None
    ):
        return None, needs_gvisor

    return (
        k8s_client.V1SecurityContext(
            privileged=privileged or None,
            read_only_root_filesystem=read_only or None,
            run_as_user=run_as_user,
            run_as_group=run_as_group,
            capabilities=capabilities,
        ),
        needs_gvisor,
    )


def _collect_service_hostnames(sname: str, sspec: dict[str, Any]) -> set[str]:
    """Collect all Compose hostnames and network aliases for a service."""
    names: set[str] = {sname}
    sanitized = _sanitize_kubernetes_resource_name(sname)
    if sanitized:
        names.add(sanitized)

    hname = sspec.get("hostname")
    if hname:
        names.add(str(hname))

    cname = sspec.get("container_name")
    if cname:
        names.add(str(cname))

    nets = sspec.get("networks")
    if isinstance(nets, dict):
        for net_cfg in nets.values():
            if isinstance(net_cfg, dict):
                for alias in net_cfg.get("aliases") or []:
                    if alias:
                        names.add(str(alias))
    return names


def _build_host_aliases(
    services: dict[str, Any],
    dind_service_ips: dict[str, str] | None = None,
) -> list[k8s_client.V1HostAlias]:
    """Synthesize Pod hostAliases mapping 127.0.0.1 to native services and bridge IPs to DinD services."""
    dind_ips = dind_service_ips or {}
    ip_to_hosts: dict[str, set[str]] = {"127.0.0.1": set()}

    for sname, sspec in services.items():
        if not isinstance(sspec, dict):
            continue
        target_ip = dind_ips.get(sname, "127.0.0.1")
        ip_to_hosts.setdefault(target_ip, set()).update(
            _collect_service_hostnames(sname, sspec)
        )

        extra_hosts = sspec.get("extra_hosts")
        if isinstance(extra_hosts, list):
            for entry in extra_hosts:
                entry_str = str(entry)
                if ":" in entry_str:
                    host_part, ip_part = entry_str.split(":", 1)
                    ip_clean = ip_part.strip()
                    host_clean = host_part.strip()
                    if ip_clean and host_clean and ip_clean != "host-gateway":
                        ip_to_hosts.setdefault(ip_clean, set()).add(host_clean)
        elif isinstance(extra_hosts, dict):
            for host_part, ip_part in extra_hosts.items():
                ip_clean = str(ip_part).strip()
                host_clean = str(host_part).strip()
                if ip_clean and host_clean and ip_clean != "host-gateway":
                    ip_to_hosts.setdefault(ip_clean, set()).add(host_clean)

    aliases: list[k8s_client.V1HostAlias] = []
    for ip in sorted(ip_to_hosts.keys()):
        hosts = sorted(h for h in ip_to_hosts[ip] if h)
        if hosts:
            aliases.append(k8s_client.V1HostAlias(ip=ip, hostnames=hosts))
    return aliases


def _make_scratch_or_empty_dir_volume(
    name: str, scratch_volume_size: str | None = None
) -> k8s_client.V1Volume:
    """Create a generic ephemeral volume when ``scratch_volume_size`` is set, else emptyDir."""
    if scratch_volume_size:
        return k8s_client.V1Volume(
            name=name,
            ephemeral=k8s_client.V1EphemeralVolumeSource(
                volume_claim_template=k8s_client.V1PersistentVolumeClaimTemplate(
                    spec=k8s_client.V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=k8s_client.V1VolumeResourceRequirements(
                            requests={"storage": scratch_volume_size}
                        ),
                    )
                )
            ),
        )
    return k8s_client.V1Volume(
        name=name,
        empty_dir=k8s_client.V1EmptyDirVolumeSource(),
    )


def _extract_volumes_and_seed_container(
    project: dict[str, Any],
    placement: PlacementPlan,
    *,
    task_dir: Path,
    base_dir: Path,
    is_autopilot: bool,
    main_image_url: str,
    seed_image_url: str | None = None,
    scratch_volume_size: str | None = None,
    task_memory_budget_mb: int | None = None,
) -> tuple[
    dict[str, k8s_client.V1Volume],
    dict[str, list[k8s_client.V1VolumeMount]],
    k8s_client.V1Container | None,
    dict[str, dict[str, str]],
    bool,
]:
    """Build all Pod volumes, per-service volume mounts, and the harbor-seed container.

    Returns:
        ``(volumes_dict, service_mounts, seed_init_container, bind_mount_annotation_map, seed_streaming_required)``
    """
    volumes_dict: dict[str, k8s_client.V1Volume] = {}
    service_mounts: dict[str, list[k8s_client.V1VolumeMount]] = {}
    mounted_targets: dict[str, set[str]] = {}

    # 1. Shared Harbor log volumes
    for log_path, vol_name in _HARBOR_SHARED_LOG_PATHS.items():
        volumes_dict[vol_name] = k8s_client.V1Volume(
            name=vol_name,
            empty_dir=k8s_client.V1EmptyDirVolumeSource(),
        )

    # 2. Project-level named volumes
    for top_vol in project.get("volumes") or {}:
        vol_name = _sanitize_kubernetes_resource_name(top_vol)
        if vol_name not in volumes_dict:
            volumes_dict[vol_name] = _make_scratch_or_empty_dir_volume(
                vol_name, scratch_volume_size
            )

    # 3. Gate volume for post-main sidecars (Decision Q1)
    if placement.post_main_sidecars:
        volumes_dict["harbor-gates"] = k8s_client.V1Volume(
            name="harbor-gates",
            empty_dir=k8s_client.V1EmptyDirVolumeSource(),
        )

    # 4. DinD volumes if Shape B or Shape C
    if placement.shape in ("B", "C"):
        volumes_dict["harbor-dind-socket"] = k8s_client.V1Volume(
            name="harbor-dind-socket",
            empty_dir=k8s_client.V1EmptyDirVolumeSource(),
        )
        volumes_dict["harbor-dind-storage"] = _make_scratch_or_empty_dir_volume(
            "harbor-dind-storage", scratch_volume_size
        )

    services = project.get("services") or {}
    resolved_task_dir = task_dir.resolve()
    resolved_base_dir = base_dir.resolve()

    # Track anonymous volume overlay targets per service so we can exclude them from bind tarballs
    anon_targets_by_service: dict[str, set[str]] = {}
    for sname, sspec in services.items():
        if not isinstance(sspec, dict):
            continue
        for vol in sspec.get("volumes") or []:
            if (
                isinstance(vol, dict)
                and vol.get("type") == "volume"
                and not vol.get("source")
            ):
                tgt = str(vol.get("target") or "").strip()
                if tgt:
                    anon_targets_by_service.setdefault(sname, set()).add(tgt)
            elif isinstance(vol, str) and ":" not in vol:
                tgt = vol.strip()
                if tgt:
                    anon_targets_by_service.setdefault(sname, set()).add(tgt)

    # Track relative bind volumes: vol_name -> (rel_key, local_path, excluded_rel_subdirs)
    bind_volumes_info: dict[str, tuple[str, Path, set[str]]] = {}
    bind_mount_annotation: dict[str, dict[str, str]] = {}

    for sname, sspec in services.items():
        if not isinstance(sspec, dict):
            continue
        service_mounts[sname] = []
        mounted_targets[sname] = set()

        for vol in sspec.get("volumes") or []:
            vtype = "volume"
            vsrc = ""
            vtgt = ""
            read_only = False

            if isinstance(vol, dict):
                vtype = str(vol.get("type") or "volume")
                vsrc = str(vol.get("source") or "").strip()
                vtgt = str(vol.get("target") or "").strip()
                read_only = bool(vol.get("read_only", False))
            elif isinstance(vol, str):
                parts = vol.split(":")
                if len(parts) == 1:
                    vtype = "volume"
                    vsrc = ""
                    vtgt = parts[0].strip()
                else:
                    vsrc = parts[0].strip()
                    vtgt = parts[1].strip()
                    if len(parts) > 2 and "ro" in parts[2].split(","):
                        read_only = True
                    vtype = (
                        "bind"
                        if (vsrc.startswith("/") or vsrc.startswith("."))
                        else "volume"
                    )

            if not vtgt or vtgt in mounted_targets[sname]:
                continue

            # Harbor log mount
            if is_harbor_synthetic_log_mount(vsrc, vtgt):
                log_vol_name = _HARBOR_SHARED_LOG_PATHS[posixpath.normpath(vtgt)]
                service_mounts[sname].append(
                    k8s_client.V1VolumeMount(
                        name=log_vol_name,
                        mount_path=vtgt,
                        read_only=read_only or None,
                    )
                )
                mounted_targets[sname].add(vtgt)
                continue

            # docker.sock mount on DinD sidecars
            if vsrc in ("/var/run/docker.sock", "/run/docker.sock") and vtgt in (
                "/var/run/docker.sock",
                "/run/docker.sock",
            ):
                if "harbor-dind-socket" in volumes_dict:
                    service_mounts[sname].append(
                        k8s_client.V1VolumeMount(
                            name="harbor-dind-socket",
                            mount_path=vtgt,
                            sub_path="docker.sock",
                        )
                    )
                    mounted_targets[sname].add(vtgt)
                continue

            # Anonymous volume overlay (e.g. - /opt/task-src/solution)
            if vtype == "volume" and not vsrc:
                digest = hashlib.sha1(f"{sname}:{vtgt}".encode()).hexdigest()[:8]
                anon_vol_name = _sanitize_kubernetes_resource_name(
                    f"anon-{sname}-{digest}"
                )
                volumes_dict[anon_vol_name] = k8s_client.V1Volume(
                    name=anon_vol_name,
                    empty_dir=k8s_client.V1EmptyDirVolumeSource(),
                )
                service_mounts[sname].append(
                    k8s_client.V1VolumeMount(
                        name=anon_vol_name,
                        mount_path=vtgt,
                        read_only=read_only or None,
                    )
                )
                mounted_targets[sname].add(vtgt)
                continue

            # Named volume
            if vtype == "volume" and vsrc:
                named_vol = _sanitize_kubernetes_resource_name(vsrc)
                if named_vol not in volumes_dict:
                    volumes_dict[named_vol] = _make_scratch_or_empty_dir_volume(
                        named_vol, scratch_volume_size
                    )
                service_mounts[sname].append(
                    k8s_client.V1VolumeMount(
                        name=named_vol,
                        mount_path=vtgt,
                        read_only=read_only or None,
                    )
                )
                mounted_targets[sname].add(vtgt)
                continue

            # Relative / task-tree bind mount
            if vtype == "bind" and vsrc:
                local_path = resolve_contained_task_path(
                    vsrc,
                    base_dir=resolved_base_dir,
                    allowed_roots=(resolved_base_dir, resolved_task_dir),
                    field_name=f"services.{sname}.volumes",
                )

                try:
                    rel_key = os.path.relpath(local_path, resolved_task_dir)
                except ValueError:
                    rel_key = local_path.name

                vol_hash = hashlib.sha1(rel_key.encode()).hexdigest()[:8]
                bind_vol_name = _sanitize_kubernetes_resource_name(
                    f"bind-{local_path.name or 'root'}-{vol_hash}"
                )
                if bind_vol_name not in volumes_dict:
                    volumes_dict[bind_vol_name] = k8s_client.V1Volume(
                        name=bind_vol_name,
                        empty_dir=k8s_client.V1EmptyDirVolumeSource(),
                    )

                # Identify any anonymous volume targets that overlay subpaths of this bind mount
                excluded_subdirs: set[str] = set()
                for anon_tgt in anon_targets_by_service.get(sname, set()):
                    if anon_tgt.startswith(vtgt.rstrip("/") + "/"):
                        sub = anon_tgt[len(vtgt.rstrip("/")) + 1 :].split("/")[0]
                        if sub:
                            excluded_subdirs.add(sub)

                bind_volumes_info[bind_vol_name] = (
                    rel_key,
                    local_path,
                    excluded_subdirs,
                )
                sub_path = local_path.name if local_path.is_file() else None
                service_mounts[sname].append(
                    k8s_client.V1VolumeMount(
                        name=bind_vol_name,
                        mount_path=vtgt,
                        sub_path=sub_path,
                        read_only=read_only or None,
                    )
                )
                mounted_targets[sname].add(vtgt)

                bind_mount_annotation[rel_key] = {
                    "container": "harbor-seed",
                    "target_dir": f"/harbor/compose-binds/{bind_vol_name}",
                    "local_path": str(local_path),
                }

        # Ensure Harbor log volumes are mounted on main
        if sname == MAIN_SERVICE_NAME:
            for log_path, vol_name in _HARBOR_SHARED_LOG_PATHS.items():
                if log_path not in mounted_targets[sname]:
                    service_mounts[sname].append(
                        k8s_client.V1VolumeMount(name=vol_name, mount_path=log_path)
                    )
                    mounted_targets[sname].add(log_path)

        # Mount harbor-gates volume if needed (Decision Q1)
        if placement.post_main_sidecars and (
            sname == MAIN_SERVICE_NAME or sname in placement.post_main_sidecars
        ):
            if "/harbor/gates" not in mounted_targets[sname]:
                service_mounts[sname].append(
                    k8s_client.V1VolumeMount(
                        name="harbor-gates", mount_path="/harbor/gates"
                    )
                )
                mounted_targets[sname].add("/harbor/gates")

        # Tmpfs mounts (RFC §VI.3 Autopilot fix)
        tmpfs_spec = sspec.get("tmpfs")
        if tmpfs_spec:
            tmpfs_list = (
                [tmpfs_spec] if isinstance(tmpfs_spec, str) else list(tmpfs_spec)
            )
            # A DinD-delegated service keeps Docker's native `tmpfs:` key in the
            # synthesized inner compose rather than being rewritten into an
            # emptyDir. The DinD emission turns every entry in `service_mounts`
            # into a bind string while the raw `tmpfs:` key survives the service
            # dict copy, so producing both here makes docker compose reject the
            # project ("target /tmp already mounted as ...tmpfs[0]"). Keeping
            # tmpfs is also the better semantic: it stays RAM-backed and honours
            # noexec/nosuid, which the bind rewrite silently discards.
            if sname in placement.dind_sidecars:
                # tmpfs pages are shmem, charged to the writing container's
                # cgroup, which `dockerd --cgroup-parent` nests under
                # `dind-engine`. `dind-engine` has no memory limit, so an
                # oversized entry grows the Pod's usage past its request and
                # pushes the node toward memory-pressure eviction of the whole
                # Pod. The ceiling is the task's own memory budget: no tmpfs can
                # usefully exceed the memory the whole task is allowed. A service
                # that declares a smaller limit for itself wins, since it is the
                # more specific statement.
                declared_service_mb = _parse_memory_mb_opt(sspec.get("mem_limit"))
                candidates = [
                    mb
                    for mb in (task_memory_budget_mb, declared_service_mb)
                    if mb is not None and mb > 0
                ]
                surviving = _normalize_dind_tmpfs_entries(
                    tmpfs_list,
                    occupied=mounted_targets[sname],
                    max_size_mb=min(candidates) if candidates else None,
                )
                if surviving:
                    sspec["tmpfs"] = surviving
                    for surviving_entry in surviving:
                        mounted_targets[sname].add(
                            surviving_entry.split(":", 1)[0].strip()
                        )
                else:
                    sspec.pop("tmpfs", None)
                continue
            for idx, entry in enumerate(tmpfs_list):
                entry_str = str(entry).strip()
                parts = entry_str.split(":", 1)
                mount_p = parts[0].strip()
                if not mount_p or mount_p in mounted_targets[sname]:
                    continue
                size_limit: str | None = None
                if len(parts) > 1:
                    for opt in parts[1].split(","):
                        if opt.startswith("size="):
                            raw_sz = opt.split("=", 1)[1].strip()
                            mb = _parse_memory_mb(raw_sz, 64)
                            if is_autopilot:
                                mb = min(
                                    mb, _GKE_AUTOPILOT_MAX_GENERAL_PURPOSE_STORAGE_MB
                                )
                            size_limit = f"{mb}Mi"
                t_vol_name = _sanitize_kubernetes_resource_name(f"tmpfs-{sname}-{idx}")
                # On Autopilot, medium="Memory" is rejected by admission webhooks;
                # use medium="" (ephemeral node storage) with size_limit.
                medium = "" if is_autopilot else "Memory"
                volumes_dict[t_vol_name] = k8s_client.V1Volume(
                    name=t_vol_name,
                    empty_dir=k8s_client.V1EmptyDirVolumeSource(
                        medium=medium,
                        size_limit=size_limit,
                    ),
                )
                service_mounts[sname].append(
                    k8s_client.V1VolumeMount(name=t_vol_name, mount_path=mount_p)
                )
                mounted_targets[sname].add(mount_p)

    # 5. Synthesize harbor-seed initContainer if bind_volumes_info is non-empty
    if not bind_volumes_info:
        return volumes_dict, service_mounts, None, {}, False

    total_bytes = 0
    for bind_vol_name, (_, local_path, excluded) in bind_volumes_info.items():
        if not local_path.exists():
            continue
        if local_path.is_file():
            total_bytes += local_path.stat().st_size
        elif local_path.is_dir():
            for child in local_path.rglob("*"):
                if any(
                    part in excluded for part in child.relative_to(local_path).parts
                ):
                    continue
                if child.is_file():
                    total_bytes += child.stat().st_size

    seed_mounts = [
        k8s_client.V1VolumeMount(
            name=v_name,
            mount_path=f"/harbor/compose-binds/{v_name}",
        )
        for v_name in sorted(bind_volumes_info.keys())
    ]

    seed_streaming_required = True
    seed_cmd: list[str]

    if total_bytes < 512 * 1024:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for bind_vol_name, (_, local_path, excluded) in bind_volumes_info.items():
                if not local_path.exists():
                    continue
                if local_path.is_file():
                    tf.add(
                        str(local_path),
                        arcname=f"{bind_vol_name}/{local_path.name}",
                    )
                elif local_path.is_dir():
                    for item in local_path.iterdir():
                        if item.name in excluded:
                            continue
                        tf.add(
                            str(item),
                            arcname=f"{bind_vol_name}/{item.name}",
                        )
        b64_data = base64.b64encode(buf.getvalue()).decode("ascii")
        inline_script = (
            f"mkdir -p /harbor/compose-binds && echo {shlex.quote(b64_data)} "
            "| base64 -d | tar -xzf - -C /harbor/compose-binds"
        )
        # Check the encoded length of the argv string actually handed to
        # execve(), not merely the blob: the kernel limit covers the whole
        # `sh -c <script>` argument. Exceeding it fails the container at exec
        # time with E2BIG, which surfaces as an opaque Pod startup error.
        if (
            len(b64_data) <= _SEED_INLINE_MAX_B64_BYTES
            and len(inline_script.encode("utf-8")) < _LINUX_MAX_ARG_STRLEN
        ):
            seed_streaming_required = False
            seed_cmd = ["sh", "-c", inline_script]

        else:
            seed_cmd = [
                "sh",
                "-c",
                "mkdir -p /harbor/compose-binds && until [ -f /harbor/compose-binds/.seed-ready ]; do sleep 0.2; done",
            ]
    else:
        seed_cmd = [
            "sh",
            "-c",
            "mkdir -p /harbor/compose-binds && until [ -f /harbor/compose-binds/.seed-ready ]; do sleep 0.2; done",
        ]

    seed_container = k8s_client.V1Container(
        name="harbor-seed",
        image=seed_image_url or main_image_url,
        command=seed_cmd,
        volume_mounts=seed_mounts,
        restart_policy=None,
    )

    return (
        volumes_dict,
        service_mounts,
        seed_container,
        bind_mount_annotation,
        seed_streaming_required,
    )


def _build_k8s_container(
    sname: str,
    sspec: dict[str, Any],
    *,
    image_url: str,
    is_main: bool,
    is_native_sidecar: bool,
    is_post_main_sidecar: bool,
    startup_env: dict[str, str] | None,
    volume_mounts: list[k8s_client.V1VolumeMount],
    default_reqs: dict[str, str],
    main_workdir: str | None,
    gpu_count: int = 0,
    tpu_spec: TpuSpec | None = None,
    is_one_shot_init: bool = False,
) -> tuple[k8s_client.V1Container, bool]:
    """Synthesize a single V1Container and return (container, needs_gvisor)."""
    k8s_cmd, k8s_args = _extract_command_and_args(sspec)

    # Decision Q1: wrap post-main sidecars with /harbor/gates/main-ready gate
    if is_post_main_sidecar:
        if not k8s_cmd and image_url:
            oci_cfg = _resolve_oci_image_config(image_url)
            oci_ep = oci_cfg.get("Entrypoint")
            if isinstance(oci_ep, list) and oci_ep:
                k8s_cmd = [str(x) for x in oci_ep]
            elif isinstance(oci_ep, str) and oci_ep.strip():
                k8s_cmd = ["/bin/sh", "-c", oci_ep]
            if not k8s_args:
                oci_cmd = oci_cfg.get("Cmd")
                if isinstance(oci_cmd, list) and oci_cmd:
                    k8s_args = [str(x) for x in oci_cmd]
                elif isinstance(oci_cmd, str) and oci_cmd.strip():
                    if k8s_cmd is None:
                        k8s_cmd = ["/bin/sh", "-c", oci_cmd]
                    else:
                        k8s_args = [oci_cmd]
        combined: list[str] = []
        if k8s_cmd:
            combined.extend(k8s_cmd)
        if k8s_args:
            combined.extend(k8s_args)
        if not combined:
            combined = ["sh", "-c", "sleep infinity"]
        k8s_cmd = [
            "sh",
            "-c",
            'until [ -f /harbor/gates/main-ready ]; do sleep 0.2; done; exec "$@"',
            "--",
            *combined,
        ]
        k8s_args = None

    env_list = _extract_service_env_list(
        sspec, is_main=is_main, startup_env=startup_env
    )
    resources = _extract_service_resources(sspec, default_reqs, service_name=sname)

    if gpu_count > 0:
        if resources.limits is None:
            resources.limits = {}
        if resources.requests is None:
            resources.requests = {}
        resources.limits["nvidia.com/gpu"] = str(gpu_count)
        resources.requests["nvidia.com/gpu"] = str(gpu_count)

    if is_main and tpu_spec is not None and tpu_spec.chip_count > 0:
        if resources.limits is None:
            resources.limits = {}
        if resources.requests is None:
            resources.requests = {}
        tpu_res_key = "google.com/tpu"
        chip_str = str(tpu_spec.chip_count)
        resources.limits[tpu_res_key] = chip_str
        resources.requests[tpu_res_key] = chip_str

    sec_ctx, needs_gvisor = _build_security_context(sspec, sname=sname)
    probe = _convert_dict_probe_to_k8s(raw_healthcheck=sspec.get("healthcheck"))

    workdir = sspec.get("working_dir") or (main_workdir if is_main else None)

    container_ports: list[k8s_client.V1ContainerPort] = []
    seen_ports: set[int] = set()
    for p_entry in sspec.get("expose") or []:
        p_str = str(p_entry).split("/", 1)[0].strip()
        if p_str.isdigit() and int(p_str) not in seen_ports:
            p_int = int(p_str)
            seen_ports.add(p_int)
            container_ports.append(k8s_client.V1ContainerPort(container_port=p_int))

    restart_policy: str | None = "Always" if is_native_sidecar else None
    if is_one_shot_init:
        if probe is not None:
            logger.warning(
                f"Compose one-shot init service {sname!r} declares a healthcheck; "
                "dropping readinessProbe because Kubernetes forbids readinessProbe on "
                "initContainers without restartPolicy=Always."
            )
        startup_probe = None
        readiness_probe = None
    else:
        startup_probe = probe if is_native_sidecar else None
        readiness_probe = probe if not is_native_sidecar else None

    cname = MAIN_SERVICE_NAME if is_main else _sanitize_kubernetes_resource_name(sname)

    container = k8s_client.V1Container(
        name=cname,
        image=image_url,
        command=k8s_cmd,
        args=k8s_args,
        working_dir=str(workdir) if workdir else None,
        env=env_list or None,
        ports=container_ports or None,
        volume_mounts=volume_mounts or None,
        resources=resources,
        security_context=sec_ctx,
        restart_policy=restart_policy,
        startup_probe=startup_probe,
        readiness_probe=readiness_probe,
        stdin=bool(sspec.get("stdin_open", False)) or None,
        tty=bool(sspec.get("tty", False)) or None,
    )
    return container, needs_gvisor


def _merge_ld_library_path(
    environment: Any,
) -> dict[str, Any] | list[str]:
    """Prepend the NVIDIA driver lib dir to a service's ``LD_LIBRARY_PATH``.

    Compose accepts ``environment`` as either a mapping or a ``KEY=VALUE`` list;
    both are preserved so the emitted YAML stays close to the task's original.

    The existing value is **prepended to**, not replaced: CUDA base images may
    rely on their own entries, and clobbering them breaks the runtime while
    producing a confusing error far from the cause.
    """
    if isinstance(environment, dict):
        merged = dict(environment)
        existing = merged.get("LD_LIBRARY_PATH")
        if existing:
            merged["LD_LIBRARY_PATH"] = f"{_NVIDIA_LIB_DIR}:{existing}"
        else:
            merged["LD_LIBRARY_PATH"] = _NVIDIA_LIB_DIR
        return merged

    if isinstance(environment, list):
        out: list[str] = []
        found = False
        for item in environment:
            text = str(item)
            if text.startswith("LD_LIBRARY_PATH="):
                found = True
                existing = text.split("=", 1)[1]
                out.append(
                    f"LD_LIBRARY_PATH={_NVIDIA_LIB_DIR}:{existing}"
                    if existing
                    else f"LD_LIBRARY_PATH={_NVIDIA_LIB_DIR}"
                )
            else:
                out.append(text)
        if not found:
            out.append(f"LD_LIBRARY_PATH={_NVIDIA_LIB_DIR}")
        return out

    return {"LD_LIBRARY_PATH": _NVIDIA_LIB_DIR}


def _without_nvidia_device_reservations(sspec: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``sspec`` with nvidia GPU reservations removed.

    The inner Compose project runs against a plain ``docker:dind`` daemon, which
    does **not** ship the NVIDIA container runtime. Leaving
    ``deploy.resources.reservations.devices`` (or the legacy top-level ``gpus``
    key) in place would make Compose request a runtime that cannot be resolved.

    The GPU is instead delivered by explicit passthrough -- device nodes plus a
    ``/usr/local/nvidia`` bind -- emitted at runtime by ``compose-up-gate``.
    Measured working 2026-09-17; see GKE_NOTES.md section 10.

    Non-nvidia device reservations are preserved. Nested containers are rebuilt
    rather than mutated, because callers hold only a shallow copy of the shared
    project dictionary.
    """
    out = dict(sspec)
    out.pop("gpus", None)

    deploy = out.get("deploy")
    if not isinstance(deploy, dict):
        return out
    resources = deploy.get("resources")
    if not isinstance(resources, dict):
        return out
    reservations = resources.get("reservations")
    if not isinstance(reservations, dict):
        return out
    devices = reservations.get("devices")
    if not isinstance(devices, list):
        return out

    kept: list[Any] = []
    for dev in devices:
        if isinstance(dev, dict):
            caps = dev.get("capabilities") or []
            driver = str(dev.get("driver") or "").lower()
            if "gpu" in caps or "nvidia" in caps or driver == "nvidia":
                continue
        kept.append(dev)

    if len(kept) == len(devices):
        return out

    new_reservations = {k: v for k, v in reservations.items() if k != "devices"}
    if kept:
        new_reservations["devices"] = kept

    new_resources = {k: v for k, v in resources.items() if k != "reservations"}
    if new_reservations:
        new_resources["reservations"] = new_reservations

    new_deploy = {k: v for k, v in deploy.items() if k != "resources"}
    if new_resources:
        new_deploy["resources"] = new_resources

    if new_deploy:
        out["deploy"] = new_deploy
    else:
        out.pop("deploy", None)

    return out


def _build_shape_b_dind_containers(
    project: dict[str, Any],
    placement: PlacementPlan,
    *,
    dind_image_url: str,
    sidecar_image_urls: dict[str, str],
    volumes_dict: dict[str, k8s_client.V1Volume],
    service_mounts: dict[str, list[k8s_client.V1VolumeMount]],
    is_gvisor: bool = False,
    task_storage_budget_mb: int = 0,
    dind_storage_mb: int | None = None,
    compose_up_timeout_sec: int = _GKE_DEFAULT_COMPOSE_UP_TIMEOUT_SEC,
    startup_env: dict[str, str] | None = None,
    main_workdir: str | None = None,
    allow_metadata_server: bool = False,
    wait_for_netpol: bool = False,
    main_budget: _MainBudget,
) -> tuple[
    k8s_client.V1Container,
    k8s_client.V1Container,
    k8s_client.V1Container,
    dict[str, str],
    dict[str, int],
]:
    """Synthesize the DinD plane for Shape B / Shape C.

    Returns the dind-engine native sidecar, the dind-pull and compose-up-gate
    initContainers, deterministic DinD service IPs, and dind-engine's ceiling in
    CPU (millicores) / memory (MiB): the daemon baseline plus the ceilings the
    DinD services declare, which the Pod ceiling must count (see
    ``build_pod_level_resources``).
    """
    volumes_dict["harbor-dind-images"] = k8s_client.V1Volume(
        name="harbor-dind-images",
        empty_dir=k8s_client.V1EmptyDirVolumeSource(),
    )

    dind_mounts: list[k8s_client.V1VolumeMount] = [
        k8s_client.V1VolumeMount(
            name="harbor-dind-socket", mount_path="/var/run/harbor-dind"
        ),
        k8s_client.V1VolumeMount(
            name="harbor-dind-storage", mount_path="/var/lib/docker"
        ),
        k8s_client.V1VolumeMount(
            name="harbor-dind-images",
            mount_path="/harbor/dind-images",
            mount_propagation=(
                "HostToContainer"
                if placement.shape == "C" and not is_gvisor
                else None
            ),
        ),
    ]
    mounted_vol_names = {
        "harbor-dind-socket",
        "harbor-dind-storage",
        "harbor-dind-images",
    }

    # Mount every Pod volume referenced by any DinD sidecar into dind-engine at
    # /harbor/pod-vols/<vol_name>, and record symlinks from each service's
    # original mount_path to /harbor/pod-vols/<vol_name> so nested `docker run`
    # / `docker compose` commands issued from inside a DinD container against
    # /var/run/docker.sock resolve bind-mount paths (e.g. `${CONTEXT_DIR}`) in
    # dind-engine's mount namespace.
    vol_symlinks: list[tuple[str, str]] = []
    seen_symlink_targets: set[str] = {
        "/var/run/docker.sock",
        "/var/run/harbor-dind",
        "/var/lib/docker",
        "/harbor/dind-images",
    }
    for sname in placement.dind_sidecars:
        for vm in service_mounts.get(sname, []):
            if vm.name not in mounted_vol_names and vm.name in volumes_dict:
                mounted_vol_names.add(vm.name)
                dind_mounts.append(
                    k8s_client.V1VolumeMount(
                        name=vm.name,
                        mount_path=f"/harbor/pod-vols/{vm.name}",
                    )
                )
            if (
                vm.name != "harbor-dind-socket"
                and vm.name in volumes_dict
                and vm.mount_path
                and vm.mount_path not in seen_symlink_targets
            ):
                seen_symlink_targets.add(vm.mount_path)
                sub = f"/{vm.sub_path}" if vm.sub_path else ""
                vol_symlinks.append((f"/harbor/pod-vols/{vm.name}{sub}", vm.mount_path))

    # Synthesize stripped docker-compose YAML for DinD execution
    dind_services_spec: dict[str, Any] = {}
    dind_service_ips: dict[str, str] = {}
    raw_services = project.get("services") or {}
    main_names = sorted(
        _collect_service_hostnames(
            MAIN_SERVICE_NAME, raw_services.get(MAIN_SERVICE_NAME) or {}
        )
    )

    # Pick an unused /24 in 172.30.240.0/20 for harbor_dind_net
    raw_networks = project.get("networks") or {}
    declared_subnets: set[str] = set()
    if isinstance(raw_networks, dict):
        for net_cfg in raw_networks.values():
            if isinstance(net_cfg, dict):
                ipam = net_cfg.get("ipam")
                if isinstance(ipam, dict):
                    for cfg in ipam.get("config") or []:
                        if isinstance(cfg, dict) and cfg.get("subnet"):
                            declared_subnets.add(str(cfg["subnet"]).strip())

    selected_octet = 240
    for octet in range(240, 255):
        if f"172.30.{octet}.0/24" not in declared_subnets:
            selected_octet = octet
            break
    dind_subnet = f"172.30.{selected_octet}.0/24"
    dind_gateway = f"172.30.{selected_octet}.1"

    service_mode_targets: dict[str, str] = {}
    next_ip_idx = 2
    gpu_dind_services: list[str] = []

    for sname in placement.dind_sidecars:
        orig = dict(raw_services.get(sname) or {})
        orig.pop("build", None)
        if sname in sidecar_image_urls:
            orig["image"] = sidecar_image_urls[sname]
        orig["container_name"] = sname
        existing_env = orig.get("environment")
        merged_env: dict[str, str] = {}
        if isinstance(existing_env, dict):
            merged_env.update(
                {str(k): str(v) for k, v in existing_env.items() if v is not None}
            )
        elif isinstance(existing_env, list):
            for item in existing_env:
                if "=" in str(item):
                    k, v = str(item).split("=", 1)
                    merged_env[k] = v
        # When a task's compose file passes `SNAPSHOT_CACHE_HOST_DIR=${SNAPSHOT_CACHE_HOST_DIR:-}`
        # without an outer wrapper script setting it on the host, default it to `/app`
        # (where the pre-baked snapshot cache resides inside the image and is
        # exposed inside dind-engine via the main overlay rootfs mount).
        if (
            "SNAPSHOT_CACHE_HOST_DIR" in merged_env
            and not merged_env["SNAPSHOT_CACHE_HOST_DIR"].strip()
        ):
            merged_env["SNAPSHOT_CACHE_HOST_DIR"] = "/app"
        if sname == MAIN_SERVICE_NAME:
            orig["stdin_open"] = True
            orig["tty"] = True
            if main_workdir and not orig.get("working_dir"):
                orig["working_dir"] = main_workdir
            if startup_env:
                merged_env.update(startup_env)
            orig = _apply_dind_main_budget(orig, main_budget)
        if merged_env or existing_env is not None:
            orig["environment"] = merged_env

        # GPU services: drop the compose reservation (docker:dind has no NVIDIA
        # runtime) and hand the service to the runtime passthrough emitter below.
        if placement.gpu_config.service_gpus.get(sname, 0) > 0:
            orig = _without_nvidia_device_reservations(orig)
            gpu_dind_services.append(sname)

        nmode = str(orig.get("network_mode") or "").strip()
        if nmode == "host":
            dind_service_ips[sname] = "127.0.0.1"
        elif nmode.startswith("service:"):
            target_svc = nmode.split(":", 1)[1].strip()
            service_mode_targets[sname] = target_svc
        else:
            static_ip = f"172.30.{selected_octet}.{next_ip_idx}"
            next_ip_idx += 1
            dind_service_ips[sname] = static_ip

            s_aliases = sorted(
                _collect_service_hostnames(sname, raw_services.get(sname) or {})
            )
            existing_nets = orig.get("networks")
            if isinstance(existing_nets, dict):
                net_dict = dict(existing_nets)
            elif isinstance(existing_nets, list):
                net_dict = {str(n): {} for n in existing_nets}
            else:
                net_dict = {}
            net_dict["harbor_dind_net"] = {
                "ipv4_address": static_ip,
                "aliases": s_aliases,
            }
            orig["networks"] = net_dict

            existing_extra = orig.get("extra_hosts")
            if isinstance(existing_extra, dict):
                orig_extra_dict = dict(existing_extra)
                for mname in main_names:
                    orig_extra_dict.setdefault(mname, dind_gateway)
                orig["extra_hosts"] = orig_extra_dict
            elif isinstance(existing_extra, list):
                orig_extra_list = list(existing_extra)
                existing_keys = {
                    str(item).split(":", 1)[0]
                    for item in orig_extra_list
                    if ":" in str(item)
                }
                for mname in main_names:
                    if mname not in existing_keys:
                        orig_extra_list.append(f"{mname}:{dind_gateway}")
                orig["extra_hosts"] = orig_extra_list
            else:
                orig["extra_hosts"] = [
                    f"{mname}:{dind_gateway}" for mname in main_names
                ]

        # Filter depends_on to only services inside dind_sidecars
        deps = orig.get("depends_on")
        if isinstance(deps, dict):
            orig["depends_on"] = {
                k: v for k, v in deps.items() if k in placement.dind_sidecars
            }
            if not orig["depends_on"]:
                orig.pop("depends_on", None)
        elif isinstance(deps, list):
            orig["depends_on"] = [k for k in deps if str(k) in placement.dind_sidecars]
            if not orig["depends_on"]:
                orig.pop("depends_on", None)

        # Rewrite volumes to bind-mount from /harbor/pod-vols/<vol_name> inside dind-engine
        rewritten_vols: list[str] = []
        for vm in service_mounts.get(sname, []):
            if vm.name == "harbor-dind-socket":
                rewritten_vols.append(
                    "/var/run/harbor-dind/docker.sock:/var/run/docker.sock"
                )
            else:
                ro_suffix = ":ro" if vm.read_only else ""
                sub = f"/{vm.sub_path}" if vm.sub_path else ""
                rewritten_vols.append(
                    f"/harbor/pod-vols/{vm.name}{sub}:{vm.mount_path}{ro_suffix}"
                )

        # GPU services also need the host driver user-mode libraries. These are
        # static, so they are emitted here rather than in the runtime override --
        # only the device node list genuinely requires runtime enumeration.
        if sname in gpu_dind_services:
            rewritten_vols.append(f"{_NVIDIA_HOST_DIR}:{_NVIDIA_HOST_DIR}:ro")
            orig["environment"] = _merge_ld_library_path(orig.get("environment"))

        if rewritten_vols:
            orig["volumes"] = rewritten_vols
        else:
            orig.pop("volumes", None)

        dind_services_spec[sname] = orig

    for sname, target_svc in service_mode_targets.items():
        dind_service_ips[sname] = dind_service_ips.get(target_svc, "127.0.0.1")

    dind_compose_doc: dict[str, Any] = {"services": dind_services_spec}
    compose_networks = dict(raw_networks) if isinstance(raw_networks, dict) else {}
    for s_spec in dind_services_spec.values():
        if isinstance(s_spec.get("networks"), dict):
            for net_name in s_spec["networks"]:
                if net_name not in compose_networks:
                    compose_networks[net_name] = {}
    compose_networks["harbor_dind_net"] = {
        "driver": "bridge",
        "ipam": {
            "config": [
                {
                    "subnet": dind_subnet,
                    "gateway": dind_gateway,
                }
            ]
        },
    }
    dind_compose_doc["networks"] = compose_networks

    dind_compose_yaml = yaml.safe_dump(dind_compose_doc, sort_keys=False)
    b64_compose = base64.b64encode(dind_compose_yaml.encode("utf-8")).decode("ascii")

    # Materialize DinD service images via `dind-pull` (running trusted `docker:dind`
    # under the pre-task Bootstrap NetworkPolicy) before scrubbing `.docker`,
    # blocking metadata access, and applying the task's restrictive NetworkPolicy.
    # Omitting per-service `dind-cache-<sname>` init containers prevents Kubelet
    # (`containerd`) from downloading and unpacking a second copy of every DinD
    # image onto the host node just to run `exit 0`.
    pull_cmds: list[str] = []
    load_cmds: list[str] = []
    # Every DinD service's image, including the ones skipped below because they
    # have no `image:` and are built in-Pod instead. Storage sizing needs to know
    # about those too, precisely because their footprint cannot be measured.
    dind_service_images: dict[str, str | None] = {}
    for sname in placement.dind_sidecars:
        s_img = str(dind_services_spec[sname].get("image") or "").strip()
        dind_service_images[sname] = s_img or None
        if not s_img:
            continue
        q_sname = shlex.quote(sname)
        q_img = shlex.quote(s_img)
        gcfs_import_branch = ""
        if placement.shape == "C" and sname == MAIN_SERVICE_NAME and not is_gvisor:
            oci_cfg = _resolve_oci_image_config(s_img)
            change_flags = _format_oci_import_changes(oci_cfg)
            change_part = f" {change_flags}" if change_flags else ""
            gcfs_import_branch = (
                'elif [ "$SN" = "main" ] && [ -f /harbor/dind-images/.main-rootfs-captured ] && { '
                "mkdir -p /tmp/harbor-empty-seed && : > /tmp/harbor-empty-seed/.harbor-seed && "
                f'tar -C /tmp/harbor-empty-seed -cf - . | DOCKER_HOST="$DH" "$DCLI" import{change_part} - "$IMG" >/dev/null 2>&1; '
                "}; then "
                'echo "harbor: ${SN}: image ${IMG} stub imported for Kubelet gcfs rootfs bridge"; '
            )
        pull_cmds.append(
            f"SN={q_sname}; IMG={q_img}; "
            'if DOCKER_HOST="$DH" "$DCLI" image inspect "$IMG" >/dev/null 2>&1; then '
            'echo "harbor: ${SN}: image ${IMG} already present in dind-engine (deduplicated)"; '
            f"{gcfs_import_branch}"
            'elif DOCKER_CONFIG="/harbor/dind-images/.docker" DOCKER_HOST="$DH" '
            '"$DCLI" pull -q "$IMG" >/dev/null 2>"/harbor/dind-images/${SN}.pull-err"; then '
            'rm -f "/harbor/dind-images/${SN}.pull-err"; '
            'echo "harbor: ${SN}: image ${IMG} materialized via docker pull '
            '(OCI layers and hardlinks preserved)"; '
            "else "
            'echo "HARBOR_ERROR: could not pull image ${IMG} for DinD service ${SN}:" >&2; '
            'cat "/harbor/dind-images/${SN}.pull-err" >&2 2>/dev/null || true; '
            'rm -rf "/harbor/dind-images/${SN}.pull-err" /harbor/dind-images/.docker; '
            "exit 1; "
            "fi; "
        )
        load_cmds.append(f"load_image {shlex.quote(sname)} {shlex.quote(s_img)}")

    # Invariant 5: Unix socket only, NO TCP listener, NO --dns=8.8.8.8 flag.
    # `--dns-opt=ndots:1 --dns-opt=timeout:2 --dns-opt=attempts:1` overrides
    # Kubelet's `ndots:5` inside inner DinD containers so FQDNs with >= 1 dot
    # (e.g. `example.com`) are queried directly without walking the 5 Kubernetes
    # `.svc.cluster.local` search domains (which otherwise stalls `getaddrinfo`
    # for 48s under `no-network`).
    #
    # dind-engine is the Docker host of the DinD services: the daemon and every
    # container it starts are nested in its cgroup (see `cgroup_nesting_cmd`).
    # It requests the daemon baseline plus what those services reserve, so the
    # scheduler sees the workload it places. It has no CPU or memory limit of
    # its own: the Pod ceiling bounds the host (see `build_pod_level_resources`),
    # and inside it `main` is capped at the task budget and every DinD service
    # at the limits it declares, as on Docker. Its true ceiling -- baseline plus
    # the services' ceilings -- is returned for the Pod ceiling to count.
    dind_declared = _dind_declared_totals(dind_services_spec)
    dind_engine_requests: dict[str, str] = {
        "cpu": f"{DIND_ENGINE_BASELINE_CPU_M + dind_declared.reservations['cpu']}m",
        "memory": (
            f"{DIND_ENGINE_BASELINE_MEMORY_MB + dind_declared.reservations['memory']}Mi"
        ),
    }
    dind_engine_ceiling: dict[str, int] = {
        "cpu": DIND_ENGINE_BASELINE_CPU_M + dind_declared.ceilings["cpu"],
        "memory": DIND_ENGINE_BASELINE_MEMORY_MB + dind_declared.ceilings["memory"],
    }
    dind_engine_limits: dict[str, str] = {}

    # `harbor-dind-storage` is an emptyDir mounted at /var/lib/docker. Without a
    # matching request the scheduler sees this Pod as needing no disk and will
    # place it on a node that cannot hold the images, after which the kubelet
    # evicts it for exceeding node ephemeral storage -- typically well into the
    # trial, with the work already spent. A request only, no limit: the task-wide
    # budget is what caps the Pod, and an individual limit here would evict
    # dind-engine at that exact figure even when the node has room to spare.
    resolved_dind_storage_mb = (
        dind_storage_mb
        if dind_storage_mb is not None
        else compute_dind_storage_mb(
            dind_service_images, task_storage_budget_mb=task_storage_budget_mb
        )
    )
    dind_engine_requests["ephemeral-storage"] = f"{resolved_dind_storage_mb}Mi"

    # The device plugin mounts /usr/local/nvidia only into the container holding
    # the allocation. Device nodes arrive via `privileged` regardless, but the
    # driver libraries are what actually gate CUDA, so the allocation must sit on
    # dind-engine for the inner containers to work. Requests must equal limits
    # for extended resources.
    if placement.dind_gpu_total > 0:
        gpu_str = str(placement.dind_gpu_total)
        dind_engine_requests["nvidia.com/gpu"] = gpu_str
        dind_engine_limits["nvidia.com/gpu"] = gpu_str

    # `--storage-driver` is deliberately NOT pinned: dockerd auto-selects
    # overlay2 on the ext4-backed emptyDir at /var/lib/docker. Measured
    # 2026-09-17 on GKE 1.35.6 (DRIVER=overlay2, dockerd ready in 3s). The
    # previous `--storage-driver=vfs` pin cost a full copy per layer.
    registry_hosts: set[str] = {"gcr.io", "us-docker.pkg.dev"}
    for raw_img in dind_service_images.values():
        if not raw_img or "/" not in raw_img:
            continue
        first = raw_img.split("/", 1)[0].strip()
        if _is_google_registry_host(first):
            registry_hosts.add(first)
    hosts_joined = " ".join(shlex.quote(h) for h in sorted(registry_hosts))
    symlink_cmds = ""
    for src_vol_path, dst_mount_path in vol_symlinks:
        parent_dir = posixpath.dirname(dst_mount_path.rstrip("/")) or "/"
        symlink_cmds += (
            f"mkdir -p {shlex.quote(parent_dir)} && "
            f"ln -sfn {shlex.quote(src_vol_path)} {shlex.quote(dst_mount_path)}; "
        )
    stage_gcfs_bridge_cmds = (
        "cp /bin/busybox /harbor/dind-images/busybox; "
        "cp /lib/ld-musl-*.so.1 /harbor/dind-images/ld-musl.so.1; "
        "chmod 0755 /harbor/dind-images/busybox /harbor/dind-images/ld-musl.so.1; "
        "(while [ ! -f /harbor/dind-images/.main-rootfs-mounted ]; do sleep 0.05; done; "
        "mkdir -p /tmp/harbor-main-rootfs; "
        "if mount --bind /harbor/dind-images/main-rootfs /tmp/harbor-main-rootfs 2>>/harbor/dind-images/.main-layers-err; then "
        "mount --make-private /tmp/harbor-main-rootfs 2>/dev/null || true; "
        ": > /harbor/dind-images/.main-rootfs-captured; "
        "fi; "
        ": > /harbor/dind-images/.main-rootfs-ack) >/dev/null 2>&1 & "
        if placement.shape == "C" and not is_gvisor
        else ""
    )
    main_layers_watcher = (
        "(while [ ! -s /harbor/dind-images/.main-layers ]; do sleep 0.1; done; "
        "LAYERS=$(cat /harbor/dind-images/.main-layers); "
        "mkdir -p /tmp/harbor-main-rootfs; "
        "if [ -f /harbor/dind-images/.main-rootfs-captured ]; then "
        'if [ -n "$LAYERS" ] && [ -d "$LAYERS" ]; then '
        'mount --bind /tmp/harbor-main-rootfs "$LAYERS" 2>>/harbor/dind-images/.main-layers-err '
        '|| echo "mount --bind $LAYERS exited $?" >> /harbor/dind-images/.main-layers-err; '
        "fi; "
        "else "
        'case "$LAYERS" in '
        "*:*) "
        'mount -t overlay overlay -o "ro,lowerdir=${LAYERS}" /tmp/harbor-main-rootfs 2>/dev/null || true ;; '
        "*) "
        'mount --bind -o ro "$LAYERS" /tmp/harbor-main-rootfs 2>/dev/null || true ;; '
        "esac; "
        "fi; "
        "if [ -d /tmp/harbor-main-rootfs/app ] && [ ! -e /app ]; then "
        "ln -sfn /tmp/harbor-main-rootfs/app /app; "
        "fi; "
        "touch /harbor/dind-images/.main-layers-ready; "
        "for _j in $(seq 1 150); do "
        "MM=$(DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock docker inspect -f '{{.GraphDriver.Data.MergedDir}}' main 2>/dev/null || true); "
        'if [ -n "$MM" ] && [ -d "$MM/app" ]; then ln -sfn "$MM/app" /app; break; fi; '
        "sleep 0.2; "
        "done) >/dev/null 2>&1 & "
    )
    metadata_lockdown_watcher = (
        "(while [ ! -f /harbor/dind-images/.auth-scrubbed ]; do sleep 0.1; done; "
        "if grep -q '169\\.254\\.169\\.254' /etc/resolv.conf 2>/dev/null && command -v iptables >/dev/null 2>&1; then "
        "iptables -I OUTPUT -d 169.254.169.254/32 -p udp --dport 53 -j ACCEPT 2>/dev/null || true; "
        "iptables -I OUTPUT -d 169.254.169.254/32 -p tcp --dport 53 -j ACCEPT 2>/dev/null || true; "
        "iptables -A OUTPUT -d 169.254.169.254/32 -j REJECT 2>/dev/null || true; "
        "else "
        "ip route replace unreachable 169.254.169.254/32 2>/dev/null || true; "
        "fi; "
        "ip route replace unreachable 169.254.169.252/32 2>/dev/null || true; "
        "touch /harbor/dind-images/.metadata-blocked) >/dev/null 2>&1 & "
        if not allow_metadata_server
        else ""
    )
    # A privileged container shares the node's cgroup namespace, so a default
    # dockerd (cgroupfs driver) creates `/docker/<id>` at the node root for every
    # container it starts: outside the Pod, invisible to the kubelet's
    # accounting and eviction, and left behind after the Pod is deleted
    # (measured on GKE 1.35.6, 2026-10-03). Move this container's processes into
    # a `harbor-daemon` leaf, delegate every available controller, and point
    # `--cgroup-parent` below our own cgroup so nested containers count against
    # dind-engine and are removed with the Pod.
    #
    # This must run before any background job: cgroup v2 refuses to enable
    # controllers on a cgroup that still holds processes. Short-lived processes
    # (the `$(...)` subshells themselves) can race the move, hence the retry.
    # Failing closed is deliberate: a dind-engine that cannot nest would
    # silently put the task's workload outside the Pod again.
    #
    # gVisor is exempt: its dockerd runs inside the sandbox, whose own cgroup
    # already contains everything it starts.
    cgroup_nesting_cmd = (
        "harbor_nest_fail() { "
        "echo \"HARBOR_ERROR: dind-engine cannot nest the Docker daemon's "
        "containers inside its own cgroup ($1). Refusing to start: they would "
        "run outside the Pod's resource accounting and outlive it. A cgroup v2 "
        'node is required." >&2; exit 1; }; '
        "HARBOR_CG_SELF=$(sed -n 's/^0:://p' /proc/self/cgroup); "
        'HARBOR_CG="/sys/fs/cgroup${HARBOR_CG_SELF}"; '
        '{ [ -n "$HARBOR_CG_SELF" ] && [ -f "$HARBOR_CG/cgroup.subtree_control" ]; } '
        '|| harbor_nest_fail "no cgroup v2 hierarchy at ${HARBOR_CG}"; '
        'mkdir -p "$HARBOR_CG/harbor-daemon" '
        '|| harbor_nest_fail "cannot create ${HARBOR_CG}/harbor-daemon"; '
        "HARBOR_CG_NESTED=0; "
        "for _try in 1 2 3 4 5 6 7 8 9 10; do "
        'for _pid in $(cat "$HARBOR_CG/cgroup.procs"); do '
        '{ echo "$_pid" > "$HARBOR_CG/harbor-daemon/cgroup.procs"; } 2>/dev/null; '
        "done; "
        'if { echo "+cpu +memory +pids" > "$HARBOR_CG/cgroup.subtree_control"; } 2>/dev/null; '
        "then HARBOR_CG_NESTED=1; break; fi; "
        "sleep 0.1; "
        "done; "
        '[ "$HARBOR_CG_NESTED" = 1 ] '
        '|| harbor_nest_fail "cannot enable +cpu +memory +pids in ${HARBOR_CG}/cgroup.subtree_control"; '
        # Required controllers are in; anything else the node offers (io,
        # hugetlb, ...) is delegated when possible but never blocks startup.
        'for _ctrl in $(cat "$HARBOR_CG/cgroup.controllers"); do '
        '{ echo "+$_ctrl" > "$HARBOR_CG/cgroup.subtree_control"; } 2>/dev/null; '
        "done; "
        # No attempt is made to clear the kubelet's `memory.oom.group=1` on
        # this cgroup: containerd keeps it in the container's spec and runc
        # writes it back on every UpdateContainerResources (the static CPU
        # manager issues one seconds after start; measured on GKE 1.35.6).
        # Per-process OOM kills, as on a Docker host, come from the node's
        # kubelet setting `singleProcessOOMKill: true`; the teardown usage
        # report names it when a group kill happens.
        if not is_gvisor
        else ""
    )
    stage_cli_cmd = (
        f"{cgroup_nesting_cmd}"
        "mkdir -p /harbor/dind-images /harbor/dind-images/.docker; "
        "cp /usr/local/bin/docker /harbor/dind-images/docker-cli; "
        "chmod 0755 /harbor/dind-images/docker-cli; "
        f"{stage_gcfs_bridge_cmds}"
        f"{symlink_cmds}"
        f"{main_layers_watcher}"
        f"{metadata_lockdown_watcher}"
        "harbor_refresh_gcr_auth() { "
        "[ -f /harbor/dind-images/.auth-scrubbed ] && return 0; "
        'TOK=$(wget -qO- -T 3 --header="Metadata-Flavor: Google" '
        '"http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token" '
        '2>/dev/null | sed -n \'s/.*"access_token":"\\([^"]*\\)".*/\\1/p\'); '
        'if [ -n "$TOK" ] && [ ! -f /harbor/dind-images/.auth-scrubbed ]; then '
        'B64=$(printf "oauth2accesstoken:%s" "$TOK" | base64 | tr -d "\\n\\r "); '
        'SEP=""; ENTRIES=""; '
        f"for H in {hosts_joined}; do "
        'ENTRIES="${ENTRIES}${SEP}\\"https://${H}\\":{\\"auth\\":\\"${B64}\\"}"; '
        'SEP=","; '
        "done; "
        'printf \'{"auths":{%s}}\\n\' "$ENTRIES" '
        "> /harbor/dind-images/.docker/config.json.tmp "
        "&& mv /harbor/dind-images/.docker/config.json.tmp "
        "/harbor/dind-images/.docker/config.json; "
        "fi; "
        "}; "
        "harbor_refresh_gcr_auth; "
        "(while sleep 900; do [ -f /harbor/dind-images/.auth-scrubbed ] && break; harbor_refresh_gcr_auth; done) >/dev/null 2>&1 & "
    )
    dind_dns_opts = "--dns-opt=ndots:1 --dns-opt=timeout:2 --dns-opt=attempts:1"
    dind_pull_opts = "--max-concurrent-downloads=10"
    dockerd_argv = (
        f"dockerd --host=unix:///var/run/harbor-dind/docker.sock {dind_dns_opts} {dind_pull_opts} --iptables=false --ip6tables=false"
        if is_gvisor
        else f"dockerd --host=unix:///var/run/harbor-dind/docker.sock {dind_dns_opts} {dind_pull_opts} "
        '--cgroup-parent="${HARBOR_CG_SELF}/docker"'
    )

    dind_engine_command: list[str] = [
        "sh",
        "-c",
        f"{stage_cli_cmd}exec {dockerd_argv}",
    ]

    if gpu_dind_services:
        # GPU facts are observable ONLY from inside dind-engine:
        #   - /usr/local/nvidia is mounted solely into the container holding the
        #     nvidia.com/gpu allocation (GKE device plugin `-container-path`).
        #   - /dev/nvidia* is visible only because this container is privileged.
        # `compose-up-gate` is neither privileged nor an allocation holder, so it
        # cannot perform this check itself; it reads the result from the shared
        # harbor-dind-gpu volume.
        #
        # Ordering is guaranteed without a handshake: dockerd is only exec'd
        # after the probe writes, and `compose-up-gate` is a later initContainer
        # that cannot start until this sidecar's startup probe (`docker info`)
        # succeeds. The files therefore always exist by the time the gate reads.
        #
        # The probe must never fail. If it exited non-zero, dockerd would never
        # start, the Pod would die as an opaque sidecar crash, and the gate would
        # lose its chance to report a precise, actionable error.
        gpu_probe_script = (
            f"{stage_cli_cmd}"
            f"NVDIR={shlex.quote(_NVIDIA_HOST_DIR)}; "
            f"GPUDIR={shlex.quote(_DIND_GPU_FACTS_DIR)}; "
            'mkdir -p "$GPUDIR"; '
            ': > "$GPUDIR/devices"; '
            "DRV=0; "
            'if [ -d "$NVDIR/lib64" ] && [ -n "$(ls -A "$NVDIR/lib64" 2>/dev/null)" ]; '
            "then DRV=1; fi; "
            'echo "$DRV" > "$GPUDIR/driver-ok"; '
            "for d in /dev/nvidiactl /dev/nvidia-uvm /dev/nvidia-uvm-tools "
            "/dev/nvidia[0-9]*; do "
            '[ -e "$d" ] && echo "$d" >> "$GPUDIR/devices"; '
            "done; "
            "{ echo 'harbor: dind-engine GPU probe'; "
            'echo "driver_dir=$NVDIR lib64_present=$DRV"; '
            'echo "lib64_files=$(ls -1 "$NVDIR/lib64" 2>/dev/null | wc -l)"; '
            'echo "bin_files=$(ls -1 "$NVDIR/bin" 2>/dev/null | wc -l)"; '
            "echo 'devices:'; "
            'cat "$GPUDIR/devices"; '
            '} > "$GPUDIR/diag.txt" 2>&1; '
            'cat "$GPUDIR/diag.txt"; '
            f"exec {dockerd_argv}"
        )
        dind_engine_command = ["sh", "-c", gpu_probe_script]
        volumes_dict["harbor-dind-gpu"] = k8s_client.V1Volume(
            name="harbor-dind-gpu",
            empty_dir=k8s_client.V1EmptyDirVolumeSource(),
        )
        dind_mounts.append(
            k8s_client.V1VolumeMount(
                name="harbor-dind-gpu", mount_path=_DIND_GPU_FACTS_DIR
            )
        )

    dind_security_context = (
        k8s_client.V1SecurityContext(
            capabilities=k8s_client.V1Capabilities(
                add=["SYS_ADMIN", "NET_ADMIN", "MKNOD"]
            )
        )
        if is_gvisor
        else k8s_client.V1SecurityContext(privileged=True)
    )

    dind_engine = k8s_client.V1Container(
        name=DIND_ENGINE_CONTAINER,
        image=dind_image_url,
        command=dind_engine_command,
        security_context=dind_security_context,
        volume_mounts=dind_mounts,
        resources=k8s_client.V1ResourceRequirements(
            requests=dind_engine_requests or None,
            limits=dind_engine_limits or None,
        ),
        restart_policy="Always",
        startup_probe=k8s_client.V1Probe(
            _exec=k8s_client.V1ExecAction(
                command=[
                    "sh",
                    "-c",
                    "DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock docker info >/dev/null 2>&1",
                ]
            ),
            initial_delay_seconds=1,
            period_seconds=2,
            timeout_seconds=5,
            failure_threshold=30,
        ),
    )

    metadata_lockdown_wait = (
        "while [ ! -f /harbor/dind-images/.metadata-blocked ]; do sleep 0.1; done && "
        if not allow_metadata_server
        else ""
    )
    netpol_handshake_wait = (
        "touch /harbor/dind-images/.ready-for-netpol && "
        "while [ ! -f /harbor/dind-images/.netpol-applied ]; do sleep 0.1; done && "
        if wait_for_netpol
        else ""
    )
    pull_script = (
        "DH='unix:///var/run/harbor-dind/docker.sock'; "
        "DCLI='/harbor/dind-images/docker-cli'; "
        "mkdir -p /harbor/dind-images; "
        f"{''.join(pull_cmds)}"
        "touch /harbor/dind-images/.auth-scrubbed && "
        "rm -rf /harbor/dind-images/.docker && "
        f"{metadata_lockdown_wait}"
        f"{netpol_handshake_wait}"
        "exit 0"
    )
    dind_pull = k8s_client.V1Container(
        name="dind-pull",
        image=dind_image_url,
        command=["sh", "-c", pull_script],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                name="harbor-dind-socket",
                mount_path="/var/run/harbor-dind",
            ),
            k8s_client.V1VolumeMount(
                name="harbor-dind-images",
                mount_path="/harbor/dind-images",
            ),
        ],
        restart_policy=None,
    )

    # `load_image` verifies that each DinD sidecar image was materialized into
    # dind-engine by `dind-pull` or its preceding `dind-cache-<sname>` initContainer.
    # When `SN` is `main`, it also records the image's overlay2 layer directories in
    # `/harbor/dind-images/.main-layers` so `dind-engine` can expose `/app` in
    # its mount namespace before nested compose services bind-mount from `/app`.
    load_fn_def = (
        "load_image() { "
        'SN="$1"; IMG="$2"; '
        "DH=unix:///var/run/harbor-dind/docker.sock; "
        'if DOCKER_HOST="${DH}" docker image inspect "${IMG}" >/dev/null 2>&1; then '
        'echo "harbor: ${SN}: image ${IMG} verified in dind-engine"; '
        'if [ "$SN" = "main" ]; then '
        'GD=$(DOCKER_HOST="${DH}" docker image inspect "${IMG}" --format '
        "'{{if .GraphDriver.Data}}{{if .GraphDriver.Data.LowerDir}}{{.GraphDriver.Data.UpperDir}}:{{.GraphDriver.Data.LowerDir}}{{else}}{{.GraphDriver.Data.UpperDir}}{{end}}{{end}}' "
        "2>/dev/null || true); "
        'if [ -n "$GD" ]; then '
        'printf "%s\\n" "$GD" > /harbor/dind-images/.main-layers; '
        "for _i in $(seq 1 50); do "
        "[ -f /harbor/dind-images/.main-layers-ready ] && break; "
        "sleep 0.2; "
        "done; "
        "if [ -s /harbor/dind-images/.main-layers-err ]; then "
        "cat /harbor/dind-images/.main-layers-err >&2; "
        "fi; "
        "fi; "
        "fi; "
        "return 0; "
        "fi; "
        'echo "HARBOR_ERROR: could not materialize image ${IMG} for DinD '
        'service ${SN}: image is missing from dind-engine." >&2; '
        "return 1; "
        "}; "
    )
    loads_joined = " && ".join(load_cmds)
    load_step = f"{load_fn_def}{loads_joined} && " if load_cmds else ""

    # GPU passthrough cannot be decided at translation time:
    #  - The GKE device plugin leaves NVIDIA_VISIBLE_DEVICES empty (measured
    #    2026-09-17), so the allocated index is unknowable.
    #  - `privileged` exposes every /dev/nvidia* on the node regardless of the
    #    allocation, so the visible set is a node property, not a Pod property.
    #
    # It also cannot be probed from *this* container: `compose-up-gate` is
    # neither privileged nor an allocation holder, so it sees neither the device
    # nodes nor the driver bind. It therefore reads the facts that `dind-engine`
    # recorded at startup from the shared harbor-dind-gpu volume.
    #
    # The precondition checked here is the DRIVER BIND, not the device nodes.
    # Device nodes are present even without an allocation; the userspace driver
    # is what actually gates CUDA. Checking devices would pass in exactly the
    # case this guard exists to catch, and the task would then die with
    # "CUDA driver version is insufficient" -- which reads like a task bug.
    #
    # Only `devices` is overridden; the driver bind and LD_LIBRARY_PATH are
    # static and already in the base file, which keeps the reliance on Compose's
    # override merge semantics to a single key.
    gpu_step = ""
    compose_files = "-f /harbor/dind-compose.yaml"
    if gpu_dind_services:
        svc_list = " ".join(shlex.quote(s) for s in sorted(gpu_dind_services))
        facts = shlex.quote(_DIND_GPU_FACTS_DIR)
        gpu_step = (
            f"GPUDIR={facts}; "
            # Always surface what dind-engine saw, success or failure.
            "echo 'harbor: GPU facts reported by dind-engine:'; cat \"$GPUDIR/diag.txt\" 2>/dev/null || "
            "echo '  (no diagnostics written)'; "
            'if [ "$(cat "$GPUDIR/driver-ok" 2>/dev/null)" != "1" ]; then '
            "echo 'HARBOR_ERROR: compose service(s) request a GPU inside DinD but "
            f"the NVIDIA driver directory {_NVIDIA_LIB_DIR} is missing or empty "
            "inside dind-engine. The GKE device plugin mounts it only into the "
            "container holding the nvidia.com/gpu allocation, so this means "
            "dind-engine did not receive one. Check that the Pod scheduled onto "
            "a GPU node and that the allocation was placed on dind-engine.' >&2; "
            "exit 1; "
            "fi; "
            'DEVS=$(cat "$GPUDIR/devices" 2>/dev/null); '
            'if [ -z "$DEVS" ]; then '
            "echo 'HARBOR_ERROR: the NVIDIA driver is present inside dind-engine "
            "but no /dev/nvidia* device node is visible. This is unexpected for a "
            "privileged container on a GPU node and likely indicates a node-level "
            "driver installation failure.' >&2; "
            "exit 1; "
            "fi; "
            "{ echo 'services:'; "
            f"for s in {svc_list}; do "
            'echo "  $s:"; '
            "echo '    devices:'; "
            'for d in $DEVS; do echo "      - \\"$d:$d\\""; done; '
            "done; } > /harbor/dind-compose.gpu.yaml && "
            "echo 'harbor: GPU passthrough override:' && "
            "cat /harbor/dind-compose.gpu.yaml && "
        )
        compose_files = "-f /harbor/dind-compose.yaml -f /harbor/dind-compose.gpu.yaml"

    main_env_ready_wait = ""
    if MAIN_SERVICE_NAME in placement.dind_sidecars:
        main_env_ready_wait = (
            "if DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock "
            "docker exec main grep -q '/tmp/env-ready' /app/entrypoint.sh 2>/dev/null; then "
            "echo 'harbor: waiting for /tmp/env-ready in main...'; "
            "while ! DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock "
            "docker exec main test -f /tmp/env-ready 2>/dev/null; do "
            'if [ "$(DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock '
            "docker inspect -f '{{.State.Running}}' main 2>/dev/null)\" != 'true' ]; then "
            "echo 'HARBOR_ERROR: container main exited before creating /tmp/env-ready:' >&2; "
            "DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock "
            "docker logs --tail=100 main >&2 || true; "
            "exit 1; "
            "fi; "
            "sleep 2; "
            "done; "
            "echo 'harbor: /tmp/env-ready detected in main'; "
            "fi; "
        )

    # Without a bound here the gate blocks until `pod_ready_timeout` (1200s) and
    # the failure reaches the operator as "pod not ready", with no hint that the
    # inner Compose project was the cause.
    gate_script = (
        f"touch /harbor/dind-images/.auth-scrubbed && rm -rf /harbor/dind-images/.docker && "
        f"{metadata_lockdown_wait}"
        f"mkdir -p /harbor && "
        f"echo {shlex.quote(b64_compose)} | base64 -d > /harbor/dind-compose.yaml && "
        f"{gpu_step}"
        f"{load_step}"
        f"DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock "
        f"timeout {compose_up_timeout_sec} "
        f"docker compose {compose_files} --project-name harbor up -d --wait "
        f"--pull never; rc=$?; "
        f'if [ "$rc" -ne 0 ]; then '
        f"DOCKER_HOST=unix:///var/run/harbor-dind/docker.sock "
        f"docker compose {compose_files} --project-name harbor logs --tail=100 >&2 || true; "
        f"fi; "
        f'if [ "$rc" -eq 124 ]; then '
        f"echo 'HARBOR_ERROR: the inner Compose project did not come up within "
        f"{compose_up_timeout_sec}s (compose_up_timeout_sec).' >&2; "
        f"fi; "
        f'if [ "$rc" -ne 0 ]; then exit $rc; fi; '
        f"{main_env_ready_wait}"
        f"exit 0"
    )

    gate_mounts = [
        k8s_client.V1VolumeMount(
            name="harbor-dind-socket", mount_path="/var/run/harbor-dind"
        ),
        k8s_client.V1VolumeMount(
            name="harbor-dind-images", mount_path="/harbor/dind-images"
        ),
    ]
    if gpu_dind_services:
        # Read-only: the gate consumes the facts, dind-engine owns them.
        gate_mounts.append(
            k8s_client.V1VolumeMount(
                name="harbor-dind-gpu",
                mount_path=_DIND_GPU_FACTS_DIR,
                read_only=True,
            )
        )

    compose_up_gate = k8s_client.V1Container(
        name="compose-up-gate",
        image=dind_image_url,
        command=["sh", "-c", gate_script],
        volume_mounts=gate_mounts,
        restart_policy=None,
    )

    return (
        dind_engine,
        dind_pull,
        compose_up_gate,
        dind_service_ips,
        dind_engine_ceiling,
    )


def translate_compose(
    compose_path: Path | Sequence[Path] | None = None,
    pod_name: str = "harbor-pod",
    namespace: str = "default",
    environment_name: str | None = None,
    run_id: str | None = None,
    main_image_url: str | None = None,
    sidecar_image_urls: dict[str, str] | None = None,
    labels: dict[str, str] | None = None,
    startup_env: dict[str, str] | None = None,
    compose_env: dict[str, str] | None = None,
    cpu_request: str | None = None,
    cpu_limit: str | None = None,
    memory_request: str | None = None,
    memory_limit: str | None = None,
    ephemeral_storage_request: str | None = None,
    scratch_volume_size: str | None = None,
    dind_storage_mb: int | None = None,
    compose_up_timeout_sec: int = _GKE_DEFAULT_COMPOSE_UP_TIMEOUT_SEC,
    machine_type: str | None = None,
    node_pool: str | None = None,
    effective_gpus: int = 0,
    gpu_types: list[str] | str | None = None,
    gpu_override: str | None = None,
    default_gpu_type: str | None = None,
    default_gpu_count: int | None = None,
    tpu: TpuSpec | None = None,
    main_workdir: str | None = None,
    service_account_name: str | None = None,
    compose_placement: ComposePlacementMode | None = None,
    image_pull_secrets: list[str] | None = None,
    runtime_class_name: str | None = None,
    is_autopilot: bool = False,
    cluster_capabilities: ClusterCapabilities | None = None,
    image_resolver: ImageResolver | None = None,
    task_dir: Path | None = None,
    active_deadline_seconds: int | None = None,
    *,
    compose_paths: Path | Sequence[Path] | None = None,
    main_image: str | None = None,
    sidecar_images: dict[str, str] | None = None,
    compute_class: str | None = None,
    allow_metadata_server: bool = False,
    wait_for_netpol: bool = False,
    logger: Any | None = None,
    **_extra_kwargs: Any,
) -> k8s_client.V1Pod:
    """Translate a Compose project into a Kubernetes V1Pod.

    The placement classifier chooses the Pod shape: Shape A (all services
    native), Shape B (main native, some sidecars delegated to an in-Pod Docker
    daemon), or Shape C (main and all sidecars in the in-Pod Docker daemon).
    ``compose_placement`` of ``None`` is treated as ``"auto"``.
    """
    eff_paths = compose_paths if compose_paths is not None else compose_path
    if eff_paths is None:
        raise ValueError("translate_compose requires compose_path or compose_paths")
    paths = [eff_paths] if isinstance(eff_paths, Path) else list(eff_paths)
    if not paths:
        raise ValueError("translate_compose requires at least one compose file path")

    eff_main_image = main_image or main_image_url
    if not eff_main_image:
        raise ValueError("translate_compose requires main_image_url or main_image")

    base_dir = Path(paths[0]).resolve().parent
    eff_task_dir = (
        task_dir.resolve()
        if task_dir is not None
        else (base_dir.parent if base_dir.name == "environment" else base_dir)
    )

    resolver = image_resolver or ImageResolver()
    caps = cluster_capabilities or ClusterCapabilities(is_autopilot=is_autopilot)

    effective_mode: ComposePlacementMode = compose_placement or "auto"

    project = normalize_compose_project(
        paths,
        compose_env,
        context_dir=base_dir,
        task_dir=eff_task_dir,
    )

    placement = classify_compose_placement(
        project,
        task_dir=eff_task_dir,
        base_dir=base_dir,
        compose_placement=effective_mode,
        cluster_capabilities=caps,
        toml_gpus=effective_gpus,
        toml_gpu_types=gpu_types,
        gpu_override=gpu_override,
        default_gpu_type=default_gpu_type,
        default_gpu_count=default_gpu_count,
        logger=logger,
    )

    services = project.get("services") or {}

    task_cpu_m = _parse_cpu_millicores_opt(cpu_request or cpu_limit)
    task_mem_mb = _parse_memory_mb_opt(memory_request or memory_limit)
    task_cpu_limit_m = _parse_cpu_millicores_opt(cpu_limit)
    task_mem_limit_mb = _parse_memory_mb_opt(memory_limit)
    main_budget = _MainBudget(
        cpu_request_m=task_cpu_m,
        cpu_limit_m=task_cpu_limit_m,
        memory_request_mb=task_mem_mb,
        memory_limit_mb=task_mem_limit_mb,
    )

    # The task budget caps `main`, as in Harbor's Docker environment (see
    # `_MainBudget`), and bounds the whole Pod -- the "host" -- through
    # `spec.resources` (see `build_pod_level_resources`). Other containers carry
    # only what their own Compose service declares. Ephemeral storage stays a
    # `main` reservation, because the API server accepts only cpu, memory and
    # hugepages-* at Pod level.
    main_reqs: dict[str, str] = {}
    if ephemeral_storage_request:
        main_reqs["ephemeral-storage"] = ephemeral_storage_request

    # Resolve all images through ImageResolver chokepoint (Invariant 6)
    resolved_main_image = resolver.resolve(
        eff_main_image, origin=ImageOrigin.MAIN_BUILT
    )
    resolved_dind_image = resolver.resolve(_DIND_ENGINE_IMAGE, origin=ImageOrigin.INFRA)
    sidecar_urls = dict(sidecar_image_urls or sidecar_images or {})
    resolved_sidecar_images: dict[str, str] = {}
    for sname, sspec in services.items():
        if sname == MAIN_SERVICE_NAME or not isinstance(sspec, dict):
            continue
        if sname in sidecar_urls:
            resolved_sidecar_images[sname] = resolver.resolve(
                sidecar_urls[sname], origin=ImageOrigin.SIDECAR_BUILT
            )
        else:
            raw_img = str(sspec.get("image") or "").strip()
            if not raw_img:
                raise ValueError(
                    f"Compose sidecar service {sname!r} has no 'image' and no prebuilt URL in sidecar_image_urls"
                )
            resolved_sidecar_images[sname] = resolver.resolve(
                raw_img, origin=ImageOrigin.SIDECAR_EXTERNAL
            )

    (
        volumes_dict,
        service_mounts,
        seed_container,
        bind_mount_annotation,
        seed_streaming_required,
    ) = _extract_volumes_and_seed_container(
        project,
        placement,
        task_dir=eff_task_dir,
        base_dir=base_dir,
        is_autopilot=caps.is_autopilot,
        main_image_url=resolved_main_image,
        seed_image_url=resolved_dind_image,
        scratch_volume_size=scratch_volume_size,
        task_memory_budget_mb=task_mem_mb,
    )

    init_containers: list[k8s_client.V1Container] = []
    app_containers: list[k8s_client.V1Container] = []
    any_needs_gvisor = False

    if seed_container is not None:
        init_containers.append(seed_container)

    # 1. Pre-main init containers & native sidecars in topological dependency order
    init_service_set = set(placement.native_init_services)
    ordered_pre_main = (
        list(placement.pre_main_ordered_services)
        if placement.pre_main_ordered_services
        else [*placement.native_init_services, *placement.native_sidecar_services]
    )
    ordered_pre_main_containers: list[k8s_client.V1Container] = []
    for sname in ordered_pre_main:
        sspec = services[sname]
        is_one_shot = sname in init_service_set
        c_obj, gvisor = _build_k8s_container(
            sname,
            sspec,
            image_url=resolved_sidecar_images[sname],
            is_main=False,
            is_native_sidecar=not is_one_shot,
            is_post_main_sidecar=False,
            startup_env=None,
            volume_mounts=service_mounts.get(sname, []),
            default_reqs={},
            main_workdir=None,
            gpu_count=placement.gpu_config.service_gpus.get(sname, 0),
            is_one_shot_init=is_one_shot,
        )
        any_needs_gvisor = any_needs_gvisor or gvisor
        ordered_pre_main_containers.append(c_obj)

    # 3. Shape B / Shape C DinD containers (dind-engine + dind-pull + ordered_pre_main + compose-up-gate)
    # Ordering invariant (WP-3 / F-05):
    #   harbor-seed (trusted) -> dind-engine (trusted) -> dind-pull (trusted: pulls images, scrubs .docker,
    #   blocks metadata, and optionally waits for restrictive NetworkPolicy)
    #   -> ordered_pre_main_containers (untrusted task init/sidecar containers) -> compose-up-gate (trusted).
    dind_service_ips: dict[str, str] = {}
    dind_engine_ceiling: dict[str, int] = {}
    if placement.shape in ("B", "C"):
        (
            dind_engine,
            dind_pull,
            compose_up_gate,
            dind_service_ips,
            dind_engine_ceiling,
        ) = _build_shape_b_dind_containers(
            project,
            placement,
            dind_image_url=resolved_dind_image,
            sidecar_image_urls={
                MAIN_SERVICE_NAME: resolved_main_image,
                **resolved_sidecar_images,
            },
            volumes_dict=volumes_dict,
            service_mounts=service_mounts,
            is_gvisor=(runtime_class_name == "gvisor"),
            task_storage_budget_mb=_parse_memory_mb(ephemeral_storage_request, 0),
            dind_storage_mb=dind_storage_mb,
            compose_up_timeout_sec=compose_up_timeout_sec,
            startup_env=startup_env,
            main_workdir=main_workdir,
            allow_metadata_server=bool(allow_metadata_server),
            wait_for_netpol=bool(wait_for_netpol),
            main_budget=main_budget,
        )
        init_containers.append(dind_engine)
        if placement.shape == "C" and runtime_class_name != "gvisor":
            dind_main_rootfs = k8s_client.V1Container(
                name=DIND_MAIN_ROOTFS_CONTAINER,
                image=resolved_main_image,
                command=[
                    "/harbor/dind-images/ld-musl.so.1",
                    "/harbor/dind-images/busybox",
                    "sh",
                    "-c",
                    'BB="/harbor/dind-images/ld-musl.so.1 /harbor/dind-images/busybox"; '
                    "if [ ! -f /harbor/dind-images/.main-rootfs-captured ]; then "
                    "$BB mkdir -p /harbor/dind-images/main-rootfs && "
                    "$BB mount --bind / /harbor/dind-images/main-rootfs && "
                    ": > /harbor/dind-images/.main-rootfs-mounted && "
                    "while [ ! -f /harbor/dind-images/.main-rootfs-ack ]; do "
                    "$BB sleep 0.05; "
                    "done; "
                    "$BB umount -l /harbor/dind-images/main-rootfs 2>/dev/null || true; "
                    "$BB rmdir /harbor/dind-images/main-rootfs 2>/dev/null || true; "
                    "fi; "
                    ": > /harbor/dind-images/.main-rootfs-ready && "
                    "exec $BB sleep 3650d",
                ],
                security_context=k8s_client.V1SecurityContext(privileged=True),
                volume_mounts=[
                    k8s_client.V1VolumeMount(
                        name="harbor-dind-images",
                        mount_path="/harbor/dind-images",
                        mount_propagation="Bidirectional",
                    ),
                ],
                restart_policy="Always",
                startup_probe=k8s_client.V1Probe(
                    _exec=k8s_client.V1ExecAction(
                        command=[
                            "/harbor/dind-images/ld-musl.so.1",
                            "/harbor/dind-images/busybox",
                            "test",
                            "-f",
                            "/harbor/dind-images/.main-rootfs-ready",
                        ]
                    ),
                    initial_delay_seconds=0,
                    period_seconds=1,
                    timeout_seconds=5,
                    failure_threshold=30,
                ),
            )
            init_containers.append(dind_main_rootfs)
        init_containers.append(dind_pull)
        init_containers.extend(ordered_pre_main_containers)
        init_containers.append(compose_up_gate)
    else:
        init_containers.extend(ordered_pre_main_containers)

    # 4. Main container (native in Shape A/B; lifecycle mirror in Shape C per RFC 0004)
    main_spec = services[MAIN_SERVICE_NAME]
    if placement.shape == "C":
        proxy_mounts = [
            k8s_client.V1VolumeMount(
                name="harbor-dind-socket",
                mount_path="/var/run/harbor-dind",
            ),
            *[
                vm
                for vm in service_mounts.get(MAIN_SERVICE_NAME, [])
                if vm.name != "harbor-dind-socket"
            ],
        ]
        main_container = k8s_client.V1Container(
            name=MAIN_SERVICE_NAME,
            image=resolved_dind_image,
            command=[
                "sh",
                "-c",
                "rc=$(docker wait main 2>/dev/null || echo 1); exit ${rc:-1}",
            ],
            env=[
                k8s_client.V1EnvVar(
                    name="DOCKER_HOST",
                    value="unix:///var/run/harbor-dind/docker.sock",
                )
            ],
            resources=k8s_client.V1ResourceRequirements(
                requests=main_reqs or None,
            ),
            volume_mounts=proxy_mounts,
        )
    else:
        main_container, main_gvisor = _build_k8s_container(
            MAIN_SERVICE_NAME,
            main_spec,
            image_url=resolved_main_image,
            is_main=True,
            is_native_sidecar=False,
            is_post_main_sidecar=False,
            startup_env=startup_env,
            volume_mounts=service_mounts.get(MAIN_SERVICE_NAME, []),
            default_reqs=main_reqs,
            main_workdir=main_workdir,
            gpu_count=placement.gpu_config.service_gpus.get(MAIN_SERVICE_NAME, 0),
            tpu_spec=tpu,
        )
        any_needs_gvisor = any_needs_gvisor or main_gvisor
        main_budget.apply_to(main_container)
    app_containers.append(main_container)

    # 5. Post-main sidecars (Decision Q1: services declaring depends_on: main)
    for sname in placement.post_main_sidecars:
        sspec = services[sname]
        c_obj, gvisor = _build_k8s_container(
            sname,
            sspec,
            image_url=resolved_sidecar_images[sname],
            is_main=False,
            is_native_sidecar=False,
            is_post_main_sidecar=True,
            startup_env=None,
            volume_mounts=service_mounts.get(sname, []),
            default_reqs={},
            main_workdir=None,
            gpu_count=placement.gpu_config.service_gpus.get(sname, 0),
        )
        any_needs_gvisor = any_needs_gvisor or gvisor
        app_containers.append(c_obj)

    # 6. Pod-level metadata & scheduling
    annotations: dict[str, str] = {
        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
        "harbor.dev/compose-placement": (
            "dind" if placement.shape in ("B", "C") else "native"
        ),
        "harbor.dev/compose-placement-shape": placement.shape,
        "harbor.dev/compose-placement-summary": json.dumps(
            {
                "shape": placement.shape,
                "dind_reasons": placement.dind_reasons,
                "warnings": placement.warnings,
                "streaming": resolver.streaming_report(),
            }
        ),
    }
    if placement.dind_sidecars:
        annotations["harbor.dev/dind-delegated-services"] = json.dumps(
            placement.dind_sidecars
        )
    if placement.post_main_sidecars:
        annotations["harbor.dev/post-main-sidecars"] = ",".join(
            placement.post_main_sidecars
        )
    if bind_mount_annotation:
        annotations["harbor.dev/compose-bind-mounts"] = json.dumps(
            bind_mount_annotation
        )
        if seed_streaming_required:
            annotations["harbor.dev/seed-required"] = "true"

    node_selector: dict[str, str] = {}
    tolerations: list[k8s_client.V1Toleration] = []

    if node_pool:
        node_selector["cloud.google.com/gke-nodepool"] = node_pool
    if compute_class:
        node_selector["cloud.google.com/compute-class"] = compute_class
    elif machine_type:
        # `machine_type` selects the machine family so the Pod can land on any
        # node of that family that satisfies its resource requests (including
        # larger sizes). Minimum vCPU size is checked at preflight.
        family = machine_type.split("-")[0]
        if family:
            node_selector["cloud.google.com/machine-family"] = family

    if placement.gpu_config.gpus > 0:
        if placement.gpu_config.accelerator_label and not compute_class:
            node_selector["cloud.google.com/gke-accelerator"] = (
                placement.gpu_config.accelerator_label
            )
        tolerations.append(
            k8s_client.V1Toleration(
                key="nvidia.com/gpu",
                operator="Exists",
                effect="NoSchedule",
            )
        )

    if tpu is not None and tpu.chip_count > 0:
        tpu_label = resolve_tpu_accelerator_label(tpu.type)
        if tpu_label:
            node_selector["cloud.google.com/gke-tpu-accelerator"] = tpu_label
        if tpu.topology:
            node_selector["cloud.google.com/gke-tpu-topology"] = str(tpu.topology)
        tolerations.append(
            k8s_client.V1Toleration(
                key="google.com/tpu",
                operator="Exists",
                effect="NoSchedule",
            )
        )

    # Decision Q9: terminationGracePeriodSeconds = max across services
    grace_periods = [30]
    for sspec in services.values():
        if isinstance(sspec, dict) and sspec.get("stop_grace_period"):
            parsed_g = parse_duration_seconds(sspec["stop_grace_period"])
            if parsed_g is not None:
                grace_periods.append(parsed_g)
    max_grace = max(grace_periods)

    # Pod hostname from main if declared
    pod_hostname: str | None = None
    if main_spec.get("hostname"):
        pod_hostname = _sanitize_kubernetes_resource_name(str(main_spec["hostname"]))

    effective_runtime_class = runtime_class_name
    if any_needs_gvisor:
        above_baseline_entries: list[str] = []
        for s_name, s_spec in services.items():
            if not isinstance(s_spec, dict) or s_name in placement.dind_sidecars:
                continue
            for raw_cap in s_spec.get("cap_add") or []:
                clean_c = str(raw_cap).upper().removeprefix("CAP_")
                if clean_c not in _PSS_BASELINE_CAPABILITIES:
                    above_baseline_entries.append(f"{s_name}:{clean_c}")
        if above_baseline_entries:
            annotations["harbor.dev/gvisor-capabilities"] = ",".join(
                sorted(above_baseline_entries)
            )
    if (
        not effective_runtime_class
        and caps.is_autopilot
        and any_needs_gvisor
        and placement.shape == "A"
    ):
        effective_runtime_class = "gvisor"

    pull_secrets = (
        [k8s_client.V1LocalObjectReference(name=s) for s in image_pull_secrets]
        if image_pull_secrets
        else None
    )

    effective_labels: dict[str, str] = {
        "app": "sandbox",
        "session": pod_name,
    }
    if environment_name:
        effective_labels["environment"] = _sanitize_kubernetes_resource_name(
            environment_name
        )
    if run_id:
        effective_labels["run"] = _sanitize_kubernetes_resource_name(run_id)
    if labels:
        effective_labels.update(labels)

    # Derived from the assembled containers, not from the inputs: the API server
    # rejects a Pod whose `spec.resources.requests` fall below the aggregate
    # container requests, and that aggregate depends on the order init
    # containers were appended in. Building it here is the only way to be sure
    # the two agree.
    #
    # Every shape: the Pod is the task's Docker host, and its ceiling keeps the
    # task off memory and CPU the scheduler gave to other Pods. In a DinD Pod
    # the daemon and its containers are nested in `dind-engine`, whose ceiling
    # only the DinD builder knows.
    pod_level_resources = build_pod_level_resources(
        init_containers,
        app_containers,
        task_cpu_m=task_cpu_m,
        task_mem_mb=task_mem_mb,
        task_cpu_limit_m=task_cpu_limit_m,
        task_mem_limit_mb=task_mem_limit_mb,
        host_ceilings=(
            {DIND_ENGINE_CONTAINER: dind_engine_ceiling}
            if dind_engine_ceiling
            else None
        ),
        is_autopilot=caps.is_autopilot,
        log=logger,
    )

    pod = k8s_client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=k8s_client.V1ObjectMeta(
            name=pod_name,
            namespace=namespace,
            labels=effective_labels,
            annotations=annotations,
        ),
        spec=k8s_client.V1PodSpec(
            restart_policy="Never",
            # Never project a ServiceAccount token into a sandbox. Workload
            # Identity on GKE resolves through the metadata server using the
            # Pod's service account, so this does not disable WIF. Keep this in
            # step with build_direct_pod().
            automount_service_account_token=False,
            init_containers=init_containers or None,
            containers=app_containers,
            volumes=list(volumes_dict.values()) or None,
            host_aliases=_build_host_aliases(
                services, dind_service_ips=dind_service_ips
            )
            or None,
            hostname=pod_hostname,
            node_selector=node_selector or None,
            tolerations=tolerations or None,
            service_account_name=service_account_name,
            image_pull_secrets=pull_secrets,
            runtime_class_name=effective_runtime_class,
            termination_grace_period_seconds=max_grace,
            active_deadline_seconds=active_deadline_seconds,
            resources=pod_level_resources,
        ),
    )

    # Invariant 6: assert every container image on the Pod came from ImageResolver
    resolver.assert_pod_images_resolved(pod)
    return pod


class _GKENativeComposeServiceTransport(ComposeServiceTransport):
    """Service transport executing directly against sibling containers in a Unified Native Pod."""

    def __init__(self, env: GKEEnvironment):
        self._env = env

    def _is_dind_delegated(self, service: str) -> bool:
        pod = getattr(self._env, "_created_pod", None)
        if not pod or not pod.metadata or not pod.metadata.annotations:
            return False
        raw = pod.metadata.annotations.get("harbor.dev/dind-delegated-services")
        if not raw:
            return False
        try:
            delegated = json.loads(raw)
            return (
                service in delegated
                or _sanitize_kubernetes_resource_name(service) in delegated
            )
        except Exception:
            return False

    @staticmethod
    def _container_name(service: str) -> str:
        return (
            MAIN_SERVICE_NAME
            if service == MAIN_SERVICE_NAME
            else _sanitize_kubernetes_resource_name(service)
        )

    async def _dind_staged_download(
        self,
        *,
        staging: str,
        cp_cmd: str,
        download_fn: Any,
    ) -> None:
        await self._env.exec(cp_cmd, container="dind-engine")
        try:
            await download_fn(staging)
        finally:
            await self._env.exec(
                f"rm -rf {shlex.quote(staging)}", container="dind-engine"
            )

    @override
    async def service_exec(
        self,
        command: str,
        *,
        service: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        if self._is_dind_delegated(service):
            docker_flags: list[str] = []
            if cwd:
                docker_flags.append(f"-w {shlex.quote(cwd)}")
            if user is not None:
                docker_flags.append(f"-u {shlex.quote(str(user))}")
            if env:
                for k, v in env.items():
                    docker_flags.append(f"-e {shlex.quote(f'{k}={v}')}")
            flags_str = " ".join(docker_flags)
            dind_cmd = (
                f"docker --host=unix:///var/run/harbor-dind/docker.sock exec "
                f"{flags_str} {shlex.quote(service)} sh -c {shlex.quote(command)}"
            )
            return await self._env.exec(
                dind_cmd,
                timeout_sec=timeout_sec,
                container="dind-engine",
            )

        return await self._env.exec(
            command,
            cwd=cwd,
            env=env,
            timeout_sec=timeout_sec,
            user=user,
            container=self._container_name(service),
        )

    @override
    async def service_download_file(
        self,
        source_path: str,
        target_path: Path | str,
        *,
        service: str,
    ) -> None:
        if self._is_dind_delegated(service):
            staging = (
                f"/var/run/harbor-dind/dl_f_{os.getpid()}_{abs(hash(source_path))}"
            )
            cp_cmd = (
                f"docker --host=unix:///var/run/harbor-dind/docker.sock cp "
                f"{shlex.quote(service)}:{shlex.quote(source_path)} {shlex.quote(staging)}"
            )
            await self._dind_staged_download(
                staging=staging,
                cp_cmd=cp_cmd,
                download_fn=lambda path: self._env.download_file(
                    source_path=path,
                    target_path=target_path,
                    container="dind-engine",
                ),
            )
            return

        await self._env.download_file(
            source_path=source_path,
            target_path=target_path,
            container=self._container_name(service),
        )

    @override
    async def service_download_dir(
        self,
        source_dir: str,
        target_dir: Path | str,
        *,
        service: str,
    ) -> None:
        if self._is_dind_delegated(service):
            staging = f"/var/run/harbor-dind/dl_d_{os.getpid()}_{abs(hash(source_dir))}"
            cp_cmd = (
                f"mkdir -p {shlex.quote(staging)} && "
                f"docker --host=unix:///var/run/harbor-dind/docker.sock cp "
                f"{shlex.quote(service)}:{shlex.quote(source_dir.rstrip('/'))}/. {shlex.quote(staging)}"
            )
            await self._dind_staged_download(
                staging=staging,
                cp_cmd=cp_cmd,
                download_fn=lambda path: self._env.download_dir(
                    source_dir=path,
                    target_dir=target_dir,
                    container="dind-engine",
                ),
            )
            return

        await self._env.download_dir(
            source_dir=source_dir,
            target_dir=target_dir,
            container=self._container_name(service),
        )

    @override
    async def stop_service(self, service: str) -> None:
        if self._is_dind_delegated(service):
            freeze_cmd = (
                f"docker --host=unix:///var/run/harbor-dind/docker.sock kill "
                f"--signal=STOP {shlex.quote(service)} 2>/dev/null || true"
            )
            await self._env.exec(freeze_cmd, container="dind-engine", timeout_sec=10)
            return

        container = self._container_name(service)
        if container == MAIN_SERVICE_NAME:
            kill_cmd = (
                "pids=$(ps -e -o pid= 2>/dev/null | tr -d ' ' | grep -v -E '^(1|'\"$$\"')$' || true); "
                'if [ -n "$pids" ]; then '
                "kill -TERM $pids 2>/dev/null || true; "
                "sleep 0.5; "
                "kill -KILL $pids 2>/dev/null || true; "
                "fi"
            )
            await self._env.exec(kill_cmd, container=MAIN_SERVICE_NAME, timeout_sec=15)
        else:
            freeze_cmd = "kill -STOP -1 2>/dev/null || true"
            await self._env.exec(freeze_cmd, container=container, timeout_sec=10)


__all__ = [
    "MAIN_SERVICE_NAME",
    "_GKENativeComposeServiceTransport",
    "_convert_dict_probe_to_k8s",
    "_parse_compose_duration_sec",
    "aggregate_pod_resource",
    "build_pod_level_resources",
    "discover_compose_build_services",
    "resolve_compose_infra_env",
    "translate_compose",
]
