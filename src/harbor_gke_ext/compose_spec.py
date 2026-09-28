"""Compose CLI normalization and spec loader for the GKE environment backend.

Why this module exists (Commit S1)
----------------------------------
Previously, GKE compose translation maintained two independent YAML parsers:
1. A custom regex variable interpolator (`interpolate_compose_variables`) and
   shallow dictionary merger (`load_and_merge_compose_specs`).
2. An external `kompose` binary (v1.35.0) invoked as a subprocess.

Those two parsers disagreed on Compose variable interpolation (`$$` escaping,
`${VAR:-default}`), `env_file` precedence, multi-file merging (`-f base.yaml
-f override.yaml`), duration formats, and volume syntax normalization, causing
11 reproducible translation defects.

This module replaces both with a single source of truth: the official
`docker compose config --format json` CLI (or standalone `docker-compose` /
`podman compose`). When no Compose CLI plugin or binary is installed on the
host, Harbor automatically downloads and SHA-256-verifies a pinned standalone
`docker-compose` release binary (Decision D4 / Q7).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import threading
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
from typing import Any
import urllib.request

from harbor.utils.logger import logger

MAIN_SERVICE_NAME = "main"

_PINNED_COMPOSE_VERSION = "v2.32.4"
_PINNED_COMPOSE_SHA256: dict[tuple[str, str], str] = {
    (
        "darwin",
        "arm64",
    ): "dc30b0276c0ba45857eef021b677d4fb2bbf13bcf809f99b691db9512bca47cc",
    (
        "darwin",
        "aarch64",
    ): "dc30b0276c0ba45857eef021b677d4fb2bbf13bcf809f99b691db9512bca47cc",
    (
        "darwin",
        "x86_64",
    ): "bd6f3b3b93032f47ad2c017e92df302651c9a649f4b533455371877eaaa76585",
    (
        "linux",
        "arm64",
    ): "0c4591cf3b1ed039adcd803dbbeddf757375fc08c11245b0154135f838495a2f",
    (
        "linux",
        "aarch64",
    ): "0c4591cf3b1ed039adcd803dbbeddf757375fc08c11245b0154135f838495a2f",
    (
        "linux",
        "x86_64",
    ): "ed1917fb54db184192ea9d0717bcd59e3662ea79db48bff36d3475516c480a6b",
}

_DURATION_PART_RE = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")
_CACHED_COMPOSE_CMD: list[str] | None = None


def parse_duration_seconds(val: str | int | float | None) -> int | None:
    """Parse a Compose duration (Go duration string, seconds, or nanoseconds) into integer seconds.

    Positive sub-second durations round up to 1 second so Kubernetes probe
    intervals and timeouts never become 0.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if val <= 0:
            return 0
        # Go / Docker healthcheck structs sometimes serialize durations as nanoseconds
        if val >= 1_000_000_000:
            return max(1, int(math.ceil(val / 1_000_000_000)))
        return max(1, int(math.ceil(val)))

    raw = str(val).strip()
    if not raw:
        return None
    if raw.isdigit():
        num = int(raw)
        if num >= 1_000_000_000:
            return max(1, int(math.ceil(num / 1_000_000_000)))
        return num

    total_sec = 0.0
    matched = False
    for match in _DURATION_PART_RE.finditer(raw):
        matched = True
        amount = float(match.group(1))
        unit = match.group(2)
        if unit == "h":
            total_sec += amount * 3600.0
        elif unit == "m":
            total_sec += amount * 60.0
        elif unit == "s":
            total_sec += amount
        elif unit == "ms":
            total_sec += amount / 1000.0
        elif unit in ("us", "µs"):
            total_sec += amount / 1_000_000.0
        elif unit == "ns":
            total_sec += amount / 1_000_000_000.0

    if not matched:
        try:
            total_sec = float(raw)
        except ValueError:
            return None

    if total_sec <= 0:
        return 0
    return max(1, int(math.ceil(total_sec)))


def _probe_compose_cmd(cmd: list[str]) -> bool:
    """Return True if ``cmd + ['version']`` executes cleanly."""
    env = os.environ.copy()
    env["DOCKER_HOST"] = "unix:///nonexistent.sock"
    try:
        res = subprocess.run(
            [*cmd, "version"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return res.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _download_standalone_compose(target_path: Path) -> None:
    """Download and SHA-256 verify standalone docker-compose binary."""
    sys_name = platform.system().lower()
    machine = platform.machine().lower()
    expected_sha = _PINNED_COMPOSE_SHA256.get((sys_name, machine))
    if not expected_sha:
        raise RuntimeError(
            f"Unsupported platform for standalone docker-compose download: "
            f"system={sys_name!r}, machine={machine!r}"
        )

    arch_slug = "aarch64" if machine in ("arm64", "aarch64") else "x86_64"
    asset_name = f"docker-compose-{sys_name}-{arch_slug}"
    url = (
        f"https://github.com/docker/compose/releases/download/"
        f"{_PINNED_COMPOSE_VERSION}/{asset_name}"
    )

    target_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug(
        f"Downloading standalone docker-compose {_PINNED_COMPOSE_VERSION} from {url}"
    )

    with tempfile.NamedTemporaryFile(
        dir=target_path.parent, prefix=".docker-compose-tmp-", delete=False
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        hasher = hashlib.sha256()
        with urllib.request.urlopen(url, timeout=60) as response:
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                hasher.update(chunk)
                with tmp_path.open("ab") as out_f:
                    out_f.write(chunk)

        actual_sha = hasher.hexdigest().lower()
        if actual_sha != expected_sha.lower():
            raise RuntimeError(
                f"SHA-256 verification failed for {url}: "
                f"expected {expected_sha}, got {actual_sha}"
            )

        tmp_path.chmod(0o755)
        tmp_path.replace(target_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


_COMPOSE_BINARY_LOCK = threading.Lock()


def ensure_compose_binary(*, force_probe: bool = False) -> list[str]:
    """Locate a working Compose CLI command or download the pinned fallback binary (gated once per process)."""
    global _CACHED_COMPOSE_CMD
    if _CACHED_COMPOSE_CMD is not None and not force_probe:
        return list(_CACHED_COMPOSE_CMD)

    with _COMPOSE_BINARY_LOCK:
        if _CACHED_COMPOSE_CMD is not None and not force_probe:
            return list(_CACHED_COMPOSE_CMD)

        cache_bin = Path.home() / ".cache" / "harbor" / "bin" / "docker-compose"
        candidates: list[list[str]] = [
            ["docker", "compose"],
            ["docker-compose"],
            ["/usr/local/bin/docker-compose"],
            ["/opt/homebrew/bin/docker-compose"],
            [str(cache_bin)],
            ["podman", "compose"],
            ["podman-compose"],
        ]

        for cmd in candidates:
            if _probe_compose_cmd(cmd):
                _CACHED_COMPOSE_CMD = cmd
                return list(cmd)

        # Fallback: download pinned standalone docker-compose binary
        _download_standalone_compose(cache_bin)
        if not _probe_compose_cmd([str(cache_bin)]):
            raise RuntimeError(
                f"Downloaded docker-compose binary at {cache_bin} failed execution check"
            )
        _CACHED_COMPOSE_CMD = [str(cache_bin)]
        return list(_CACHED_COMPOSE_CMD)


def _needs_base_main_overlay(compose_files: Sequence[Path]) -> bool:
    """Return True if ``main`` lacks both ``image`` and ``build`` across input files.

    Harbor task compose files are often fragments where ``main`` only specifies
    ``command``, ``volumes``, or ``environment`` because ``image``/``build`` is
    supplied by Harbor's base compose wrapper.

    This gates only the ``image`` line of that wrapper. The wrapper's ``command``
    is written unconditionally, because keeping ``main`` alive is independent of
    where its image comes from.
    """
    import yaml

    has_image_or_build = False
    for path in compose_files:
        if not path.is_file():
            continue
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        services = doc.get("services")
        if not isinstance(services, dict):
            continue
        main_spec = services.get(MAIN_SERVICE_NAME)
        if isinstance(main_spec, dict) and (
            main_spec.get("image") or main_spec.get("build")
        ):
            has_image_or_build = True
            break
    return not has_image_or_build


def build_default_compose_env(
    env_vars: Mapping[str, str] | None = None,
    *,
    context_dir: Path | str | None = None,
    main_image_name: str = "hb__task:latest",
) -> dict[str, str]:
    """Assemble environment variable dictionary for Compose CLI config parsing.

    Guarantees non-empty defaults for Harbor infrastructure variables (``CPUS``,
    ``MEMORY``, ``CONTEXT_DIR``, ``MAIN_IMAGE_NAME``, log paths) so compose files
    referencing ``${CPUS}`` or ``${MEMORY}`` without inline YAML defaults never
    trigger ``strconv.ParseFloat`` errors in ``compose-go``.
    """
    merged = os.environ.copy()
    merged["DOCKER_HOST"] = "unix:///nonexistent.sock"

    ctx_str = (
        str(Path(context_dir).resolve().absolute())
        if context_dir is not None
        else "/harbor/environment"
    )

    defaults = {
        "CONTEXT_DIR": ctx_str,
        "MAIN_IMAGE_NAME": main_image_name,
        "PREBUILT_IMAGE_NAME": main_image_name,
        "CPUS": "1",
        "MEMORY": "2048M",
        "ENV_ARTIFACTS_PATH": "/logs/artifacts",
        "HOST_ARTIFACTS_PATH": "/logs/artifacts",
        "ENV_VERIFIER_LOGS_PATH": "/logs/verifier",
        "HOST_VERIFIER_LOGS_PATH": "/logs/verifier",
        "ENV_AGENT_LOGS_PATH": "/logs/agent",
        "HOST_AGENT_LOGS_PATH": "/logs/agent",
        "TEST_DIR": "/tests",
    }

    for k, v in defaults.items():
        if not merged.get(k):
            merged[k] = v

    if env_vars:
        for k, v in env_vars.items():
            if v is not None and str(v) != "":
                merged[k] = str(v)
            elif k not in defaults:
                merged[k] = ""

    return merged


def normalize_compose_project(
    compose_files: Sequence[Path],
    env_vars: Mapping[str, str] | None = None,
    *,
    project_name: str = "harbor",
    context_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Normalize and merge Compose files via ``docker compose config --format json``.

    Returns the parsed Compose project dictionary with variables interpolated,
    multi-file overrides merged, ``env_file`` loaded, and volume/dependency
    structures canonicalized.
    """
    valid_files = [Path(p).resolve() for p in compose_files if Path(p).is_file()]
    if not valid_files:
        raise FileNotFoundError(
            f"No existing compose files provided in {compose_files!r}"
        )

    eff_context_dir = (
        Path(context_dir).resolve()
        if context_dir is not None
        else valid_files[0].parent
    )
    proc_env = build_default_compose_env(
        env_vars,
        context_dir=eff_context_dir,
        main_image_name=(env_vars or {}).get("MAIN_IMAGE_NAME", "hb__task:latest"),
    )

    compose_cmd = ensure_compose_binary()
    temp_base_file: Path | None = None

    try:
        files_arg: list[str] = []

        # Harbor's own Docker path always prepends a base overlay
        # (`harbor/environments/docker/docker-compose-{build,prebuilt}.yaml`),
        # and both of those declare `command: ["sh", "-c", "sleep infinity"]`.
        # That command is what keeps `main` alive; without it the container runs
        # the image's default CMD, which across the task corpus is overwhelmingly
        # `python3`. With no TTY that reads EOF immediately and exits 0, the Pod
        # reports Ready with a Completed `main`, and the trial dies on the next
        # exec with a message that points nowhere near the cause.
        #
        # The overlay is first on the `-f` list, so a task declaring its own
        # `command` still wins -- same precedence Compose gives it under Docker.
        #
        # `image` stays conditional: a service declaring `build:` resolves its
        # own image, and adding one here would be a second, contradictory source
        # of truth. `command` has no such coupling, so it is always written.
        overlay_lines = ["services:", "  main:"]
        if _needs_base_main_overlay(valid_files):
            overlay_lines.append("    image: ${MAIN_IMAGE_NAME:-harbor-main:latest}")
        overlay_lines.append('    command: [ "sh", "-c", "sleep infinity" ]')

        with tempfile.NamedTemporaryFile(
            "w", suffix=".yaml", prefix="harbor-compose-base-", delete=False
        ) as tf:
            tf.write("\n".join(overlay_lines) + "\n")
            temp_base_file = Path(tf.name)
        files_arg.extend(["-f", str(temp_base_file)])

        for fpath in valid_files:
            files_arg.extend(["-f", str(fpath)])

        cmd = [
            *compose_cmd,
            "--project-name",
            project_name,
            "--project-directory",
            str(eff_context_dir),
            *files_arg,
            "config",
            "--format",
            "json",
        ]
        res = subprocess.run(
            cmd,
            env=proc_env,
            cwd=str(eff_context_dir),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if res.returncode != 0:
            raise ValueError(
                f"Compose specification validation failed (exit {res.returncode}):\n"
                f"{res.stderr.strip() or res.stdout.strip()}"
            )

        project_data = json.loads(res.stdout)
        if not isinstance(project_data, dict):
            raise ValueError(
                f"Expected JSON object from compose config, got {type(project_data).__name__}"
            )
        return project_data
    finally:
        if temp_base_file is not None and temp_base_file.exists():
            temp_base_file.unlink(missing_ok=True)


def discover_compose_build_services(
    compose_files: Path | Sequence[Path],
    compose_env: Mapping[str, str] | None = None,
    *,
    require_dir: bool = True,
) -> dict[str, tuple[Path, str | None]]:
    """Discover non-main services that declare a ``build`` section.

    Returns ``{service_name: (resolved_context_path, optional_dockerfile_name)}``.
    Used by :mod:`harbor_gke_ext.image_plan` to pre-build sidecars in
    Artifact Registry.
    """
    paths = [compose_files] if isinstance(compose_files, Path) else list(compose_files)
    if not paths:
        return {}
    base_dir = Path(paths[0]).resolve().parent
    project = normalize_compose_project(
        paths,
        compose_env,
        context_dir=base_dir,
    )
    services = project.get("services") or {}
    discovered: dict[str, tuple[Path, str | None]] = {}

    for sname, sspec in services.items():
        if sname == MAIN_SERVICE_NAME or not isinstance(sspec, dict):
            continue
        build_spec = sspec.get("build")
        if not build_spec:
            continue
        if isinstance(build_spec, str):
            ctx_path = (base_dir / build_spec).resolve()
            dockerfile = None
        elif isinstance(build_spec, dict):
            ctx_raw = build_spec.get("context") or "."
            ctx_path = (base_dir / str(ctx_raw)).resolve()
            df = build_spec.get("dockerfile")
            dockerfile = None if df in (None, "", "Dockerfile") else str(df)
        else:
            continue
        if not require_dir or ctx_path.is_dir():
            discovered[sname] = (ctx_path, dockerfile)

    return discovered
