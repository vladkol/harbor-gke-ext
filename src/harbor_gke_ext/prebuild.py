"""Dataset Environment Image Pre-Builder and Harbor JobPlugin for GKE.

Provides both:
1. CloudBuildPlugin: A native Harbor JobPlugin (attachable via `--plugin cloud-build`
   or `--plugin harbor_gke_ext.prebuild:CloudBuildPlugin`) that discovers
   all task environments in a job, computes content-addressed digests, and pre-builds
   images in parallel with bounded concurrency (e.g. 25-30) before trial workers start.
2. CLI entrypoint: Invokable via `harbor-gke-ext-prebuild <dataset_dir>`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, override

from rich.console import Console
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

from harbor.environments.definition import (
    COMPOSE_FILE_NAME,
    DOCKERFILE_NAME,
)
from harbor_gke_ext.client import (
    derive_region,
    ensure_gcloud_ready,
    get_active_gke_context,
    resolve_default_project_id,
)
from harbor_gke_ext.cloud_build import (
    build_task_image_if_missing,
    check_image_exists_in_registry,
    resolve_task_image_url,
    submit_cloud_build,
)
from harbor_gke_ext.constants import _parse_bool
from harbor_gke_ext.image_plan import (
    ImageSpec,
    TaskImagePlan,
    dedupe_specs,
    plan_task_images,
    record_planned_images,
)
from harbor.models.job.plugin import BaseJobPlugin
from harbor.utils.logger import logger

if TYPE_CHECKING:
    from harbor.job import Job
    from harbor.models.job.result import JobResult

__all__ = [
    "CloudBuildPlugin",
    "build_task_image_if_missing",
    "check_image_exists_in_registry",
    "resolve_task_image_url",
    "submit_cloud_build",
]


def _resolve_task_dir(task_obj: Any) -> Path | None:
    """Resolve the task root directory from any Harbor task representation."""
    if task_obj is None:
        return None
    paths = getattr(task_obj, "paths", None)
    if paths is not None:
        task_dir = getattr(paths, "task_dir", None)
        if isinstance(task_dir, Path) and task_dir.is_dir():
            return task_dir
        env_dir = getattr(paths, "environment_dir", None)
        if isinstance(env_dir, Path) and env_dir.is_dir():
            return env_dir.parent if env_dir.name == "environment" else env_dir
    raw = getattr(task_obj, "path", None) or (
        task_obj if isinstance(task_obj, (str, Path)) else None
    )
    if raw is None:
        return None
    p = Path(raw)
    if not p.is_dir():
        return None
    if (
        (p / "task.toml").is_file()
        or (p / "environment").is_dir()
        or (p / DOCKERFILE_NAME).is_file()
        or (p / COMPOSE_FILE_NAME).is_file()
    ):
        return p
    return None


def _resolve_task_environment_dir(task_obj: Any) -> Path | None:
    """Resolve the environment directory from any Harbor task representation."""
    paths = getattr(task_obj, "paths", None)
    env_dir = getattr(paths, "environment_dir", None)
    if isinstance(env_dir, Path) and env_dir.is_dir():
        return env_dir
    td = _resolve_task_dir(task_obj)
    if td is None:
        return None
    if (td / "environment").is_dir():
        return td / "environment"
    if (td / DOCKERFILE_NAME).is_file() or (td / COMPOSE_FILE_NAME).is_file():
        return td
    return None


def _extract_task_docker_image(task_obj: Any) -> str | None:
    """Extract prebuilt docker_image URL from in-memory task object if specified."""
    for cfg in (task_obj, getattr(task_obj, "config", None)):
        env_cfg = getattr(cfg, "environment", None)
        img = getattr(env_cfg, "docker_image", None)
        if isinstance(img, str) and img.strip():
            return img.strip()
    return None


def _resolve_task_name(task_key: Any, task_obj: Any, task_dir: Path) -> str:
    """Best-effort display name for a discovered task."""
    for candidate in (task_key, task_obj):
        if candidate is None:
            continue
        getter = getattr(candidate, "get_name", None)
        if callable(getter):
            try:
                name = getter()
            except Exception:
                name = None
            if isinstance(name, str) and name:
                return name
        name = getattr(candidate, "name", None)
        if isinstance(name, str) and name:
            return name
    return task_dir.name


@dataclass(frozen=True)
class _DiscoveredTask:
    """A task the pre-builder found, normalised for planning."""

    name: str
    task_dir: Path
    environment_dir: Path | None
    docker_image: str | None


class CloudBuildPlugin(BaseJobPlugin):
    """Harbor JobPlugin that pre-builds dataset environment images for GKE.

    Intercepts job start via `on_job_start()`, scans all tasks in the job,
    and runs semaphore-gated Cloud Build submissions before trials execute.
    """

    def __init__(
        self,
        *,
        concurrency: int | str = 25,
        timeout_sec: int | str = 10800,
        project_id: str | None = None,
        location: str | None = None,
        region: str | None = None,
        registry_name: str | None = None,
        registry_location: str | None = None,
        force_build: bool | str = False,
        cloud_build_machine_type: str | None = None,
        cloud_build_disk_size_gb: int | str | None = None,
        cloud_build_worker_pool: str | None = None,
        private_pool: str | None = None,
        fail_on_incomplete: bool | str | None = None,
        build_attempts: int | str = 3,
        **kwargs: Any,
    ) -> None:
        self.concurrency = int(concurrency)
        self.timeout_sec = int(timeout_sec)
        self.project_id = project_id
        self.location = location or region
        self._explicit_registry_name = registry_name
        self.registry_name = registry_name or "harbor-tasks"
        self.registry_location = registry_location
        self.force_build = _parse_bool(force_build, False)
        self.fail_on_incomplete: bool | None = (
            None if fail_on_incomplete is None else _parse_bool(fail_on_incomplete)
        )
        self.build_attempts = max(1, int(build_attempts))
        self.cloud_build_worker_pool = cloud_build_worker_pool or private_pool
        self.cloud_build_machine_type = cloud_build_machine_type
        if self.cloud_build_worker_pool and self.cloud_build_machine_type:
            raise ValueError(
                f"Cannot specify cloud_build_machine_type ({self.cloud_build_machine_type!r}) "
                f"when a private worker_pool ({self.cloud_build_worker_pool!r}) is used. "
                "Private pools manage machine sizing within the pool configuration."
            )
        self.cloud_build_disk_size_gb = (
            int(cloud_build_disk_size_gb) if cloud_build_disk_size_gb else None
        )
        # Harbor passes only user-supplied --pk values to this constructor, so
        # anything left in **kwargs is a key this plugin does not recognize.
        unknown_kwargs = sorted(k for k in kwargs if not k.startswith("_"))
        if unknown_kwargs:
            logger.warning(
                "Unrecognized CloudBuildPlugin kwargs (--pk) ignored: %s",
                ", ".join(unknown_kwargs),
            )

    @staticmethod
    def _env_kwargs(job: Job) -> dict[str, Any]:
        env_cfg = getattr(getattr(job, "config", None), "environment", None)
        return getattr(env_cfg, "kwargs", None) or {}

    def _resolve_force_build(self, job: Job) -> bool:
        if self.force_build:
            return True
        env_cfg = getattr(getattr(job, "config", None), "environment", None)
        return getattr(env_cfg, "force_build", False) is True

    async def _resolve_project_id(self, job: Job) -> str:
        if self.project_id:
            return self.project_id
        env_kwargs = self._env_kwargs(job)
        if "project_id" in env_kwargs and env_kwargs["project_id"]:
            return str(env_kwargs["project_id"]).strip()
        env_project = (
            os.environ.get("GOOGLE_CLOUD_PROJECT")
            or os.environ.get("CLOUDSDK_CORE_PROJECT")
            or os.environ.get("GCP_PROJECT")
        )
        if env_project and env_project.strip():
            return env_project.strip()
        active_ctx = get_active_gke_context()
        if active_ctx is not None:
            ctx_proj, ctx_loc, ctx_cluster = active_ctx
            raw_loc = (
                self.location
                or env_kwargs.get("location")
                or env_kwargs.get("region")
                or env_kwargs.get("zone")
            )
            raw_cluster = env_kwargs.get("cluster_name")
            loc_ok = (
                not raw_loc
                or raw_loc == ctx_loc
                or derive_region(str(raw_loc)) == derive_region(ctx_loc)
            )
            cluster_ok = not raw_cluster or raw_cluster == ctx_cluster
            if loc_ok and cluster_ok and ctx_proj:
                return ctx_proj
        proc = await asyncio.create_subprocess_exec(
            "gcloud",
            "config",
            "get-value",
            "project",
            "-q",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        project = stdout.decode().strip()
        if project and project != "(unset)":
            return project
        raise ValueError(
            "CloudBuildPlugin requires project_id (via kwarg, --ek, GOOGLE_CLOUD_PROJECT, GCP_PROJECT, active GKE kubectl context, or active gcloud config)."
        )

    def _resolve_region(self, job: Job) -> str:
        env_kwargs = self._env_kwargs(job)
        loc = (
            self.location
            or env_kwargs.get("location")
            or env_kwargs.get("region")
            or env_kwargs.get("zone")
        )
        if loc and str(loc).strip():
            return derive_region(str(loc).strip())
        active_ctx = get_active_gke_context()
        if active_ctx is not None:
            ctx_proj, ctx_loc, ctx_cluster = active_ctx
            raw_proj = self.project_id or env_kwargs.get("project_id")
            raw_cluster = env_kwargs.get("cluster_name")
            proj_ok = not raw_proj or raw_proj == ctx_proj
            cluster_ok = not raw_cluster or raw_cluster == ctx_cluster
            if proj_ok and cluster_ok and ctx_loc:
                return derive_region(ctx_loc)
        raise ValueError(
            "CloudBuildPlugin requires location or region (via kwarg, --ek location/region/zone, or an active GKE kubectl context)."
        )

    def _resolve_registry_name(self, job: Job) -> str:
        if self._explicit_registry_name is not None:
            return self._explicit_registry_name
        return self._env_kwargs(job).get("registry_name", "harbor-tasks")

    def _resolve_registry_location(self, job: Job) -> str:
        loc = self.registry_location or self._env_kwargs(job).get("registry_location")
        return derive_region(loc) if loc else self._resolve_region(job)

    def _resolve_worker_pool(self, job: Job) -> str | None:
        if self.cloud_build_worker_pool:
            return self.cloud_build_worker_pool
        env_kwargs = self._env_kwargs(job)
        return (
            env_kwargs.get("cloud_build_worker_pool")
            or env_kwargs.get("private_pool")
            or env_kwargs.get("worker_pool")
        )

    def _resolve_machine_type(self, job: Job) -> str | None:
        return self.cloud_build_machine_type or self._env_kwargs(job).get(
            "cloud_build_machine_type"
        )

    def _resolve_disk_size_gb(self, job: Job) -> int | None:
        if self.cloud_build_disk_size_gb:
            return self.cloud_build_disk_size_gb
        raw = self._env_kwargs(job).get("cloud_build_disk_size_gb")
        return int(raw) if raw is not None else None

    def _resolve_extra_compose_paths(self, job: Job) -> list[Path]:
        env_cfg = getattr(getattr(job, "config", None), "environment", None)
        raw = getattr(env_cfg, "extra_docker_compose", None) or []
        return [Path(p) for p in raw]

    def _resolve_fail_on_incomplete(self, job: Job) -> bool:
        if self.fail_on_incomplete is not None:
            return self.fail_on_incomplete
        return _parse_bool(
            self._env_kwargs(job).get("prebuild_fail_on_incomplete"), False
        )

    def _discover_tasks(self, job: Job) -> list[_DiscoveredTask]:
        """Locate every task root in the job, using the first source that yields any.

        Each tier is a fallback for a different way a job can be configured;
        they are tried in order of fidelity, not merged, because a lower tier
        would only re-discover the same tasks with worse metadata.
        """
        discovered: dict[Path, _DiscoveredTask] = {}

        def _record(task_key: Any, task_obj: Any) -> None:
            environment_dir = _resolve_task_environment_dir(task_obj)
            task_dir = _resolve_task_dir(task_obj)
            if task_dir is None and environment_dir is not None:
                # Some task representations only expose the environment
                # directory; its parent is the task root, which is where the
                # verifier build contexts live.
                task_dir = (
                    environment_dir.parent
                    if environment_dir.name == "environment"
                    else environment_dir
                )
            if task_dir is None:
                return
            resolved = task_dir.resolve()
            if resolved in discovered:
                return
            docker_image = _extract_task_docker_image(task_obj)
            discovered[resolved] = _DiscoveredTask(
                name=_resolve_task_name(task_key, task_obj, task_dir),
                task_dir=task_dir,
                environment_dir=environment_dir,
                docker_image=docker_image,
            )

        # 1. Downloaded task caches (remote datasets via -d, or package tasks).
        for task_key, task_obj in (
            getattr(job, "_task_download_results", None) or {}
        ).items():
            _record(task_key, task_obj)

        # 2. Trial configs.
        if not discovered:
            for trial_cfg in getattr(job, "_trial_configs", []):
                _record(None, getattr(trial_cfg, "task", None))

        # 3. Direct task configs.
        if not discovered:
            for task_cfg in getattr(job, "_task_configs", []):
                _record(None, task_cfg)

        # 4. Local dataset directories with task subdirectories.
        if not discovered:
            for ds in getattr(getattr(job, "config", None), "datasets", []):
                ds_path = getattr(ds, "path", None)
                if not isinstance(ds_path, (str, Path)):
                    continue
                p = Path(ds_path)
                if not p.is_dir():
                    continue
                if _resolve_task_dir(p) is not None:
                    _record(None, p)
                    continue
                for child in sorted(p.iterdir()):
                    if child.is_dir():
                        _record(None, child)

        return list(discovered.values())

    @override
    async def on_job_start(self, job: Job) -> None:
        """Pre-build every registry image the job's tasks will need."""
        force_build = self._resolve_force_build(job)
        worker_pool = self._resolve_worker_pool(job)
        machine_type = self._resolve_machine_type(job)
        if worker_pool and machine_type:
            raise ValueError(
                f"Cannot specify machine_type ({machine_type!r}) when a private worker_pool "
                f"({worker_pool!r}) is used. Private pools manage machine sizing within the pool configuration."
            )
        disk_size_gb = self._resolve_disk_size_gb(job)
        extra_compose_paths = self._resolve_extra_compose_paths(job)

        discovered = self._discover_tasks(job)
        if not discovered:
            logger.info(
                "CloudBuildPlugin: No local task directories found in job configuration."
            )
            return

        project_id = await self._resolve_project_id(job)
        region = self._resolve_region(job)
        registry_name = self._resolve_registry_name(job)
        registry_location = self._resolve_registry_location(job)

        plans: list[TaskImagePlan] = []
        for task in discovered:
            plans.append(
                plan_task_images(
                    task.task_dir,
                    project_id=project_id,
                    registry_name=registry_name,
                    registry_location=registry_location,
                    force_build=force_build,
                    task_name=task.name,
                    environment_dir=task.environment_dir,
                    docker_image=task.docker_image,
                    extra_compose_paths=extra_compose_paths,
                )
            )

        for plan in plans:
            for skip_reason in plan.skipped:
                logger.debug(
                    f"CloudBuildPlugin: [{plan.task_name}] skipped {skip_reason}"
                )

        specs = dedupe_specs(plans)
        if not specs:
            logger.info(
                f"CloudBuildPlugin: No build contexts require a pre-build across "
                f"{len(discovered)} task(s)."
            )
            return

        ensure_gcloud_ready()

        # Publish the plan so GKEEnvironment can flag an inline build the
        # pre-builder was supposed to have covered.
        record_planned_images(spec.image_url for spec in specs)

        by_kind: dict[str, int] = {}
        for spec in specs:
            by_kind[spec.kind] = by_kind.get(spec.kind, 0) + 1
        breakdown = ", ".join(
            f"{count} {kind}" for kind, count in sorted(by_kind.items())
        )
        logger.info(
            f"CloudBuildPlugin: Discovered {len(specs)} unique images ({breakdown}) "
            f"across {len(discovered)} task(s); building with concurrency "
            f"{self.concurrency} (force_build={force_build})..."
        )

        failures: list[tuple[ImageSpec, BaseException | None]] = []
        semaphore = asyncio.Semaphore(self.concurrency)

        async def _build(spec: ImageSpec) -> None:
            try:
                built = await self._build_image_with_retry(
                    spec,
                    project_id=project_id,
                    region=region,
                    force_build=force_build,
                    semaphore=semaphore,
                    machine_type=machine_type,
                    disk_size_gb=disk_size_gb,
                    worker_pool=worker_pool,
                )
            except Exception as exc:
                logger.warning(
                    f"CloudBuildPlugin: Failed to pre-build {spec.label} "
                    f"({spec.image_url}) from {spec.build_context}: {exc}"
                )
                failures.append((spec, exc))
                return
            if not built:
                logger.warning(
                    f"CloudBuildPlugin: Cloud Build reported failure for {spec.label} "
                    f"({spec.image_url}) from {spec.build_context}."
                )
                failures.append((spec, None))

        console = Console()
        with console.status("[bold cyan]Waiting for builds to finish..."):
            async with asyncio.TaskGroup() as tg:
                for spec in specs:
                    tg.create_task(_build(spec))

        succeeded = len(specs) - len(failures)
        logger.info(
            f"CloudBuildPlugin: Pre-building finished. {succeeded}/{len(specs)} images "
            "ready in Artifact Registry."
        )
        if not failures:
            return

        summary = "; ".join(
            f"{spec.label} -> {spec.image_url}" for spec, _ in failures[:10]
        )
        if len(failures) > 10:
            summary += f"; (+{len(failures) - 10} more)"
        message = (
            f"CloudBuildPlugin: {len(failures)} image(s) could not be pre-built: {summary}. "
            "Trials needing them will build inline, which is slower and unthrottled."
        )
        if self._resolve_fail_on_incomplete(job):
            raise RuntimeError(message)
        logger.warning(message)

    async def _build_image_with_retry(
        self,
        spec: ImageSpec,
        *,
        project_id: str,
        region: str,
        force_build: bool,
        semaphore: asyncio.Semaphore,
        machine_type: str | None,
        disk_size_gb: int | None,
        worker_pool: str | None,
    ) -> bool:
        """Build one image, retrying transient Cloud Build failures.

        ``reraise=True`` is deliberate: the previous implementation swallowed
        every failure, so the success counter reported phantom progress and no
        failure was ever retried.
        """
        built = False
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.build_attempts),
            wait=wait_exponential(multiplier=2, min=5, max=60),
            reraise=True,
        ):
            with attempt:
                built = await _build_spec_image(
                    spec,
                    project_id=project_id,
                    region=region,
                    force_build=force_build,
                    timeout_sec=self.timeout_sec,
                    semaphore=semaphore,
                    machine_type=machine_type,
                    disk_size_gb=disk_size_gb,
                    worker_pool=worker_pool,
                )
        return built

    @override
    async def on_job_end(self, job_result: JobResult) -> None:
        """No-op on job termination."""
        pass


async def _build_spec_image(
    spec: ImageSpec,
    *,
    project_id: str,
    region: str,
    force_build: bool,
    timeout_sec: int,
    semaphore: asyncio.Semaphore,
    machine_type: str | None,
    disk_size_gb: int | None,
    worker_pool: str | None,
) -> bool:
    """Build a single ``ImageSpec`` through Cloud Build.

    ``build_task_image_if_missing`` checks Artifact Registry first and returns
    early on a hit, so a cached image costs one existence check and nothing
    else. Only when a build is genuinely required does ``submit_cloud_build``
    reach ``ensure_artifact_registry_exists`` -- meaning the target repository
    is created lazily, on the same condition as the build itself, and never
    merely because a task was inspected. ``reraise=True`` makes a failed
    repository creation fail the build rather than silently degrade.
    """
    if spec.build_context is None:
        raise ValueError(f"ImageSpec for '{spec.image_url}' is missing build_context")
    return await build_task_image_if_missing(
        build_context=spec.build_context,
        image_url=spec.image_url,
        project_id=project_id,
        region=region,
        force_build=force_build,
        timeout_sec=timeout_sec,
        semaphore=semaphore,
        dockerfile=spec.dockerfile,
        machine_type=machine_type,
        disk_size_gb=disk_size_gb,
        worker_pool=worker_pool,
        reraise=True,
    )


def main():
    """CLI entry point for standalone execution."""
    parser = argparse.ArgumentParser(
        description="Harbor GKE Dataset Image Pre-Builder."
    )
    parser.add_argument(
        "dataset_dir", type=Path, help="Path to dataset directory containing tasks."
    )
    parser.add_argument("--project-id", help="GCP project ID.")
    parser.add_argument(
        "--location",
        "--region",
        default=None,
        help="GCP region or zone (required if no active GKE kubectl context is configured).",
    )
    parser.add_argument(
        "--registry-location",
        help="Artifact Registry location/region (defaults to derived --location).",
    )
    parser.add_argument(
        "--registry-name", default="harbor-tasks", help="Artifact Registry repository."
    )
    parser.add_argument(
        "--concurrency", type=int, default=25, help="Max parallel Cloud Build jobs."
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=10800,
        help="Build timeout per image in seconds.",
    )
    parser.add_argument(
        "--machine-type", help="Cloud Build machine type (e.g. E2_HIGHCPU_32)."
    )
    parser.add_argument(
        "--worker-pool",
        "--private-pool",
        dest="worker_pool",
        help="Private Cloud Build worker pool resource name.",
    )
    parser.add_argument("--disk-size-gb", type=int, help="Cloud Build disk size in GB.")
    parser.add_argument(
        "--force-build",
        action="store_true",
        help="Force rebuild images even if present in registry or task config.",
    )
    args = parser.parse_args()

    if args.worker_pool and args.machine_type:
        print(
            f"Error: Cannot specify --machine-type ({args.machine_type!r}) when "
            f"--worker-pool ({args.worker_pool!r}) is used. Private pools manage machine sizing within the pool configuration.",
            file=sys.stderr,
        )
        sys.exit(1)

    active_ctx = get_active_gke_context()
    raw_location = args.location
    if not raw_location and active_ctx is not None:
        raw_location = active_ctx[1]
    if not raw_location:
        print(
            "Error: --location / --region is required when no active GKE kubectl context is configured.",
            file=sys.stderr,
        )
        sys.exit(1)

    project_id = (
        args.project_id
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("CLOUDSDK_CORE_PROJECT")
        or os.environ.get("GCP_PROJECT")
        or (active_ctx[0] if active_ctx is not None else None)
    )
    if not project_id:
        try:
            project_id = resolve_default_project_id()
        except ValueError:
            project_id = None
    if not project_id:
        print(
            "Error: --project-id required (or set GOOGLE_CLOUD_PROJECT, an active GKE kubectl context, or gcloud active project).",
            file=sys.stderr,
        )
        sys.exit(1)

    region = derive_region(raw_location)
    registry_location = (
        derive_region(args.registry_location) if args.registry_location else region
    )
    tasks: list[Path] = [p.parent for p in args.dataset_dir.rglob("task.toml")]
    print(f"Discovered {len(tasks)} tasks under {args.dataset_dir}")

    plans = [
        plan_task_images(
            task_dir,
            project_id=project_id,
            registry_name=args.registry_name,
            registry_location=registry_location,
            force_build=args.force_build,
        )
        for task_dir in tasks
    ]
    for plan in plans:
        for skip_reason in plan.skipped:
            print(f"  [{plan.task_name}] skipped {skip_reason}")

    specs = dedupe_specs(plans)
    by_kind: dict[str, int] = {}
    for spec in specs:
        by_kind[spec.kind] = by_kind.get(spec.kind, 0) + 1
    breakdown = ", ".join(f"{count} {kind}" for kind, count in sorted(by_kind.items()))
    print(f"Identified {len(specs)} unique images ({breakdown or 'none'}).")
    if not specs:
        return

    ensure_gcloud_ready()

    async def _run_all() -> list[bool]:
        semaphore = asyncio.Semaphore(args.concurrency)

        async def _safe_build(spec: ImageSpec) -> bool:
            try:
                return await _build_spec_image(
                    spec,
                    project_id=project_id or "",
                    region=region,
                    force_build=args.force_build,
                    timeout_sec=args.timeout_sec,
                    machine_type=args.machine_type,
                    disk_size_gb=args.disk_size_gb,
                    worker_pool=args.worker_pool,
                    semaphore=semaphore,
                )
            except Exception as e:
                print(f"FAILED {spec.label}: {e}", file=sys.stderr)
                return False

        async with asyncio.TaskGroup() as tg:
            build_tasks = [tg.create_task(_safe_build(spec)) for spec in specs]
        return [t.result() for t in build_tasks]

    results = asyncio.run(_run_all())
    successes = sum(1 for r in results if r)
    for spec, ok in zip(specs, results, strict=True):
        if not ok:
            print(
                f"  FAILED {spec.label}: {spec.image_url} (context {spec.build_context})",
                file=sys.stderr,
            )
    print(f"Pre-build complete: {successes}/{len(specs)} images ready in registry.")


if __name__ == "__main__":
    main()
