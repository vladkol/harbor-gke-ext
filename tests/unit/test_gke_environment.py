"""Unit tests for src/harbor/environments/gke/environment.py and GKEPrebuildPlugin."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from kubernetes import client as k8s_client
from kubernetes.client.rest import ApiException

import harbor_gke_ext.environment as env_mod
from harbor.environments.base import ExecResult, HealthcheckError
from harbor.models.task.config import (
    EnvironmentConfig,
    HealthcheckConfig,
    TpuSpec,
)
from harbor.models.trial.paths import TrialPaths
from harbor.utils.optional_import import MissingExtraError
from harbor_gke_ext.cluster_probe import ClusterCapabilities
from harbor_gke_ext.compose_translator import (
    DIND_STORAGE_FLOOR_MB,
    _GKENativeComposeServiceTransport,
)
from harbor_gke_ext.constants import (
    EphemeralStorageUnschedulableError,
    GKEExecStreamClosedError,
    PlacementConflictError,
    TrialContainerLostError,
    UnsatisfiableMachineTypeError,
)
from harbor_gke_ext.environment import (
    GKEEnvironment,
    _get_cluster_autopilot_lock,
    _parse_bool,
    reset_cluster_autopilot_cache,
)
from harbor_gke_ext.exec_stream import ExecOutputAccumulator


@pytest.fixture(autouse=True)
def _reset_globals():
    """Reset module-level caches and singletons before/after tests."""
    reset_cluster_autopilot_cache()
    GKEEnvironment._image_build_locks.clear()
    yield
    reset_cluster_autopilot_cache()
    GKEEnvironment._image_build_locks.clear()


@pytest.fixture(autouse=True)
def mock_k8s_manager(monkeypatch):
    """Mock the KubernetesClientManager singleton and APIs offline."""
    from harbor_gke_ext.client import _shutdown_exec_executor

    _shutdown_exec_executor()
    env_mod.reset_cluster_autopilot_cache()
    mock_core_api = MagicMock()
    mock_core_api.api_client = MagicMock()
    mock_batch_api = MagicMock()
    mock_networking_api = MagicMock()
    mock_custom_api = MagicMock()

    mock_mgr = MagicMock()
    mock_mgr.get_client = AsyncMock(return_value=mock_core_api)
    mock_mgr.release_client = AsyncMock()

    monkeypatch.setattr(
        env_mod.KubernetesClientManager,
        "get_instance",
        AsyncMock(return_value=mock_mgr),
    )
    monkeypatch.setattr(
        env_mod.KubernetesClientManager,
        "is_fqdn_network_policy_supported",
        classmethod(lambda cls, *args, **kwargs: False),
    )
    monkeypatch.setattr(
        "harbor_gke_ext.client.get_active_gke_context",
        lambda: None,
    )
    yield {
        "manager": mock_mgr,
        "core_api": mock_core_api,
        "batch_api": mock_batch_api,
        "networking_api": mock_networking_api,
        "custom_api": mock_custom_api,
    }
    _shutdown_exec_executor()
    env_mod.reset_cluster_autopilot_cache()


def make_gke_env(
    tmp_path: Path,
    dockerfile: str | None = "FROM ubuntu:22.04\n",
    compose_yaml: str | None = None,
    task_env_config: EnvironmentConfig | None = None,
    mock_autopilot: bool | None = False,
    session_id: str = "test-session-1234",
    cluster_name: str = "test-cluster",
    location: str = "us-central1-a",
    project_id: str = "test-project",
    registry_name: str = "harbor-tasks",
    trial_paths: TrialPaths | None = None,
    **kwargs,
) -> GKEEnvironment:
    env_dir = tmp_path / "env"
    env_dir.mkdir(parents=True, exist_ok=True)
    if dockerfile is not None:
        (env_dir / "Dockerfile").write_text(dockerfile)
    if compose_yaml is not None:
        (env_dir / "docker-compose.yaml").write_text(compose_yaml)

    if trial_paths is None:
        trial_dir = tmp_path / "trial"
        trial_dir.mkdir(parents=True, exist_ok=True)
        trial_paths = TrialPaths(trial_dir=trial_dir)

    cfg = task_env_config or EnvironmentConfig(cpus=2, memory_mb=4096)

    if "autopilot" not in kwargs and mock_autopilot is not None:
        kwargs["autopilot"] = mock_autopilot

    env_name = kwargs.pop("environment_name", "test-env")
    env = GKEEnvironment(
        environment_dir=env_dir,
        environment_name=env_name,
        session_id=session_id,
        trial_paths=trial_paths,
        task_env_config=cfg,
        cluster_name=cluster_name,
        location=location,
        project_id=project_id,
        registry_name=registry_name,
        **kwargs,
    )
    if mock_autopilot is not None:
        env._is_autopilot = mock_autopilot
    return env


def mock_monotonic_sequence(*values: float, start_default: float = 99999.0):
    val_iter = iter(values)
    counter = [start_default]

    def _next():
        try:
            return next(val_iter)
        except StopIteration:
            counter[0] += 100.0
            return counter[0]

    return _next


# ─────────────────────────────────────────────────────────────────────────────
# 1. Module-level helpers
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_parse_bool():
    assert _parse_bool(None, default=True) is True
    assert _parse_bool(None, default=False) is False
    assert _parse_bool(True) is True
    assert _parse_bool(False) is False
    assert _parse_bool(1) is True
    assert _parse_bool(0) is False
    assert _parse_bool(1.5) is True
    assert _parse_bool("true") is True
    assert _parse_bool("TRUE") is True
    assert _parse_bool("1") is True
    assert _parse_bool("yes") is True
    assert _parse_bool("t") is True
    assert _parse_bool("y") is True
    assert _parse_bool("false") is False
    assert _parse_bool("no") is False
    assert _parse_bool([1]) is True
    assert _parse_bool([]) is False


@pytest.mark.unit
def test_cluster_autopilot_lock():
    lock1 = _get_cluster_autopilot_lock()
    lock2 = _get_cluster_autopilot_lock()
    assert lock1 is lock2
    assert isinstance(lock1, asyncio.Lock)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Preflight checks
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_preflight_missing_gcloud(monkeypatch):
    monkeypatch.setattr(
        "shutil.which", lambda cmd: None if cmd == "gcloud" else "/usr/bin/" + cmd
    )
    with pytest.raises(SystemExit) as exc_info:
        GKEEnvironment.preflight()
    assert "gcloud CLI to be installed" in str(exc_info.value)


@pytest.mark.unit
def test_preflight_missing_auth_plugin_warning(monkeypatch, tmp_path):
    def fake_which(cmd):
        if cmd == "gke-gcloud-auth-plugin":
            return None
        return f"/usr/bin/{cmd}"

    monkeypatch.setattr("shutil.which", fake_which)
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("dummy")
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))

    with (
        patch("subprocess.run") as mock_run,
        patch.object(env_mod.logger, "warning") as mock_warn,
    ):
        mock_run.return_value = SimpleNamespace(
            returncode=0, stdout="user@example.com\n"
        )
        GKEEnvironment.preflight()
        # Second call must not re-run gcloud auth list
        GKEEnvironment.preflight()
        assert mock_run.call_count == 1
        assert mock_warn.call_count >= 1
        assert (
            "gke-gcloud-auth-plugin is not found in PATH" in mock_warn.call_args[0][0]
        )


@pytest.mark.unit
def test_preflight_unauthenticated_gcloud_fails(monkeypatch, tmp_path):
    monkeypatch.setattr("shutil.which", lambda cmd: f"/usr/bin/{cmd}")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("dummy")
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = SimpleNamespace(returncode=0, stdout="\n")
        with pytest.raises(SystemExit, match="active authenticated gcloud account"):
            GKEEnvironment.preflight()


@pytest.mark.unit
def test_preflight_missing_kubeconfig(monkeypatch, tmp_path):
    monkeypatch.setattr("shutil.which", lambda cmd: f"/usr/bin/{cmd}")
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "nonexistent_kubeconfig"))
    with pytest.raises(SystemExit) as exc_info:
        GKEEnvironment.preflight()
    assert "Kubernetes credentials" in str(exc_info.value)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Initialization & configuration validation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_init_missing_kubernetes(monkeypatch, tmp_path):
    monkeypatch.setattr(env_mod, "_HAS_KUBERNETES", False)
    with pytest.raises(MissingExtraError):
        make_gke_env(tmp_path)


@pytest.mark.unit
def test_init_missing_location(tmp_path):
    env_dir = tmp_path / "env"
    env_dir.mkdir(parents=True)
    (env_dir / "Dockerfile").write_text("FROM alpine")
    trial_paths = TrialPaths(tmp_path / "trial")
    with pytest.raises(
        ValueError, match="Either location, region or zone must be specified"
    ):
        GKEEnvironment(
            environment_dir=env_dir,
            environment_name="test-env",
            session_id="session",
            trial_paths=trial_paths,
            task_env_config=EnvironmentConfig(),
            cluster_name="test-cluster",
            project_id="test-proj",
            location=None,
            region=None,
            zone=None,
        )


@pytest.mark.unit
def test_init_resolves_from_active_kubectl_context(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "harbor_gke_ext.client.get_active_gke_context",
        lambda: ("ctx-proj", "us-east4-a", "ctx-cluster"),
    )
    env_dir = tmp_path / "env"
    env_dir.mkdir(parents=True)
    (env_dir / "Dockerfile").write_text("FROM alpine")
    trial_paths = TrialPaths(tmp_path / "trial")
    env = GKEEnvironment(
        environment_dir=env_dir,
        environment_name="test-env",
        session_id="session",
        trial_paths=trial_paths,
        task_env_config=EnvironmentConfig(),
        cluster_name=None,
        project_id=None,
        location=None,
    )
    assert env.project_id == "ctx-proj"
    assert env.location == "us-east4-a"
    assert env.region == "us-east4"
    assert env.cluster_name == "ctx-cluster"


@pytest.mark.unit
def test_init_worker_pool_and_machine_type_conflict(tmp_path):
    with pytest.raises(ValueError, match="Cannot specify cloud_build_machine_type"):
        make_gke_env(
            tmp_path,
            cloud_build_machine_type="e2-highcpu-8",
            cloud_build_worker_pool="projects/p/locations/l/workerPools/wp",
        )


@pytest.mark.unit
def test_get_default_project(monkeypatch, tmp_path):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.delenv("CLOUDSDK_CORE_PROJECT", raising=False)

    # 1. From gcloud config (cached once per process)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = SimpleNamespace(stdout="gcloud-proj\n")
        env1 = make_gke_env(tmp_path, project_id=None)
        env2 = make_gke_env(tmp_path, project_id=None)
        assert env1.project_id == "gcloud-proj"
        assert env2.project_id == "gcloud-proj"
        assert mock_run.call_count == 1

    # 2. From env var GCP_PROJECT
    monkeypatch.setenv("GCP_PROJECT", "env-proj")
    env = make_gke_env(tmp_path, project_id=None)
    assert env.project_id == "env-proj"

    # 3. From GOOGLE_CLOUD_PROJECT
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "google-env-proj")
    env = make_gke_env(tmp_path, project_id=None)
    assert env.project_id == "google-env-proj"

    # 4. Failure raises ValueError once cache is cleared
    env_mod.reset_cluster_autopilot_cache()
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd")):
        with pytest.raises(ValueError, match="No GCP project specified"):
            make_gke_env(tmp_path, project_id=None)


@pytest.mark.unit
def test_init_memory_limit_multiplier_and_direct_limit(tmp_path):
    # Default (auto) caps both CPU and memory to the declared budget (Guaranteed QoS)
    env_default = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(cpus=2, memory_mb=2048),
    )
    assert env_default.cpu_request == "2"
    assert env_default.cpu_limit == "2"
    assert env_default.memory_request == "2048Mi"
    assert env_default.memory_limit == "2048Mi"
    pod_default = env_default._build_direct_pod()
    assert pod_default.spec.resources is None
    assert pod_default.spec.containers[0].resources.requests["cpu"] == "2"
    assert pod_default.spec.containers[0].resources.limits["cpu"] == "2"
    assert pod_default.spec.containers[0].resources.requests["memory"] == "2048Mi"
    assert pod_default.spec.containers[0].resources.limits["memory"] == "2048Mi"

    # Explicit request mode omits limits (uncapped)
    env_req = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(cpus=2, memory_mb=2048),
        cpu_enforcement_policy="request",
        memory_enforcement_policy="request",
    )
    assert env_req.cpu_request == "2"
    assert env_req.cpu_limit is None
    assert env_req.memory_request == "2048Mi"
    assert env_req.memory_limit is None

    # Multipliers in auto mode override the default cap
    env = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(cpus=2, memory_mb=2048),
        cpu_limit_multiplier=2.0,
        memory_limit_multiplier=1.5,
    )
    assert env.cpu_request == "2"
    assert env.cpu_limit == "4000m"
    assert env.memory_request == "2048Mi"
    assert env.memory_limit == "3072Mi"

    # Direct memory limit via memory_enforcement_policy=LIMIT
    env2 = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(cpus=2, memory_mb=2048),
        memory_enforcement_policy="limit",
    )
    assert env2.memory_limit == "2048Mi"
    assert "prebuild_fail_on_incomplete" in env_mod._KNOWN_EK_KEYS
    assert "network_policy_settlement_sec" in env_mod._KNOWN_EK_KEYS
    assert "resource_mode" not in env_mod._KNOWN_EK_KEYS


@pytest.mark.unit
def test_accelerator_validation(tmp_path):
    # Both GPU and TPU requested raises RuntimeError
    with pytest.raises(RuntimeError, match="only target one accelerator family"):
        make_gke_env(
            tmp_path,
            task_env_config=EnvironmentConfig(
                gpus=1,
                gpu_types=["nvidia-tesla-t4"],
                tpu=TpuSpec(type="v4", topology="2x2x1"),
            ),
        )

    # Invalid short name for gpu_override raises RuntimeError
    with pytest.raises(
        RuntimeError, match="gpu_override must be specified as the full"
    ):
        make_gke_env(
            tmp_path,
            task_env_config=EnvironmentConfig(gpus=1),
            gpu_override="t4",
        )

    # Valid gpu_override
    env = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(gpus=1),
        gpu_override="nvidia-tesla-t4",
    )
    assert env._effective_gpu_types == ["nvidia-tesla-t4"]

    # GPU types without gpu_override
    env_types = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(gpus=1, gpu_types=["nvidia-tesla-t4"]),
    )
    assert env_types._effective_gpu_types == ["nvidia-tesla-t4"]

    # Valid TPU
    env_tpu = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(tpu=TpuSpec(type="v4", topology="2x2x1")),
    )
    assert env_tpu.task_env_config.tpu is not None


# ─────────────────────────────────────────────────────────────────────────────
# 4. Capabilities & API Client
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_capabilities(tmp_path):
    env = make_gke_env(tmp_path, enable_fqdn_network_policy=True)
    caps = env.capabilities
    assert caps.gpus is True
    assert caps.tpus is True
    assert caps.network_allowlist_hostnames is True
    assert caps.docker_compose is True

    # Compose mode runs natively in Unified Native Pod, supporting GPU/TPU
    env_dind = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  web:\n    image: nginx\n",
        compose_mode="dind",
    )
    dind_caps = env_dind.capabilities
    assert dind_caps.gpus is True
    assert dind_caps.tpus is True


@pytest.mark.unit
def test_type_and_compose_paths(tmp_path):
    extra = tmp_path / "extra-compose.yaml"
    extra.write_text("services:\n  extra:\n    image: alpine\n")
    env = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  app:\n    image: redis\n",
        extra_docker_compose=[extra],
    )
    assert env.type() == "gke"
    assert env._uses_compose is True
    assert env._environment_docker_compose_path.name == "docker-compose.yaml"
    assert len(env._all_compose_paths) == 2
    env._validate_definition()


@pytest.mark.unit
def test_api_property_uninitialized(tmp_path):
    env = make_gke_env(tmp_path)
    with pytest.raises(RuntimeError, match="Kubernetes client not initialized"):
        _ = env._api


# ─────────────────────────────────────────────────────────────────────────────
# 5. Image URLs, Cache, and Building
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_image_urls(tmp_path):
    env = make_gke_env(tmp_path, registry_name="test-images")
    task_url = env._get_task_artifact_registry_url()
    assert "us-central1-docker.pkg.dev/test-project/test-images/task-" in task_url
    assert task_url.endswith(":latest")
    assert env._get_image_url() == task_url

    # Prebuilt docker image without local Dockerfile/Compose
    env_prebuilt = make_gke_env(
        tmp_path,
        dockerfile=None,
        task_env_config=EnvironmentConfig(docker_image="ubuntu:latest"),
    )
    assert env_prebuilt._get_image_url() == "ubuntu:latest"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_exists_prebuilt(tmp_path):
    env = make_gke_env(
        tmp_path,
        dockerfile=None,
        task_env_config=EnvironmentConfig(docker_image="python:3.11-slim"),
    )
    with patch(
        "harbor_gke_ext.environment.check_image_exists_in_registry",
        AsyncMock(return_value=True),
    ):
        exists = await env._image_exists()
        assert exists is True
        assert "python" in env._get_image_url()

    # Registry cache miss falls back to docker_image
    with patch(
        "harbor_gke_ext.environment.check_image_exists_in_registry",
        AsyncMock(return_value=False),
    ):
        exists = await env._image_exists()
        assert exists is True
        assert env._get_image_url() == "python:3.11-slim"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_exists_buildable(tmp_path):
    env = make_gke_env(tmp_path)
    # 1. Shared url exists
    with patch(
        "harbor_gke_ext.environment.check_image_exists_in_registry",
        AsyncMock(side_effect=[True]),
    ):
        assert await env._image_exists() is True

    # 2. Shared missing, named url exists
    env._resolved_image_url = None
    with patch(
        "harbor_gke_ext.environment.check_image_exists_in_registry",
        AsyncMock(side_effect=[False, True]),
    ):
        assert await env._image_exists() is True

    # 3. Neither exists
    env._resolved_image_url = None
    with patch(
        "harbor_gke_ext.environment.check_image_exists_in_registry",
        AsyncMock(side_effect=[False, False]),
    ):
        assert await env._image_exists() is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_build_and_push_image(tmp_path):
    env = make_gke_env(tmp_path)
    with patch(
        "harbor_gke_ext.environment.submit_cloud_build",
        new=AsyncMock(),
    ) as mock_submit:
        with patch("harbor_gke_ext.environment.is_plan_published", return_value=True):
            with patch(
                "harbor_gke_ext.environment.was_image_planned",
                return_value=False,
            ):
                await env._build_and_push_image()
                mock_submit.assert_awaited_once()


# ─────────────────────────────────────────────────────────────────────────────
# 6. Autopilot Detection
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_is_autopilot_fallback_to_nodes(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path, mock_autopilot=None)

    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.communicate = AsyncMock(return_value=(b"", b"permission denied"))

    mock_node = MagicMock()
    mock_node.metadata.labels = {"cloud.google.com/gke-autopilot": "true"}
    mock_k8s_manager["core_api"].list_node.return_value = SimpleNamespace(
        items=[mock_node]
    )

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        with patch.object(env, "_ensure_client", AsyncMock()):
            env._core_api = mock_k8s_manager["core_api"]
            assert await env.is_autopilot() is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_is_autopilot_fallback_to_nodepool_label(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path, mock_autopilot=None)

    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.communicate = AsyncMock(return_value=(b"", b"err"))

    mock_node = MagicMock()
    mock_node.metadata.labels = {"cloud.google.com/gke-nodepool": "autopilot-pool-1"}
    mock_k8s_manager["core_api"].list_node.return_value = SimpleNamespace(
        items=[mock_node]
    )

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        with patch.object(env, "_ensure_client", AsyncMock()):
            env._core_api = mock_k8s_manager["core_api"]
            assert await env.is_autopilot() is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_is_autopilot_fallback_fails(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path, mock_autopilot=None)

    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.communicate = AsyncMock(return_value=(b"", b"err"))

    mock_k8s_manager["core_api"].list_node.side_effect = Exception(
        "failed to list nodes"
    )

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        with patch.object(env, "_ensure_client", AsyncMock()):
            env._core_api = mock_k8s_manager["core_api"]
            assert await env.is_autopilot() is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_is_autopilot_concurrency(tmp_path):
    env = make_gke_env(tmp_path, mock_autopilot=None)
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.communicate = AsyncMock(return_value=(b"true\n", b""))

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        res1, res2 = await asyncio.gather(env.is_autopilot(), env.is_autopilot())
        assert res1 is True
        assert res2 is True


# ─────────────────────────────────────────────────────────────────────────────
# 7. Deadline Calculation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"active_deadline_seconds": 1800}, 1800),
        (
            {
                "task_config": {
                    "agent": {"timeout_sec": 300},
                    "verifier": {"timeout_sec": 120},
                },
                "deadline_buffer_minutes": 5,
            },
            1080,  # setup(360) + agent(300) + verifier(120) + buffer(300)
        ),
        (
            {
                "task_config": {
                    "agent": {},
                    "verifier": {"timeout_sec": 120},
                }
            },
            87780,  # setup(360) + default_agent(86400) + verifier(120) + buffer(900)
        ),
        (
            {
                "task_config": {
                    "agent": {},
                    "verifier": {"timeout_sec": 120},
                },
                "default_agent_timeout_minutes": 10,
                "deadline_buffer_minutes": 5,
            },
            1380,  # setup(360) + default_agent(600) + verifier(120) + buffer(300)
        ),
        (
            {
                "session_id": "trial__verifier__1234",
                "task_config": {"verifier": {"timeout_sec": 300}},
                "deadline_buffer_minutes": 1,
            },
            720,  # setup(360) + verifier(300) + buffer(60)
        ),
        (
            {
                "session_id": "trial__verifier__1234",
                "task_config": {
                    "steps": [
                        {"verifier": {"timeout_sec": 100}},
                        {"verifier": {"timeout_sec": 200}},
                    ]
                },
                "deadline_buffer_minutes": 1,
            },
            1080,  # setup(720) + verifier(300) + buffer(60)
        ),
        (
            {
                "task_config": {
                    "steps": [
                        {
                            "agent": {"timeout_sec": 100},
                            "verifier": {"timeout_sec": 50},
                        },
                        {
                            "agent": {"timeout_sec": 200},
                            "verifier": {"timeout_sec": 60},
                        },
                    ]
                },
                "deadline_buffer_minutes": 2,
            },
            1250,  # setup(720) + agent(300) + verifier(110) + buffer(120)
        ),
        (
            {
                "task_config": {
                    "steps": [
                        {"agent": {"timeout_sec": 100}},
                        {"agent": {}},
                    ]
                }
            },
            89320,  # setup(720) + agent(100+86400) + verifier(600+600) + buffer(900)
        ),
        (
            {
                "task_config": MagicMock(
                    model_dump=lambda: {
                        "agent": {"timeout_sec": 400},
                        "verifier": {"timeout_sec": 200},
                    }
                ),
                "agent_timeout_sec": 150,
                "verifier_timeout_sec": 80,
                "agent_setup_timeout_sec": 100,
                "deadline_buffer_minutes": 5,
            },
            630,  # setup(100) + agent(150) + verifier(80) + buffer(300)
        ),
    ],
)
def test_resolve_active_deadline_seconds_scenarios(tmp_path, kwargs, expected):
    env = make_gke_env(tmp_path, **kwargs)
    assert env._resolve_active_deadline_seconds() == expected


@pytest.mark.unit
def test_resolve_active_deadline_seconds_with_trial_config_and_dind(tmp_path):
    """Verify max_timeout_sec caps base timeout before timeout_multiplier is applied."""
    trial_cfg = {
        "timeout_multiplier": 2.0,
        "agent_setup_timeout_multiplier": 1.5,
        "agent": {"override_timeout_sec": 500, "max_timeout_sec": 600},
        "verifier": {"override_timeout_sec": 300, "max_timeout_sec": 400},
    }
    trial_dir = tmp_path / "trial"
    trial_dir.mkdir(parents=True, exist_ok=True)
    trial_paths = TrialPaths(trial_dir=trial_dir)
    trial_paths.config_path.write_text(json.dumps(trial_cfg))

    env = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  app:\n    image: nginx\n",
        compose_mode="dind",
        trial_paths=trial_paths,
        agent_setup_timeout_sec=200,
        deadline_buffer_minutes=5,
    )
    # setup: 200 * 1.5 = 300
    # agent: min(500, 600) * 2.0 = 1000
    # verifier: min(300, 400) * 2.0 = 600
    # buffer: 5 * 60 = 300
    # total = 2200
    assert env._resolve_active_deadline_seconds() == 2200


# ─────────────────────────────────────────────────────────────────────────────
# 8. Compute Class & Ephemeral Storage
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_parse_task_compute_classes():
    assert GKEEnvironment._parse_task_compute_classes(None) == {}
    assert GKEEnvironment._parse_task_compute_classes({"task1": "class1"}) == {
        "task1": "class1"
    }
    assert GKEEnvironment._parse_task_compute_classes("t1=c1, t2:c2") == {
        "t1": "c1",
        "t2": "c2",
    }
    assert GKEEnvironment._parse_task_compute_classes(["t1=c1", "t2=c2"]) == {
        "t1": "c1",
        "t2": "c2",
    }


@pytest.mark.unit
def test_parse_image_pull_secrets():
    assert GKEEnvironment._parse_image_pull_secrets(None) == []
    assert GKEEnvironment._parse_image_pull_secrets('["s1", "s2"]') == ["s1", "s2"]
    assert GKEEnvironment._parse_image_pull_secrets("s1, s2") == ["s1", "s2"]
    assert GKEEnvironment._parse_image_pull_secrets(["s1", "s2"]) == ["s1", "s2"]
    assert GKEEnvironment._parse_image_pull_secrets("[invalid json") == [
        "[invalid json"
    ]


@pytest.mark.unit
def test_resolve_active_compute_class(tmp_path):
    # 1. Standard cluster: task specific class
    env = make_gke_env(
        tmp_path,
        mock_autopilot=False,
        task_compute_classes="test-env=Scale-Out",
    )
    assert env._resolve_active_compute_class(is_autopilot=False) == "Scale-Out"

    # 2. Standard cluster fallback to global
    env_global = make_gke_env(tmp_path, mock_autopilot=False, compute_class="General")
    assert env_global._resolve_active_compute_class(is_autopilot=False) == "General"

    # 3. Autopilot: Accelerators return None
    env_gpu = make_gke_env(
        tmp_path,
        mock_autopilot=True,
        compute_class="General",
        task_env_config=EnvironmentConfig(gpus=1, gpu_types=["nvidia-tesla-t4"]),
    )
    assert env_gpu._resolve_active_compute_class(is_autopilot=True) is None

    # 4. Autopilot: High ephemeral storage auto-promotes to Performance
    env_storage = make_gke_env(
        tmp_path,
        mock_autopilot=True,
        task_env_config=EnvironmentConfig(storage_mb=15000),
    )
    assert env_storage._resolve_active_compute_class(is_autopilot=True) == "Performance"

    # 5. Node pool specified returns None
    env_nodepool = make_gke_env(tmp_path, node_pool="my-pool", compute_class="General")
    assert env_nodepool._resolve_active_compute_class() is None

    # 6. Autopilot fallback default None
    env_default = make_gke_env(tmp_path, mock_autopilot=True)
    assert env_default._resolve_active_compute_class(is_autopilot=True) is None


@pytest.mark.unit
def test_dind_compose_pod_promotes_to_performance(tmp_path, monkeypatch):
    """A DinD plane pushes the Pod over the Autopilot general-purpose ceiling.

    `dind-engine` reserves at least DIND_STORAGE_FLOOR_MB for /var/lib/docker,
    which is exactly the 10 GiB ceiling, so any task storage on top clears it.
    """
    env = make_gke_env(
        tmp_path,
        mock_autopilot=True,
        task_env_config=EnvironmentConfig(storage_mb=1024),
    )
    env._compose_mode = True

    monkeypatch.setattr(env, "_compose_needs_dind", lambda: False)
    assert env._effective_total_ephemeral_storage_mb() == 1024
    assert env._resolve_active_compute_class(is_autopilot=True) is None

    monkeypatch.setattr(env, "_compose_needs_dind", lambda: True)
    assert env._effective_total_ephemeral_storage_mb() == 1024 + DIND_STORAGE_FLOOR_MB
    assert env._resolve_active_compute_class(is_autopilot=True) == "Performance"


@pytest.mark.unit
def test_dind_placement_probe_never_runs_from_constructor(tmp_path, monkeypatch):
    """Determining DinD placement shells out, which must not happen in __init__."""

    def _explode(*_args, **_kwargs):
        raise AssertionError(
            "normalize_compose_project must not run during construction"
        )

    monkeypatch.setattr(
        "harbor_gke_ext.compose_spec.normalize_compose_project", _explode
    )

    env = make_gke_env(
        tmp_path,
        mock_autopilot=True,
        task_env_config=EnvironmentConfig(storage_mb=1024),
    )
    env._compose_mode = True

    assert env._compose_dind_probe_enabled is False
    assert env._compose_needs_dind() is False


@pytest.mark.unit
def test_dind_placement_probe_is_computed_once(tmp_path, monkeypatch):
    """The subprocess is on every trial's path, so the answer must be cached."""
    compose_file = tmp_path / "extra-compose.yaml"
    compose_file.write_text("services:\n  main:\n    image: busybox\n")

    env = make_gke_env(tmp_path, mock_autopilot=True)
    env._compose_mode = True
    env._compose_dind_probe_enabled = True
    env.extra_docker_compose_paths = [compose_file]

    calls: list[int] = []

    def _fake_normalize(*_args, **_kwargs):
        calls.append(1)
        raise RuntimeError("docker is unavailable in unit tests")

    monkeypatch.setattr(
        "harbor_gke_ext.compose_spec.normalize_compose_project", _fake_normalize
    )

    assert env._compose_needs_dind() is False
    assert env._compose_needs_dind() is False
    assert len(calls) == 1


@pytest.mark.unit
def test_parse_task_mapping():
    assert GKEEnvironment._parse_task_mapping(None) == {}
    assert GKEEnvironment._parse_task_mapping({"t1": "p1"}) == {"t1": "p1"}
    assert GKEEnvironment._parse_task_mapping("t1=p1, t2:p2") == {
        "t1": "p1",
        "t2": "p2",
    }
    assert GKEEnvironment._parse_task_mapping(["t1=p1", "t2=p2"]) == {
        "t1": "p1",
        "t2": "p2",
    }
    # '=' binds before ':' so values may themselves contain a colon.
    assert GKEEnvironment._parse_task_mapping("t1=ns:pool") == {"t1": "ns:pool"}


@pytest.mark.unit
def test_resolve_active_node_pool(tmp_path):
    # 1. Nothing configured
    assert make_gke_env(tmp_path)._resolve_active_node_pool() is None

    # 2. Global option only
    env_global = make_gke_env(tmp_path, node_pool="global-pool")
    assert env_global._resolve_active_node_pool() == "global-pool"

    # 3. Task-specific mapping outranks the global option
    env_task = make_gke_env(
        tmp_path, node_pool="global-pool", task_node_pools="test-env=task-pool"
    )
    assert env_task._resolve_active_node_pool() == "task-pool"

    # 4. Namespaced task names also match on their basename
    env_base = make_gke_env(tmp_path, task_node_pools="my-task=base-pool")
    env_base.environment_name = "suite/my-task"
    assert env_base._resolve_active_node_pool() == "base-pool"

    # 5. A mapping for a different task falls through to the global option
    env_miss = make_gke_env(
        tmp_path, node_pool="global-pool", task_node_pools="other=other-pool"
    )
    assert env_miss._resolve_active_node_pool() == "global-pool"


@pytest.mark.unit
def test_task_node_pool_suppresses_compute_class(tmp_path):
    """A node pool and a ComputeClass emit conflicting node selectors."""
    env = make_gke_env(
        tmp_path,
        task_node_pools="test-env=dind-pool",
        compute_class="General",
        task_compute_classes="test-env=Scale-Out",
    )
    assert env._active_node_pool == "dind-pool"
    assert env._resolve_active_compute_class() is None


# ─────────────────────────────────────────────────────────────────────────────
# 9. Pod & Job Creation (_create_pod)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_batch_api_409_recreate(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]

    conflict_err = ApiException(status=409, reason="Conflict")
    not_found_err = ApiException(status=404, reason="NotFound")

    mock_k8s_manager["batch_api"].create_namespaced_job.side_effect = [
        conflict_err,
        None,
    ]
    mock_k8s_manager["batch_api"].delete_namespaced_job.return_value = None
    mock_k8s_manager["batch_api"].read_namespaced_job.side_effect = not_found_err

    mock_pod = MagicMock()
    mock_pod.metadata.name = "spawned-pod-123"
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()
    mock_k8s_manager["core_api"].list_namespaced_pod.return_value = SimpleNamespace(
        items=[mock_pod]
    )

    await env._create_pod(mock_pod)
    assert mock_k8s_manager["batch_api"].create_namespaced_job.call_count == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_batch_api_409_delete_timeout(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]

    conflict_err = ApiException(status=409, reason="Conflict")
    mock_k8s_manager["batch_api"].create_namespaced_job.side_effect = conflict_err
    mock_k8s_manager["batch_api"].delete_namespaced_job.return_value = None

    stuck_job = MagicMock()
    stuck_job.metadata.finalizers = ["foregroundDeletion"]
    mock_k8s_manager["batch_api"].read_namespaced_job.return_value = stuck_job

    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with (
        patch(
            "harbor_gke_ext.environment.time.monotonic",
            side_effect=mock_monotonic_sequence(0.0, 100.0, 101.0),
        ),
        patch("harbor_gke_ext.environment.asyncio.sleep", AsyncMock()),
        pytest.raises(RuntimeError, match="Timed out waiting for existing Job"),
    ):
        await env._create_pod(mock_pod)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_batch_api_spawn_warning_error(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]

    mock_k8s_manager["core_api"].list_namespaced_pod.return_value = SimpleNamespace(
        items=[]
    )

    mock_ev = MagicMock()
    mock_ev.type = "Warning"
    mock_ev.reason = "FailedCreate"
    mock_ev.message = "Quota exceeded"
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[mock_ev]
    )

    mock_pod = MagicMock()
    mock_pod.metadata.name = "spawned-pod-123"
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with pytest.raises(RuntimeError, match="failed to create Pod: Quota exceeded"):
        await env._create_pod(mock_pod)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_batch_api_spawn_timeout(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]

    mock_k8s_manager["core_api"].list_namespaced_pod.return_value = SimpleNamespace(
        items=[]
    )
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[]
    )

    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with (
        patch(
            "harbor_gke_ext.environment.time.monotonic",
            side_effect=mock_monotonic_sequence(0.0, 1000.0, 1001.0),
        ),
        patch("harbor_gke_ext.environment.asyncio.sleep", AsyncMock()),
        pytest.raises(
            TimeoutError,
            match="Timed out after .* waiting for Kubernetes Job controller",
        ),
    ):
        await env._create_pod(mock_pod)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_direct_without_batch_api(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = None  # Force direct pod creation

    mock_pod = MagicMock()
    mock_pod.metadata.name = "direct-pod"

    await env._create_pod(mock_pod)
    mock_k8s_manager["core_api"].create_namespaced_pod.assert_called_once_with(
        namespace="default", body=mock_pod
    )
    assert env.pod_name == "direct-pod"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_direct_409_recreate(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = None

    conflict_err = ApiException(status=409, reason="Conflict")
    not_found_err = ApiException(status=404, reason="NotFound")

    mock_k8s_manager["core_api"].create_namespaced_pod.side_effect = [
        conflict_err,
        None,
    ]
    mock_k8s_manager["core_api"].delete_namespaced_pod.return_value = None
    mock_k8s_manager["core_api"].read_namespaced_pod.side_effect = not_found_err

    mock_pod = MagicMock()
    mock_pod.metadata.name = "direct-pod"

    await env._create_pod(mock_pod)
    assert mock_k8s_manager["core_api"].create_namespaced_pod.call_count == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_error_handling(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    mock_k8s_manager["batch_api"].create_namespaced_job.side_effect = ApiException(
        status=400, reason="Bad Request"
    )

    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with pytest.raises(RuntimeError, match="Failed to create job"):
        await env._create_pod(mock_pod)


_WARDEN_TIMEOUT_BODY = (
    '{"kind":"Status","message":"Internal error occurred: failed calling webhook '
    '\\"warden-validating.common-webhooks.networking.gke.io\\": failed to call webhook: '
    'context deadline exceeded","code":500}'
)


def _webhook_overload_error() -> ApiException:
    err = ApiException(status=500, reason="Internal Server Error")
    err.body = _WARDEN_TIMEOUT_BODY
    return err


def _created_job(uid: str) -> SimpleNamespace:
    return SimpleNamespace(metadata=SimpleNamespace(uid=uid))


def _listed_pod(name: str, deletion_timestamp=None) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, deletion_timestamp=deletion_timestamp),
        status=SimpleNamespace(phase="Pending", reason=None),
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_job_retries_webhook_overload_and_shrinks_limiter(
    tmp_path, mock_k8s_manager
):
    from harbor_gke_ext.control_plane import get_control_plane_limiter

    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    mock_k8s_manager["batch_api"].create_namespaced_job.side_effect = [
        _webhook_overload_error(),
        _webhook_overload_error(),
        _created_job("uid-new"),
    ]
    mock_k8s_manager["core_api"].list_namespaced_pod.return_value = SimpleNamespace(
        items=[_listed_pod("spawned-pod")]
    )
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    sleep_mock = AsyncMock()
    with patch("harbor_gke_ext.environment.asyncio.sleep", sleep_mock):
        await env._create_pod(mock_pod)

    assert mock_k8s_manager["batch_api"].create_namespaced_job.call_count == 3
    assert env.pod_name == "spawned-pod"
    assert env._job_uid == "uid-new"
    # Two jittered backoffs around the 2 s and 4 s nominal delays.
    delays = [c.args[0] for c in sleep_mock.await_args_list]
    assert 1.6 <= delays[0] <= 2.4
    assert 3.2 <= delays[1] <= 4.8
    # Both failures fell inside one cooldown: a single halving.
    assert get_control_plane_limiter().limit == 64


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_job_webhook_overload_exhausts_retries(tmp_path, mock_k8s_manager):
    from harbor_gke_ext.constants import _GKE_CONTROL_PLANE_WRITE_MAX_ATTEMPTS

    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    mock_k8s_manager[
        "batch_api"
    ].create_namespaced_job.side_effect = _webhook_overload_error()
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with patch("harbor_gke_ext.environment.asyncio.sleep", AsyncMock()):
        with pytest.raises(RuntimeError, match="Failed to create job .*failed calling"):
            await env._create_pod(mock_pod)
    assert (
        mock_k8s_manager["batch_api"].create_namespaced_job.call_count
        == _GKE_CONTROL_PLANE_WRITE_MAX_ATTEMPTS
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_job_non_overload_500_is_not_retried(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    err = ApiException(status=500, reason="Internal Server Error")
    err.body = '{"message":"etcdserver: request timed out"}'
    mock_k8s_manager["batch_api"].create_namespaced_job.side_effect = err
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with pytest.raises(RuntimeError, match="Failed to create job"):
        await env._create_pod(mock_pod)
    assert mock_k8s_manager["batch_api"].create_namespaced_job.call_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_job_resolves_pod_by_controller_uid_and_skips_terminating(
    tmp_path, mock_k8s_manager
):
    """A retried trial reuses the Job name; the predecessor's terminating Pod must not be adopted."""
    import datetime as dt

    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    mock_k8s_manager["batch_api"].create_namespaced_job.return_value = _created_job(
        "uid-new"
    )
    stale = _listed_pod("old-attempt-pod", deletion_timestamp=dt.datetime.now(dt.UTC))
    mock_k8s_manager["core_api"].list_namespaced_pod.side_effect = [
        SimpleNamespace(items=[stale]),
        SimpleNamespace(items=[stale, _listed_pod("new-attempt-pod")]),
    ]
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[]
    )
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with patch("harbor_gke_ext.environment.asyncio.sleep", AsyncMock()):
        await env._create_pod(mock_pod)

    assert env.pod_name == "new-attempt-pod"
    for call in mock_k8s_manager["core_api"].list_namespaced_pod.call_args_list:
        assert (
            call.kwargs["label_selector"]
            == "batch.kubernetes.io/controller-uid=uid-new"
        )
    event_call = mock_k8s_manager["core_api"].list_namespaced_event.call_args
    assert event_call.kwargs["field_selector"] == "involvedObject.uid=uid-new"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_job_tolerates_webhook_overload_failed_create_event(
    tmp_path, mock_k8s_manager
):
    from harbor_gke_ext.control_plane import get_control_plane_limiter

    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    mock_k8s_manager["batch_api"].create_namespaced_job.return_value = _created_job(
        "uid-new"
    )
    mock_k8s_manager["core_api"].list_namespaced_pod.side_effect = [
        SimpleNamespace(items=[]),
        SimpleNamespace(items=[]),
        SimpleNamespace(items=[_listed_pod("spawned-pod")]),
    ]
    overload_event = SimpleNamespace(
        type="Warning",
        reason="FailedCreate",
        message=f"Error creating: Internal error occurred: {_WARDEN_TIMEOUT_BODY}",
        count=1,
        metadata=SimpleNamespace(name="job.ev1"),
    )
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[overload_event]
    )
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {}
    mock_pod.spec = MagicMock()

    with patch("harbor_gke_ext.environment.asyncio.sleep", AsyncMock()):
        await env._create_pod(mock_pod)

    assert env.pod_name == "spawned-pod"
    # The same event seen on two polls counts as one overload signal.
    assert get_control_plane_limiter().limit == 64


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reresolve_pod_name_uses_controller_uid_when_known(
    tmp_path, mock_k8s_manager
):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._job_uid = "uid-new"
    mock_k8s_manager["core_api"].list_namespaced_pod.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(
                metadata=SimpleNamespace(name="replacement", deletion_timestamp=None),
                status=SimpleNamespace(phase="Running", reason=None),
            )
        ]
    )
    assert await env._reresolve_pod_name() == "replacement"
    call = mock_k8s_manager["core_api"].list_namespaced_pod.call_args
    assert call.kwargs["label_selector"] == "batch.kubernetes.io/controller-uid=uid-new"


# ─────────────────────────────────────────────────────────────────────────────
# 10. Lifecycle: start & stop
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_direct_pod_build_and_push(tmp_path):
    env = make_gke_env(tmp_path)
    with patch.object(env, "_ensure_client", AsyncMock()):
        with patch.object(env, "_image_exists", AsyncMock(return_value=False)):
            with patch.object(env, "_build_and_push_image", AsyncMock()) as mock_build:
                with patch.object(env, "_build_direct_pod", return_value=MagicMock()):
                    with patch.object(env, "_create_pod", AsyncMock()):
                        with patch.object(env, "_apply_network_policy", AsyncMock()):
                            with patch.object(env, "_wait_for_pod_ready", AsyncMock()):
                                with patch.object(
                                    env, "_wait_for_container_exec_ready", AsyncMock()
                                ):
                                    with patch.object(
                                        env,
                                        "ensure_dirs",
                                        AsyncMock(
                                            return_value=ExecResult(
                                                stdout="", return_code=0
                                            )
                                        ),
                                    ):
                                        with patch.object(
                                            env,
                                            "_upload_environment_dir_after_start",
                                            AsyncMock(),
                                        ):
                                            await env.start(force_build=True)
                                            mock_build.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_direct_pod_mkdir_failure(tmp_path):
    env = make_gke_env(tmp_path)
    with patch.object(env, "_ensure_client", AsyncMock()):
        with patch.object(env, "_image_exists", AsyncMock(return_value=True)):
            with patch.object(env, "_build_direct_pod", return_value=MagicMock()):
                with patch.object(env, "_create_pod", AsyncMock()):
                    with patch.object(env, "_wait_for_pod_ready", AsyncMock()):
                        with patch.object(
                            env, "_wait_for_container_exec_ready", AsyncMock()
                        ):
                            with patch.object(
                                env,
                                "ensure_dirs",
                                AsyncMock(
                                    return_value=ExecResult(
                                        stdout="",
                                        stderr="Permission denied",
                                        return_code=1,
                                    )
                                ),
                            ):
                                with pytest.raises(
                                    RuntimeError,
                                    match="Failed to create mounted directories",
                                ):
                                    await env.start(force_build=False)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_compose_dind_mode_enables_sidecar(tmp_path):
    env = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  app:\n    image: nginx\n",
        compose_mode="dind",
    )
    with patch.object(env, "_ensure_client", AsyncMock()):
        with patch.object(env, "is_autopilot", AsyncMock(return_value=True)):
            with patch(
                "harbor_gke_ext.environment.discover_compose_build_services",
                return_value={},
            ):
                with patch.object(env, "_image_exists", AsyncMock(return_value=True)):
                    with patch.object(
                        env, "_build_compose_pod", return_value=MagicMock()
                    ):
                        with patch.object(env, "_create_pod", AsyncMock()):
                            with patch.object(env, "_wait_for_pod_ready", AsyncMock()):
                                with patch.object(
                                    env, "_wait_for_container_exec_ready", AsyncMock()
                                ):
                                    with patch.object(
                                        env, "ensure_dirs", AsyncMock(return_value=None)
                                    ):
                                        await env.start(force_build=False)
                                        assert (
                                            env._compose_spec_args["compose_placement"]
                                            == "dind"
                                        )
                                        assert (
                                            env._compose_spec_args["is_autopilot"]
                                            is True
                                        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_pod_gvisor_retry_on_security_rejection(tmp_path):
    env = make_gke_env(tmp_path)
    pod = MagicMock()
    pod.spec.runtime_class_name = None
    env._batch_api = MagicMock()

    with patch.object(
        env,
        "_attempt_create_pod",
        AsyncMock(
            side_effect=[
                RuntimeError("Pod violated PodSecurity capabilities requirement"),
                None,
            ]
        ),
    ) as mock_attempt:
        await env._create_pod(pod)
        assert pod.spec.runtime_class_name == "gvisor"
        assert mock_attempt.await_count == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_compose_native_success(tmp_path):
    env = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  worker:\n    image: redis\n",
        compose_mode="native",
    )
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch(
            "harbor_gke_ext.environment.discover_compose_build_services",
            return_value={"worker": (tmp_path / "env", "Dockerfile")},
        ),
        patch.object(env, "_image_exists", AsyncMock(return_value=True)),
        patch(
            "harbor_gke_ext.environment.check_image_exists_in_registry",
            AsyncMock(return_value=True),
        ),
        patch.object(env, "_build_compose_pod", return_value=MagicMock()),
        patch.object(env, "_create_pod", AsyncMock()),
        patch.object(env, "_apply_network_policy", AsyncMock()),
        patch.object(env, "_wait_for_pod_ready", AsyncMock()),
        patch.object(
            env,
            "_wait_for_container_exec_ready",
            AsyncMock(),
        ),
        patch.object(
            env,
            "ensure_dirs",
            AsyncMock(return_value=ExecResult(return_code=0)),
        ),
        patch.object(
            env,
            "_upload_environment_dir_after_start",
            AsyncMock(),
        ),
    ):
        await env.start(force_build=False)
        assert env._active_strategy == "native"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_compose_native_builds_sidecars(tmp_path):
    env = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  worker:\n    image: redis\n",
        compose_mode="native",
    )
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch(
            "harbor_gke_ext.environment.discover_compose_build_services",
            return_value={"worker": (tmp_path / "env", "Dockerfile")},
        ),
        patch.object(env, "_image_exists", AsyncMock(return_value=False)),
        patch(
            "harbor_gke_ext.environment.check_image_exists_in_registry",
            AsyncMock(return_value=False),
        ),
        patch.object(env, "_build_and_push_image", AsyncMock()) as mock_build,
        patch.object(env, "_build_compose_pod", return_value=MagicMock()),
        patch.object(env, "_create_pod", AsyncMock()),
        patch.object(env, "_apply_network_policy", AsyncMock()),
        patch.object(env, "_wait_for_pod_ready", AsyncMock()),
        patch.object(
            env,
            "_wait_for_container_exec_ready",
            AsyncMock(),
        ),
        patch.object(
            env,
            "ensure_dirs",
            AsyncMock(return_value=ExecResult(return_code=0)),
        ),
    ):
        await env.start(force_build=True)
        assert mock_build.await_count == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stop_service_main_and_sidecar(tmp_path):
    env = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  web:\n    image: nginx\n",
        compose_mode="native",
    )
    with patch.object(
        env, "exec", AsyncMock(return_value=ExecResult(return_code=0))
    ) as mock_exec:
        await env.stop_service("main")
        assert "kill -TERM" in mock_exec.call_args.args[0]
        assert mock_exec.call_args.kwargs["container"] == "main"

        await env.stop_service("web")
        assert "kill -STOP -1" in mock_exec.call_args.args[0]
        assert mock_exec.call_args.kwargs["container"] == "web"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stop_delete_pod_and_release(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    env._networking_api = mock_k8s_manager["networking_api"]

    with patch(
        "harbor_gke_ext.environment.delete_network_policies",
        new=AsyncMock(),
    ) as mock_del_net:
        await env.stop(delete=True)
        mock_del_net.assert_awaited_once()
        mock_k8s_manager["batch_api"].delete_namespaced_job.assert_called_once()
        mock_k8s_manager["core_api"].delete_namespaced_pod.assert_called_once()
        mock_k8s_manager["manager"].release_client.assert_awaited_once()
        assert env._core_api is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stop_delete_handles_api_exception(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()
    env._batch_api = mock_k8s_manager["batch_api"]
    mock_k8s_manager["batch_api"].delete_namespaced_job.side_effect = ApiException(
        status=500
    )
    mock_k8s_manager["core_api"].delete_namespaced_pod.side_effect = ApiException(
        status=500
    )

    # Should log warnings and not raise, but still release client
    await env.stop(delete=True)
    mock_k8s_manager["manager"].release_client.assert_awaited_once()


# ─────────────────────────────────────────────────────────────────────────────
# 11. Command Execution (exec, supervised, decoupled, timeouts)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_invalid_env_name(tmp_path):
    env = make_gke_env(tmp_path)
    with patch.object(env, "_ensure_client", AsyncMock()):
        with pytest.raises(ValueError, match="Invalid environment variable name"):
            await env.exec("echo hi", env={"123BAD": "val"})


# The execs below run on the local host through the fake kubelet: the "Pod" is
# this machine, so the supervised and decoupled shell scripts really execute.


@pytest.fixture
def local_exec_env(tmp_path, fake_kubelet):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch.object(env, "_connect_exec_stream", fake_kubelet.connect),
        patch(
            "harbor_gke_ext.environment.check_pod_terminated",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "harbor_gke_ext.exec_engine.check_pod_terminated",
            new=AsyncMock(return_value=None),
        ),
        patch("harbor_gke_ext.exec_engine._RECOVERY_INITIAL_POLL_INTERVAL_SEC", 0.1),
    ):
        yield env


def _is_supervisor(argv: list[str]) -> bool:
    return "tail -n +1 -f" in argv[-1]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_user_and_cwd(tmp_path):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    commands: list[list[str]] = []

    async def fake_connect(command, **kwargs):
        commands.append(command)
        raise GKEExecStreamClosedError("stop here")

    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch.object(env, "_connect_exec_stream", fake_connect),
    ):
        with pytest.raises(GKEExecStreamClosedError):
            await env.exec("ls", cwd="/home/app", user=1000, supervised=False)
        assert "su $(getent passwd 1000" in commands[-1][2]
        assert "cd /home/app" in commands[-1][2]

        with pytest.raises(GKEExecStreamClosedError):
            await env.exec("ls", user="alice", supervised=False)
        assert "su alice" in commands[-1][2]


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("supervised", [True, False])
async def test_exec_streams_output_and_exit_code(local_exec_env, tmp_path, supervised):
    chunks: list[tuple[str, str]] = []

    async def callback(text, stream_name):
        chunks.append((stream_name, text))

    with local_exec_env.scoped_output_callback(callback):
        res = await local_exec_env.exec(
            "printf 'héllo '; printf warn >&2; printf world; exit 3",
            cwd=str(tmp_path),
            supervised=supervised,
        )
    assert (res.stdout, res.stderr, res.return_code) == ("héllo world", "warn", 3)
    assert "".join(t for s, t in chunks if s == "stdout") == "héllo world"
    assert "".join(t for s, t in chunks if s == "stderr") == "warn"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_callback_exception_propagates(local_exec_env, tmp_path):
    async def callback(text, stream_name):
        raise RuntimeError("consumer failed")

    with local_exec_env.scoped_output_callback(callback):
        with pytest.raises(RuntimeError, match="consumer failed"):
            await local_exec_env.exec("echo hi", cwd=str(tmp_path), supervised=False)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_unsupervised_timeout_returns_124(local_exec_env, tmp_path):
    started = time.monotonic()
    res = await local_exec_env.exec(
        "printf early; sleep 5", cwd=str(tmp_path), timeout_sec=1, supervised=False
    )
    assert res.return_code == 124
    assert res.stdout == "early"
    assert "timed out after 1 seconds" in (res.stderr or "")
    assert time.monotonic() - started < 4


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_supervised_timeout_kills_command(
    local_exec_env, fake_kubelet, tmp_path
):
    marker = f"{os.getpid()}{time.monotonic_ns() % 100000}"
    res = await local_exec_env.exec(
        f"sleep 7.{marker}", cwd=str(tmp_path), timeout_sec=1, supervised=True
    )
    assert res.return_code == 124
    assert "timed out after 1 seconds" in (res.stderr or "")
    kill_scripts = [
        c.command[-1]
        for c in fake_kubelet.connections
        if "kill -s KILL" in c.command[-1]
    ]
    assert len(kill_scripts) == 1
    workdir = re.search(r"(/tmp/harbor_[0-9a-f]+)/pid", kill_scripts[0]).group(1)
    assert not Path(workdir).exists()

    def gone() -> bool:
        # The marker is also in the supervisor's and wrapper's command lines, so
        # this checks that the command, its wrapper and the supervisor all exited.
        found = subprocess.run(
            ["pgrep", "-f", f"sleep 7.{marker}"], capture_output=True, text=True
        )
        return not found.stdout.strip()

    deadline = time.monotonic() + 5
    while not gone() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert gone(), "the command, its wrapper and its supervisor must all exit"
    assert not Path(f"{workdir}.status").exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_supervised_recovers_after_stream_drop(
    local_exec_env, fake_kubelet, tmp_path
):
    fake_kubelet.drop_stream_after = lambda argv: 6 if _is_supervisor(argv) else None
    res = await local_exec_env.exec(
        "printf first-; sleep 0.5; printf second; exit 3",
        cwd=str(tmp_path),
        timeout_sec=30,
        supervised=True,
    )
    assert any(c.dropped for c in fake_kubelet.connections)
    assert res.return_code == 3
    # Output produced after the drop may be lost once the supervisor removed its
    # workdir; everything streamed before the drop must be kept.
    assert (res.stdout or "").startswith("first-")
    probes = [c for c in fake_kubelet.connections if "DONE_NO_OUTPUT" in c.command[-1]]
    assert probes, "recovery must probe the workdir"
    # Probes run as the supervisor's identity: no su/cd wrapping.
    assert all(c.command[0] == "sh" and "su " not in c.command[-1] for c in probes)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_unsupervised_stream_drop_raises(
    local_exec_env, fake_kubelet, tmp_path
):
    fake_kubelet.drop_stream_after = lambda argv: 3
    with pytest.raises(GKEExecStreamClosedError, match="connection_lost"):
        await local_exec_env.exec(
            "printf abcdef; sleep 0.3; printf ghi",
            cwd=str(tmp_path),
            supervised=False,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_failure_status_without_exit_code_raises(
    local_exec_env, fake_kubelet, tmp_path
):
    fake_kubelet.status_override = lambda argv: json.dumps(
        {"status": "Failure", "message": "container not found (main)"}
    ).encode()
    with pytest.raises(GKEExecStreamClosedError, match="container not found"):
        await local_exec_env.exec("true", cwd=str(tmp_path), supervised=False)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_decoupled_mode_end_to_end(tmp_path, fake_kubelet):
    env = make_gke_env(tmp_path, decoupled=True)
    env._core_api = MagicMock()
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch.object(env, "_connect_exec_stream", fake_kubelet.connect),
        patch(
            "harbor_gke_ext.exec_engine.check_pod_terminated",
            new=AsyncMock(return_value=None),
        ),
        patch("harbor_gke_ext.exec_engine._RECOVERY_INITIAL_POLL_INTERVAL_SEC", 0.1),
    ):
        res = await env.exec(
            "printf one; sleep 0.3; printf ' two'; exit 4",
            cwd=str(tmp_path),
            supervised=True,
        )
    assert (res.stdout, res.return_code) == ("one two", 4)
    launch = fake_kubelet.connections[0].command[-1]
    assert "trap" in launch and "tail -n +1 -f" not in launch


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_decoupled_exec_uses_raw_connect(tmp_path):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch(
            "harbor_gke_ext.environment.poll_decoupled_exec",
            AsyncMock(return_value=ExecResult(return_code=0)),
        ) as mock_poll,
    ):
        await env._poll_decoupled_exec(
            workdir="/tmp/work",
            output=ExecOutputAccumulator(),
            timeout_sec=10,
            start_time=time.monotonic(),
        )
    mock_poll.assert_awaited_once()
    assert mock_poll.await_args.kwargs["connect"] == env._connect_exec_stream


# ─────────────────────────────────────────────────────────────────────────────
# 12. File Transfers & Path Kind Checks
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upload_and_download_file(tmp_path):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    env.pod_name = "test-pod"
    src_file = tmp_path / "test.txt"
    src_file.write_text("content")

    with patch.object(env, "_ensure_client", AsyncMock()):
        with patch.object(env, "_wait_for_container_exec_ready", AsyncMock()):
            with patch(
                "harbor_gke_ext.environment.ft_upload_file",
                new=AsyncMock(),
            ) as mock_up:
                await env.upload_file(src_file, "/remote/test.txt")
                mock_up.assert_awaited_once()

            with patch(
                "harbor_gke_ext.environment.ft_download_file",
                new=AsyncMock(),
            ) as mock_down:
                await env.download_file("/remote/test.txt", tmp_path / "downloaded.txt")
                mock_down.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upload_and_download_dir(tmp_path):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    env.pod_name = "test-pod"
    src_dir = tmp_path / "mydir"
    src_dir.mkdir()
    (src_dir / "file.txt").write_text("hello")

    with patch.object(env, "_ensure_client", AsyncMock()):
        with patch.object(env, "_wait_for_container_exec_ready", AsyncMock()):
            with patch(
                "harbor_gke_ext.environment.ft_upload_dir",
                new=AsyncMock(),
            ) as mock_up_dir:
                await env.upload_dir(src_dir, "/remote/dir")
                mock_up_dir.assert_awaited_once()

            with patch(
                "harbor_gke_ext.environment.ft_download_dir",
                new=AsyncMock(),
            ) as mock_down_dir:
                await env.download_dir("/remote/dir", tmp_path / "out_dir")
                mock_down_dir.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_container_lost_raises_without_local_retry(tmp_path):
    """A dead Pod must surface as TrialContainerLostError, never a silent return.

    A silent return makes the verifier report RewardFileNotFoundError, which
    reads as a task failure and is excluded from Harbor's retries.
    """
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    env.pod_name = "test-pod"
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch(
            "harbor_gke_ext.environment.ft_download_file",
            new=AsyncMock(side_effect=RuntimeError("exec stream failed")),
        ) as mock_down,
        patch(
            "harbor_gke_ext.environment.check_pod_terminated",
            new=AsyncMock(
                side_effect=TrialContainerLostError(
                    "Pod test-pod does not exist in cluster."
                )
            ),
        ),
    ):
        with pytest.raises(TrialContainerLostError, match="does not exist"):
            await env.download_file("/remote/test.txt", tmp_path / "out.txt")
        mock_down.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_other_error_raises(tmp_path):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    env.pod_name = "test-pod"
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch(
            "harbor_gke_ext.environment.ft_download_file",
            side_effect=RuntimeError("unexpected disk failure"),
        ),
        patch(
            "harbor_gke_ext.environment.check_pod_terminated",
            new=AsyncMock(return_value=None),
        ),
        pytest.raises(RuntimeError, match="unexpected disk failure"),
    ):
        await env.download_file("/remote/test.txt", tmp_path / "out.txt")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_dir_container_lost_raises_without_local_retry(tmp_path):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    env.pod_name = "test-pod"
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch(
            "harbor_gke_ext.environment.ft_download_dir",
            new=AsyncMock(side_effect=ApiException(status=404, reason="Not Found")),
        ) as mock_down,
        patch(
            "harbor_gke_ext.environment.check_pod_terminated",
            new=AsyncMock(
                side_effect=TrialContainerLostError(
                    "Container 'main' in pod test-pod has terminated "
                    "(reason='OOMKilled', exit_code=137)."
                )
            ),
        ),
    ):
        with pytest.raises(TrialContainerLostError, match="OOMKilled"):
            await env.download_dir("/remote/dir", tmp_path / "out_dir")
        mock_down.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_dir_other_error_raises(tmp_path):
    env = make_gke_env(tmp_path)
    env._core_api = MagicMock()
    env.pod_name = "test-pod"
    with (
        patch.object(env, "_ensure_client", AsyncMock()),
        patch(
            "harbor_gke_ext.environment.ft_download_dir",
            side_effect=ApiException(status=500, reason="Internal Server Error"),
        ),
        patch(
            "harbor_gke_ext.environment.check_pod_terminated",
            new=AsyncMock(return_value=None),
        ),
        pytest.raises(ApiException),
    ):
        await env.download_dir("/remote/dir", tmp_path / "out_dir")


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("return_code", [1, 137])
async def test_exec_nonzero_exit_on_dead_container_raises_lost(
    local_exec_env, tmp_path, return_code
):
    """An exec that ends because its container died is an infra failure, not an exit code."""
    with (
        patch(
            "harbor_gke_ext.environment.check_pod_terminated",
            new=AsyncMock(
                side_effect=TrialContainerLostError(
                    "Container 'main' in pod p has terminated "
                    "(reason='OOMKilled', exit_code=137)."
                )
            ),
        ),
        pytest.raises(TrialContainerLostError),
    ):
        await local_exec_env.exec(f"exit {return_code}", cwd=str(tmp_path))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_nonzero_exit_on_live_container_returns_code(
    local_exec_env, tmp_path
):
    with patch(
        "harbor_gke_ext.environment.check_pod_terminated",
        new=AsyncMock(return_value=None),
    ) as mock_check:
        res = await local_exec_env.exec(
            "printf out; printf err >&2; exit 2", cwd=str(tmp_path)
        )
    assert (res.stdout, res.stderr, res.return_code) == ("out", "err", 2)
    mock_check.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_zero_exit_skips_liveness_probe(local_exec_env, tmp_path):
    with patch(
        "harbor_gke_ext.environment.check_pod_terminated",
        new=AsyncMock(return_value=None),
    ) as mock_check:
        await local_exec_env.exec("true", cwd=str(tmp_path))
    mock_check.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_is_dir_and_is_file(tmp_path):
    env = make_gke_env(tmp_path)
    with patch.object(
        env,
        "exec",
        AsyncMock(side_effect=[ExecResult(return_code=0), ExecResult(return_code=1)]),
    ):
        assert await env.is_dir("/my/path") is True
        assert await env.is_file("/my/path") is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_service_is_dir_main_service(tmp_path):
    env = make_gke_env(tmp_path)
    with patch.object(env, "is_dir", AsyncMock(return_value=True)) as mock_is_dir:
        res = await env.service_is_dir("/app", service=None)
        assert res is True
        mock_is_dir.assert_awaited_once_with("/app", user=None)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_service_is_dir_sidecar(tmp_path):
    env = make_gke_env(tmp_path, compose_yaml="services:\n  db:\n    image: postgres\n")
    with patch.object(
        env, "exec", AsyncMock(return_value=ExecResult(return_code=0))
    ) as mock_exec:
        res = await env.service_is_dir("/var/lib/postgresql", service="db")
        assert res is True
        assert mock_exec.call_args.kwargs.get("container") == "db"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_service_is_dir_sidecar_native(tmp_path):
    env = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  db:\n    image: postgres\n",
    )
    with patch.object(
        env, "exec", AsyncMock(return_value=ExecResult(return_code=0))
    ) as mock_exec:
        res = await env.service_is_dir("/var/lib/postgresql", service="db")
        assert res is True
        mock_exec.assert_awaited_once()
        assert mock_exec.call_args.kwargs.get("container") == "db"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_path_kind_timeout(tmp_path):
    env = make_gke_env(tmp_path)
    with patch.object(env, "exec", AsyncMock(return_value=ExecResult(return_code=124))):
        with pytest.raises(TimeoutError, match="timed out after"):
            await env.is_dir("/some/path")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Healthcheck
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_healthcheck_none(tmp_path):
    env = make_gke_env(tmp_path)
    await env.run_healthcheck(None)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_healthcheck_success(tmp_path):
    env = make_gke_env(tmp_path)
    hc = HealthcheckConfig(
        command="curl -f http://localhost:8080/health",
        interval_sec=1,
        retries=2,
        start_period_sec=0,
    )
    with patch.object(env, "exec", AsyncMock(return_value=ExecResult(return_code=0))):
        await env.run_healthcheck(hc)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_healthcheck_failure_exhaustion(tmp_path):
    env = make_gke_env(tmp_path)
    hc = HealthcheckConfig(
        command="curl -f http://localhost:8080/health",
        interval_sec=0.01,
        retries=2,
        start_period_sec=0,
    )
    with patch.object(env, "exec", AsyncMock(return_value=ExecResult(return_code=1))):
        with pytest.raises(
            HealthcheckError, match="Healthcheck failed after 2 consecutive retries"
        ):
            await env.run_healthcheck(hc)


# ─────────────────────────────────────────────────────────────────────────────
# 14. Pod Readiness & Port Collision Diagnostics
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_for_pod_ready_events_logging(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()

    mock_container = MagicMock(ready=True)
    mock_pod = MagicMock()
    mock_pod.status.phase = "Running"
    mock_pod.status.container_statuses = [mock_container]
    mock_pod.status.init_container_statuses = []

    mock_ev = MagicMock(
        type="Warning", reason="Unhealthy", message="Readiness probe failed"
    )
    mock_k8s_manager["core_api"].read_namespaced_pod.return_value = mock_pod
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[mock_ev]
    )

    await env._wait_for_pod_ready(timeout_sec=5)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_for_pod_ready_failed_phase(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()

    mock_pod = MagicMock()
    mock_pod.status.phase = "Failed"
    mock_pod.status.reason = "DeadlineExceeded"
    mock_pod.status.message = "Pod exceeded deadline"
    mock_pod.status.container_statuses = []
    mock_pod.status.init_container_statuses = []
    mock_pod.spec.active_deadline_seconds = 600

    mock_k8s_manager["core_api"].read_namespaced_pod.return_value = mock_pod
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[]
    )

    with pytest.raises(RuntimeError, match="Pod failed to start"):
        await env._wait_for_pod_ready(timeout_sec=5)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_for_pod_ready_fails_fast_on_not_trigger_scale_up(
    tmp_path, mock_k8s_manager
):
    """An unsatisfiable reservation must not pend for the whole timeout.

    `NotTriggerScaleUp` is the cluster autoscaler saying no node group can be
    grown to fit the Pod, so waiting cannot help. Its message names the
    shortfall; Harbor adds what it asked for.
    """
    env = make_gke_env(tmp_path)
    await env._ensure_client()

    mock_pod = MagicMock()
    mock_pod.status.phase = "Pending"
    mock_pod.status.container_statuses = []
    mock_pod.status.init_container_statuses = []

    env._created_pod = SimpleNamespace(
        spec=SimpleNamespace(
            resources=SimpleNamespace(requests={"cpu": "4000m", "memory": "8192Mi"}),
            init_containers=[],
            containers=[
                SimpleNamespace(
                    name="main",
                    resources=SimpleNamespace(requests={"ephemeral-storage": "400Gi"}),
                )
            ],
        )
    )

    mock_ev = MagicMock(
        type="Warning",
        reason="NotTriggerScaleUp",
        message="pod didn't trigger scale-up: 3 Insufficient ephemeral-storage",
    )
    mock_k8s_manager["core_api"].read_namespaced_pod.return_value = mock_pod
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[mock_ev]
    )

    with pytest.raises(RuntimeError) as excinfo:
        await env._wait_for_pod_ready(timeout_sec=300)

    message = str(excinfo.value)
    assert "Insufficient ephemeral-storage" in message
    assert "main ephemeral-storage=400Gi" in message
    assert "pod-level cpu=4000m, memory=8192Mi" in message


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_for_pod_ready_crashloop(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()

    mock_c = MagicMock()
    mock_c.name = "c1"
    mock_c.state.waiting.reason = "CrashLoopBackOff"
    mock_pod = MagicMock()
    mock_pod.status.phase = "Pending"
    mock_pod.status.container_statuses = [mock_c]
    mock_pod.status.init_container_statuses = []

    mock_k8s_manager["core_api"].read_namespaced_pod.return_value = mock_pod
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[]
    )
    mock_k8s_manager["core_api"].read_namespaced_pod_log.return_value = "normal crash"

    with patch.object(env, "_check_container_port_collision", AsyncMock()) as mock_coll:
        with patch(
            "harbor_gke_ext.environment.time.monotonic",
            side_effect=mock_monotonic_sequence(0.0, 0.1, 0.1, 100.0, 101.0),
        ):
            with patch("harbor_gke_ext.environment.asyncio.sleep", AsyncMock()):
                with pytest.raises(RuntimeError, match="Pod not ready after"):
                    await env._wait_for_pod_ready(timeout_sec=2)
                mock_coll.assert_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_for_pod_ready_image_pull_backoff(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()

    mock_c = MagicMock()
    mock_c.state.waiting.reason = "ImagePullBackOff"
    mock_c.state.waiting.message = "Repository not found"
    mock_pod = MagicMock()
    mock_pod.status.phase = "Pending"
    mock_pod.status.container_statuses = [mock_c]
    mock_pod.status.init_container_statuses = []

    mock_k8s_manager["core_api"].read_namespaced_pod.return_value = mock_pod
    mock_k8s_manager["core_api"].list_namespaced_event.return_value = SimpleNamespace(
        items=[]
    )

    with pytest.raises(RuntimeError, match="Failed to pull image"):
        await env._wait_for_pod_ready(timeout_sec=5)


@pytest.mark.unit
def test_get_pod_failure_summary_exit_127(tmp_path):
    env = make_gke_env(tmp_path)
    mock_c = MagicMock()
    mock_c.name = "web"
    mock_c.state.waiting = None
    mock_c.state.terminated.reason = "Error"
    mock_c.state.terminated.exit_code = 127
    mock_c.state.terminated.message = "/bin/sh: not found"

    mock_pod = MagicMock()
    mock_pod.status.reason = "Error"
    mock_pod.status.message = "Container exited"
    mock_pod.status.container_statuses = [mock_c]
    mock_pod.status.init_container_statuses = []
    mock_pod.spec = None

    summary = env._get_pod_failure_summary(mock_pod)
    assert "exit code 127" in summary


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_container_port_collision(tmp_path, mock_k8s_manager):
    env = make_gke_env(tmp_path)
    await env._ensure_client()

    mock_k8s_manager[
        "core_api"
    ].read_namespaced_pod_log.return_value = (
        "Fatal error: address already in use 0.0.0.0:8080"
    )
    with pytest.raises(RuntimeError, match="Port collision detected in service"):
        await env._check_container_port_collision("web")


# ─────────────────────────────────────────────────────────────────────────────
# 15. Compose Transport & Pod Builders
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_compose_service_transport(tmp_path):
    env_no_compose = make_gke_env(tmp_path)
    with pytest.raises(Exception):
        env_no_compose._compose_service_transport("web")

    assert isinstance(
        env_no_compose._compose_service_transport("main"),
        _GKENativeComposeServiceTransport,
    )

    env_native = make_gke_env(
        tmp_path,
        compose_yaml="services:\n  web:\n    image: nginx\n",
        compose_mode="native",
    )
    transport = env_native._compose_service_transport("web")
    assert isinstance(transport, _GKENativeComposeServiceTransport)


# ─────────────────────────────────────────────────────────────────────────────
# 16. Transient Download Retries & max_storage_request_mb
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    "storage_mb,max_storage_request_mb,task_compute_classes,expected_mb",
    [
        (10240, 40960, None, 10240),
        (51200, 40960, None, 40960),
        (51200, "35840", None, 35840),
        (307200, 40960, "test-env:harbor-storage-500g", 307200),
    ],
)
def test_max_storage_request_mb(
    tmp_path,
    mock_k8s_manager,
    storage_mb,
    max_storage_request_mb,
    task_compute_classes,
    expected_mb,
):
    kwargs = {"max_storage_request_mb": max_storage_request_mb}
    if task_compute_classes:
        kwargs["task_compute_classes"] = task_compute_classes
    env = make_gke_env(
        tmp_path,
        task_env_config=EnvironmentConfig(storage_mb=storage_mb),
        **kwargs,
    )
    assert env._effective_storage_mb == expected_mb
    assert env.ephemeral_storage_request == f"{expected_mb}Mi"
    pod = env._build_direct_pod()
    assert (
        pod.spec.containers[0].resources.requests["ephemeral-storage"]
        == f"{expected_mb}Mi"
    )


@pytest.mark.unit
def test_resolve_active_machine_type(tmp_path):
    # Without task_machine_types or machine_type, no machine type is pinned
    env_default = make_gke_env(
        tmp_path,
        environment_name="example-bench/avx2-task",
        task_env_config=EnvironmentConfig(storage_mb=20480),
        autopilot=True,
    )
    assert env_default._active_machine_type is None

    # Explicit task_machine_types pins the machine family and suppresses Autopilot Performance promotion
    env_pinned = make_gke_env(
        tmp_path,
        environment_name="example-bench/avx2-task",
        task_env_config=EnvironmentConfig(storage_mb=20480),
        task_machine_types="avx2-task:n2-standard-4",
        autopilot=True,
    )
    assert env_pinned._active_machine_type == "n2-standard-4"
    pod = env_pinned._build_direct_pod()
    assert "node.kubernetes.io/instance-type" not in pod.spec.node_selector
    assert pod.spec.node_selector["cloud.google.com/machine-family"] == "n2"
    assert "cloud.google.com/compute-class" not in pod.spec.node_selector

    # Explicit task_machine_types overrides global machine_type
    env_override = make_gke_env(
        tmp_path,
        environment_name="example-bench/avx2-task",
        machine_type="c2-standard-8",
        task_machine_types="avx2-task:c3-standard-4",
    )
    assert env_override._active_machine_type == "c3-standard-4"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cluster_admission_controller_and_capacity_derivation():
    from harbor_gke_ext.cluster_probe import (
        ClusterAdmissionController,
        parse_gcloud_cluster_describe,
    )

    data = {
        "currentMasterVersion": "1.34.1-gke.100",
        "nodePools": [
            {
                "name": "default-pool",
                "config": {"machineType": "e2-standard-4"},
                "autoscaling": {"enabled": True, "totalMaxNodeCount": 10},
            },
            {
                "name": "gvisor-pool",
                "config": {
                    "machineType": "e2-standard-4",
                    "sandboxConfig": {"type": "GVISOR"},
                },
            },
        ],
    }
    data["nodePools"][1]["autoscaling"] = {"enabled": True, "totalMaxNodeCount": 2}
    caps = parse_gcloud_cluster_describe(data)
    assert caps.max_schedulable_cpu_cores == 40
    assert caps.max_gvisor_cpu_cores == 8
    assert dict(caps.node_pool_machine_types) == {
        "default-pool": "e2-standard-4",
        "gvisor-pool": "e2-standard-4",
    }

    ctrl = ClusterAdmissionController(max_cpu_cores=4, max_gvisor_cpu_cores=4)
    tok1 = await ctrl.acquire(4.0, is_gvisor=False)
    assert ctrl.in_flight_cpu == 4.0

    # A second standard pod must block until tok1 is released
    admitted_second = False

    async def _waiter():
        nonlocal admitted_second
        tok2 = await ctrl.acquire(2.0, is_gvisor=False)
        admitted_second = True
        await ctrl.release(tok2)

    waiter_task = asyncio.create_task(_waiter())
    await asyncio.sleep(0.02)
    assert admitted_second is False

    await ctrl.release(tok1)
    await waiter_task
    assert admitted_second is True
    assert ctrl.in_flight_cpu == 0.0


# ---------------------------------------------------------------------------
# F2: refuse a machine-type pin the cluster cannot satisfy
#
# The failure being prevented is silent: a `nodeSelector` that matches no node
# does not error, it pends until the trial's clock expires. Every test below
# therefore asserts on one of exactly two outcomes -- refused with a reason, or
# explicitly allowed through -- because "pends quietly" is the bug.
# ---------------------------------------------------------------------------

ISA_PINNED_TASK = "avx2-task"


def _standard_caps(**overrides) -> ClusterCapabilities:
    """A Standard cluster whose inventory has been successfully probed."""
    base = {
        "is_autopilot": False,
        "available_machine_types": ("e2-standard-2", "e2-standard-16"),
        "available_machine_families": ("e2",),
        "node_auto_provisioning_enabled": False,
    }
    base.update(overrides)
    return ClusterCapabilities(**base)


@pytest.mark.unit
def test_isa_pin_refused_when_cluster_has_no_matching_family(tmp_path):
    """U2.1 -- the whole point: a pin no node can satisfy is refused, not pended."""
    env = make_gke_env(
        tmp_path,
        environment_name=ISA_PINNED_TASK,
        task_machine_types={ISA_PINNED_TASK: "n2-standard-4"},
    )

    with pytest.raises(UnsatisfiableMachineTypeError) as excinfo:
        env._assert_machine_type_satisfiable(_standard_caps())

    message = str(excinfo.value)
    # The operator needs three things from this message: what was required, what
    # exists instead, and how to override it.
    assert "n2-standard-4" in message
    assert "e2-standard-16" in message
    assert "task_machine_types" in message


@pytest.mark.unit
def test_isa_pin_accepts_a_larger_size_in_the_right_family(tmp_path):
    """U2.2 -- a larger n2 pool in the family satisfies n2-standard-4 without mutating _active_machine_type."""
    env = make_gke_env(
        tmp_path,
        environment_name=ISA_PINNED_TASK,
        task_machine_types={ISA_PINNED_TASK: "n2-standard-4"},
    )
    caps = _standard_caps(
        available_machine_types=("e2-standard-2", "n2-standard-16"),
        available_machine_families=("e2", "n2"),
        node_auto_provisioning_enabled=False,
    )

    env._assert_machine_type_satisfiable(caps)
    assert env._active_machine_type == "n2-standard-4"


@pytest.mark.unit
def test_isa_pin_refused_when_family_pool_is_too_small_without_nap(tmp_path):
    """U2.2b -- n2-standard-16 is refused when the cluster only has n2-standard-4 and no NAP."""
    env = make_gke_env(
        tmp_path,
        environment_name=ISA_PINNED_TASK,
        task_machine_types={ISA_PINNED_TASK: "n2-standard-16"},
    )
    caps = _standard_caps(
        available_machine_types=("e2-standard-2", "n2-standard-4"),
        available_machine_families=("e2", "n2"),
        node_auto_provisioning_enabled=False,
    )

    with pytest.raises(UnsatisfiableMachineTypeError) as excinfo:
        env._assert_machine_type_satisfiable(caps)

    assert "at least 16 vCPUs" in str(excinfo.value)
    assert "n2-standard-4" in str(excinfo.value)


@pytest.mark.unit
def test_isa_pin_not_refused_when_inventory_is_unknown(tmp_path):
    """U2.3 -- absence of evidence is not evidence of absence; never block on it."""
    env = make_gke_env(
        tmp_path,
        environment_name=ISA_PINNED_TASK,
        task_machine_types={ISA_PINNED_TASK: "n2-standard-4"},
    )

    env._assert_machine_type_satisfiable(ClusterCapabilities(is_autopilot=False))
    assert env._active_machine_type == "n2-standard-4"


@pytest.mark.unit
@pytest.mark.parametrize(
    "caps",
    [
        pytest.param(
            _standard_caps(node_auto_provisioning_enabled=True),
            id="node-auto-provisioning",
        ),
        pytest.param(_standard_caps(is_autopilot=True), id="autopilot"),
    ],
)
def test_isa_pin_exempt_when_the_cluster_can_grow_new_shapes(tmp_path, caps):
    """U2.4/U2.5 -- NAP and Autopilot may create a node in the family dynamically."""
    env = make_gke_env(
        tmp_path,
        environment_name=ISA_PINNED_TASK,
        task_machine_types={ISA_PINNED_TASK: "n2-standard-4"},
    )

    env._assert_machine_type_satisfiable(caps)
    assert env._active_machine_type == "n2-standard-4"


@pytest.mark.unit
def test_task_without_a_pin_is_unaffected(tmp_path):
    """The gate must be inert for tasks that pin nothing."""
    env = make_gke_env(tmp_path, environment_name="some-ordinary-task")

    env._assert_machine_type_satisfiable(_standard_caps())


@pytest.mark.unit
def test_validate_placement_conflicts_and_allowed_overrides(tmp_path):
    """Verify placement conflict matrix across pool, ComputeClass, and machine_type."""
    caps = _standard_caps(
        available_machine_types=("e2-standard-4", "n2-standard-16"),
        available_machine_families=("e2", "n2"),
        node_pool_machine_types=(
            ("default-pool", "e2-standard-4"),
            ("workers", "n2-standard-16"),
        ),
    )

    # 1. Autopilot + node_pool -> PlacementConflictError
    env_ap_pool = make_gke_env(
        tmp_path,
        environment_name="bench/task-a",
        node_pool="workers",
        autopilot=True,
    )
    with pytest.raises(PlacementConflictError, match="Autopilot"):
        env_ap_pool._validate_placement(caps, is_autopilot=True)

    # 2. Job-wide node_pool + compute_class -> PlacementConflictError
    env_pool_cc = make_gke_env(
        tmp_path,
        environment_name="bench/task-a",
        node_pool="workers",
        compute_class="harbor-static-cpu",
    )
    with pytest.raises(PlacementConflictError, match="job-wide `node_pool="):
        env_pool_cc._validate_placement(caps)

    # 3. Same task in task_node_pools and task_compute_classes -> PlacementConflictError
    env_both_task = make_gke_env(
        tmp_path,
        environment_name="bench/task-a",
        task_node_pools="task-a:workers",
        task_compute_classes="task-a:harbor-static-cpu",
    )
    with pytest.raises(PlacementConflictError, match="mapped in both"):
        env_both_task._validate_placement(caps)

    # 4. Allowed: job-wide compute_class + task-specific task_node_pools override
    env_cc_with_pool_override = make_gke_env(
        tmp_path,
        environment_name="bench/task-a",
        compute_class="harbor-static-cpu",
        task_node_pools="task-a:workers",
    )
    env_cc_with_pool_override._validate_placement(caps)
    assert env_cc_with_pool_override._active_node_pool == "workers"
    assert env_cc_with_pool_override._active_compute_class is None

    # 5. Machine type + ComputeClass on the same task -> PlacementConflictError
    env_mt_cc = make_gke_env(
        tmp_path,
        environment_name="bench/task-a",
        compute_class="harbor-static-cpu",
        task_machine_types="task-a:n2-standard-4",
    )
    with pytest.raises(PlacementConflictError, match="combines `task_machine_types="):
        env_mt_cc._validate_placement(caps)

    # 6. Machine type + node pool with mismatched family -> PlacementConflictError
    env_mt_bad_pool = make_gke_env(
        tmp_path,
        environment_name="bench/task-a",
        node_pool="default-pool",
        task_machine_types="task-a:n2-standard-4",
    )
    with pytest.raises(PlacementConflictError, match="No node in 'default-pool'"):
        env_mt_bad_pool._validate_placement(caps)

    # 7. Allowed: machine type + node pool with matching family
    env_mt_good_pool = make_gke_env(
        tmp_path,
        environment_name="bench/task-a",
        node_pool="workers",
        task_machine_types="task-a:n2-standard-4",
    )
    env_mt_good_pool._validate_placement(caps)


# ---------------------------------------------------------------------------
# F7: refuse an ephemeral-storage reservation no single node can hold
# ---------------------------------------------------------------------------


def _pod_with_storage(
    app_mb: list[int] | None = None,
    one_shot_init_mb: list[int] | None = None,
    sidecar_init_mb: list[int] | None = None,
) -> k8s_client.V1Pod:
    """Build a Pod whose only interesting property is its storage reservations."""

    def _container(name: str, mb: int, restart: str | None) -> k8s_client.V1Container:
        return k8s_client.V1Container(
            name=name,
            resources=k8s_client.V1ResourceRequirements(
                requests={"ephemeral-storage": f"{mb}Mi"}
            ),
            restart_policy=restart,
        )

    containers = [_container(f"app-{i}", mb, None) for i, mb in enumerate(app_mb or [])]
    inits = [
        _container(f"init-{i}", mb, None) for i, mb in enumerate(one_shot_init_mb or [])
    ] + [
        _container(f"sidecar-{i}", mb, "Always")
        for i, mb in enumerate(sidecar_init_mb or [])
    ]
    return k8s_client.V1Pod(
        spec=k8s_client.V1PodSpec(containers=containers, init_containers=inits)
    )


@pytest.mark.unit
def test_peak_storage_does_not_stack_one_shot_init_containers(tmp_path):
    """U7.1 -- one-shot init containers run in sequence, so they take the max."""
    env = make_gke_env(tmp_path)
    pod = _pod_with_storage(app_mb=[1024], one_shot_init_mb=[8192, 4096, 2048])

    # 1024 (app) + 0 (no sidecars) + 8192 (largest one-shot), NOT 1024 + 14336.
    assert env._peak_ephemeral_storage_request_mb(pod) == 9216


@pytest.mark.unit
def test_peak_storage_stacks_restartable_sidecars(tmp_path):
    """U7.2 -- `restartPolicy: Always` init containers stay alive and do stack."""
    env = make_gke_env(tmp_path)
    pod = _pod_with_storage(
        app_mb=[1024], one_shot_init_mb=[2048], sidecar_init_mb=[4096, 512]
    )

    assert env._peak_ephemeral_storage_request_mb(pod) == 1024 + 4096 + 512 + 2048


@pytest.mark.unit
def test_ephemeral_storage_over_node_ceiling_is_refused(tmp_path):
    """U7.3 -- the silent-pend case the preflight exists to convert into a message."""
    env = make_gke_env(tmp_path, environment_name="orca-bench/701e915cf705cdf6")
    caps = _standard_caps(max_node_allocatable_ephemeral_storage_mb=44_880)
    pod = _pod_with_storage(one_shot_init_mb=[82_838])

    with pytest.raises(EphemeralStorageUnschedulableError) as excinfo:
        env._assert_ephemeral_storage_schedulable(pod, caps)

    message = str(excinfo.value)
    assert "82,838" in message
    assert "44,880" in message
    assert "37,958" in message  # the shortfall, stated explicitly
    assert "task_dind_storage_mb" in message


@pytest.mark.unit
def test_ephemeral_storage_within_ceiling_passes(tmp_path):
    """A reservation the largest node can hold must submit normally."""
    env = make_gke_env(tmp_path)
    caps = _standard_caps(max_node_allocatable_ephemeral_storage_mb=347_694)
    pod = _pod_with_storage(one_shot_init_mb=[82_838])

    env._assert_ephemeral_storage_schedulable(pod, caps)


@pytest.mark.unit
@pytest.mark.parametrize(
    "caps",
    [
        pytest.param(_standard_caps(), id="ceiling-unknown"),
        pytest.param(
            _standard_caps(max_node_allocatable_ephemeral_storage_mb=0),
            id="ceiling-zero",
        ),
        pytest.param(
            _standard_caps(
                max_node_allocatable_ephemeral_storage_mb=44_880,
                node_auto_provisioning_enabled=True,
            ),
            id="node-auto-provisioning",
        ),
        pytest.param(
            _standard_caps(
                max_node_allocatable_ephemeral_storage_mb=44_880, is_autopilot=True
            ),
            id="autopilot",
        ),
    ],
)
def test_ephemeral_storage_gate_stays_out_of_the_way(tmp_path, caps):
    """U7.4 -- a false refusal is worse than a slow pend, so three cases exempt."""
    env = make_gke_env(tmp_path)
    pod = _pod_with_storage(one_shot_init_mb=[82_838])

    env._assert_ephemeral_storage_schedulable(pod, caps)


@pytest.mark.unit
def test_network_policy_enforcement_capabilities_and_fail_closed(tmp_path):
    env = make_gke_env(tmp_path, allow_metadata_server=False)
    unforced_caps = _standard_caps(network_policy_enforced=False)
    env_mod._CLUSTER_CAPABILITIES_CACHE[
        (env.project_id, env.location, env.cluster_name)
    ] = unforced_caps
    assert env.capabilities.disable_internet is False
    assert env.capabilities.network_allowlist is False
    with pytest.raises(
        RuntimeError, match="active Kubernetes NetworkPolicy enforcement is required"
    ):
        env._verify_network_enforcement(unforced_caps)
    # Opting into allow_metadata_server=True in PUBLIC mode succeeds without enforcement
    env_opt_out = make_gke_env(tmp_path, allow_metadata_server=True)
    env_opt_out._verify_network_enforcement(unforced_caps)
