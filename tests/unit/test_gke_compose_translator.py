"""Unit tests for src/harbor/environments/gke/compose_translator.py."""

import logging
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from harbor.constants import MAIN_SERVICE_NAME
from harbor.environments.base import ExecResult
from harbor_gke_ext.cluster_probe import (
    ClusterCapabilities,
    DindAvailability,
)
from harbor_gke_ext.compose_translator import (
    _GKENativeComposeServiceTransport,
    discover_compose_build_services,
    resolve_compose_infra_env,
    translate_compose,
)
from harbor_gke_ext.image_ref import ImageResolver


def _make_mock_env(tmp_path: Path) -> MagicMock:
    env_dir = tmp_path / "environment"
    env_dir.mkdir(exist_ok=True)
    env = MagicMock()
    env.environment_name = "test-env"
    env.environment_dir = env_dir
    env.task_dir = tmp_path
    env._effective_cpus = 4
    env._effective_memory_mb = 8192
    env._mounts = []
    env.task_env_config.docker_image = (
        "us-central1-docker.pkg.dev/proj/repo/main:latest"
    )
    env.task_env_config.env = {"USER_KEY": "user_val"}
    env._persistent_env = {"PERSISTENT_KEY": "persist_val"}
    env.logger = MagicMock()
    return env


# ============================================================================
# 1. resolve_compose_infra_env (CONTEXT_DIR & Non-Empty CPU/MEMORY Defaults)
# ============================================================================
@pytest.mark.unit
def test_resolve_compose_infra_env_context_dir_and_defaults(tmp_path):
    env = _make_mock_env(tmp_path)
    res = resolve_compose_infra_env(env, use_prebuilt=True)

    # Fix for ml-dev-bench: CONTEXT_DIR must be absolute host path to environment_dir
    assert res["CONTEXT_DIR"] == str(env.environment_dir.resolve().absolute())
    assert res["CPUS"] == "4"
    assert res["MEMORY"] == "8192M"
    assert res["ENV_ARTIFACTS_PATH"] == "/logs/artifacts"
    assert res["HOST_ARTIFACTS_PATH"] == "/logs/artifacts"
    assert res["ENV_VERIFIER_LOGS_PATH"] == "/logs/verifier"
    assert res["USER_KEY"] == "user_val"
    assert res["PERSISTENT_KEY"] == "persist_val"


@pytest.mark.unit
def test_resolve_compose_infra_env_unconfigured_cpus_memory_fallbacks(tmp_path):
    env = _make_mock_env(tmp_path)
    env._effective_cpus = None
    env._effective_memory_mb = None
    res = resolve_compose_infra_env(env, use_prebuilt=True)

    # Must provide non-empty fallback strings so compose-go never fails with strconv.ParseFloat
    assert res["CPUS"] == "1"
    assert res["MEMORY"] == "2048M"


# ============================================================================
# 2. Discovery & Resource Quota Math
# ============================================================================
@pytest.mark.unit
def test_discover_compose_build_services(tmp_path):
    (tmp_path / "sidecar_ctx").mkdir()
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    build: .
  worker:
    build:
      context: ./sidecar_ctx
      dockerfile: Dockerfile.worker
  redis:
    image: redis:7
"""
    )
    builds = discover_compose_build_services(compose_file)
    assert "main" not in builds
    assert "redis" not in builds
    assert "worker" in builds
    ctx_dir, dockerfile = builds["worker"]
    assert ctx_dir == (tmp_path / "sidecar_ctx").resolve()
    assert dockerfile == "Dockerfile.worker"


# ============================================================================
# 3. Shape A Translation: Native Sidecars, Probes, Resources, Autopilot tmpfs
# ============================================================================
@pytest.mark.unit
def test_translate_compose_shape_a_native_sidecars_and_probes(tmp_path):
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
    depends_on:
      db:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
  db:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: secret
    deploy:
      resources:
        limits:
          cpus: '1.5'
          memory: 1024M
        reservations:
          cpus: '0.5'
          memory: 512M
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 5
  migrate:
    image: Flyway/flyway:latest
    command: ["migrate"]
    restart: "no"
"""
    )

    resolver = ImageResolver(
        project_id="proj",
        registry_name="repo",
        registry_location="us-central1",
    )
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest",
            "CPUS": "4",
            "MEMORY": "8192M",
        },
        pod_name="trial-pod-1",
        namespace="default",
        labels={"app": "harbor"},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="4",
        cpu_limit="4",
        memory_request="8Gi",
        memory_limit="8Gi",
        is_autopilot=True,
        image_resolver=resolver,
        task_dir=tmp_path,
    )

    # Invariant 6: All pod images must be resolved through ImageResolver
    resolver.assert_pod_images_resolved(pod)

    # Check main container
    assert len(pod.spec.containers) == 1
    main_c = pod.spec.containers[0]
    assert main_c.name == MAIN_SERVICE_NAME
    assert main_c.image == "us-central1-docker.pkg.dev/proj/repo/main:latest"

    # Check initContainers: migrate (one-shot) and db (native sidecar with restartPolicy="Always")
    init_by_name = {c.name: c for c in (pod.spec.init_containers or [])}
    assert "db" in init_by_name
    assert "migrate" in init_by_name

    db_c = init_by_name["db"]
    assert db_c.restart_policy == "Always"
    assert db_c.startup_probe is not None
    assert db_c.startup_probe._exec.command == [
        "/bin/sh",
        "-c",
        "pg_isready -U postgres",
    ]
    assert db_c.startup_probe.period_seconds == 5
    # Explicit compose deploy.resources honored
    assert db_c.resources.limits["cpu"] == "1500m"
    assert db_c.resources.limits["memory"] == "1024Mi"
    assert db_c.resources.requests["cpu"] == "500m"
    assert db_c.resources.requests["memory"] == "512Mi"

    migrate_c = init_by_name["migrate"]
    assert migrate_c.restart_policy is None


@pytest.mark.unit
def test_translate_compose_autopilot_tmpfs_uses_disk_backed_emptydir(tmp_path):
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
  cache:
    image: redis:7
    tmpfs:
      - /tmp/cache:size=128m
"""
    )
    resolver = ImageResolver()

    pod_autopilot = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="autopilot-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        is_autopilot=True,
        image_resolver=resolver,
        task_dir=tmp_path,
    )
    tmpfs_vols_ap = [
        v for v in (pod_autopilot.spec.volumes or []) if v.name.startswith("tmpfs-")
    ]
    assert len(tmpfs_vols_ap) == 1
    # Autopilot rejects medium: Memory; must use medium: ""
    assert tmpfs_vols_ap[0].empty_dir.medium == ""
    assert tmpfs_vols_ap[0].empty_dir.size_limit == "128Mi"

    pod_standard = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="standard-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        is_autopilot=False,
        image_resolver=resolver,
        task_dir=tmp_path,
    )
    tmpfs_vols_std = [
        v for v in (pod_standard.spec.volumes or []) if v.name.startswith("tmpfs-")
    ]
    assert len(tmpfs_vols_std) == 1
    assert tmpfs_vols_std[0].empty_dir.medium == "Memory"


# ============================================================================
# 4. ml-dev-bench Relative Bind Mounts + Anonymous Volumes + harbor-seed
# ============================================================================
@pytest.mark.unit
def test_translate_compose_ml_dev_bench_relative_binds_and_anonymous_volumes(tmp_path):
    # Create task directory layout matching ml-dev-bench
    env_dir = tmp_path / "environment"
    env_dir.mkdir()
    (tmp_path / "app.py").write_text("print('hello workspace')")
    solution_dir = tmp_path / "solution"
    solution_dir.mkdir()
    (solution_dir / "secret.txt").write_text("DO NOT LEAK")

    compose_file = env_dir / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    volumes:
      - ${CONTEXT_DIR}/..:/opt/task-src:ro
      - /opt/task-src/solution
      - /opt/task-src/tests
"""
    )

    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest",
            "CONTEXT_DIR": str(env_dir.resolve()),
        },
        pod_name="mldev-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        is_autopilot=True,
        image_resolver=resolver,
        task_dir=tmp_path,
    )

    resolver.assert_pod_images_resolved(pod)

    # Check harbor-seed init container exists
    init_names = [c.name for c in (pod.spec.init_containers or [])]
    assert "harbor-seed" in init_names

    # Check main container mounts: /opt/task-src from compose-binds-shared + anonymous emptyDir overlays
    main_c = pod.spec.containers[0]
    mounts_by_path = {m.mount_path: m for m in (main_c.volume_mounts or [])}
    assert "/opt/task-src" in mounts_by_path
    assert mounts_by_path["/opt/task-src"].read_only is True
    assert "/opt/task-src/solution" in mounts_by_path
    assert "/opt/task-src/tests" in mounts_by_path
    # Anonymous volumes must be mounted read-write over the read-only bind
    assert not mounts_by_path["/opt/task-src/solution"].read_only


# ============================================================================
# 5. Decision Q1: kv-live-surgery (depends_on: main) Gate & Warning
# ============================================================================
@pytest.mark.unit
def test_translate_compose_kv_live_surgery_depends_on_main_gate(tmp_path):
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
  traffic-gen:
    image: alpine:3.20
    command: ["sh", "-c", "echo generating traffic"]
    depends_on:
      - main
"""
    )

    logger = MagicMock()
    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="kv-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        is_autopilot=True,
        image_resolver=resolver,
        task_dir=tmp_path,
        logger=logger,
    )

    resolver.assert_pod_images_resolved(pod)

    # Loud warning must have been logged per Decision Q1
    warning_calls = [str(call) for call in logger.warning.call_args_list]
    assert any("depends_on 'main'" in msg for msg in warning_calls)

    # Check annotation
    assert (
        pod.metadata.annotations.get("harbor.dev/post-main-sidecars") == "traffic-gen"
    )

    # traffic-gen must be in spec.containers (not initContainers) wrapped with gate wait
    containers_by_name = {c.name: c for c in pod.spec.containers}
    assert "main" in containers_by_name
    assert "traffic-gen" in containers_by_name

    tg = containers_by_name["traffic-gen"]
    tg_cmd_str = " ".join((tg.command or []) + (tg.args or []))
    assert "/harbor/gates/main-ready" in tg_cmd_str
    assert "echo generating traffic" in tg_cmd_str
    assert any(m.mount_path == "/harbor/gates" for m in (tg.volume_mounts or []))


@pytest.mark.unit
def test_translate_compose_post_main_sidecar_uses_oci_cmd_when_compose_omits_command(
    tmp_path, monkeypatch
):
    """Post-main sidecars without Compose command/entrypoint must use image OCI Entrypoint/Cmd."""
    import harbor_gke_ext.compose_translator as ct

    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
  loadgen:
    image: harborframework/terminal-bench:kv-live-surgery-sidecar-loadgen
    depends_on:
      main:
        condition: service_healthy
  worker:
    image: harborframework/terminal-bench:custom-worker
    command: ["--flag", "1"]
    depends_on:
      - main
"""
    )

    def _fake_resolve_oci_image_config(image_ref: str) -> dict:
        if "kv-live-surgery-sidecar-loadgen" in image_ref:
            return {"Entrypoint": None, "Cmd": ["/app/loadgen"]}
        if "custom-worker" in image_ref:
            return {"Entrypoint": ["/usr/local/bin/worker-ep"], "Cmd": ["--default"]}
        return {}

    monkeypatch.setattr(ct, "_resolve_oci_image_config", _fake_resolve_oci_image_config)

    logger = MagicMock()
    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="kv-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        is_autopilot=False,
        image_resolver=resolver,
        task_dir=tmp_path,
        logger=logger,
    )

    containers_by_name = {c.name: c for c in pod.spec.containers}
    loadgen = containers_by_name["loadgen"]
    assert loadgen.command == [
        "sh",
        "-c",
        'until [ -f /harbor/gates/main-ready ]; do sleep 0.2; done; exec "$@"',
        "--",
        "/app/loadgen",
    ]
    assert loadgen.args is None

    worker = containers_by_name["worker"]
    assert worker.command == [
        "sh",
        "-c",
        'until [ -f /harbor/gates/main-ready ]; do sleep 0.2; done; exec "$@"',
        "--",
        "/usr/local/bin/worker-ep",
        "--flag",
        "1",
    ]
    assert worker.args is None


# ============================================================================
# 6. Shape B Translation: Hybrid DinD Sidecar (No TCP, No --dns=8.8.8.8)
# ============================================================================
@pytest.mark.unit
def test_translate_compose_shape_b_dind_invariants_and_shell_validity(tmp_path):
    """Verify Shape B invariants, direct-pipe materialization, and POSIX sh -n syntax."""
    import shutil
    import subprocess

    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
  docker-proxy:
    image: tecnativa/docker-socket-proxy:latest
    privileged: true
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
"""
    )

    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="dind-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="4",
        cpu_limit="4",
        memory_request="8Gi",
        memory_limit="8Gi",
        is_autopilot=False,
        compose_placement="auto",
        cluster_capabilities=ClusterCapabilities(
            is_autopilot=False,
            gke_version="1.35.1-gke.100",
            dind_availability=DindAvailability.DIND_AVAILABLE,
        ),
        image_resolver=resolver,
        task_dir=tmp_path,
    )

    resolver.assert_pod_images_resolved(pod)
    assert pod.metadata.annotations.get("harbor.dev/compose-placement") == "dind"

    # Invariant 1 & 5: main is native; dind/containerd sockets never mounted into main
    assert len(pod.spec.containers) == 1
    main_c = pod.spec.containers[0]
    assert main_c.name == MAIN_SERVICE_NAME
    main_mounts = [m.mount_path for m in (main_c.volume_mounts or [])]
    for forbidden in (
        "/var/run/harbor-dind",
        "/var/run/docker.sock",
        "/run/containerd/containerd.sock",
        "/harbor/dind-images",
    ):
        assert forbidden not in main_mounts

    init_names = [c.name for c in (pod.spec.init_containers or [])]
    init_by_name = {c.name: c for c in (pod.spec.init_containers or [])}
    assert init_names.index("dind-engine") < init_names.index("dind-cache-docker-proxy")
    assert init_names.index("dind-cache-docker-proxy") < init_names.index(
        "compose-up-gate"
    )

    # Host containerd socket is never mounted anywhere on the Pod
    pod_vol_names = {v.name for v in (pod.spec.volumes or [])}
    assert "harbor-host-containerd-sock" not in pod_vol_names

    cache_cmd = " ".join(init_by_name["dind-cache-docker-proxy"].command or [])
    gate_cmd = " ".join(init_by_name["compose-up-gate"].command or [])
    dind_cmd = " ".join(init_by_name["dind-engine"].command or [])

    assert "2>/dev/null || true" not in cache_cmd
    assert "tar -cf -" in cache_cmd
    assert 'import "$@" - "$IMG"' in cache_cmd
    assert "WORKDIR ${WORKDIR}" in cache_cmd
    assert "ENV ${K}=" in cache_cmd
    assert "--pull never" in gate_cmd
    assert gate_cmd.index("load_image docker-proxy") < gate_cmd.index("docker compose")
    assert "--host=unix:///var/run/harbor-dind/docker.sock" in dind_cmd
    assert "tcp://" not in dind_cmd
    assert "--dns" not in dind_cmd
    assert "/harbor/dind-images/.auth-scrubbed" in dind_cmd
    assert "rm -rf /harbor/dind-images/.docker" in gate_cmd

    sh = shutil.which("sh")
    if sh is not None:
        for c in list(pod.spec.init_containers or []) + list(pod.spec.containers or []):
            if c.command and c.command[0] == "sh" and "-c" in c.command:
                result = subprocess.run(
                    [sh, "-n"], input=c.command[-1], text=True, capture_output=True
                )
                assert result.returncode == 0, (
                    f"generated shell for '{c.name}' is invalid: {result.stderr}"
                )


# ============================================================================
# 7. _GKENativeComposeServiceTransport Routing
# ============================================================================
@pytest.mark.unit
@pytest.mark.asyncio
async def test_native_compose_service_transport_exec_and_stop():
    mock_env = MagicMock()
    mock_env._pod_name = "test-pod"
    mock_env.namespace = "default"
    mock_env.exec = AsyncMock(
        return_value=ExecResult(stdout="ok", stderr="", return_code=0)
    )

    transport = _GKENativeComposeServiceTransport(mock_env)
    res = await transport.service_exec("pg_isready", service="db")
    assert res.return_code == 0
    mock_env.exec.assert_awaited_once_with(
        "pg_isready",
        cwd=None,
        env=None,
        timeout_sec=None,
        user=None,
        container="db",
    )

    await transport.stop_service("db")
    assert mock_env.exec.await_count == 2


# ============================================================================
# 8. Native GPU Sidecar Allocation
# ============================================================================
@pytest.mark.unit
def test_translate_compose_native_gpu_sidecar(tmp_path):
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
  model:
    image: nvidia/cuda:12.1.0-base-ubuntu22.04
    command: ["python3", "-m", "http.server", "8080"]
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
"""
    )

    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="gpu-sidecar-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        effective_gpus=1,
        gpu_types=["L4"],
        is_autopilot=False,
        image_resolver=resolver,
        task_dir=tmp_path,
    )

    resolver.assert_pod_images_resolved(pod)

    # Pod-level selector & toleration for L4
    assert pod.spec.node_selector.get("cloud.google.com/gke-accelerator") == "nvidia-l4"
    assert any(t.key == "nvidia.com/gpu" for t in (pod.spec.tolerations or []))

    # model sidecar in initContainers receives nvidia.com/gpu = 1
    init_by_name = {c.name: c for c in (pod.spec.init_containers or [])}
    assert "model" in init_by_name
    model_c = init_by_name["model"]
    assert model_c.restart_policy == "Always"
    assert model_c.resources.limits.get("nvidia.com/gpu") == "1"
    assert model_c.resources.requests.get("nvidia.com/gpu") == "1"

    # main container does NOT receive nvidia.com/gpu because task's 1 GPU belongs to model
    main_c = pod.spec.containers[0]
    assert main_c.name == MAIN_SERVICE_NAME
    assert "nvidia.com/gpu" not in (main_c.resources.limits or {})
    assert "nvidia.com/gpu" not in (main_c.resources.requests or {})


def test_translate_compose_shape_b_host_aliases_and_etc_hosts_sync(
    tmp_path: Path,
) -> None:
    """When sidecars collide on port 8080 (Shape B), they must NOT map to 127.0.0.1 in hostAliases and must sync bridge IPs to /etc/hosts."""
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  model:
    image: python:3.12-slim
    expose:
      - "8080"
  mini-service:
    image: python:3.12-slim
    expose:
      - "8080"
  main:
    image: ${MAIN_IMAGE_NAME}
    command: ["sleep", "infinity"]
    depends_on:
      - model
      - mini-service
"""
    )

    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="collision-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        is_autopilot=False,
        image_resolver=resolver,
        task_dir=tmp_path,
    )

    # Verify hostAliases maps 'main' -> 127.0.0.1 and DinD sidecars -> deterministic 172.30.240.x IPs
    ha_map = {ha.ip: set(ha.hostnames or []) for ha in (pod.spec.host_aliases or [])}
    assert "main" in ha_map.get("127.0.0.1", set())
    assert "model" not in ha_map.get("127.0.0.1", set())
    assert "mini-service" not in ha_map.get("127.0.0.1", set())
    assert "mini-service" in ha_map.get("172.30.240.2", set())
    assert "model" in ha_map.get("172.30.240.3", set())


def test_translate_compose_shape_b_gvisor_sandboxed_dind(tmp_path):
    """When runtime_class_name='gvisor', dind-engine uses capabilities (no privileged=True) and disables iptables."""
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  proxy:
    image: alpine:3.20
    privileged: true
  main:
    image: ${MAIN_IMAGE_NAME}
    command: ["sleep", "infinity"]
    depends_on:
      - proxy
"""
    )

    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="gvisor-dind-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        is_autopilot=False,
        runtime_class_name="gvisor",
        image_resolver=resolver,
        task_dir=tmp_path,
    )

    assert pod.spec.runtime_class_name == "gvisor"
    dind_engine = next(
        c for c in (pod.spec.init_containers or []) if c.name == "dind-engine"
    )
    assert dind_engine.security_context.privileged is None
    assert dind_engine.security_context.capabilities.add == [
        "SYS_ADMIN",
        "NET_ADMIN",
        "MKNOD",
    ]
    cmd_str = " ".join(dind_engine.command or [])
    assert "--iptables=false --ip6tables=false" in cmd_str


@pytest.mark.unit
def test_compose_pod_automount_service_account_token_false_and_storage_options(
    tmp_path: Path,
) -> None:
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
    depends_on:
      helper:
        condition: service_completed_successfully
    volumes:
      - shared-data:/data
  helper:
    image: us-central1-docker.pkg.dev/proj/repo/helper:latest
    restart: "no"
    command: ["echo", "init"]
    healthcheck:
      test: ["CMD", "true"]
volumes:
  shared-data: {}
"""
    )

    resolver = ImageResolver()
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="secure-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        ephemeral_storage_request="8Gi",
        scratch_volume_size="50Gi",
        is_autopilot=False,
        image_resolver=resolver,
        task_dir=tmp_path,
    )

    # P0-2: automount_service_account_token is explicitly False
    assert pod.spec.automount_service_account_token is False

    # P1-3: main container has ephemeral-storage request set
    main_c = next(c for c in pod.spec.containers if c.name == MAIN_SERVICE_NAME)
    assert main_c.resources.requests["ephemeral-storage"] == "8Gi"

    # P1-3: named volume uses generic ephemeral volumeClaimTemplate when scratch_volume_size is set
    shared_vol = next(v for v in (pod.spec.volumes or []) if v.name == "shared-data")
    assert shared_vol.ephemeral is not None
    assert (
        shared_vol.ephemeral.volume_claim_template.spec.resources.requests["storage"]
        == "50Gi"
    )
    assert shared_vol.empty_dir is None

    # P1-8: one-shot init container ('helper' with restart=no) drops readinessProbe
    helper_init = next(
        c for c in (pod.spec.init_containers or []) if c.name == "helper"
    )
    assert helper_init.restart_policy is None
    assert helper_init.readiness_probe is None


# ============================================================================
# 10. D5: harbor-seed E2BIG guard (_LINUX_MAX_ARG_STRLEN / _SEED_INLINE_MAX_B64_BYTES)
# ============================================================================
@pytest.mark.unit
def test_harbor_seed_incompressible_payload_switches_to_streaming(tmp_path):
    """Incompressible bind payload <512 KiB raw but >64 KiB base64 must use streaming seed."""
    import os

    from harbor_gke_ext.compose_translator import _LINUX_MAX_ARG_STRLEN

    data_dir = tmp_path / "seed_data"
    data_dir.mkdir()
    # 120 KiB of random incompressible bytes (< 512 KiB raw gate, > 64 KiB b64 gate)
    (data_dir / "random.bin").write_bytes(os.urandom(120 * 1024))

    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        f"""
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    volumes:
      - {data_dir}:/workspace/seed_data
"""
    )

    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="seed-large-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        is_autopilot=False,
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
    )
    seed_init = next(c for c in pod.spec.init_containers if c.name == "harbor-seed")
    assert len(seed_init.command[2].encode("utf-8")) < _LINUX_MAX_ARG_STRLEN
    assert ".seed-ready" in seed_init.command[2]


@pytest.mark.unit
def test_harbor_seed_small_payload_remains_inline(tmp_path):
    """Small bind payload stays inline inside harbor-seed."""
    data_dir = tmp_path / "seed_small"
    data_dir.mkdir()
    (data_dir / "tiny.txt").write_text("hello world\n")

    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        f"""
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    volumes:
      - {data_dir}:/workspace/seed_small
"""
    )

    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="seed-small-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="2",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="4Gi",
        is_autopilot=False,
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
    )
    seed_init = next(c for c in pod.spec.init_containers if c.name == "harbor-seed")
    assert "base64 -d | tar -xzf -" in seed_init.command[2]
    assert ".seed-ready" not in seed_init.command[2]


# ============================================================================
# 11. D2: DinD tmpfs normalization & no duplicate target in inner compose YAML
# ============================================================================
@pytest.mark.unit
def test_normalize_dind_tmpfs_entries_invariants():
    from harbor_gke_ext.compose_translator import _normalize_dind_tmpfs_entries

    # Duplicate target against occupied mounts is dropped; unbounded entry gains size=;
    # explicit size is capped at max_size_mb; non-size mount opts are preserved.
    res = _normalize_dind_tmpfs_entries(
        [
            "/already/mounted",
            "/tmp",
            "/run:noexec,nosuid,size=512m",
            "/tmp",  # duplicate within tmpfs_list itself
        ],
        occupied={"/already/mounted"},
        max_size_mb=128,
    )
    assert res == [
        "/tmp:size=64m",
        "/run:noexec,nosuid,size=128m",
    ]

    # When all targets are occupied, returns empty list so caller pops the key.
    assert (
        _normalize_dind_tmpfs_entries(
            ["/tmp"],
            occupied={"/tmp"},
            max_size_mb=256,
        )
        == []
    )


@pytest.mark.unit
def test_translate_compose_dind_tmpfs_not_double_mounted_in_inner_compose(tmp_path):
    """End-to-end: decode inner compose YAML from compose-up-gate and verify no duplicate /tmp mount."""
    import base64
    import re

    import yaml

    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
  worker:
    image: alpine:3.20
    privileged: true
    tmpfs:
      - /tmp
      - /run:noexec,size=512m
"""
    )

    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="dind-tmpfs-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        cpu_request="4",
        cpu_limit="4",
        memory_request="8Gi",
        memory_limit="8Gi",
        is_autopilot=False,
        compose_placement="auto",
        cluster_capabilities=ClusterCapabilities(
            is_autopilot=False,
            gke_version="1.35.1-gke.100",
            dind_availability=DindAvailability.DIND_AVAILABLE,
        ),
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
    )

    # No orphan tmpfs-worker-* emptyDir volume should exist on the Pod
    pod_vol_names = {v.name for v in (pod.spec.volumes or [])}
    assert not any(v.startswith("tmpfs-worker") for v in pod_vol_names)

    gate = next(c for c in pod.spec.init_containers if c.name == "compose-up-gate")
    gate_script = gate.command[2]
    m = re.search(
        r"echo\s+['\"]?([A-Za-z0-9+/=]+)['\"]?\s*\|\s*base64\s+-d\s*>\s*/harbor/dind-compose\.yaml",
        gate_script,
    )
    assert m is not None, (
        f"Could not find base64 compose payload in gate script: {gate_script}"
    )
    inner_doc = yaml.safe_load(base64.b64decode(m.group(1)).decode("utf-8"))
    worker_spec = inner_doc["services"]["worker"]

    # /tmp and /run must appear in worker_spec["tmpfs"] with explicit size=,
    # and must NOT appear as bind mounts in worker_spec.get("volumes").
    assert "tmpfs" in worker_spec
    assert any(t.startswith("/tmp:size=") for t in worker_spec["tmpfs"])
    assert any(t.startswith("/run:") and "size=" in t for t in worker_spec["tmpfs"])

    worker_vol_targets = [
        str(v).split(":")[1]
        for v in (worker_spec.get("volumes") or [])
        if ":" in str(v)
    ]
    assert "/tmp" not in worker_vol_targets
    assert "/run" not in worker_vol_targets


# ============================================================================
# 12. Audit P1-2: _build_security_context PSS Baseline Capability Allowlist
# ============================================================================
@pytest.mark.unit
def test_build_security_context_pss_baseline_allowlist():
    from harbor_gke_ext.compose_translator import _build_security_context

    # Baseline-only capabilities (including CAP_ prefix) do NOT trigger gVisor
    sec_base, needs_gvisor_base = _build_security_context(
        {"cap_add": ["CAP_SETUID", "SETGID", "KILL", "NET_RAW"]},
        sname="main",
    )
    assert needs_gvisor_base is False
    assert sec_base is not None
    assert sec_base.capabilities.add == ["SETUID", "SETGID", "KILL", "NET_RAW"]

    # Any above-Baseline capability (e.g. SYS_MODULE / SYS_RAWIO / SYS_PTRACE) triggers gVisor
    sec_above, needs_gvisor_above = _build_security_context(
        {"cap_add": ["SETUID", "CAP_SYS_MODULE"], "cap_drop": ["CAP_MKNOD"]},
        sname="sidecar",
    )
    assert needs_gvisor_above is True
    assert sec_above is not None
    assert sec_above.capabilities.add == ["SETUID", "SYS_MODULE"]
    assert sec_above.capabilities.drop == ["MKNOD"]


# ============================================================================
# 13. Defect D1: OCI Image Config Resolution and docker import --change Flags
# ============================================================================
@pytest.mark.unit
def test_format_oci_config_changes_and_registry_ref_parsing():
    from harbor_gke_ext.compose_translator import (
        _format_oci_config_changes,
        _parse_registry_image_ref,
    )

    assert _parse_registry_image_ref("python:3.12-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.12-slim",
    )
    assert _parse_registry_image_ref("us-central1-docker.pkg.dev/proj/repo/app:v1") == (
        "us-central1-docker.pkg.dev",
        "proj/repo/app",
        "v1",
    )
    assert _parse_registry_image_ref(
        "public.ecr.aws/docker/library/python:3.13@sha256:abcdef"
    ) == ("public.ecr.aws", "docker/library/python", "sha256:abcdef")

    oci_cfg = {
        "Entrypoint": ["/docker-entrypoint.sh"],
        "Cmd": ["postgres", "-c", "max_connections=200"],
        "ExposedPorts": {"5432/tcp": {}, "8080/tcp": {}},
        "Labels": {"org.opencontainers.image.title": "postgres"},
        "StopSignal": "SIGINT",
        "Healthcheck": {
            "Test": ["CMD-SHELL", "pg_isready -U postgres"],
            "Interval": 10000000000,
            "Timeout": 5000000000,
            "Retries": 5,
        },
    }
    changes = _format_oci_config_changes(oci_cfg)
    assert 'ENTRYPOINT ["/docker-entrypoint.sh"]' in changes
    assert 'CMD ["postgres", "-c", "max_connections=200"]' in changes
    assert "EXPOSE 5432/tcp" in changes
    assert "EXPOSE 8080/tcp" in changes
    assert 'LABEL "org.opencontainers.image.title"="postgres"' in changes
    assert "STOPSIGNAL SIGINT" in changes
    assert (
        "HEALTHCHECK --interval=10000000000ns --timeout=5000000000ns --retries=5 CMD pg_isready -U postgres"
        in changes
    )


@pytest.mark.unit
def test_translate_compose_dind_cache_emits_oci_config_changes(tmp_path: Path):
    from harbor_gke_ext.compose_translator import _OCI_CONFIG_CACHE

    sidecar_img = "us-central1-docker.pkg.dev/proj/repo/custom-db:v1"
    _OCI_CONFIG_CACHE[sidecar_img] = {
        "Entrypoint": ["/usr/local/bin/db-entry.sh"],
        "Cmd": ["run-server", "--port", "9000"],
        "ExposedPorts": {"9000/tcp": {}},
        "StopSignal": "SIGTERM",
    }
    try:
        compose_file = tmp_path / "docker-compose.yaml"
        compose_file.write_text(
            f"""
services:
  main:
    image: python:3.12-slim
    command: ["sleep", "infinity"]
  db:
    image: {sidecar_img}
    privileged: true
"""
        )
        pod = translate_compose(
            compose_paths=[compose_file],
            compose_env={
                "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
            },
            pod_name="d1-oci-pod",
            namespace="default",
            labels={},
            main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
            is_autopilot=False,
            image_resolver=ImageResolver(),
            task_dir=tmp_path,
        )
        cache_c = next(
            c for c in (pod.spec.init_containers or []) if c.name == "dind-cache-db"
        )
        script = cache_c.command[2]
        assert 'ENTRYPOINT ["/usr/local/bin/db-entry.sh"]' in script
        assert 'CMD ["run-server", "--port", "9000"]' in script
        assert "EXPOSE 9000/tcp" in script
        assert "STOPSIGNAL SIGTERM" in script
    finally:
        _OCI_CONFIG_CACHE.pop(sidecar_img, None)


@contextmanager
def _seeded_image_sizes(sizes: dict[str, int]):
    """Seed the OCI caches so size lookups resolve without touching a registry.

    ``_resolve_oci_manifest_facts`` short-circuits on ``_OCI_CONFIG_CACHE``, so
    both caches have to be populated for the size cache to be consulted.
    """
    from harbor_gke_ext.compose_translator import (
        _OCI_COMPRESSED_SIZE_CACHE,
        _OCI_CONFIG_CACHE,
    )

    for ref, size in sizes.items():
        _OCI_CONFIG_CACHE[ref] = {}
        _OCI_COMPRESSED_SIZE_CACHE[ref] = size
    try:
        yield
    finally:
        for ref in sizes:
            _OCI_CONFIG_CACHE.pop(ref, None)
            _OCI_COMPRESSED_SIZE_CACHE.pop(ref, None)


@pytest.mark.unit
def test_compute_dind_storage_adds_image_estimate_to_task_budget():
    """The image estimate and the task's declared budget add, they do not overlap."""
    from harbor_gke_ext.compose_translator import (
        DIND_IMAGE_EXPANSION_RATIO,
        compute_dind_storage_mb,
    )

    # 8 GiB compressed x 3.0 = 24576 MiB, plus a 4096 MiB declared budget.
    compressed_mb = 8 * 1024
    with _seeded_image_sizes({"reg/big:v1": compressed_mb * 1024 * 1024}):
        result = compute_dind_storage_mb(
            {"db": "reg/big:v1"}, task_storage_budget_mb=4096
        )

    expected = int(DIND_IMAGE_EXPANSION_RATIO * compressed_mb) + 4096
    assert result == expected
    # max() would have discarded the task's own 4 GiB of scratch.
    assert result > int(DIND_IMAGE_EXPANSION_RATIO * compressed_mb)


@pytest.mark.unit
def test_compute_dind_storage_applies_floor_for_small_images():
    """A tiny image must not produce a reservation below the floor."""
    from harbor_gke_ext.compose_translator import (
        DIND_STORAGE_FLOOR_MB,
        compute_dind_storage_mb,
    )

    with _seeded_image_sizes({"reg/tiny:v1": 30 * 1024 * 1024}):
        result = compute_dind_storage_mb(
            {"db": "reg/tiny:v1"}, task_storage_budget_mb=64
        )

    assert result == DIND_STORAGE_FLOOR_MB


@pytest.mark.unit
def test_compute_dind_storage_sums_every_dind_image():
    """Each DinD service materialises its own image into the same /var/lib/docker."""
    from harbor_gke_ext.compose_translator import (
        DIND_IMAGE_EXPANSION_RATIO,
        compute_dind_storage_mb,
    )

    sizes = {
        "reg/a:v1": 5 * 1024 * 1024 * 1024,
        "reg/b:v1": 3 * 1024 * 1024 * 1024,
    }
    with _seeded_image_sizes(sizes):
        result = compute_dind_storage_mb(
            {"svc-a": "reg/a:v1", "svc-b": "reg/b:v1"}, task_storage_budget_mb=2048
        )

    expected = int(DIND_IMAGE_EXPANSION_RATIO * 8 * 1024) + 2048
    assert result == expected


@pytest.mark.unit
def test_compute_dind_storage_warns_when_image_is_built_in_pod(caplog):
    """A `build:`-only service has no manifest, so the shortfall must be announced."""
    from harbor_gke_ext.compose_translator import (
        DIND_STORAGE_FLOOR_MB,
        compute_dind_storage_mb,
    )

    with caplog.at_level(logging.WARNING):
        result = compute_dind_storage_mb(
            {"built-here": None}, task_storage_budget_mb=4096
        )

    assert result == DIND_STORAGE_FLOOR_MB
    assert "built-here" in caplog.text
    assert "dind_storage_mb" in caplog.text


@pytest.mark.unit
def test_compute_dind_storage_counts_measurable_images_despite_one_unknown():
    """One unmeasurable service must not discard the sizes we do know."""
    from harbor_gke_ext.compose_translator import (
        DIND_IMAGE_EXPANSION_RATIO,
        compute_dind_storage_mb,
    )

    with _seeded_image_sizes({"reg/known:v1": 20 * 1024 * 1024 * 1024}):
        result = compute_dind_storage_mb(
            {"known": "reg/known:v1", "built-here": None},
            task_storage_budget_mb=1024,
        )

    expected = int(DIND_IMAGE_EXPANSION_RATIO * 20 * 1024) + 1024
    assert result == expected


# ============================================================================
# Pod-level resources (KEP-2837, spec.resources)
#
# The three API-server rules asserted here were measured with
# `kubectl apply --dry-run=server` against GKE 1.35.6, not inferred:
#   1. spec.resources.requests[X] >= aggregate container requests
#   2. no single container limit > the matching pod limit
#   3. only cpu / memory / hugepages-* are accepted at Pod level
# ============================================================================
def _container(
    name, *, cpu_req=None, mem_req=None, cpu_lim=None, mem_lim=None, sidecar=False
):
    from kubernetes import client as k8s_client

    reqs = {}
    lims = {}
    if cpu_req:
        reqs["cpu"] = cpu_req
    if mem_req:
        reqs["memory"] = mem_req
    if cpu_lim:
        lims["cpu"] = cpu_lim
    if mem_lim:
        lims["memory"] = mem_lim
    return k8s_client.V1Container(
        name=name,
        image="busybox:1.36",
        resources=k8s_client.V1ResourceRequirements(
            requests=reqs or None, limits=lims or None
        ),
        restart_policy="Always" if sidecar else None,
    )


@pytest.mark.unit
def test_aggregate_pod_resource_sums_sidecars_and_maxes_init_containers():
    """Mirrors the Kubernetes effective-pod-request rule."""
    from harbor_gke_ext.compose_translator import aggregate_pod_resource

    # Native sidecar declared first, so it is already running when the ordinary
    # init container executes and both are charged together: 200 + 250 = 450.
    # The app phase charges 200 + 100 = 300. The peak wins.
    inits = [
        _container("dind-engine", cpu_req="200m", sidecar=True),
        _container("dind-cache", cpu_req="250m"),
    ]
    apps = [_container("main", cpu_req="100m")]
    assert aggregate_pod_resource(inits, apps, "cpu") == 450

    # Reverse the order: the init container now runs before the sidecar starts,
    # so its peak is 250 on its own and the app phase's 300 wins instead.
    inits_reversed = [
        _container("dind-cache", cpu_req="250m"),
        _container("dind-engine", cpu_req="200m", sidecar=True),
    ]
    assert aggregate_pod_resource(inits_reversed, apps, "cpu") == 300


@pytest.mark.unit
def test_aggregate_pod_resource_treats_a_lone_limit_as_the_request():
    """Kubernetes defaults an unset request to the declared limit."""
    from harbor_gke_ext.compose_translator import aggregate_pod_resource

    apps = [_container("svc", mem_lim="512Mi")]
    assert aggregate_pod_resource([], apps, "memory") == 512


@pytest.mark.unit
def test_build_pod_level_resources_carries_the_task_budget():
    from harbor_gke_ext.compose_translator import build_pod_level_resources

    res = build_pod_level_resources(
        [],
        [_container("main")],
        task_cpu_m=4000,
        task_mem_mb=1024,
    )
    assert res is not None
    assert res.requests == {"cpu": "4000m", "memory": "1024Mi"}
    assert res.limits == {"cpu": "4000m", "memory": "1024Mi"}
    # Rule 3: ephemeral-storage is rejected at Pod level, so it must be absent.
    assert "ephemeral-storage" not in res.requests


@pytest.mark.unit
def test_build_pod_level_resources_never_falls_below_container_aggregate():
    """Rule 1: a Pod request under the aggregate is rejected at admission."""
    from harbor_gke_ext.compose_translator import build_pod_level_resources

    # Containers request 3000m in total against a 1000m task budget.
    apps = [
        _container("main", cpu_req="1000m"),
        _container("svc-a", cpu_req="1000m"),
        _container("svc-b", cpu_req="1000m"),
    ]
    res = build_pod_level_resources(
        [],
        apps,
        task_cpu_m=1000,
        task_mem_mb=None,
    )
    assert res is not None
    assert res.requests["cpu"] == "3000m"


@pytest.mark.unit
def test_build_pod_level_resources_raises_ceiling_and_warns_when_declared_exceeds_budget(
    caplog,
):
    """The user-chosen max(budget, declared) rule, with the required warning."""
    import logging

    from harbor_gke_ext.compose_translator import build_pod_level_resources

    apps = [_container("main"), _container("greedy", mem_lim="8192Mi")]
    with caplog.at_level(logging.WARNING):
        res = build_pod_level_resources(
            [],
            apps,
            task_cpu_m=None,
            task_mem_mb=1024,
        )
    assert res is not None
    # Rule 2: no single container limit may exceed the Pod limit.
    assert res.limits["memory"] == "8192Mi"
    assert "exceed the task budget" in caplog.text


@pytest.mark.unit
def test_build_pod_level_resources_honours_an_explicit_task_limit():
    """`cpu_limit_multiplier` / `memory_limit_multiplier` must reach the Pod.

    They deliberately set a task limit above the task request, so the ceiling
    and the reservation differ. If only the request reached `spec.resources`
    the multiplier would have no effect at all.
    """
    from harbor_gke_ext.compose_translator import build_pod_level_resources

    res = build_pod_level_resources(
        [],
        [_container("main")],
        task_cpu_m=1000,
        task_mem_mb=2048,
        task_cpu_limit_m=2000,
        task_mem_limit_mb=4096,
    )
    assert res is not None
    assert res.requests == {"cpu": "1000m", "memory": "2048Mi"}
    assert res.limits == {"cpu": "2000m", "memory": "4096Mi"}


@pytest.mark.unit
def test_build_pod_level_resources_warns_when_task_declares_no_budget(caplog):
    """`cpus`/`memory_mb` are optional in Harbor, and absent means unlimited."""
    import logging

    from harbor_gke_ext.compose_translator import build_pod_level_resources

    with caplog.at_level(logging.WARNING):
        res = build_pod_level_resources(
            [],
            [_container("main")],
            task_cpu_m=None,
            task_mem_mb=None,
        )
    assert res is None
    assert "BestEffort" in caplog.text


@pytest.mark.unit
def test_build_pod_level_resources_floors_bare_containers_on_autopilot():
    """Autopilot injects 500m / 2Gi into bare containers; a token request stops it.

    Measured on GKE Autopilot 1.35.8: a 7-container Pod with a 2 CPU / 4Gi
    Pod-level budget and bare sidecars is rejected ("aggregate container
    requests of 14Gi") on general-purpose and custom ComputeClasses.
    """
    from harbor_gke_ext.compose_translator import build_pod_level_resources

    inits = [
        _container("sidecar", sidecar=True),
        _container("one-shot"),
    ]
    apps = [
        _container("main"),
        _container("declared", cpu_req="250m", mem_lim="512Mi"),
        *[_container(f"side{i}") for i in range(5)],
    ]
    res = build_pod_level_resources(
        inits,
        apps,
        task_cpu_m=2000,
        task_mem_mb=4096,
        is_autopilot=True,
    )
    assert res is not None
    # The task budget still wins; the floor only adds a few millicores / MiB.
    assert res.requests == {"cpu": "2000m", "memory": "4096Mi"}
    for c in [*inits, *apps]:
        if c.name == "declared":
            continue
        assert c.resources.requests == {"cpu": "1m", "memory": "1Mi"}, c.name
        assert c.resources.limits is None, c.name
    declared = apps[1]
    # Declared values are left untouched; a lone limit counts as a declaration.
    assert declared.resources.requests == {"cpu": "250m"}
    assert declared.resources.limits == {"memory": "512Mi"}


@pytest.mark.unit
def test_build_pod_level_resources_floors_only_resources_that_reach_the_pod():
    """Without a memory budget or declaration, memory is not on spec.resources.

    Autopilot's memory default then cannot conflict with anything, so the
    container must stay undeclared rather than carry an invented 1Mi.
    """
    from harbor_gke_ext.compose_translator import build_pod_level_resources

    apps = [_container("main"), _container("svc")]
    res = build_pod_level_resources(
        [],
        apps,
        task_cpu_m=1000,
        task_mem_mb=None,
        is_autopilot=True,
    )
    assert res is not None
    assert res.requests == {"cpu": "1000m"}
    for c in apps:
        assert c.resources.requests == {"cpu": "1m"}


@pytest.mark.unit
def test_build_pod_level_resources_never_floors_on_standard():
    """The floor must stay Autopilot-gated; Standard never defaults containers."""
    from harbor_gke_ext.compose_translator import build_pod_level_resources

    apps = [_container("main"), _container("svc")]
    res = build_pod_level_resources(
        [],
        apps,
        task_cpu_m=1000,
        task_mem_mb=2048,
    )
    assert res is not None
    assert res.requests == {"cpu": "1000m", "memory": "2048Mi"}
    for c in apps:
        assert c.resources.requests is None
        assert c.resources.limits is None


@pytest.mark.unit
def test_build_pod_level_resources_does_not_floor_without_a_budget_on_autopilot():
    """No Pod-level resources means no aggregate rule for defaults to break."""
    from harbor_gke_ext.compose_translator import build_pod_level_resources

    apps = [_container("main"), _container("svc")]
    res = build_pod_level_resources(
        [],
        apps,
        task_cpu_m=None,
        task_mem_mb=None,
        is_autopilot=True,
    )
    assert res is None
    for c in apps:
        assert c.resources.requests is None


@pytest.mark.unit
def test_extract_service_resources_drops_unparseable_values(caplog):
    """An unreadable Compose value must not become an invented cap."""
    import logging

    from harbor_gke_ext.compose_translator import _extract_service_resources

    with caplog.at_level(logging.WARNING):
        res = _extract_service_resources(
            {"mem_limit": "not-a-size!", "cpus": "also-bad!"},
            {},
            service_name="broken",
        )
    # Previously these became 512Mi and 250m.
    assert res.limits is None
    assert res.requests is None
    assert "broken" in caplog.text


def _shape_b_gate_command(tmp_path, **translate_kwargs) -> str:
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    command: ["sleep", "infinity"]
  docker-proxy:
    image: tecnativa/docker-socket-proxy:latest
    privileged: true
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
"""
    )
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="dind-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        compose_placement="auto",
        cluster_capabilities=ClusterCapabilities(
            is_autopilot=False,
            gke_version="1.35.1-gke.100",
            dind_availability=DindAvailability.DIND_AVAILABLE,
        ),
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
        **translate_kwargs,
    )
    gate = next(c for c in pod.spec.init_containers if c.name == "compose-up-gate")
    return " ".join(gate.command or [])


@pytest.mark.unit
def test_compose_up_timeout_reaches_the_gate(tmp_path):
    """`compose_up_timeout_sec` was parsed and stored but never read."""
    gate_cmd = _shape_b_gate_command(tmp_path, compose_up_timeout_sec=45)
    assert "timeout 45 docker compose" in gate_cmd
    # 124 is what `timeout` exits with after killing the command.
    assert '"$rc" -eq 124' in gate_cmd


@pytest.mark.unit
def test_compose_up_timeout_defaults_to_300(tmp_path):
    gate_cmd = _shape_b_gate_command(tmp_path)
    assert "timeout 300 docker compose" in gate_cmd


@pytest.mark.unit
def test_compose_up_gate_script_is_valid_posix_sh(tmp_path):
    """The gate script grew branching and quoting; `sh -n` must still accept it."""
    import shutil
    import subprocess

    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("no /bin/sh available")
    gate_cmd = _shape_b_gate_command(tmp_path, compose_up_timeout_sec=45)
    # `command` is ["sh", "-c", script]; recover the script itself.
    script = gate_cmd.split("-c ", 1)[1]
    result = subprocess.run([sh, "-n"], input=script, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.unit
def test_translate_compose_shape_c_main_in_dind(tmp_path: Path):
    """Shape C (`main` needing DinD) places `main` inside `dind-engine` and emits a lifecycle mirror in `spec.containers[0]`."""
    import json

    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: python:3.12-slim
    working_dir: /workspace
    environment:
      - FOO=bar
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
"""
    )
    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={"MAIN_IMAGE_NAME": "python:3.12-slim"},
        pod_name="job-shape-c",
        namespace="default",
        labels={"app": "test"},
        main_image="python:3.12-slim",
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
        compose_placement="auto",
        startup_env={"AGENT_KEY": "secret"},
        cluster_capabilities=ClusterCapabilities(
            is_autopilot=False,
            gke_version="1.35.1-gke.100",
            dind_availability=DindAvailability.DIND_AVAILABLE,
        ),
    )
    annotations = pod.metadata.annotations or {}
    assert annotations.get("harbor.dev/compose-placement") == "dind"
    assert annotations.get("harbor.dev/compose-placement-shape") == "C"
    assert json.loads(annotations.get("harbor.dev/dind-delegated-services", "[]")) == [
        "main"
    ]

    init_names = [c.name for c in (pod.spec.init_containers or [])]
    assert "dind-engine" in init_names
    assert "dind-cache-main" in init_names
    assert "compose-up-gate" in init_names

    gate = next(c for c in pod.spec.init_containers if c.name == "compose-up-gate")
    gate_script = (gate.command or ["", "", ""])[2]
    import base64
    import re

    import yaml

    m = re.search(
        r"echo\s+'?([A-Za-z0-9+/=]+)'?\s+\|\s+base64\s+-d\s+>\s+/harbor/dind-compose\.yaml",
        gate_script,
    )
    assert m is not None
    inner_compose = yaml.safe_load(base64.b64decode(m.group(1)).decode("utf-8"))
    inner_main = inner_compose["services"]["main"]
    assert inner_main["stdin_open"] is True
    assert inner_main["tty"] is True
    assert inner_main["working_dir"] == "/workspace"
    assert inner_main["environment"]["AGENT_KEY"] == "secret"

    assert len(pod.spec.containers) == 1
    main_ctr = pod.spec.containers[0]
    assert main_ctr.name == "main"
    main_script = (main_ctr.command or ["", "", ""])[2]
    assert "docker wait main" in main_script
    assert any(
        e.name == "DOCKER_HOST" and e.value == "unix:///var/run/harbor-dind/docker.sock"
        for e in (main_ctr.env or [])
    )


# ============================================================================
# F1: `main` must never be left running the image's default CMD.
#
# These are the end-to-end counterparts of the overlay tests in
# `test_compose_spec.py`. They assert on the artifact that actually reaches the
# cluster -- the `V1Container` on the native path, and the inner Compose
# document on the DinD path -- because that is the level at which the live
# failure occurred.
# ============================================================================
@pytest.mark.unit
def test_translate_compose_main_without_command_gets_keepalive(tmp_path: Path):
    """U1.5: Shape A `main` declaring no `command` must still be kept alive.

    Before the base-overlay fix this produced ``command=None``, kubelet ran the
    image's default CMD (``python3`` for most of the corpus), the process read
    EOF on a non-tty stdin and exited 0, and the trial died on the next exec
    with "Exec stream disconnected prematurely".
    """
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/repo/main:latest
    working_dir: /app
"""
    )

    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={
            "MAIN_IMAGE_NAME": "us-central1-docker.pkg.dev/proj/repo/main:latest"
        },
        pod_name="keepalive-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/repo/main:latest",
        image_resolver=ImageResolver(
            project_id="proj",
            registry_name="repo",
            registry_location="us-central1",
        ),
        task_dir=tmp_path,
    )

    main_c = next(c for c in pod.spec.containers if c.name == MAIN_SERVICE_NAME)
    # A Compose `command` maps to the Kubernetes `args`, not `command`: `command`
    # is reserved for the Compose `entrypoint`. Leaving it unset is what keeps
    # an image's own ENTRYPOINT in play, exactly as Compose does under Docker.
    assert main_c.command is None
    assert main_c.args == ["sh", "-c", "sleep infinity"]


@pytest.mark.unit
def test_translate_compose_shape_c_inner_main_gets_keepalive(tmp_path: Path):
    """U1.6: the same guarantee must hold for `main` delegated into DinD.

    Shape C's outer `main` is a proxy that blocks on ``docker wait main``, so an
    inner `main` that exits immediately collapses the whole Pod rather than just
    one container.
    """
    import base64
    import re

    import yaml

    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    image: python:3.12-slim
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
"""
    )

    pod = translate_compose(
        compose_paths=[compose_file],
        compose_env={"MAIN_IMAGE_NAME": "python:3.12-slim"},
        pod_name="keepalive-shape-c",
        namespace="default",
        labels={},
        main_image="python:3.12-slim",
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
        compose_placement="auto",
        cluster_capabilities=ClusterCapabilities(
            is_autopilot=False,
            gke_version="1.35.1-gke.100",
            dind_availability=DindAvailability.DIND_AVAILABLE,
        ),
    )

    gate = next(c for c in pod.spec.init_containers if c.name == "compose-up-gate")
    gate_script = (gate.command or ["", "", ""])[2]
    m = re.search(
        r"echo\s+'?([A-Za-z0-9+/=]+)'?\s+\|\s+base64\s+-d\s+>\s+/harbor/dind-compose\.yaml",
        gate_script,
    )
    assert m is not None
    inner_compose = yaml.safe_load(base64.b64decode(m.group(1)).decode("utf-8"))
    assert inner_compose["services"]["main"]["command"] == [
        "sh",
        "-c",
        "sleep infinity",
    ]


# ============================================================================
# F3: OCI manifest resolution failures must be loud and must not be cached.
#
# `_resolve_oci_manifest_facts` short-circuits under `PYTEST_CURRENT_TEST`, so
# each of these tests removes that variable for its duration.
# ============================================================================
@contextmanager
def _oci_resolution_enabled(monkeypatch):
    """Clear the caches and lift the pytest short-circuit for one test."""
    from harbor_gke_ext import compose_translator as ct

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    ct._OCI_CONFIG_CACHE.clear()
    ct._OCI_COMPRESSED_SIZE_CACHE.clear()
    try:
        yield ct
    finally:
        ct._OCI_CONFIG_CACHE.clear()
        ct._OCI_COMPRESSED_SIZE_CACHE.clear()


@pytest.mark.unit
def test_oci_manifest_failure_logs_warning(monkeypatch, caplog):
    """U3.1: a failed manifest fetch degrades two unrelated subsystems.

    At `debug` the operator sees neither a broken `--change` set nor an
    under-reserved `dind-engine`, so the level must be `warning`.
    """
    with _oci_resolution_enabled(monkeypatch) as ct:
        monkeypatch.setattr(
            ct,
            "_fetch_oci_config_from_registry",
            MagicMock(side_effect=RuntimeError("401 Unauthorized")),
        )
        with caplog.at_level(logging.WARNING, logger=ct.logger.name):
            cfg, size = ct._resolve_oci_manifest_facts("example.com/repo/img:tag")

    assert cfg == {}
    assert size is None
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "example.com/repo/img:tag" in message
    assert "401 Unauthorized" in message


@pytest.mark.unit
def test_oci_manifest_failure_is_not_cached(monkeypatch):
    """U3.2: a transient failure must not become permanent for the process.

    `harbor run` resolves many trials in one process. Negatively caching a
    single 5xx would silently degrade every later trial using that image.
    """
    with _oci_resolution_enabled(monkeypatch) as ct:
        fetch = MagicMock(side_effect=RuntimeError("503 Service Unavailable"))
        monkeypatch.setattr(ct, "_fetch_oci_config_from_registry", fetch)

        ct._resolve_oci_manifest_facts("example.com/repo/img:tag")
        assert "example.com/repo/img:tag" not in ct._OCI_CONFIG_CACHE
        assert "example.com/repo/img:tag" not in ct._OCI_COMPRESSED_SIZE_CACHE

        # Second call must retry rather than replay the cached failure.
        fetch.side_effect = None
        fetch.return_value = ({"Cmd": ["python3"]}, 1234)
        cfg, size = ct._resolve_oci_manifest_facts("example.com/repo/img:tag")

    assert fetch.call_count == 2
    assert cfg == {"Cmd": ["python3"]}
    assert size == 1234


@pytest.mark.unit
def test_oci_manifest_success_is_cached(monkeypatch):
    """U3.3: the positive-caching behaviour F3 was careful not to disturb."""
    with _oci_resolution_enabled(monkeypatch) as ct:
        fetch = MagicMock(return_value=({"Entrypoint": ["/entry.sh"]}, 4096))
        monkeypatch.setattr(ct, "_fetch_oci_config_from_registry", fetch)

        first_cfg, first_size = ct._resolve_oci_manifest_facts("example.com/a:1")
        second_cfg, second_size = ct._resolve_oci_manifest_facts("example.com/a:1")

    assert fetch.call_count == 1
    assert first_cfg == second_cfg == {"Entrypoint": ["/entry.sh"]}
    assert first_size == second_size == 4096


@pytest.mark.unit
def test_dind_cache_prefers_authenticated_docker_pull_before_rootfs_tar(
    tmp_path: Path,
) -> None:
    """D9: `dind-cache-*` must try `docker pull` (with GCE metadata-server auth
    staged by `dind-engine`) before falling back to `tar -cf - / | docker import`,
    so GKE Image Streaming (`gcfs`) `st_nlink = 1` does not inflate hard-linked
    OCI layers 5.5x during materialization."""
    compose_path = tmp_path / "docker-compose.yaml"
    compose_path.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/harbor-tasks/orca:latest
    privileged: true
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
"""
    )

    pod = translate_compose(
        compose_paths=[compose_path],
        compose_env={},
        pod_name="d9-pull-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/harbor-tasks/orca:latest",
        is_autopilot=False,
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
    )
    inits = {c.name: c for c in (pod.spec.init_containers or [])}
    dind_script = inits["dind-engine"].command[2]
    cache_script = inits["dind-cache-main"].command[2]

    assert "harbor_refresh_gcr_auth" in dind_script
    assert "us-central1-docker.pkg.dev" in dind_script
    assert "/harbor/dind-images/.docker/config.json" in dind_script
    assert 'DOCKER_CONFIG="/harbor/dind-images/.docker"' in cache_script
    assert '"$DCLI" pull -q "$IMG"' in cache_script
    assert cache_script.index('"$DCLI" pull -q "$IMG"') < cache_script.index(
        "tar -cf -"
    )


@pytest.mark.unit
def test_dind_nested_compose_support_and_pod_memory_limit_floor(
    tmp_path: Path,
) -> None:
    """Verify DinD Shape C support for nested compose workloads (e.g. orca-bench):
    - Pod memory limit floor is raised to 8192Mi while keeping Pod memory request at 1024Mi.
    - Pod volumes mounted into DinD services are symlinked at their mount_path in dind-engine.
    - Empty SNAPSHOT_CACHE_HOST_DIR is defaulted to /app.
    - compose-up-gate waits for /tmp/env-ready when /app/entrypoint.sh references it.
    """
    compose_path = tmp_path / "docker-compose.yaml"
    compose_path.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/harbor-tasks/orca:latest
    privileged: true
    environment:
      - SNAPSHOT_CACHE_HOST_DIR=${SNAPSHOT_CACHE_HOST_DIR:-}
      - CONTEXT_DIR=/workspace/ctx
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./ctx:/workspace/ctx
"""
    )
    (tmp_path / "ctx").mkdir()

    pod = translate_compose(
        compose_paths=[compose_path],
        compose_env={},
        pod_name="orca-dind-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/harbor-tasks/orca:latest",
        is_autopilot=False,
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
        cpu_request="2000m",
        memory_request="1024Mi",
    )
    assert pod.spec.resources.requests["memory"] == "1024Mi"
    assert pod.spec.resources.limits["memory"] == "8192Mi"

    inits = {c.name: c for c in (pod.spec.init_containers or [])}
    dind_script = inits["dind-engine"].command[2]
    gate_script = inits["compose-up-gate"].command[2]

    assert "/workspace/ctx" in dind_script
    assert "/tmp/harbor-main-rootfs/app" in dind_script
    assert "/tmp/env-ready" in gate_script


@pytest.mark.unit
def test_dind_registry_token_scoped_strictly_to_google_registries(
    tmp_path: Path,
) -> None:
    """Verify node SA OAuth token is never written for third-party registries (B5)."""
    from harbor_gke_ext.compose_translator import _is_google_registry_host

    assert _is_google_registry_host("gcr.io")
    assert _is_google_registry_host("eu.gcr.io")
    assert _is_google_registry_host("us-central1-docker.pkg.dev")
    assert not _is_google_registry_host("ghcr.io")
    assert not _is_google_registry_host("quay.io")
    assert not _is_google_registry_host("public.ecr.aws")
    assert not _is_google_registry_host("evil-gcr.io")

    compose_path = tmp_path / "docker-compose.yaml"
    compose_path.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/harbor-tasks/main:latest
    privileged: true
  ext_gh:
    image: ghcr.io/org/tool:1.0
    privileged: true
  ext_ecr:
    image: public.ecr.aws/org/service:latest
    privileged: true
"""
    )
    pod = translate_compose(
        compose_paths=[compose_path],
        compose_env={},
        pod_name="dind-reg-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/harbor-tasks/main:latest",
        is_autopilot=False,
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
    )
    inits = {c.name: c for c in (pod.spec.init_containers or [])}
    dind_script = inits["dind-engine"].command[2]
    assert "us-central1-docker.pkg.dev" in dind_script
    assert "ghcr.io" not in dind_script
    assert "public.ecr.aws" not in dind_script


@pytest.mark.unit
def test_dind_pre_main_metadata_lockdown_and_token_scrub(
    tmp_path: Path,
) -> None:
    """Verify DinD scrubs temporary registry token and locks down metadata routes before compose up when allow_metadata_server=False."""
    compose_path = tmp_path / "docker-compose.yaml"
    compose_path.write_text(
        """
services:
  main:
    image: us-central1-docker.pkg.dev/proj/harbor-tasks/main:latest
    privileged: true
"""
    )

    pod_locked = translate_compose(
        compose_paths=[compose_path],
        compose_env={},
        pod_name="dind-locked-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/harbor-tasks/main:latest",
        is_autopilot=False,
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
        allow_metadata_server=False,
    )
    inits_locked = {c.name: c for c in (pod_locked.spec.init_containers or [])}
    dind_locked = inits_locked["dind-engine"].command[2]
    gate_locked = inits_locked["compose-up-gate"].command[2]

    assert "ip route replace unreachable 169.254.169.254/32" in dind_locked
    assert "ip route replace unreachable 169.254.169.252/32" in dind_locked
    assert "/harbor/dind-images/.metadata-blocked" in dind_locked
    assert "/harbor/dind-images/.metadata-blocked" in gate_locked
    assert gate_locked.index(".metadata-blocked") < gate_locked.index(
        "docker compose -f /harbor/dind-compose.yaml --project-name harbor up"
    )

    pod_allowed = translate_compose(
        compose_paths=[compose_path],
        compose_env={},
        pod_name="dind-allowed-pod",
        namespace="default",
        labels={},
        main_image="us-central1-docker.pkg.dev/proj/harbor-tasks/main:latest",
        is_autopilot=False,
        image_resolver=ImageResolver(),
        task_dir=tmp_path,
        allow_metadata_server=True,
    )
    inits_allowed = {c.name: c for c in (pod_allowed.spec.init_containers or [])}
    dind_allowed = inits_allowed["dind-engine"].command[2]
    gate_allowed = inits_allowed["compose-up-gate"].command[2]

    assert "ip route replace unreachable" not in dind_allowed
    assert ".metadata-blocked" not in gate_allowed
    assert "rm -rf /harbor/dind-images/.docker" in gate_allowed
