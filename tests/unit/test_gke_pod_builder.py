import pytest
from harbor_gke_ext.pod_builder import build_direct_pod
from harbor.models.task.config import TpuSpec


@pytest.mark.unit
def test_build_direct_pod_resources():
    pod = build_direct_pod(
        pod_name="test-pod",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={},
        cpu_request="2",
        cpu_limit="4",
        memory_request="4Gi",
        memory_limit="8Gi",
        ephemeral_storage_request="50Gi",
        effective_gpus=2,
        gpu_types=["nvidia-l4"],
        tpu=TpuSpec(type="v5e", topology="2x2", chip_count=4),
        compute_class="accelerator-class",
    )
    assert pod.spec.resources is not None
    assert pod.spec.resources.requests == {"cpu": "2", "memory": "4Gi"}
    assert pod.spec.resources.limits == {"cpu": "4", "memory": "8Gi"}

    resources = pod.spec.containers[0].resources
    assert "cpu" not in (resources.requests or {})
    assert "memory" not in (resources.requests or {})
    assert "cpu" not in (resources.limits or {})
    assert "memory" not in (resources.limits or {})
    assert resources.requests["ephemeral-storage"] == "50Gi"
    assert resources.requests["nvidia.com/gpu"] == "2"
    assert resources.limits["nvidia.com/gpu"] == "2"
    assert resources.requests["google.com/tpu"] == "4"
    assert resources.limits["google.com/tpu"] == "4"

    assert pod.spec.node_selector["cloud.google.com/gke-tpu-topology"] == "2x2"
    assert (
        pod.spec.node_selector["cloud.google.com/compute-class"] == "accelerator-class"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "cpu_req,cpu_lim,mem_req,mem_lim",
    [
        ("1", "1", "4096Mi", "4096Mi"),
        # Numerically equal quantities in different notation are still Guaranteed.
        ("1", "1000m", "4096Mi", "4Gi"),
    ],
)
def test_build_direct_pod_guaranteed_budget_goes_on_container(
    cpu_req, cpu_lim, mem_req, mem_lim
):
    """The kubelet CPU Manager ignores Pod-level resources by default.

    Honouring them requires the `PodLevelResourceManagers` feature gate (alpha
    in Kubernetes 1.36). A Guaranteed direct Pod must carry cpu/memory on
    `main` so that a node pool with `cpuManagerPolicy: static` can grant it
    exclusive cores.
    """
    pod = build_direct_pod(
        pod_name="test-pod",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={},
        cpu_request=cpu_req,
        cpu_limit=cpu_lim,
        memory_request=mem_req,
        memory_limit=mem_lim,
        ephemeral_storage_request="10Gi",
    )
    assert pod.spec.resources is None
    resources = pod.spec.containers[0].resources
    assert resources.requests == {
        "cpu": cpu_req,
        "memory": mem_req,
        "ephemeral-storage": "10Gi",
    }
    assert resources.limits == {"cpu": cpu_lim, "memory": mem_lim}


@pytest.mark.unit
@pytest.mark.parametrize(
    "cpu_lim,mem_lim",
    [
        (None, None),  # Burstable, requests only (the GKE default)
        ("1", None),  # CPU capped, memory not
        ("2", "4096Mi"),  # both capped, CPU above request
    ],
)
def test_build_direct_pod_non_guaranteed_budget_stays_pod_level(cpu_lim, mem_lim):
    pod = build_direct_pod(
        pod_name="test-pod",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={},
        cpu_request="1",
        cpu_limit=cpu_lim,
        memory_request="4096Mi",
        memory_limit=mem_lim,
    )
    assert pod.spec.resources is not None
    assert pod.spec.resources.requests == {"cpu": "1", "memory": "4096Mi"}
    resources = pod.spec.containers[0].resources
    assert "cpu" not in (resources.requests or {})
    assert "memory" not in (resources.requests or {})
    assert resources.limits is None


@pytest.mark.unit
def test_build_direct_pod_machine_type_and_pool():
    pod = build_direct_pod(
        pod_name="test-pod",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={},
        machine_type="n2-standard-4",
        node_pool="custom-pool",
    )
    assert pod.spec.node_selector["cloud.google.com/machine-family"] == "n2"
    assert "node.kubernetes.io/instance-type" not in pod.spec.node_selector
    assert pod.spec.node_selector["cloud.google.com/gke-nodepool"] == "custom-pool"


@pytest.mark.unit
def test_build_direct_pod_entrypoint_and_stdin_tty():
    pod_default = build_direct_pod(
        pod_name="test-pod-default",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={},
        override_entrypoint=False,
    )
    c_default = pod_default.spec.containers[0]
    assert c_default.command is None
    assert c_default.args == ["sh", "-c", "sleep infinity"]
    assert c_default.stdin is True
    assert c_default.tty is True

    pod_override = build_direct_pod(
        pod_name="test-pod-override",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={},
        override_entrypoint=True,
    )
    c_override = pod_override.spec.containers[0]
    assert c_override.command == ["sleep", "infinity"]
    assert c_override.args is None
    assert c_override.stdin is False
    assert c_override.tty is False


@pytest.mark.unit
def test_build_direct_pod_gpu_ldconfig_prelude_and_no_env_clobber():
    from harbor_gke_ext.constants import GKE_NVIDIA_LDCONFIG_SNIPPET

    gpu_default = build_direct_pod(
        pod_name="gpu-pod-default",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={"USER_VAR": "1"},
        effective_gpus=1,
        override_entrypoint=False,
    )
    c_gpu_default = gpu_default.spec.containers[0]
    assert c_gpu_default.command is None
    assert c_gpu_default.args is not None
    assert c_gpu_default.args[:2] == ["sh", "-c"]
    assert GKE_NVIDIA_LDCONFIG_SNIPPET in c_gpu_default.args[2]
    assert c_gpu_default.args[2].endswith("exec sleep infinity")
    # LD_LIBRARY_PATH must NOT be injected via container env (which would clobber image ENV)
    env_names = {e.name for e in (c_gpu_default.env or [])}
    assert "LD_LIBRARY_PATH" not in env_names
    assert env_names == {"USER_VAR"}

    gpu_override = build_direct_pod(
        pod_name="gpu-pod-override",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={},
        effective_gpus=1,
        override_entrypoint=True,
    )
    c_gpu_override = gpu_override.spec.containers[0]
    assert c_gpu_override.command is not None
    assert c_gpu_override.command[:2] == ["sh", "-c"]
    assert GKE_NVIDIA_LDCONFIG_SNIPPET in c_gpu_override.command[2]
    assert c_gpu_override.command[2].endswith("exec sleep infinity")
    assert c_gpu_override.args is None


@pytest.mark.unit
def test_nvidia_prelude_links_driver_binaries_onto_path(tmp_path):
    """The prelude must put the driver utilities somewhere already on PATH.

    `PATH` cannot be set on the container spec -- that replaces the image's own
    value -- so the prelude symlinks into /usr/local/bin instead. This runs the
    generated shell against a fake driver tree and asserts the outcome.
    """
    import shutil
    import subprocess

    from harbor_gke_ext.constants import GKE_NVIDIA_LDCONFIG_SNIPPET

    root = tmp_path / "root"
    driver_bin = root / "usr/local/nvidia/bin"
    driver_bin.mkdir(parents=True)
    (driver_bin / "nvidia-smi").write_text("#!/bin/sh\n")
    (driver_bin / "nvidia-smi").chmod(0o755)
    (driver_bin / "not-executable").write_text("data\n")

    # An image that ships its own utility of the same name must win.
    local_bin = root / "usr/local/bin"
    local_bin.mkdir(parents=True)
    (local_bin / "nvidia-debugdump").write_text("#!/bin/sh\necho image\n")
    (driver_bin / "nvidia-debugdump").write_text("#!/bin/sh\necho driver\n")
    (driver_bin / "nvidia-debugdump").chmod(0o755)

    # Re-root the absolute paths so the snippet can run without a container.
    script = GKE_NVIDIA_LDCONFIG_SNIPPET.replace("/usr/local", f"{root}/usr/local")
    script = script.replace("/etc/ld.so.conf.d", f"{root}/etc/ld.so.conf.d")
    assert subprocess.run(["sh", "-n", "-c", script]).returncode == 0
    assert subprocess.run(["sh", "-c", script]).returncode == 0

    linked = local_bin / "nvidia-smi"
    assert linked.is_symlink()
    assert linked.resolve() == (driver_bin / "nvidia-smi").resolve()
    assert not (local_bin / "not-executable").exists()
    # Pre-existing name untouched.
    assert not (local_bin / "nvidia-debugdump").is_symlink()
    assert "image" in (local_bin / "nvidia-debugdump").read_text()

    shutil.rmtree(root)


@pytest.mark.unit
def test_build_direct_pod_gpu_never_sets_path_or_ld_library_path():
    """Both variables are image-owned; setting either on the spec replaces it."""
    pod = build_direct_pod(
        pod_name="gpu-pod-env",
        namespace="default",
        environment_name="env-name",
        run_id="run-123",
        image_url="test-image:latest",
        startup_env={"USER_VAR": "1"},
        effective_gpus=2,
    )
    env_names = {e.name for e in (pod.spec.containers[0].env or [])}
    assert "PATH" not in env_names
    assert "LD_LIBRARY_PATH" not in env_names
