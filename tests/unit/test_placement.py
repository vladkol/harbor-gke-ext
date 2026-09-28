"""Unit tests for harbor_gke_ext.placement (Commit S2)."""

from __future__ import annotations

from pathlib import Path

import pytest

from harbor_gke_ext.cluster_probe import (
    ClusterCapabilities,
    DindAvailability,
)
from harbor_gke_ext.placement import (
    UnsupportedComposeFeatureError,
    classify_compose_placement,
    reconcile_gpu_config,
)


@pytest.mark.unit
def test_reconcile_gpu_config_featurebench_cases() -> None:
    # Case 1: featurebench (non-modal) has count=all in compose, gpus=0 in task.toml -> GPU_COMPOSE_ONLY
    project_count_all = {
        "services": {
            "main": {
                "deploy": {
                    "resources": {
                        "reservations": {
                            "devices": [
                                {
                                    "driver": "nvidia",
                                    "count": "all",
                                    "capabilities": ["gpu"],
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
    with pytest.raises(UnsupportedComposeFeatureError) as exc_info:
        reconcile_gpu_config(
            project_count_all,
            toml_gpus=0,
            default_gpu_type="l4",
        )
    assert any("GPU_COMPOSE_ONLY" in c for c in exc_info.value.causes)

    # Case 2: featurebench-modal has count=all in compose, gpus=1 in task.toml, no gpu_types -> GPU_TYPE_UNRESOLVED
    with pytest.raises(UnsupportedComposeFeatureError) as exc_info2:
        reconcile_gpu_config(
            project_count_all,
            toml_gpus=1,
            toml_gpu_types=None,
            default_gpu_type=None,
        )
    assert any("GPU_TYPE_UNRESOLVED" in c for c in exc_info2.value.causes)

    # Case 3: featurebench-modal with default_gpu_type='l4' (short name) -> resolves to 'nvidia-l4'
    cfg = reconcile_gpu_config(
        project_count_all,
        toml_gpus=1,
        default_gpu_type="l4",
    )
    assert cfg.gpus == 1
    assert cfg.accelerator_label == "nvidia-l4"
    assert cfg.gpu_services == ("main",)

    # Case 4: gpu_override rejects short name 'l4' but accepts full label 'nvidia-l4'
    with pytest.raises(ValueError, match="Invalid gpu_override"):
        reconcile_gpu_config(
            project_count_all,
            toml_gpus=1,
            gpu_override="l4",
        )
    cfg_override = reconcile_gpu_config(
        project_count_all,
        toml_gpus=1,
        gpu_override="nvidia-a100-80gb",
    )
    assert cfg_override.accelerator_label == "nvidia-a100-80gb"


@pytest.mark.unit
def test_phase2_router_unclassified_key_device_and_external_volume(
    tmp_path: Path,
) -> None:
    # Sidecar with unclassified key, non-GPU device, or external volume routes to DinD (Shape B)
    # Unclassified compose key fails placement immediately (P1-9)
    project_unclassified = {
        "services": {
            "main": {"image": "alpine:latest"},
            "sidecar": {
                "image": "redis:7",
                "totally_made_up_compose_key": True,
            },
        },
    }
    with pytest.raises(UnsupportedComposeFeatureError) as unclass_exc:
        classify_compose_placement(
            project_unclassified,
            task_dir=tmp_path,
            base_dir=tmp_path,
        )
    assert any(
        "UNCLASSIFIED_KEY:sidecar:totally_made_up_compose_key" in c
        for c in unclass_exc.value.causes
    )

    # Recognized DinD-only keys (non-GPU devices, external volume) route sidecar to DinD
    project = {
        "volumes": {"extdata": {"external": True}},
        "services": {
            "main": {"image": "alpine:latest"},
            "sidecar": {
                "image": "redis:7",
                "devices": ["/dev/net/tun:/dev/net/tun"],
                "volumes": ["extdata:/data"],
            },
        },
    }
    plan = classify_compose_placement(
        project,
        task_dir=tmp_path,
        base_dir=tmp_path,
    )
    assert plan.shape == "B"
    assert plan.dind_sidecars == ["sidecar"]
    reasons = plan.dind_reasons["sidecar"]
    assert any("DEVICE_NON_GPU:/dev/net/tun:/dev/net/tun" in r for r in reasons)
    assert any("EXTERNAL_VOLUME:extdata" in r for r in reasons)

    # If 'main' itself requests a DinD-only feature, auto/dind placement selects Shape C,
    # while native placement raises MAIN_NEEDS_DIND.
    project_main = {
        "services": {
            "main": {
                "image": "alpine:latest",
                "volumes": [
                    {
                        "type": "bind",
                        "source": "/var/run/docker.sock",
                        "target": "/var/run/docker.sock",
                    }
                ],
            }
        }
    }
    plan_c = classify_compose_placement(
        project_main,
        task_dir=tmp_path,
        base_dir=tmp_path,
    )
    assert plan_c.shape == "C"
    assert plan_c.dind_sidecars == ["main"]
    assert any("DOCKER_SOCK" in r for r in plan_c.dind_reasons["main"])

    with pytest.raises(UnsupportedComposeFeatureError) as exc_info:
        classify_compose_placement(
            project_main,
            task_dir=tmp_path,
            base_dir=tmp_path,
            compose_placement="native",
        )
    assert any("MAIN_NEEDS_DIND:DOCKER_SOCK" in c for c in exc_info.value.causes)


@pytest.mark.unit
def test_ml_dev_bench_relative_bind_and_anonymous_volumes_native(
    tmp_path: Path,
) -> None:
    task_dir = tmp_path / "ml_task"
    env_dir = task_dir / "environment"
    setup_dir = task_dir / "setup_workspace"
    env_dir.mkdir(parents=True)
    setup_dir.mkdir(parents=True)

    project = {
        "services": {
            "main": {
                "image": "hb__ml:latest",
                "volumes": [
                    {
                        "type": "bind",
                        "source": str(task_dir),
                        "target": "/opt/task-src",
                        "read_only": True,
                    },
                    {"type": "volume", "target": "/opt/task-src/solution"},
                    {"type": "volume", "target": "/opt/task-src/tests"},
                    {
                        "type": "bind",
                        "source": str(setup_dir),
                        "target": "/opt/setup_workspace",
                        "read_only": True,
                    },
                    {
                        "type": "bind",
                        "source": "/logs/verifier",
                        "target": "/logs/verifier",
                    },
                ],
            }
        }
    }

    plan = classify_compose_placement(
        project,
        task_dir=task_dir,
        base_dir=env_dir,
    )
    assert plan.shape == "A"
    assert plan.dind_sidecars == []


@pytest.mark.unit
def test_seta_env_static_ip_and_network_segmentation_failures(tmp_path: Path) -> None:
    # seta-env 1133: static IPv4
    project_1133 = {
        "services": {
            "main": {
                "image": "main:1",
                "networks": {"corp": {"ipv4_address": "10.133.0.2"}},
            },
            "web": {
                "image": "web:1",
                "networks": {"corp": {"ipv4_address": "10.133.0.10"}},
            },
        }
    }
    with pytest.raises(UnsupportedComposeFeatureError) as exc_info:
        classify_compose_placement(
            project_1133,
            task_dir=tmp_path,
            base_dir=tmp_path,
        )
    assert any("STATIC_IP:main" in c for c in exc_info.value.causes)

    # seta-env 1198: db-internal isolated on internal_net unreachable from main
    project_1198 = {
        "networks": {"external_net": {}, "internal_net": {}},
        "services": {
            "main": {"image": "main:1", "networks": ["external_net"]},
            "bastion": {
                "image": "bastion:1",
                "networks": ["external_net", "internal_net"],
            },
            "db-internal": {
                "image": "postgres:15",
                "networks": ["internal_net"],
            },
        },
    }
    with pytest.raises(UnsupportedComposeFeatureError) as exc_info2:
        classify_compose_placement(
            project_1198,
            task_dir=tmp_path,
            base_dir=tmp_path,
        )
    assert any(
        "MAIN_NETWORK_SEGMENTATION:db-internal" in c for c in exc_info2.value.causes
    )


@pytest.mark.unit
def test_kv_live_surgery_depends_on_main_gate_and_warning(tmp_path: Path) -> None:
    project = {
        "services": {
            "main": {"image": "kv-main:1"},
            "redis": {"image": "redis:7"},
            "loadgen": {
                "image": "loadgen:1",
                "depends_on": {"main": {"condition": "service_started"}},
            },
        }
    }
    plan = classify_compose_placement(
        project,
        task_dir=tmp_path,
        base_dir=tmp_path,
    )
    assert plan.shape == "A"
    assert plan.native_sidecar_services == ["redis"]
    assert plan.post_main_sidecars == ["loadgen"]
    assert len(plan.warnings) == 1
    assert "Decision Q1 workaround" in plan.warnings[0]


@pytest.mark.unit
def test_autopilot_dind_unavailable_raises_when_sidecar_needs_dind(
    tmp_path: Path,
) -> None:
    project = {
        "services": {
            "main": {"image": "main:1"},
            "docker_helper": {
                "image": "docker:cli",
                "privileged": True,
            },
        }
    }
    autopilot_caps = ClusterCapabilities(
        is_autopilot=True,
        gke_version="1.34.0-gke.100",
        dind_availability=DindAvailability.DIND_UNAVAILABLE,
        dind_reason="Cluster version < 1.35",
    )
    with pytest.raises(UnsupportedComposeFeatureError) as exc_info:
        classify_compose_placement(
            project,
            task_dir=tmp_path,
            base_dir=tmp_path,
            cluster_capabilities=autopilot_caps,
        )
    assert any("AUTOPILOT_DIND_UNAVAILABLE" in c for c in exc_info.value.causes)


@pytest.mark.unit
def test_default_gpu_count_fallback_when_compose_declares_count_all() -> None:
    from harbor_gke_ext.placement import reconcile_gpu_config

    project = {
        "services": {
            "main": {
                "image": "nvidia/cuda:12.4.0-base-ubuntu22.04",
                "deploy": {
                    "resources": {
                        "reservations": {
                            "devices": [
                                {
                                    "driver": "nvidia",
                                    "count": "all",
                                    "capabilities": ["gpu"],
                                }
                            ]
                        }
                    }
                },
            }
        }
    }
    # Without default_gpu_count or task.toml gpus -> raises UnsupportedComposeFeatureError
    with pytest.raises(UnsupportedComposeFeatureError):
        reconcile_gpu_config(
            project, toml_gpus=None, default_gpu_type="L4", default_gpu_count=None
        )

    # With default_gpu_count=1 and default_gpu_type="L4" -> succeeds with 1 L4 GPU
    resolved = reconcile_gpu_config(
        project,
        toml_gpus=None,
        default_gpu_type="L4",
        default_gpu_count=1,
    )
    assert resolved.gpus == 1
    assert resolved.accelerator_label == "nvidia-l4"
    assert resolved.gpu_services == ("main",)
