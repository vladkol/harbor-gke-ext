"""Unit tests for harbor_gke_ext.cluster_probe."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from harbor_gke_ext.cluster_probe import (
    DindAvailability,
    estimate_gke_allocatable_ephemeral_storage_mb,
    evaluate_autopilot_dind_capability,
    parse_gcloud_cluster_describe,
    parse_gke_minor_version,
    parse_quantity_to_mib,
    probe_cluster_via_gcloud,
    probe_fqdn_network_policy_support,
    probe_kube_dns_cluster_ip,
    probe_pod_level_resources_support,
)


@pytest.mark.unit
def test_parse_gke_minor_version() -> None:
    assert parse_gke_minor_version("1.35.1-gke.1200000") == (1, 35)
    assert parse_gke_minor_version("v1.34.2-gke.100") == (1, 34)
    assert parse_gke_minor_version(None) is None
    assert parse_gke_minor_version("invalid") is None


@pytest.mark.unit
def test_evaluate_autopilot_dind_matrix() -> None:
    std = evaluate_autopilot_dind_capability(
        is_autopilot=False, gke_version="1.32.0-gke.100"
    )
    assert std.dind_availability == DindAvailability.DIND_AVAILABLE
    assert std.allow_net_admin is True

    below_135 = evaluate_autopilot_dind_capability(
        is_autopilot=True,
        gke_version="1.34.2-gke.100",
        allowlist_paths=["gs://my-bucket/allowlist.yaml"],
    )
    assert below_135.dind_availability == DindAvailability.DIND_UNAVAILABLE
    assert "below 1.35" in below_135.dind_reason

    no_allowlist = evaluate_autopilot_dind_capability(
        is_autopilot=True,
        gke_version="1.35.0-gke.100",
        allowlist_paths=[],
    )
    assert no_allowlist.dind_availability == DindAvailability.DIND_UNAVAILABLE
    assert "allowlistPaths" in no_allowlist.dind_reason

    available = evaluate_autopilot_dind_capability(
        is_autopilot=True,
        gke_version="1.35.0-gke.100",
        allowlist_paths=["gs://my-bucket/allowlist.yaml"],
    )
    assert available.dind_availability == DindAvailability.DIND_AVAILABLE


@pytest.mark.unit
def test_parse_gcloud_cluster_describe_capabilities() -> None:
    raw_ap = {
        "currentMasterVersion": "1.35.1-gke.1000",
        "autopilot": {
            "enabled": True,
            "workloadPolicyConfig": {"allowNetAdmin": True},
            "privilegedAdmissionConfig": {
                "allowlistPaths": ["gs://org-allowlists/dind.yaml"]
            },
        },
    }
    caps_ap = parse_gcloud_cluster_describe(raw_ap)
    assert caps_ap.is_autopilot is True
    assert caps_ap.gke_version == "1.35.1-gke.1000"
    assert caps_ap.allow_net_admin is True
    assert caps_ap.allowlist_paths == ("gs://org-allowlists/dind.yaml",)
    assert caps_ap.dind_availability == DindAvailability.DIND_AVAILABLE

    raw_std = {
        "currentMasterVersion": "1.32.0-gke.100",
        "autopilot": {"enabled": False},
    }
    caps_std = parse_gcloud_cluster_describe(raw_std)
    assert caps_std.is_autopilot is False
    assert caps_std.dind_availability == DindAvailability.DIND_AVAILABLE


# ============================================================================
# F2: machine-type inventory, so a CPU ISA pin can be checked against reality.
# ============================================================================
@pytest.mark.unit
def test_parse_machine_type_inventory_from_node_pools() -> None:
    caps = parse_gcloud_cluster_describe(
        {
            "currentMasterVersion": "1.35.6-gke.1250000",
            "autopilot": {"enabled": False},
            "nodePools": [
                {"name": "default-pool", "config": {"machineType": "e2-standard-2"}},
                {"name": "bench-pool", "config": {"machineType": "e2-standard-16"}},
                {"name": "gpu-pool", "config": {"machineType": "g2-standard-8"}},
                # Duplicate type and a malformed entry must not disturb the result.
                {"name": "extra", "config": {"machineType": "e2-standard-2"}},
                {"name": "broken"},
            ],
        }
    )
    assert caps.available_machine_types == (
        "e2-standard-2",
        "e2-standard-16",
        "g2-standard-8",
    )
    assert caps.available_machine_families == ("e2", "g2")
    assert caps.node_auto_provisioning_enabled is False


@pytest.mark.unit
def test_parse_node_auto_provisioning_flag() -> None:
    caps = parse_gcloud_cluster_describe(
        {
            "autopilot": {"enabled": False},
            "autoscaling": {"enableNodeAutoprovisioning": True},
        }
    )
    assert caps.node_auto_provisioning_enabled is True


@pytest.mark.unit
def test_machine_type_inventory_empty_on_autopilot() -> None:
    """Autopilot has no node pools to read; that is 'unknown', not 'nothing'."""
    caps = parse_gcloud_cluster_describe(
        {
            "currentMasterVersion": "1.35.1-gke.1000",
            "autopilot": {
                "enabled": True,
                "privilegedAdmissionConfig": {"allowlistPaths": ["gs://a/b.yaml"]},
            },
        }
    )
    assert caps.available_machine_types == ()
    assert caps.available_machine_families == ()


@pytest.mark.unit
def test_evaluate_carries_inventory_through_every_branch() -> None:
    """The pass-through refactor exists so no branch can silently drop a field.

    Each of these four inputs lands on a different `return` inside
    `evaluate_autopilot_dind_capability`.
    """
    common = {
        "available_machine_types": ("n2-standard-4",),
        "available_machine_families": ("n2",),
        "node_auto_provisioning_enabled": True,
    }
    branches = [
        evaluate_autopilot_dind_capability(is_autopilot=False, **common),
        evaluate_autopilot_dind_capability(
            is_autopilot=True, gke_version="1.34.2-gke.100", **common
        ),
        evaluate_autopilot_dind_capability(
            is_autopilot=True, gke_version="1.35.0-gke.100", **common
        ),
        evaluate_autopilot_dind_capability(
            is_autopilot=True,
            gke_version="1.35.0-gke.100",
            allowlist_paths=["gs://a/b.yaml"],
            **common,
        ),
    ]
    for caps in branches:
        assert caps.available_machine_types == ("n2-standard-4",)
        assert caps.available_machine_families == ("n2",)
        assert caps.node_auto_provisioning_enabled is True


# ============================================================================
# F4: Pod-level `spec.resources` support, established by dry run not by version.
# ============================================================================
@pytest.mark.unit
def test_pod_level_resources_probe_detects_retention() -> None:
    echoed = SimpleNamespace(
        spec=SimpleNamespace(
            resources=SimpleNamespace(requests={"cpu": "500m"}, limits=None)
        )
    )
    api = MagicMock()
    api.create_namespaced_pod.return_value = echoed

    assert probe_pod_level_resources_support(api) is True
    # Must be a dry run: the probe may never create anything.
    assert api.create_namespaced_pod.call_args.kwargs["dry_run"] == "All"
    submitted_pod = api.create_namespaced_pod.call_args.kwargs["body"]
    # Must meet Autopilot's default container mutation floor (500m CPU / 2Gi memory)
    # so autopilot-default-resources-mutator does not trigger a 422 mismatch.
    assert parse_quantity_to_mib(submitted_pod.spec.resources.requests["memory"]) >= 2048
    assert submitted_pod.spec.resources.requests["cpu"] in ("500m", "1")


@pytest.mark.unit
def test_pod_level_resources_probe_detects_pruning() -> None:
    """An older API server drops the field silently rather than rejecting it."""
    api = MagicMock()
    api.create_namespaced_pod.return_value = SimpleNamespace(
        spec=SimpleNamespace(resources=None)
    )
    assert probe_pod_level_resources_support(api) is False


@pytest.mark.unit
def test_pod_level_resources_probe_422_on_spec_resources_means_supported() -> None:
    """If an admission mutator raises container requests above the probe's Pod-level
    requests, Kubernetes 1.34+ returns 422 referencing `spec.resources.requests`,
    which proves the API server retained and validated `spec.resources`."""
    from kubernetes.client.rest import ApiException

    exc = ApiException(status=422, reason="Unprocessable Entity")
    exc.body = (
        '{"kind":"Status","status":"Failure","reason":"Invalid",'
        '"message":"spec.resources.requests[cpu]: Invalid value: \\"500m\\": '
        'must be greater than or equal to aggregate container requests of 1"}'
    )
    api = MagicMock()
    api.create_namespaced_pod.side_effect = exc
    assert probe_pod_level_resources_support(api) is True


@pytest.mark.unit
def test_pod_level_resources_probe_raises_when_it_cannot_complete() -> None:
    """A failed probe is not a verdict, so it must not read as 'unsupported'."""
    api = MagicMock()
    api.create_namespaced_pod.side_effect = RuntimeError("403 Forbidden")
    with pytest.raises(RuntimeError, match="403 Forbidden"):
        probe_pod_level_resources_support(api)



# ============================================================================
# F7: node ephemeral-storage ceiling.
# ============================================================================
@pytest.mark.unit
def test_parse_quantity_to_mib_handles_binary_decimal_and_bare_bytes() -> None:
    assert parse_quantity_to_mib("1Gi") == 1024
    assert parse_quantity_to_mib("512Mi") == 512
    assert parse_quantity_to_mib("1048576Ki") == 1024
    # "Mi" must not be swallowed by the shorter "M" suffix.
    assert parse_quantity_to_mib("100Mi") == 100
    assert parse_quantity_to_mib("1G") == 953
    assert parse_quantity_to_mib("47060329472") == 44880
    assert parse_quantity_to_mib(None) is None
    assert parse_quantity_to_mib("") is None
    assert parse_quantity_to_mib("not-a-quantity") is None


@pytest.mark.unit
def test_storage_ceiling_skips_system_tainted_pools_and_keeps_gpu_pools() -> None:
    """Harbor Pods tolerate accelerator taints but not CriticalAddonsOnly."""
    caps = parse_gcloud_cluster_describe(
        {
            "autopilot": {"enabled": False},
            "networkConfig": {"datapathProvider": "ADVANCED_DATAPATH"},
            "nodePools": [
                {
                    "name": "system",
                    "initialNodeCount": 1,
                    "config": {
                        "machineType": "e2-standard-4",
                        "diskSizeGb": 2000,
                        "taints": [
                            {
                                "key": "CriticalAddonsOnly",
                                "value": "true",
                                "effect": "NO_SCHEDULE",
                            }
                        ],
                    },
                },
                {
                    "name": "gpu",
                    "initialNodeCount": 1,
                    "config": {
                        "machineType": "g2-standard-4",
                        "diskSizeGb": 100,
                        "taints": [
                            {
                                "key": "nvidia.com/gpu",
                                "value": "present",
                                "effect": "NO_SCHEDULE",
                            }
                        ],
                    },
                },
            ],
        }
    )
    assert caps.max_node_allocatable_ephemeral_storage_mb == 44880


@pytest.mark.unit
def test_three_tier_standard_cluster_tainted_default_and_scale_to_zero_workers() -> (
    None
):
    """Verify minimal tainted default-pool + scale-to-zero big-disk worker pool + NAP."""
    caps_with_nap = parse_gcloud_cluster_describe(
        {
            "currentMasterVersion": "1.35.1-gke.1000",
            "autopilot": {"enabled": False},
            "nodePools": [
                {
                    "name": "default-pool",
                    "initialNodeCount": 1,
                    "config": {
                        "machineType": "e2-standard-4",
                        "diskSizeGb": 100,
                        "taints": [
                            {
                                "key": "CriticalAddonsOnly",
                                "value": "true",
                                "effect": "NO_SCHEDULE",
                            }
                        ],
                    },
                },
                {
                    "name": "harbor-workers",
                    "initialNodeCount": 0,
                    "config": {
                        "machineType": "n2-standard-16",
                        "diskSizeGb": 2000,
                    },
                    "autoscaling": {
                        "enabled": True,
                        "totalMinNodeCount": 0,
                        "totalMaxNodeCount": 16,
                    },
                },
            ],
            "autoscaling": {
                "enableNodeAutoprovisioning": True,
                "resourceLimits": [{"resourceType": "cpu", "maximum": 1500}],
                "autoprovisioningNodePoolDefaults": {"diskSizeGb": 1500},
            },
        }
    )
    # NAP cpu.maximum (1500) must not be clamped down to default-pool (4) or worker pool (256).
    assert caps_with_nap.max_schedulable_cpu_cores == 1500
    # Tainted default-pool (e2-standard-4) is excluded from worker inventory.
    assert caps_with_nap.available_machine_types == ("n2-standard-16",)
    assert caps_with_nap.available_machine_families == ("n2",)
    # 2000 GB worker pool gives ~1,668.1 GiB (~1,708,144 MiB) allocatable storage.
    assert caps_with_nap.max_node_allocatable_ephemeral_storage_mb == 1708144

    # Also verify without NAP (pure scale-to-zero harbor-workers pool at 0 nodes):
    caps_no_nap = parse_gcloud_cluster_describe(
        {
            "currentMasterVersion": "1.35.1-gke.1000",
            "autopilot": {"enabled": False},
            "nodePools": [
                {
                    "name": "default-pool",
                    "initialNodeCount": 1,
                    "config": {
                        "machineType": "e2-standard-4",
                        "diskSizeGb": 100,
                        "taints": [
                            {
                                "key": "CriticalAddonsOnly",
                                "value": "true",
                                "effect": "NO_SCHEDULE",
                            }
                        ],
                    },
                },
                {
                    "name": "harbor-workers",
                    "initialNodeCount": 0,
                    "config": {
                        "machineType": "n2-standard-16",
                        "diskSizeGb": 1500,
                    },
                    "autoscaling": {
                        "enabled": True,
                        "totalMinNodeCount": 0,
                        "totalMaxNodeCount": 16,
                    },
                },
            ],
            "autoscaling": {"enableNodeAutoprovisioning": False},
        }
    )
    assert (
        caps_no_nap.max_schedulable_cpu_cores == 256
    )  # 16 * 16 (excludes tainted default-pool)
    assert (
        caps_no_nap.max_node_allocatable_ephemeral_storage_mb == 1254544
    )  # ~1,225.1 GiB


@pytest.mark.unit
@pytest.mark.parametrize(
    ("local_ssd_count", "expected_gib"),
    [
        # 10% eviction threshold plus 50 / 75 / 100 GiB system reservation.
        (1, 375 * 0.9 - 50),
        (2, 750 * 0.9 - 75),
        (3, 1125 * 0.9 - 100),
        (8, 3000 * 0.9 - 100),
    ],
)
def test_estimate_allocatable_local_ssd_reservation(
    local_ssd_count: int, expected_gib: float
) -> None:
    assert estimate_gke_allocatable_ephemeral_storage_mb(
        None, local_ssd_count=local_ssd_count
    ) == int(expected_gib * 1024)


@pytest.mark.unit
def test_estimate_allocatable_local_ssd_ignores_boot_disk() -> None:
    # Local SSD-backed ephemeral storage doesn't use the boot disk.
    assert estimate_gke_allocatable_ephemeral_storage_mb(
        2000, local_ssd_count=1
    ) == int((375 * 0.9 - 50) * 1024)


@pytest.mark.unit
def test_estimate_allocatable_boot_disk_formula() -> None:
    def _expected_mb(d: int, sys_res: float) -> int:
        cap = d * (63.0 / 64.0) - 4.184
        return int((0.9 * cap - sys_res) * 1024)

    # Small disk (20 GiB): the 50% term (10 GiB) is the smallest system reservation.
    assert estimate_gke_allocatable_ephemeral_storage_mb(20) == _expected_mb(20, 10.0)
    # 100 GiB disk: 35% + 6 GiB = 41 GiB system reservation -> 44,880 MiB (43.8 GiB).
    assert estimate_gke_allocatable_ephemeral_storage_mb(100) == 44880
    assert estimate_gke_allocatable_ephemeral_storage_mb(100) == _expected_mb(100, 41.0)
    # 500 GiB disk: 100 GiB cap -> 347,344 MiB (339.2 GiB).
    assert estimate_gke_allocatable_ephemeral_storage_mb(500) == 347344
    # 1500 GiB disk: 100 GiB cap -> 1,254,544 MiB (1,225.1 GiB).
    assert estimate_gke_allocatable_ephemeral_storage_mb(1500) == 1254544
    # 2000 GiB disk: 100 GiB cap -> 1,708,144 MiB (1,668.1 GiB).
    assert estimate_gke_allocatable_ephemeral_storage_mb(2000) == _expected_mb(
        2000, 100.0
    )
    assert estimate_gke_allocatable_ephemeral_storage_mb(None) is None
    assert estimate_gke_allocatable_ephemeral_storage_mb(0) is None


@pytest.mark.unit
def test_parse_gcloud_cluster_describe_network_policy_enforced() -> None:
    # 1. Autopilot -> True
    assert (
        parse_gcloud_cluster_describe(
            {"autopilot": {"enabled": True}}
        ).network_policy_enforced
        is True
    )
    # 2. Standard with ADVANCED_DATAPATH (Dataplane V2) -> True
    assert (
        parse_gcloud_cluster_describe(
            {
                "autopilot": {"enabled": False},
                "networkConfig": {"datapathProvider": "ADVANCED_DATAPATH"},
            }
        ).network_policy_enforced
        is True
    )
    # 3. Standard with Calico enabled and addon not disabled -> True
    assert (
        parse_gcloud_cluster_describe(
            {
                "autopilot": {"enabled": False},
                "networkPolicy": {"enabled": True},
                "addonsConfig": {"networkPolicyConfig": {"disabled": False}},
            }
        ).network_policy_enforced
        is True
    )
    # 4. Standard with networkPolicy.enabled=True but addon disabled -> False
    assert (
        parse_gcloud_cluster_describe(
            {
                "autopilot": {"enabled": False},
                "networkPolicy": {"enabled": True},
                "addonsConfig": {"networkPolicyConfig": {"disabled": True}},
            }
        ).network_policy_enforced
        is False
    )
    # 5. Standard with Legacy Datapath and no Calico -> False
    assert (
        parse_gcloud_cluster_describe(
            {
                "autopilot": {"enabled": False},
                "networkConfig": {"datapathProvider": "LEGACY_DATAPATH"},
            }
        ).network_policy_enforced
        is False
    )


class _ApiError(Exception):
    def __init__(self, status: int | None) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


def _service(cluster_ip: str) -> SimpleNamespace:
    return SimpleNamespace(spec=SimpleNamespace(cluster_ip=cluster_ip))


@pytest.mark.unit
@pytest.mark.parametrize(
    "response,expected",
    [
        pytest.param(_service("34.118.224.10"), "34.118.224.10", id="cluster-ip"),
        pytest.param(_service("None"), None, id="headless"),
        pytest.param(_ApiError(404), None, id="no-kube-dns-service"),
    ],
)
def test_probe_kube_dns_cluster_ip(response, expected) -> None:
    core_api = MagicMock()
    if isinstance(response, Exception):
        core_api.read_namespaced_service.side_effect = response
    else:
        core_api.read_namespaced_service.return_value = response
    assert probe_kube_dns_cluster_ip(core_api) == expected


@pytest.mark.unit
def test_probe_kube_dns_cluster_ip_raises_when_it_cannot_complete() -> None:
    core_api = MagicMock()
    core_api.read_namespaced_service.side_effect = _ApiError(403)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        probe_kube_dns_cluster_ip(core_api)


def _api_resource(name: str) -> dict:
    return {"name": name, "kind": name, "namespaced": True, "singularName": "", "verbs": ["get"]}


def _core_api_over_http(status: int, body: dict):
    """A real ``CoreV1Api`` on our ``TimeoutApiClient``; only urllib3 is stubbed.

    Exercising the wrapper and the library together is the point: the
    discovery probe once failed only because of how ``TimeoutApiClient``
    forwards ``call_api`` arguments.
    """
    import urllib3
    from kubernetes import client as k8s_client

    from harbor_gke_ext.client import TimeoutApiClient

    api_client = TimeoutApiClient()
    api_client.rest_client.pool_manager = MagicMock()
    api_client.rest_client.pool_manager.request.return_value = urllib3.HTTPResponse(
        body=json.dumps(body).encode(),
        status=status,
        headers={"Content-Type": "application/json"},
    )
    return k8s_client.CoreV1Api(api_client)


@pytest.mark.unit
@pytest.mark.parametrize(
    "status,body,expected",
    [
        pytest.param(
            200,
            {"groupVersion": "networking.gke.io/v1alpha1",
             "resources": [_api_resource("fqdnnetworkpolicies")]},
            True,
            id="resource-listed",
        ),
        pytest.param(
            200,
            {"groupVersion": "networking.gke.io/v1alpha1",
             "resources": [_api_resource("redirectservices")]},
            False,
            id="resource-not-listed",
        ),
        pytest.param(404, {"kind": "Status", "code": 404}, False, id="group-version-absent"),
    ],
)
def test_probe_fqdn_network_policy_support(status, body, expected) -> None:
    core_api = _core_api_over_http(status, body)

    assert probe_fqdn_network_policy_support(core_api) is expected
    call = core_api.api_client.rest_client.pool_manager.request.call_args
    assert call.args[0] == "GET"
    assert call.args[1].endswith("/apis/networking.gke.io/v1alpha1")


@pytest.mark.unit
def test_probe_fqdn_network_policy_support_raises_when_it_cannot_complete() -> None:
    """A transient failure must not read as "unsupported"."""
    core_api = _core_api_over_http(503, {"kind": "Status", "code": 503})
    with pytest.raises(RuntimeError, match="503"):
        probe_fqdn_network_policy_support(core_api)


_DATAPLANE_V2_DESCRIBE = {
    "autopilot": {"enabled": False},
    "networkConfig": {"datapathProvider": "ADVANCED_DATAPATH"},
}


def _gcloud_result(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


@pytest.mark.unit
def test_probe_cluster_via_gcloud_parses_describe() -> None:
    with patch(
        "harbor_gke_ext.cluster_probe.subprocess.run",
        return_value=_gcloud_result(stdout=json.dumps(_DATAPLANE_V2_DESCRIBE)),
    ) as run:
        caps = probe_cluster_via_gcloud("cl", "proj", "us-central1")

    assert caps.is_autopilot is False
    assert caps.network_policy_enforced is True
    cmd = run.call_args.args[0]
    assert cmd[:5] == ["gcloud", "container", "clusters", "describe", "cl"]
    assert {"--project=proj", "--location=us-central1", "--format=json"} <= set(cmd)


@pytest.mark.unit
@pytest.mark.parametrize(
    "run_outcome,match",
    [
        pytest.param(
            _gcloud_result(returncode=1, stderr="PERMISSION_DENIED"),
            "PERMISSION_DENIED",
            id="nonzero-exit",
        ),
        pytest.param(_gcloud_result(stdout="{not json"), "invalid JSON", id="bad-json"),
        pytest.param(_gcloud_result(stdout="[]"), "non-object", id="not-an-object"),
        pytest.param(
            _gcloud_result(stdout=json.dumps({"currentMasterVersion": "1.35.1"})),
            "NetworkPolicy enforcement cannot be determined",
            id="no-network-config",
        ),
        pytest.param(FileNotFoundError("gcloud"), "gcloud", id="gcloud-missing"),
        pytest.param(
            subprocess.TimeoutExpired(cmd="gcloud", timeout=15),
            "timed out",
            id="timeout",
        ),
    ],
)
def test_probe_cluster_via_gcloud_raises_when_it_cannot_verify(
    run_outcome, match
) -> None:
    kwargs = (
        {"side_effect": run_outcome}
        if isinstance(run_outcome, BaseException)
        else {"return_value": run_outcome}
    )
    with (
        patch("harbor_gke_ext.cluster_probe.subprocess.run", **kwargs),
        pytest.raises(RuntimeError, match=match),
    ):
        probe_cluster_via_gcloud("cl", "proj", "us-central1")
