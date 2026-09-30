"""Unit tests for harbor_gke_ext.cluster_probe."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from harbor_gke_ext.cluster_probe import (
    DindAvailability,
    estimate_gke_allocatable_ephemeral_storage_mb,
    evaluate_autopilot_dind_capability,
    parse_gcloud_cluster_describe,
    parse_gke_minor_version,
    parse_quantity_to_mib,
    probe_max_node_ephemeral_storage_mb,
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
            resources=SimpleNamespace(requests={"cpu": "100m"}, limits=None)
        )
    )
    api = MagicMock()
    api.create_namespaced_pod.return_value = echoed

    assert probe_pod_level_resources_support(api) is True
    # Must be a dry run: the probe may never create anything.
    assert api.create_namespaced_pod.call_args.kwargs["dry_run"] == "All"


@pytest.mark.unit
def test_pod_level_resources_probe_detects_pruning() -> None:
    """An older API server drops the field silently rather than rejecting it."""
    api = MagicMock()
    api.create_namespaced_pod.return_value = SimpleNamespace(
        spec=SimpleNamespace(resources=None)
    )
    assert probe_pod_level_resources_support(api) is False


@pytest.mark.unit
def test_pod_level_resources_probe_unknown_on_error() -> None:
    """A failed probe is 'unknown' -- distinct from 'unsupported'."""
    api = MagicMock()
    api.create_namespaced_pod.side_effect = RuntimeError("403 Forbidden")
    assert probe_pod_level_resources_support(api) is None
    assert probe_pod_level_resources_support(None) is None


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


def _node(
    name: str,
    ephemeral: str | None,
    *,
    unschedulable: bool = False,
    taints: list[SimpleNamespace] | None = None,
):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(unschedulable=unschedulable, taints=taints),
        status=SimpleNamespace(
            allocatable=({"ephemeral-storage": ephemeral} if ephemeral else {})
        ),
    )


@pytest.mark.unit
def test_max_node_ephemeral_storage_takes_the_largest_schedulable_node() -> None:
    """The maximum, not the sum: a Pod is scheduled onto exactly one node."""
    api = MagicMock()
    api.list_node.return_value = SimpleNamespace(
        items=[
            _node("small-a", "47060329472"),  # 44,880 MiB
            _node("large", "364583615488"),  # 347,694 MiB
            _node("small-b", "47060329472"),
        ]
    )
    assert probe_max_node_ephemeral_storage_mb(api) == 347694


@pytest.mark.unit
def test_max_node_ephemeral_storage_skips_cordoned_nodes() -> None:
    api = MagicMock()
    api.list_node.return_value = SimpleNamespace(
        items=[
            _node("small", "47060329472"),
            _node("large-but-cordoned", "364583615488", unschedulable=True),
        ]
    )
    assert probe_max_node_ephemeral_storage_mb(api) == 44880


@pytest.mark.unit
def test_max_node_ephemeral_storage_skips_system_tainted_nodes_and_keeps_gpu_nodes() -> (
    None
):
    api = MagicMock()
    api.list_node.return_value = SimpleNamespace(
        items=[
            _node(
                "system-default-pool",
                "47060329472",
                taints=[
                    SimpleNamespace(
                        key="CriticalAddonsOnly", value="true", effect="NoSchedule"
                    )
                ],
            ),
            _node(
                "gpu-worker",
                "1242560000000",
                taints=[
                    SimpleNamespace(
                        key="nvidia.com/gpu", value="present", effect="NoSchedule"
                    )
                ],
            ),
        ]
    )
    assert probe_max_node_ephemeral_storage_mb(api) == 1184997


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
def test_max_node_ephemeral_storage_unknown_when_unreadable() -> None:
    api = MagicMock()
    api.list_node.side_effect = RuntimeError("connection refused")
    assert probe_max_node_ephemeral_storage_mb(api) is None
    assert probe_max_node_ephemeral_storage_mb(None) is None

    empty = MagicMock()
    empty.list_node.return_value = SimpleNamespace(items=[])
    assert probe_max_node_ephemeral_storage_mb(empty) is None


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

