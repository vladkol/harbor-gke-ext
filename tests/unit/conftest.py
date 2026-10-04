"""Shared fixtures for harbor-gke unit tests."""

from __future__ import annotations

import pytest
from fake_kubelet import FakeKubelet

from harbor_gke_ext import (
    client,
    cloud_build,
    compose_spec,
    compose_translator,
    control_plane,
    environment,
    image_plan,
)
from harbor_gke_ext.exec_stream import shutdown_exec_reactor

_PROCESS_CACHED_FUNCTIONS = (
    client.ensure_gcloud_ready,
    client._gcloud_default_project_id,
    compose_spec._resolve_compose_command,
    compose_translator._cached_oci_manifest_facts,
    control_plane.get_control_plane_limiter,
)

_PROCESS_CACHE_DICTS = (
    environment._CLUSTER_CAPABILITIES_CACHE,
    environment._CLUSTER_ADMISSION_CONTROLLERS,
    cloud_build._REGISTRY_EXISTS_CACHE,
    cloud_build._REGISTRY_EXISTS_TASKS,
    cloud_build._REPO_EXISTS_CACHE,
    cloud_build._REPO_ENSURE_TASKS,
    image_plan._PLANNED_IMAGE_URLS,
)


def _clear_process_state() -> None:
    for fn in _PROCESS_CACHED_FUNCTIONS:
        fn.cache_clear()
    for cache in _PROCESS_CACHE_DICTS:
        cache.clear()
    image_plan._PLAN_PUBLISHED = False


def _no_real_cluster(*args, **kwargs):
    raise AssertionError(
        "A unit test tried to describe a real GKE cluster. Seed "
        "environment._CLUSTER_CAPABILITIES_CACHE or patch the probe."
    )


@pytest.fixture(autouse=True)
def _isolated_process_state(monkeypatch):
    """Give every test a fresh copy of the extension's process-wide state.

    Production memoizes cluster, registry, gcloud and limiter facts for the
    life of the Harbor process. Without a reset, a fact recorded by one test
    leaks into the next, and asyncio objects outlive the test's event loop.

    Registry manifest reads are stubbed to "nothing known" so no test reaches
    the network; tests that exercise the read patch the fetch themselves. The
    cluster capability probe refuses to describe a real cluster: tests seed
    `_CLUSTER_CAPABILITIES_CACHE` or patch the probes.
    """
    _clear_process_state()
    monkeypatch.setattr(
        compose_translator,
        "_fetch_oci_config_from_registry",
        lambda image_ref: ({}, None),
    )
    monkeypatch.setattr(environment, "probe_cluster_via_gcloud", _no_real_cluster)
    yield
    _clear_process_state()


@pytest.fixture
def fake_kubelet():
    kubelet = FakeKubelet()
    yield kubelet
    kubelet.kill_leftovers()
    kubelet.join(timeout=5.0)
    shutdown_exec_reactor()
