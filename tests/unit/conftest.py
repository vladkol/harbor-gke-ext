"""Shared fixtures for harbor-gke unit tests."""

from __future__ import annotations

import pytest
from fake_kubelet import FakeKubelet

from harbor_gke_ext import control_plane
from harbor_gke_ext.exec_stream import shutdown_exec_reactor


@pytest.fixture
def fake_kubelet():
    kubelet = FakeKubelet()
    yield kubelet
    kubelet.kill_leftovers()
    kubelet.join(timeout=5.0)
    shutdown_exec_reactor()


@pytest.fixture(autouse=True)
def _fresh_control_plane_limiter():
    """Give every test its own process-wide control-plane limiter.

    The limiter is module-global state: an overload recorded by one test would
    otherwise shrink the limit seen by the next, and its asyncio futures would
    outlive the test's event loop.
    """
    control_plane._CONTROL_PLANE_LIMITER = None
    yield
    control_plane._CONTROL_PLANE_LIMITER = None
