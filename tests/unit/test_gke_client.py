"""Unit tests for src/harbor/environments/gke/client.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

import harbor_gke_ext.client as client_mod
from harbor_gke_ext.client import (
    KubernetesClientManager,
    TimeoutApiClient,
    _ensure_file_descriptor_limit,
    _extract_api_status_code,
    _is_matching_cluster,
    _shutdown_exec_executor,
)
from harbor_gke_ext.control_plane import is_control_plane_overload
from harbor_gke_ext.constants import (
    _GKE_API_CONNECT_TIMEOUT_SEC,
    _GKE_API_READ_TIMEOUT_SEC,
)
from harbor.utils.optional_import import MissingExtraError


@pytest.fixture(autouse=True)
def _reset_client_globals():
    """Ensure singleton and executor globals are cleaned between tests."""
    KubernetesClientManager._instance = None
    yield
    KubernetesClientManager._instance = None
    _shutdown_exec_executor()


# ── Global primitives and helper functions ─────────────────────────────


@pytest.mark.unit
def test_shutdown_exec_executor_swallows_exception():
    mock_executor = MagicMock()
    mock_executor.shutdown.side_effect = RuntimeError("shutdown fail")
    client_mod._GKE_EXEC_EXECUTOR = mock_executor

    _shutdown_exec_executor()
    assert client_mod._GKE_EXEC_EXECUTOR is None


@pytest.mark.unit
def test_ensure_file_descriptor_limit():
    # 1. resource module unavailable
    with patch.dict("sys.modules", {"resource": None}):
        _ensure_file_descriptor_limit()

    # 2. soft limit already >= target
    mock_resource = MagicMock()
    mock_resource.RLIMIT_NOFILE = 7
    mock_resource.getrlimit.return_value = (70000, 100000)
    with patch.dict("sys.modules", {"resource": mock_resource}):
        _ensure_file_descriptor_limit()
        mock_resource.setrlimit.assert_not_called()

    # 3. soft limit < target < hard limit
    mock_resource.reset_mock()
    mock_resource.getrlimit.return_value = (1024, 100000)
    with patch.dict("sys.modules", {"resource": mock_resource}):
        _ensure_file_descriptor_limit()
        mock_resource.setrlimit.assert_called_once_with(7, (65536, 100000))

    # 4. target > hard limit (caps at hard limit)
    mock_resource.reset_mock()
    mock_resource.getrlimit.return_value = (1024, 4096)
    with patch.dict("sys.modules", {"resource": mock_resource}):
        _ensure_file_descriptor_limit()
        mock_resource.setrlimit.assert_called_once_with(7, (4096, 4096))

    # 5. setrlimit raises exception (swallowed)
    mock_resource.reset_mock()
    mock_resource.getrlimit.return_value = (1024, 100000)
    mock_resource.setrlimit.side_effect = OSError("Operation not permitted")
    with patch.dict("sys.modules", {"resource": mock_resource}):
        _ensure_file_descriptor_limit()  # Should not raise


@pytest.mark.unit
def test_extract_api_status_code():
    # 1. Non-zero status
    exc = ApiException(status=404, reason="Not Found")
    assert _extract_api_status_code(exc) == 404

    # 2. Status is 0, extract from "Handshake status"
    exc_ws = ApiException(status=0)
    exc_ws.reason = "Handshake status 403 Forbidden"
    exc_ws.body = ""
    assert _extract_api_status_code(exc_ws) == 403

    # 3. Status is 0, extract from json code in body
    exc_json = ApiException(status=0)
    exc_json.reason = ""
    exc_json.body = '{"code": 409, "message": "Conflict"}'
    assert _extract_api_status_code(exc_json) == 409

    # 4. Status is 0, no code found
    exc_none = ApiException(status=0)
    exc_none.reason = "Something went wrong"
    exc_none.body = "Unknown error"
    assert _extract_api_status_code(exc_none) == 0


@pytest.mark.unit
def test_is_matching_cluster():
    # 1. Exact match on cluster field or name
    ctx1 = {"context": {"cluster": "my-cl"}, "name": "other-ctx"}
    assert _is_matching_cluster(ctx1, "my-cl") is True

    ctx2 = {"context": {"cluster": "other-cl"}, "name": "my-cl"}
    assert _is_matching_cluster(ctx2, "my-cl") is True

    # 2. GKE context format: gke_{project}_{location}_{cluster}
    ctx_gke = {
        "context": {"cluster": "gke_test-proj_us-central1-a_my-cl"},
        "name": "gke_test-proj_us-central1-a_my-cl",
    }
    # Matches with cluster and matching project + derived region
    assert (
        _is_matching_cluster(
            ctx_gke, "my-cl", project_id="test-proj", region="us-central1"
        )
        is True
    )
    assert (
        _is_matching_cluster(
            ctx_gke, "my-cl", project_id="test-proj", region="us-central1-a"
        )
        is True
    )

    # Cluster name mismatch
    assert _is_matching_cluster(ctx_gke, "different-cl") is False

    # Project mismatch
    assert (
        _is_matching_cluster(
            ctx_gke, "my-cl", project_id="wrong-proj", region="us-central1"
        )
        is False
    )

    # Region mismatch
    assert (
        _is_matching_cluster(
            ctx_gke, "my-cl", project_id="test-proj", region="europe-west1"
        )
        is False
    )

    # 3. Canonical GKE context format
    ctx_canonical = {
        "context": {"cluster": "gke_p_r_cl"},
        "name": "gke_p_r_cl",
    }
    assert _is_matching_cluster(ctx_canonical, "cl", project_id="p", region="r") is True
    # Test canonical match when context name matches but cluster name does not
    ctx_canonical_name = {
        "context": {"cluster": "other_cluster_name"},
        "name": "gke_p_r_cl",
    }
    assert (
        _is_matching_cluster(ctx_canonical_name, "cl", project_id="p", region="r")
        is True
    )

    # 4. Fallback suffix match when project_id is not specified
    ctx_suffix = {
        "context": {"cluster": "some_prefix_my-cl"},
        "name": "some_other_my-cl",
    }
    assert _is_matching_cluster(ctx_suffix, "my-cl") is True
    # If project_id is specified, fallback suffix match does not apply
    assert _is_matching_cluster(ctx_suffix, "my-cl", project_id="p") is False

    # 5. Non-matching entry
    ctx_unrelated = {
        "context": {"cluster": "other-cl"},
        "name": "other-name",
    }
    assert _is_matching_cluster(ctx_unrelated, "my-cl") is False


# ── TimeoutApiClient tests ─────────────────────────────────────────────


@pytest.mark.unit
def test_timeout_api_client_call_api_and_close():
    client = TimeoutApiClient()

    with patch("kubernetes.client.ApiClient.call_api") as mock_super_call:
        # Injects default timeout
        client.call_api("/test", "GET")
        mock_super_call.assert_called_once()
        _, kwargs = mock_super_call.call_args
        assert kwargs["_request_timeout"] == (
            _GKE_API_CONNECT_TIMEOUT_SEC,
            _GKE_API_READ_TIMEOUT_SEC,
        )

        mock_super_call.reset_mock()
        # Explicit timeout is preserved
        client.call_api("/test", "GET", _request_timeout=(10.0, 20.0))
        _, kwargs = mock_super_call.call_args
        assert kwargs["_request_timeout"] == (10.0, 20.0)

    # Test close with pool_manager
    client.rest_client = MagicMock()
    client.rest_client.pool_manager = MagicMock()
    with patch("kubernetes.client.ApiClient.close"):
        client.close()
        client.rest_client.pool_manager.clear.assert_called_once()

    # Test close when pool_manager.clear() raises
    client.rest_client.pool_manager.clear.side_effect = RuntimeError("clear error")
    with patch("kubernetes.client.ApiClient.close"):
        client.close()  # Should not raise


_WEBHOOK_HANDSHAKE_REASON = (
    "Handshake status 500 Internal Server Error -+-+- {} -+-+- "
    '{"message":"Internal error occurred: failed calling webhook '
    '\\"warden-validating.common-webhooks.networking.gke.io\\": failed to call '
    'webhook: context deadline exceeded","code":500}'
)


@pytest.mark.unit
def test_timeout_api_client_unmasks_websocket_api_exception():
    """kubernetes>=36 decodes e.body unconditionally, masking ws failures."""

    def masked_call(*args, **kwargs):
        try:
            raise ApiException(status=0, reason=_WEBHOOK_HANDSHAKE_REASON)
        except ApiException as e:
            e.body.decode("utf-8")  # ty: ignore[unresolved-attribute]

    client = TimeoutApiClient()
    with patch("kubernetes.client.ApiClient.call_api", side_effect=masked_call):
        with pytest.raises(ApiException) as exc_info:
            client.call_api("/api/v1/namespaces/ns/pods/p/exec", "GET")

    assert exc_info.value.status == 0
    assert "failed calling webhook" in exc_info.value.reason
    assert is_control_plane_overload(exc_info.value)


@pytest.mark.unit
def test_timeout_api_client_does_not_swallow_unrelated_attribute_error():
    client = TimeoutApiClient()
    with patch(
        "kubernetes.client.ApiClient.call_api",
        side_effect=AttributeError("unrelated"),
    ):
        with pytest.raises(AttributeError, match="unrelated"):
            client.call_api("/test", "GET")


@pytest.mark.unit
def test_timeout_api_client_websocket_failure_through_library_call_path():
    """Exercise the installed kubernetes ApiClient.__call_api, whatever its version.

    ws_client.websocket_call replaces ApiClient.request for exec and raises
    ApiException(status=0, body=None) on a failed handshake. The caller must
    see that ApiException, not a library-internal AttributeError.
    """
    client = TimeoutApiClient()
    with patch.object(
        client,
        "request",
        side_effect=ApiException(status=0, reason=_WEBHOOK_HANDSHAKE_REASON),
    ):
        with pytest.raises(ApiException) as exc_info:
            client.call_api(
                "/api/v1/namespaces/{namespace}/pods/{name}/exec",
                "GET",
                path_params={"namespace": "ns", "name": "p"},
                _preload_content=False,
            )

    assert exc_info.value.body is None
    assert is_control_plane_overload(exc_info.value)


# ── KubernetesClientManager tests ──────────────────────────────────────


@pytest.mark.unit
def test_manager_init_missing_extra_error():
    with patch("harbor_gke_ext.client._HAS_KUBERNETES", False):
        with pytest.raises(MissingExtraError):
            KubernetesClientManager()


@pytest.mark.unit
def test_manager_get_instance(monkeypatch):
    monkeypatch.setattr(KubernetesClientManager, "_instance", None)
    assert KubernetesClientManager.get_instance() is KubernetesClientManager.get_instance()


@pytest.mark.unit
def test_manager_init_client_matched_context_switch():
    mgr = KubernetesClientManager()

    contexts = [
        {"name": "ctx-other", "context": {"cluster": "other"}},
        {"name": "ctx-target", "context": {"cluster": "cl"}},
    ]
    active = contexts[0]
    with (
        patch(
            "kubernetes.config.list_kube_config_contexts",
            return_value=(contexts, active),
        ),
        patch("kubernetes.config.load_kube_config") as mock_load,
        patch("kubernetes.client.CoreV1Api"),
    ):
        mgr._init_client("cl", "us-central1", "p")
        mock_load.assert_called_once_with(context="ctx-target")
        assert mgr._initialized is True


@pytest.mark.unit
def test_manager_init_client_no_context_list_available():
    mgr = KubernetesClientManager()

    with (
        patch(
            "kubernetes.config.list_kube_config_contexts",
            side_effect=Exception("no contexts"),
        ),
        patch("kubernetes.config.load_kube_config") as mock_load,
        patch("kubernetes.client.CoreV1Api"),
    ):
        mgr._init_client("cl", "us-central1", "p")
        mock_load.assert_called_once_with()
        assert mgr._initialized is True


@pytest.mark.unit
def test_manager_init_client_gcloud_fallback_success_and_failure():
    mgr = KubernetesClientManager()

    contexts = [{"name": "ctx-other", "context": {"cluster": "other"}}]
    active = contexts[0]

    # Success with zonal region (--zone)
    with (
        patch(
            "kubernetes.config.list_kube_config_contexts",
            return_value=(contexts, active),
        ),
        patch("subprocess.run") as mock_run,
        patch("kubernetes.config.load_kube_config"),
        patch("kubernetes.client.CoreV1Api"),
    ):
        mock_run.return_value = MagicMock(returncode=0)
        mgr._init_client("cl", "us-central1-a", "p")
        mock_run.assert_called_once_with(
            [
                "gcloud",
                "container",
                "clusters",
                "get-credentials",
                "cl",
                "--zone",
                "us-central1-a",
                "--project",
                "p",
                "--quiet",
            ],
            capture_output=True,
            text=True,
        )
        assert mgr._initialized is True

    # Failure with regional region (--region)
    mgr2 = KubernetesClientManager()
    with (
        patch(
            "kubernetes.config.list_kube_config_contexts",
            return_value=(contexts, active),
        ),
        patch("subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=1, stderr="Cluster not found")
        with pytest.raises(RuntimeError, match="Failed to get GKE credentials"):
            mgr2._init_client("cl", "us-central1", "p")


@pytest.mark.unit
def test_manager_configure_initializes_once_and_pins_the_cluster():
    mgr = KubernetesClientManager()

    def fake_init(cluster_name, region, project_id):
        mgr._initialized = True
        mgr._cluster_name, mgr._region, mgr._project_id = cluster_name, region, project_id

    with (
        patch.object(mgr, "_init_client", side_effect=fake_init) as mock_init,
        patch("atexit.register") as mock_atexit,
    ):
        mgr.configure("cl", "us-central1", "p")
        mgr.configure("cl", "us-central1", "p")

        mock_init.assert_called_once_with("cl", "us-central1", "p")
        mock_atexit.assert_called_once_with(mgr._cleanup_sync)
        for other in (
            ("diff-cl", "us-central1", "p"),
            ("cl", "diff-region", "p"),
            ("cl", "us-central1", "diff-p"),
        ):
            with pytest.raises(ValueError, match="already initialized for cluster"):
                mgr.configure(*other)


@pytest.fixture
def configured_manager():
    mgr = KubernetesClientManager()
    with (
        patch.object(mgr, "configure") as mock_configure,
        patch("harbor_gke_ext.client.TimeoutApiClient", side_effect=MagicMock),
        patch("kubernetes.client.CoreV1Api") as mock_core_v1,
    ):
        mock_core_v1.side_effect = lambda api_client: MagicMock(api_client=api_client)
        yield mgr, mock_configure


@pytest.mark.unit
@pytest.mark.asyncio
async def test_manager_get_client_and_release(configured_manager):
    mgr, mock_configure = configured_manager

    client1 = await mgr.get_client("cl", "us-central1", "p")
    client2 = await mgr.get_client("cl", "us-central1", "p")
    assert client1 is not client2
    assert client1.api_client is not client2.api_client
    mock_configure.assert_called_with("cl", "us-central1", "p")
    assert mgr._reference_count == 2

    mgr.release_client(client1)
    client1.api_client.close.assert_called_once()
    assert mgr._reference_count == 1

    # A failing close is swallowed; the count never drops below zero.
    bad_client = MagicMock()
    bad_client.api_client.close.side_effect = RuntimeError("close error")
    mgr.release_client(bad_client)
    mgr.release_client()
    assert mgr._reference_count == 0


@pytest.mark.unit
def test_manager_scoped_client_closes_its_api_client(configured_manager):
    mgr, mock_configure = configured_manager

    with pytest.raises(RuntimeError, match="probe failed"):
        with mgr.scoped_client("cl", "us-central1", "p") as api:
            raise RuntimeError("probe failed")

    mock_configure.assert_called_once_with("cl", "us-central1", "p")
    api.api_client.close.assert_called_once()
