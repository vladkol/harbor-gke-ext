import asyncio
import contextlib
import io
import json
import os
import random
import shlex
import socket
import subprocess
import threading
import time
import unicodedata
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from harbor_gke_ext import exec_engine
from harbor_gke_ext.constants import GKEExecStreamClosedError, TrialContainerLostError
from harbor_gke_ext.exec_engine import (
    _SUPERVISED_ZERO_EXIT_CODE,
    _enable_tcp_keepalive,
    build_decoupled_launch_script,
    build_kill_script,
    build_recovery_probe_script,
    build_supervised_script,
    check_pod_terminated,
    connect_exec_stream,
    download_dir,
    download_file,
    poll_decoupled_exec,
    read_exec_output,
    run_exec_command,
    stream_tar_to_pod,
    unwrap_supervised_exit_code,
    upload_dir,
    upload_file,
)
from harbor_gke_ext.exec_stream import ExecOutputAccumulator, ExecStream

# ============================================================================
# check_pod_terminated tests
# ============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "setup_fn,target_container,expected_error",
    [
        (
            lambda api, pod: setattr(
                api.read_namespaced_pod,
                "side_effect",
                ApiException(status=404, reason="Not Found"),
            ),
            None,
            "does not exist in cluster",
        ),
        (
            lambda api, pod: setattr(
                api.read_namespaced_pod,
                "side_effect",
                ApiException(status=500, reason="Internal Server Error"),
            ),
            None,
            None,
        ),
        (
            lambda api, pod: setattr(
                pod.metadata, "deletion_timestamp", "2026-09-11T12:00:00Z"
            ),
            None,
            "is terminating",
        ),
        (
            lambda api, pod: setattr(pod.status, "reason", "Evicted"),
            None,
            "was evicted or lost",
        ),
        (
            lambda api, pod: setattr(pod.status, "phase", "Failed"),
            None,
            "is in terminal phase",
        ),
        (
            lambda api, pod: setattr(pod.spec, "node_name", None),
            None,
            "does not have a host assigned",
        ),
        (
            lambda api, pod: setattr(
                pod.status,
                "container_statuses",
                [
                    MagicMock(
                        name="main",
                        state=MagicMock(
                            terminated=MagicMock(reason="OOMKilled", exit_code=137)
                        ),
                        last_state=MagicMock(terminated=None),
                    )
                ],
            ),
            None,
            "has terminated",
        ),
        (
            lambda api, pod: setattr(
                pod.status,
                "container_statuses",
                [
                    MagicMock(
                        name="custom",
                        state=MagicMock(terminated=None, running=False),
                        last_state=MagicMock(
                            terminated=MagicMock(reason="Error", exit_code=1)
                        ),
                    )
                ],
            ),
            "custom",
            "has terminated",
        ),
        (
            lambda api, pod: None,
            None,
            None,
        ),
        (
            lambda api, pod: setattr(
                pod.status,
                "container_statuses",
                [
                    MagicMock(
                        name="other",
                        state=MagicMock(
                            terminated=MagicMock(reason="Error", exit_code=1)
                        ),
                    ),
                    MagicMock(
                        name="main",
                        state=MagicMock(terminated=None, running=True),
                        last_state=MagicMock(terminated=None),
                    ),
                ],
            ),
            "main",
            None,
        ),
    ],
)
async def test_check_pod_terminated_scenarios(
    setup_fn, target_container, expected_error
):
    api = MagicMock()
    pod = MagicMock()
    pod.metadata.deletion_timestamp = None
    pod.status.reason = None
    pod.status.phase = "Running"
    pod.spec.node_name = "node-1"
    cs = MagicMock()
    cs.name = "main"
    cs.state.terminated = None
    cs.state.running = True
    cs.last_state.terminated = None
    pod.status.container_statuses = [cs]
    pod.status.init_container_statuses = []
    api.read_namespaced_pod.return_value = pod

    setup_fn(api, pod)
    for c in pod.status.container_statuses:
        if isinstance(getattr(c, "_mock_name", None), str) and c._mock_name in (
            "main",
            "custom",
            "other",
        ):
            c.name = c._mock_name

    if expected_error:
        with pytest.raises(RuntimeError, match=expected_error) as exc_info:
            await check_pod_terminated(
                api, "pod-1", "default", target_container=target_container
            )
        # Lost Pods/containers are retryable infra failures; an unscheduled Pod
        # lost nothing, so it must not be classified as lost.
        expect_lost = expected_error != "does not have a host assigned"
        assert isinstance(exc_info.value, TrialContainerLostError) is expect_lost
    else:
        await check_pod_terminated(
            api, "pod-1", "default", target_container=target_container
        )


# ============================================================================
# connect_exec_stream tests
# ============================================================================


def _fake_ws_client():
    """A handshake result whose ``.sock.sock`` is a real, connected socket."""
    client, peer = socket.socketpair()
    ws_client = MagicMock()
    ws_client.sock.sock = client
    return ws_client, peer


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_hands_socket_to_reactor_with_keepalive():
    api = MagicMock()
    ws_client, peer = _fake_ws_client()
    with patch("harbor_gke_ext.exec_engine.stream", return_value=ws_client):
        stream = await connect_exec_stream(
            api, "pod-1", "default", ["echo", "hi"], container="main"
        )
    assert isinstance(stream, ExecStream)
    assert stream.label == "pod-1/main"
    raw = ws_client.sock.sock
    assert raw.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
    stream.close()
    assert await asyncio.wait_for(stream.read(), 5) is None
    peer.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_handshake_runs_in_exec_pool():
    """The blocking handshake must not run on the loop or the default executor."""
    api = MagicMock()
    ws_client, peer = _fake_ws_client()
    seen: list[str] = []

    def fake_stream(*args, **kwargs):
        seen.append(threading.current_thread().name)
        return ws_client

    with patch("harbor_gke_ext.exec_engine.stream", side_effect=fake_stream):
        stream = await connect_exec_stream(api, "pod-1", "default", ["ls"])
    assert seen and seen[0].startswith("gke-exec")
    stream.close()
    peer.close()


@pytest.mark.unit
def test_enable_tcp_keepalive_platform_branches(monkeypatch):
    raw = MagicMock()
    monkeypatch.setattr(socket, "TCP_KEEPIDLE", 4, raising=False)
    monkeypatch.setattr(socket, "TCP_KEEPINTVL", 5, raising=False)
    monkeypatch.setattr(socket, "TCP_KEEPCNT", 6, raising=False)
    _enable_tcp_keepalive(raw)
    assert raw.setsockopt.call_count == 4

    raw.reset_mock()
    monkeypatch.delattr(socket, "TCP_KEEPIDLE", raising=False)
    monkeypatch.delattr(socket, "TCP_KEEPALIVE", raising=False)
    monkeypatch.setattr(socket, "SIO_KEEPALIVE_VALS", 0x98000004, raising=False)
    _enable_tcp_keepalive(raw)
    raw.ioctl.assert_called_once()

    raw.setsockopt.side_effect = OSError("not supported")
    _enable_tcp_keepalive(raw)  # must not raise


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_status_400_with_pod_check():
    api = MagicMock()
    ws_client, peer = _fake_ws_client()
    exc = ApiException(status=400, reason="Bad Request")

    with (
        patch("harbor_gke_ext.exec_engine.stream", side_effect=[exc, ws_client]),
        patch("harbor_gke_ext.exec_engine.check_pod_terminated", return_value=None),
        patch("asyncio.sleep", return_value=None),
    ):
        stream = await connect_exec_stream(
            api, "pod-1", "default", ["ls"], max_attempts=2
        )
    assert isinstance(stream, ExecStream)
    stream.close()
    peer.close()


def _handshake_error(status: int, reason: str, body: str | None = None) -> ApiException:
    exc = ApiException(status=status, reason=reason)
    exc.body = body
    return exc


def _unscheduled_pod() -> MagicMock:
    pod = MagicMock()
    pod.metadata.deletion_timestamp = None
    pod.status.reason = None
    pod.status.phase = "Pending"
    pod.spec.node_name = None
    return pod


_POD_CHECKING_HANDSHAKE_ERRORS = {
    "http_400": lambda: _handshake_error(400, "Bad Request"),
    "websocket_404": lambda: _handshake_error(0, "Handshake status 404 Not Found"),
    "kubelet_container_not_found": lambda: _handshake_error(
        500, "Internal Server Error", 'container not found ("main")'
    ),
}


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "make_error",
    list(_POD_CHECKING_HANDSHAKE_ERRORS.values()),
    ids=list(_POD_CHECKING_HANDSHAKE_ERRORS),
)
@pytest.mark.parametrize(
    ("pod_read", "expected"),
    [
        pytest.param(
            {"side_effect": ApiException(status=404, reason="Not Found")},
            TrialContainerLostError,
            id="pod_gone",
        ),
        pytest.param(
            {"return_value": _unscheduled_pod()},
            ApiException,
            id="pod_unscheduled",
        ),
    ],
)
async def test_connect_exec_stream_reports_what_the_pod_check_found(
    make_error, pod_read, expected
):
    """A failed handshake against a dead Pod is a lost trial, not an API error.

    Harbor's retry filter matches exception class names, so a vanished Pod must
    surface as ``TrialContainerLostError``. An unscheduled Pod lost nothing and
    keeps the handshake error. Neither case is worth another attempt.
    """
    api = MagicMock()
    api.read_namespaced_pod.configure_mock(**pod_read)
    handshake_error = make_error()
    with (
        patch(
            "harbor_gke_ext.exec_engine.stream", side_effect=handshake_error
        ) as mock_stream,
        patch("asyncio.sleep", return_value=None),
        pytest.raises(expected) as raised,
    ):
        await connect_exec_stream(api, "pod-1", "default", ["ls"], max_attempts=3)

    assert mock_stream.call_count == 1
    if expected is TrialContainerLostError:
        assert raised.value.__cause__ is handshake_error
    else:
        assert raised.value is handshake_error


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_network_error_max_attempts():
    api = MagicMock()
    with (
        patch(
            "harbor_gke_ext.exec_engine.stream",
            side_effect=BrokenPipeError("Broken pipe"),
        ),
        patch("asyncio.sleep", return_value=None),
    ):
        with pytest.raises(BrokenPipeError):
            await connect_exec_stream(api, "pod-1", "default", ["ls"], max_attempts=2)


_WARDEN_HANDSHAKE_REASON = (
    "Handshake status 500 Internal Server Error -+-+- {'audit-id': 'x'} -+-+- "
    'b\'{"kind":"Status","message":"Internal error occurred: failed calling webhook '
    '\\"warden-validating.common-webhooks.networking.gke.io\\": context deadline '
    'exceeded","code":500}\''
)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_webhook_overload_shrinks_limiter_and_retries():
    from limiter_probe import free_slots

    api = MagicMock()
    ws_client, peer = _fake_ws_client()
    exc = ApiException(status=0, reason=_WARDEN_HANDSHAKE_REASON)
    sleep_mock = AsyncMock()
    with (
        patch("harbor_gke_ext.exec_engine.stream", side_effect=[exc, ws_client]),
        patch("harbor_gke_ext.exec_engine.asyncio.sleep", sleep_mock),
    ):
        stream = await connect_exec_stream(
            api, "pod-1", "default", ["ls"], max_attempts=3
        )
    assert isinstance(stream, ExecStream)
    # Halved once, and the handshake slot was handed back.
    assert await free_slots() == 64
    (delay,) = [c.args[0] for c in sleep_mock.await_args_list]
    assert 1.6 <= delay <= 2.4
    stream.close()
    peer.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_konnectivity_error_does_not_shrink_limiter():
    from limiter_probe import free_slots

    api = MagicMock()
    ws_client, peer = _fake_ws_client()
    exc = ApiException(status=500, reason="Internal Server Error")
    exc.body = '{"message":"error dialing backend: No agent available"}'
    with (
        patch("harbor_gke_ext.exec_engine.stream", side_effect=[exc, ws_client]),
        patch("harbor_gke_ext.exec_engine.asyncio.sleep", AsyncMock()),
    ):
        stream = await connect_exec_stream(
            api, "pod-1", "default", ["ls"], max_attempts=3
        )
    assert await free_slots() == 128
    stream.close()
    peer.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_without_socket_is_an_error():
    api = MagicMock()
    ws_client = MagicMock()
    ws_client.sock = None
    with patch("harbor_gke_ext.exec_engine.stream", return_value=ws_client):
        with pytest.raises(GKEExecStreamClosedError, match="no connected socket"):
            await connect_exec_stream(api, "pod-1", "default", ["ls"])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_isolates_api_client_and_bounds_handshake_timeout():
    from kubernetes import client as k8s_client
    from kubernetes.stream import ws_client as k8s_ws_client
    from harbor_gke_ext.constants import _GKE_EXEC_HANDSHAKE_TIMEOUT_SEC
    from harbor_gke_ext.exec_engine import _isolated_exec_api

    cfg = k8s_client.Configuration()
    shared_api = k8s_client.CoreV1Api(k8s_client.ApiClient(cfg))
    orig_request = shared_api.api_client.request

    isolated = _isolated_exec_api(shared_api)
    assert isolated is not shared_api
    assert isolated.api_client is not shared_api.api_client
    assert isolated.api_client.configuration is shared_api.api_client.configuration

    ws_client, peer = _fake_ws_client()
    captured_methods = []
    captured_kwargs = []

    def fake_stream(api_method, *args, **kwargs):
        captured_methods.append(api_method)
        captured_kwargs.append(kwargs)
        # Mutating the isolated api_client.request must not affect shared_api
        api_method.__self__.api_client.request = "mutated"
        return ws_client

    with patch("harbor_gke_ext.exec_engine.stream", side_effect=fake_stream):
        s = await connect_exec_stream(shared_api, "pod-1", "default", ["ls"])
    assert shared_api.api_client.request == orig_request
    assert captured_methods[0].__self__ is not shared_api
    assert captured_kwargs[0]["_request_timeout"] == _GKE_EXEC_HANDSHAKE_TIMEOUT_SEC

    ws_instance = k8s_ws_client.WebSocket()
    assert ws_instance.sock_opt.timeout == _GKE_EXEC_HANDSHAKE_TIMEOUT_SEC

    s.close()
    peer.close()


# ============================================================================
# Reading commands through the fake kubelet
# ============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
async def test_read_exec_output_collects_channels_and_exit_code(fake_kubelet):
    chunks: list[tuple[str, str]] = []

    async def callback(text, stream_name):
        chunks.append((stream_name, text))

    stream = await fake_kubelet.connect(
        ["sh", "-c", "printf 'héllo '; printf oops >&2; printf world; exit 5"]
    )
    output = ExecOutputAccumulator(callback)
    await read_exec_output(stream, output)
    assert output.stdout == "héllo world"
    assert output.stderr == "oops"
    assert stream.returncode() == 5
    assert "".join(t for s, t in chunks if s == "stdout") == "héllo world"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_read_exec_output_raises_on_lost_stream_and_keeps_partial(fake_kubelet):
    fake_kubelet.drop_stream_after = lambda argv: 6
    stream = await fake_kubelet.connect(
        ["sh", "-c", "printf 'first-'; sleep 0.3; printf second"]
    )
    output = ExecOutputAccumulator()
    with pytest.raises(GKEExecStreamClosedError, match="connection_lost"):
        await read_exec_output(stream, output)
    assert output.stdout_bytes == 6
    await output.finish()
    assert output.stdout == "first-"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exec_failure_status_without_exit_code_is_reported(fake_kubelet):
    fake_kubelet.status_override = lambda argv: json.dumps(
        {"status": "Failure", "message": "container not found (main)"}
    ).encode()
    stream = await fake_kubelet.connect(["true"])
    await read_exec_output(stream, ExecOutputAccumulator())
    with pytest.raises(GKEExecStreamClosedError, match="container not found"):
        stream.returncode()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_exec_command_timeout_closes_stream(fake_kubelet):
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await run_exec_command(fake_kubelet.connect, ["sleep", "5"], timeout_sec=0.3)
    assert time.monotonic() - started < 2


# ============================================================================
# Recovery probe script (runs under the local /bin/sh)
# ============================================================================


def _run_probe(workdir: Path, so: int = 0, se: int = 0) -> tuple[str, bytes, bytes]:
    script = build_recovery_probe_script(str(workdir), so, se)
    proc = subprocess.run(["sh", "-c", script], capture_output=True, timeout=10)
    header, _, body = proc.stdout.partition(b"\n")
    return header.decode(), body, proc.stderr


def _dead_pid() -> int:
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def _make_workdir(tmp_path: Path, *, out=b"", err=b"", pid=None, exitcode=None):
    w = tmp_path / "harbor_probe"
    w.mkdir()
    (w / "stdout").write_bytes(out)
    (w / "stderr").write_bytes(err)
    if pid is not None:
        (w / "pid").write_text(f"{pid}\n")
    if exitcode is not None:
        (w / "exitcode").write_text(f"{exitcode}\n")
    return w


@pytest.mark.unit
def test_probe_done_returns_output_from_offsets_and_is_idempotent(tmp_path):
    w = _make_workdir(
        tmp_path, out=b"0123456789", err=b"abcdef", pid=_dead_pid(), exitcode=3
    )
    first = _run_probe(w, so=4, se=2)
    second = _run_probe(w, so=4, se=2)
    assert first == ("DONE:3", b"456789", b"cdef")
    assert second == first, "the probe must not change state"
    assert (w / "exitcode").exists() and (w / "stdout").exists()


@pytest.mark.unit
def test_probe_done_wins_over_dead_process(tmp_path):
    """Liveness is sampled before the exit code: a finished command is DONE, not DEAD."""
    w = _make_workdir(tmp_path, pid=_dead_pid(), exitcode=0)
    assert _run_probe(w)[0] == "DONE:0"


@pytest.mark.unit
def test_probe_status_file_only(tmp_path):
    (tmp_path / "harbor_probe.status").write_text("0\n")
    header, body, err = _run_probe(tmp_path / "harbor_probe")
    assert (header, body, err) == ("DONE_NO_OUTPUT:0", b"", b"")
    assert (tmp_path / "harbor_probe.status").exists()


@pytest.mark.unit
def test_probe_lost_is_silent_on_stderr(tmp_path):
    assert _run_probe(tmp_path / "harbor_probe") == ("LOST", b"", b"")


@pytest.mark.unit
def test_probe_dead_process_without_exit_code(tmp_path):
    w = _make_workdir(tmp_path, out=b"x", pid=_dead_pid())
    assert _run_probe(w)[0] == "DEAD"


@pytest.mark.unit
def test_probe_running_streams_new_output(tmp_path):
    sleeper = subprocess.Popen(["sleep", "30"])
    try:
        w = _make_workdir(tmp_path, out=b"hello world", err=b"warn", pid=sleeper.pid)
        assert _run_probe(w, so=6, se=0) == ("RUNNING", b"world", b"warn")
    finally:
        sleeper.kill()
        sleeper.wait()


@pytest.mark.unit
def test_probe_running_before_pid_file_exists(tmp_path):
    w = _make_workdir(tmp_path)
    assert _run_probe(w)[0] == "RUNNING"


# ============================================================================
# poll_decoupled_exec against real local commands
# ============================================================================


def _detach(workdir: str, command: str) -> None:
    """Launch ``command`` exactly as decoupled mode does, on the local machine."""
    script = build_decoupled_launch_script(workdir, command)
    subprocess.run(["sh", "-c", script], check=True, timeout=10)


async def _poll(fake_kubelet, workdir, *, output=None, timeout_sec=30, fast=True):
    ctx = (
        patch("harbor_gke_ext.exec_engine._RECOVERY_INITIAL_POLL_INTERVAL_SEC", 0.1)
        if fast
        else contextlib.nullcontext()
    )
    with (
        ctx,
        patch("harbor_gke_ext.exec_engine.check_pod_terminated", AsyncMock()),
    ):
        return await poll_decoupled_exec(
            api=MagicMock(),
            pod_name="pod-1",
            namespace="default",
            workdir=workdir,
            output=output or ExecOutputAccumulator(),
            timeout_sec=timeout_sec,
            start_time=time.monotonic(),
            connect=fake_kubelet.connect,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_follows_running_command_to_done_and_cleans_up(
    fake_kubelet, tmp_path
):
    workdir = str(tmp_path / "harbor_run")
    _detach(workdir, "printf one; sleep 0.5; printf ' two'; printf e >&2; exit 4")
    res = await _poll(fake_kubelet, workdir)
    assert (res.stdout, res.stderr, res.return_code) == ("one two", "e", 4)
    assert not Path(workdir).exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_resumes_after_streamed_bytes(fake_kubelet, tmp_path):
    workdir = str(tmp_path / "harbor_resume")
    _detach(workdir, "printf 'abc€def'; exit 0")
    await asyncio.sleep(0.3)
    output = ExecOutputAccumulator()
    # The live stream had delivered "abc" and the first byte of "€" before it died.
    await output.feed("stdout", "abc€".encode()[:4])
    res = await _poll(fake_kubelet, workdir, output=output)
    assert res.stdout == "abc€def"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_survives_a_lost_probe_answer(fake_kubelet, tmp_path):
    """Regression for the root cause of the old recovery failures.

    The first probe runs to completion on the Pod, but its answer is lost in
    transit. The old probe deleted the status in that same exec, so the next
    probe answered LOST. The read-only probe must still answer DONE.
    """
    workdir = str(tmp_path / "harbor_lost_answer")
    _detach(workdir, "printf done; exit 9")
    await asyncio.sleep(0.3)
    probes = {"n": 0}

    def drop_first_probe(argv):
        if "DONE_NO_OUTPUT" in argv[-1]:
            probes["n"] += 1
            if probes["n"] == 1:
                return 1  # deliver one byte of the answer, then vanish
        return None

    fake_kubelet.drop_stream_after = drop_first_probe
    res = await _poll(fake_kubelet, workdir)
    assert probes["n"] >= 2
    assert (res.stdout, res.return_code) == ("done", 9)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_status_file_only(fake_kubelet, tmp_path):
    workdir = str(tmp_path / "harbor_status")
    Path(f"{workdir}.status").write_text("6\n")
    output = ExecOutputAccumulator()
    await output.feed("stdout", b"streamed")
    res = await _poll(fake_kubelet, workdir, output=output)
    assert (res.stdout, res.return_code) == ("streamed", 6)
    assert not Path(f"{workdir}.status").exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_lost_raises(fake_kubelet, tmp_path):
    with pytest.raises(GKEExecStreamClosedError, match="could not be recovered"):
        await _poll(fake_kubelet, str(tmp_path / "harbor_missing"))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_dead_process_reports_failure(fake_kubelet, tmp_path):
    w = _make_workdir(tmp_path, out=b"partial", pid=_dead_pid())
    res = await _poll(fake_kubelet, str(w))
    assert (res.stdout, res.return_code) == ("partial", 1)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_timeout_kills_command(fake_kubelet, tmp_path):
    workdir = str(tmp_path / "harbor_timeout")
    _detach(workdir, "sleep 3")
    pid = int(Path(workdir, "pid").read_text())
    res = await _poll(fake_kubelet, workdir, timeout_sec=1)
    assert res.return_code == 124
    assert "timed out after 1 seconds" in res.stderr
    assert not Path(workdir).exists()
    await asyncio.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_poll_raises_container_lost_on_404(tmp_path):
    async def connect(*args, **kwargs):
        raise ApiException(status=404, reason="Not Found")

    with (
        patch("harbor_gke_ext.exec_engine.check_pod_terminated", AsyncMock()),
        pytest.raises(TrialContainerLostError),
    ):
        await poll_decoupled_exec(
            api=MagicMock(),
            pod_name="pod-1",
            namespace="default",
            workdir=str(tmp_path / "w"),
            output=ExecOutputAccumulator(),
            timeout_sec=10,
            start_time=time.monotonic(),
            connect=connect,
        )


# ============================================================================
# Uploads and downloads through the fake kubelet (the "Pod" is the local host)
# ============================================================================


@pytest.fixture
def local_pod(fake_kubelet):
    async def fake_connect(api, pod_name, namespace, command, **kwargs):
        kwargs.pop("dind_services", None)
        return await fake_kubelet.connect(command, **kwargs)

    with patch("harbor_gke_ext.exec_engine.connect_exec_stream", fake_connect):
        yield fake_kubelet


def _random_tree(root: Path) -> None:
    rng = random.Random(1234)
    for i in range(20):
        sub = root / f"d{i % 4}" / f"e{i % 3}"
        sub.mkdir(parents=True, exist_ok=True)
        (sub / f"f{i}.bin").write_bytes(rng.randbytes(rng.randint(0, 400_000)))
    (root / "unicode-é.txt").write_text("naïve ✓\n")


def _tree_digest(root: Path) -> dict[str, bytes]:
    # NFC: when the test host is macOS, bsdtar may extract names in NFD.
    return {
        unicodedata.normalize("NFC", str(p.relative_to(root))): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upload_and_download_file_roundtrip(local_pod, tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(os.urandom(1_500_000))
    remote = tmp_path / "pod" / "nested" / "target.bin"  # parent does not exist yet
    await upload_file(MagicMock(), "pod-1", "default", src, str(remote))
    assert remote.read_bytes() == src.read_bytes()

    back = tmp_path / "back" / "copy.bin"
    await download_file(MagicMock(), "pod-1", "default", str(remote), back)
    assert back.read_bytes() == src.read_bytes()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upload_and_download_dir_roundtrip(local_pod, tmp_path):
    src = tmp_path / "src"
    _random_tree(src)
    remote = tmp_path / "pod" / "dir"
    await upload_dir(MagicMock(), "pod-1", "default", src, str(remote))
    assert _tree_digest(remote) == _tree_digest(src)

    back = tmp_path / "back"
    await download_dir(MagicMock(), "pod-1", "default", str(remote), back)
    assert _tree_digest(back) == _tree_digest(src)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upload_dir_missing_source_is_a_no_op(local_pod, tmp_path):
    await upload_dir(MagicMock(), "pod-1", "default", tmp_path / "nope", "/tmp/x")
    assert local_pod.connections == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upload_into_unwritable_target_reports_exit_code(local_pod, tmp_path):
    src = tmp_path / "f.txt"
    src.write_text("x")
    blocker = tmp_path / "blocker"
    blocker.write_text("a file, not a directory")
    with pytest.raises(RuntimeError, match="exit code|closed prematurely|write tar"):
        await upload_file(
            MagicMock(), "pod-1", "default", src, str(blocker / "sub" / "f.txt")
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_missing_file_reports_failure(local_pod, tmp_path):
    with pytest.raises(RuntimeError, match="exit code"):
        await download_file(
            MagicMock(), "pod-1", "default", str(tmp_path / "missing"), tmp_path / "o"
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_that_is_a_directory(local_pod, tmp_path):
    (tmp_path / "adir").mkdir()
    (tmp_path / "adir" / "x").write_text("x")
    with pytest.raises(IsADirectoryError):
        await download_file(
            MagicMock(), "pod-1", "default", str(tmp_path / "adir"), tmp_path / "o"
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_lost_stream_is_not_a_short_file(local_pod, tmp_path):
    big = tmp_path / "big.bin"
    big.write_bytes(os.urandom(2_000_000))
    local_pod.drop_stream_after = lambda argv: 100_000
    with pytest.raises(GKEExecStreamClosedError, match="downloading"):
        await download_file(MagicMock(), "pod-1", "default", str(big), tmp_path / "o")
    assert not (tmp_path / "o").exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stream_tar_to_pod_remote_exits_early(fake_kubelet):
    stream = await fake_kubelet.connect(
        ["sh", "-c", "echo 'fatal: no tar here' >&2; exit 3"], stdin=True
    )
    buf = io.BytesIO(os.urandom(4 * 1024 * 1024))
    with pytest.raises(RuntimeError, match="fatal: no tar here|exit code 3"):
        await stream_tar_to_pod(stream, buf, pod_name="pod-1")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stream_tar_to_pod_times_out_when_remote_never_finishes(
    fake_kubelet, monkeypatch
):
    monkeypatch.setattr(exec_engine, "_TAR_UPLOAD_MIN_TIMEOUT_SEC", 0.5)
    stream = await fake_kubelet.connect(["sleep", "10"], stdin=True)
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="Timed out"):
        await stream_tar_to_pod(stream, io.BytesIO(b"x" * 1024), pod_name="pod-1")
    assert time.monotonic() - started < 3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_never_adopts_a_replacement_pod(tmp_path):
    """A 404 means the trial's Pod is gone; its state cannot move to another Pod.

    The Job controller replaces a disrupted Pod even mid-trial. Exec must not
    switch to that blank Pod: the agent's work would vanish while the trial
    carried on and produced a corrupt result. The trial is lost instead.
    """
    from harbor_gke_ext.environment import GKEEnvironment

    env = GKEEnvironment(
        environment_dir=tmp_path,
        environment_name="d3-test",
        session_id="sess-d3",
        trial_paths=MagicMock(
            environment_dir=tmp_path,
            agent_dir=tmp_path,
            verifier_dir=tmp_path,
            artifacts_dir=tmp_path,
        ),
        task_env_config=MagicMock(
            cpus=1,
            memory_mb=1024,
            storage_mb=2048,
            gpus=0,
            gpu_types=[],
            tpu=None,
            allow_internet=False,
            workdir="/app",
        ),
        project_id="proj",
        region="us-central1",
        cluster_name="cluster",
    )
    env._core_api = MagicMock()
    env.pod_name = "old-pod-111"
    env.job_name = "job-d3"
    replacement = MagicMock()
    replacement.metadata.name = "new-pod-222"
    replacement.metadata.deletion_timestamp = None
    replacement.status.phase = "Running"
    replacement.status.reason = None
    env._core_api.list_namespaced_pod.return_value = MagicMock(items=[replacement])
    env._core_api.read_namespaced_pod.side_effect = ApiException(
        status=404, reason="Not Found"
    )

    ws_404 = ApiException(status=0, reason="Handshake status 404 Not Found")
    with (
        patch.object(env, "_ensure_client", new_callable=AsyncMock),
        patch("harbor_gke_ext.exec_engine.stream", side_effect=ws_404) as mock_stream,
        pytest.raises(TrialContainerLostError, match="old-pod-111"),
    ):
        await env._connect_exec_stream(["true"])

    assert env.pod_name == "old-pod-111"
    assert mock_stream.call_count == 1
    env._core_api.list_namespaced_pod.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_dind_container_routing():
    """A DinD-delegated service is reached via `docker exec` on the native `main`."""
    api = MagicMock()
    ws_client, peer = _fake_ws_client()
    with patch(
        "harbor_gke_ext.exec_engine.stream", return_value=ws_client
    ) as mock_stream:
        stream = await connect_exec_stream(
            api,
            "pod-shape-c",
            "default",
            ["bash", "-c", "echo hello"],
            container="main",
            stdin=True,
            tty=False,
            dind_services=frozenset({"main"}),
        )
        _, kwargs = mock_stream.call_args
        assert kwargs["container"] == "main"
        assert kwargs["command"] == [
            "docker",
            "exec",
            "-i",
            "main",
            "bash",
            "-c",
            "echo hello",
        ]
    stream.close()
    peer.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_dind_routing_is_independent_of_pod_name():
    """Routing must not depend on the Pod name.

    Regression: the routing set used to live in a module-level dict keyed by
    ``(namespace, pod_name)``, registered before the Job controller had assigned
    the real Pod name. Every exec then missed the lookup and ran against the bare
    ``docker:dind`` rootfs, which surfaced as
    ``su: can't execute '/bin/bash': No such file or directory``.
    """
    api = MagicMock()
    dind_services = frozenset({"main"})
    for pod_name in ("orca-env", "orca-env-frdh2", "orca-env-x9q2p"):
        ws_client, peer = _fake_ws_client()
        with patch(
            "harbor_gke_ext.exec_engine.stream", return_value=ws_client
        ) as mock_stream:
            stream = await connect_exec_stream(
                api,
                pod_name,
                "default",
                ["bash", "-lc", "mkdir -p /app"],
                container="main",
                dind_services=dind_services,
            )
            _, kwargs = mock_stream.call_args
            assert kwargs["command"][:4] == ["docker", "exec", "main", "bash"]
        stream.close()
        peer.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connect_exec_stream_without_dind_services_is_unwrapped():
    """Shape A (all-native) must keep addressing containers directly."""
    api = MagicMock()
    ws_client, peer = _fake_ws_client()
    with patch(
        "harbor_gke_ext.exec_engine.stream", return_value=ws_client
    ) as mock_stream:
        stream = await connect_exec_stream(
            api,
            "pod-native",
            "default",
            ["bash", "-c", "echo hello"],
            container="main",
        )
        _, kwargs = mock_stream.call_args
        assert kwargs["container"] == "main"
        assert kwargs["command"] == ["bash", "-c", "echo hello"]
    stream.close()
    peer.close()


# ============================================================================
# Supervisor and kill scripts (run under the local /bin/sh)
# ============================================================================


def _procs_matching(pattern: str) -> list[str]:
    proc = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    return proc.stdout.split()


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


@pytest.mark.unit
@pytest.mark.parametrize(
    "cmd_rc,expected_wire_rc",
    [
        (0, _SUPERVISED_ZERO_EXIT_CODE),
        (7, 7),
        (_SUPERVISED_ZERO_EXIT_CODE, 0),
    ],
)
def test_supervised_script_streams_output_and_exit_code(
    tmp_path, cmd_rc, expected_wire_rc
):
    w = str(tmp_path / f"harbor_sup_{cmd_rc}")
    script = build_supervised_script(
        w, f"echo out-line; echo err-line >&2; exit {cmd_rc}"
    )
    proc = subprocess.run(["sh", "-c", script], capture_output=True, timeout=20)
    assert (proc.returncode, proc.stdout, proc.stderr) == (
        expected_wire_rc,
        b"out-line\n",
        b"err-line\n",
    )
    if cmd_rc != _SUPERVISED_ZERO_EXIT_CODE:
        assert not Path(w).exists()
        assert unwrap_supervised_exit_code(proc.returncode) == cmd_rc
    assert Path(f"{w}.status").read_text().strip() == str(cmd_rc)


@pytest.mark.unit
def test_supervised_script_survives_workdir_wipe(tmp_path):
    """A task that empties /tmp while running must not change the exit code.

    Previously the supervisor treated a missing workdir as a kill: with GNU tail
    it reported exit code 1, and with tails lacking ``--pid`` (busybox, macOS) it
    also returned before the command finished and lost its later output.
    """
    w = str(tmp_path / "harbor_wipe")
    command = f"echo before; sleep 0.5; rm -rf {shlex.quote(w)}; sleep 1.5; echo after; exit 0"
    script = build_supervised_script(w, command)
    start = time.monotonic()
    proc = subprocess.run(["sh", "-c", script], capture_output=True, timeout=30)
    assert unwrap_supervised_exit_code(proc.returncode) == 0, proc.stderr
    assert time.monotonic() - start >= 2.0, (
        "supervisor returned before the command ended"
    )
    assert proc.stdout == b"before\nafter\n"


@pytest.mark.unit
def test_decoupled_wrapper_recreates_workdir_for_exit_code(tmp_path):
    """Recovery needs the exit code even if the task removed the workdir."""
    w = tmp_path / "harbor_wipe_decoupled"
    _detach(str(w), f"sleep 0.3; rm -rf {shlex.quote(str(w))}; sleep 0.3; exit 5")
    assert _wait_until(lambda: (w / "exitcode").exists(), timeout=10)
    assert (w / "exitcode").read_text().strip() == "5"


@pytest.mark.unit
def test_kill_script_kills_command_tree_and_supervisor_exits(tmp_path):
    """Regression for F8/F9.

    F8: the old kill signalled only the wrapper subshell, so the command kept
    running. F9: the supervisor then waited forever for an exit code file.
    """
    marker = f"{os.getpid()}{random.randint(1000, 9999)}"
    w = str(tmp_path / "harbor_kill")
    command = f"sh -c 'sleep 41.{marker} & sleep 42.{marker}'"
    supervisor = subprocess.Popen(
        ["sh", "-c", build_supervised_script(w, command)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        assert _wait_until(lambda: len(_procs_matching(f"sleep 4[12].{marker}")) >= 2)
        subprocess.run(["sh", "-c", build_kill_script(w)], check=True, timeout=20)
        assert _wait_until(lambda: not _procs_matching(f"sleep 4[12].{marker}"))
        supervisor.wait(timeout=10)
        assert not Path(w).exists()
        assert not Path(f"{w}.status").exists()
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(supervisor.pid, 9)


@pytest.mark.unit
@pytest.mark.parametrize("pid_text", ["", "abc", "0", "1", "12 34"])
def test_kill_script_ignores_invalid_pid_files(tmp_path, pid_text):
    w = tmp_path / "harbor_badpid"
    w.mkdir()
    (w / "pid").write_text(pid_text)
    Path(f"{w}.status").write_text("0\n")
    proc = subprocess.run(
        ["sh", "-c", build_kill_script(str(w))], capture_output=True, timeout=20
    )
    assert proc.returncode == 0
    assert not w.exists() and not Path(f"{w}.status").exists()


@pytest.mark.unit
def test_kill_script_skips_tree_walk_for_finished_command(tmp_path):
    """A finished command's pid may have been reused; only its group is signalled."""
    w = tmp_path / "harbor_finished"
    w.mkdir()
    bystander = subprocess.Popen(["sleep", "30"])
    try:
        (w / "pid").write_text(f"{bystander.pid}\n")
        (w / "exitcode").write_text("0\n")
        subprocess.run(["sh", "-c", build_kill_script(str(w))], check=True, timeout=20)
        assert bystander.poll() is None, (
            "an unrelated process with a reused pid was killed"
        )
    finally:
        bystander.kill()
        bystander.wait()
