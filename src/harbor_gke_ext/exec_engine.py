from __future__ import annotations

import asyncio
import copy
import datetime
import functools
import io
from pathlib import Path
import random
import shlex
import socket
import tarfile
import time
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from urllib3.exceptions import HTTPError

from harbor.environments.base import ExecResult
from harbor.environments.tar_transfer import (
    extract_dir_from_bytes,
    pack_dir_to_bytes,
)
from harbor_gke_ext.client import (
    _extract_api_status_code,
    _get_exec_executor,
)
from harbor_gke_ext.constants import (
    _GKE_EXEC_CONNECT_MAX_ATTEMPTS,
    _GKE_EXEC_HANDSHAKE_TIMEOUT_SEC,
    _GKE_TCP_KEEPALIVE_COUNT,
    _GKE_TCP_KEEPALIVE_IDLE_SEC,
    _GKE_TCP_KEEPALIVE_INTERVAL_SEC,
    GKEExecStreamClosedError,
    RemoteFileNotFoundError,
    TrialContainerLostError,
)
from harbor_gke_ext.control_plane import (
    get_control_plane_limiter,
    is_control_plane_overload,
    jittered_backoff_delay,
)
from harbor_gke_ext.exec_stream import (
    STDERR_CHANNEL,
    STDOUT_CHANNEL,
    ExecOutputAccumulator,
    ExecStream,
)
from harbor.utils.logger import logger

try:
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException
    from kubernetes.stream import stream

    _HAS_KUBERNETES = True
except ImportError:
    _HAS_KUBERNETES = False

if TYPE_CHECKING:
    from kubernetes import client as k8s_client

ConnectFn = Callable[..., Awaitable[ExecStream]]
"""Opens an exec stream: ``await connect(argv, container=..., stdin=...)``."""

# Recovery polling after a supervised command's stream was lost.
_RECOVERY_PROBE_TIMEOUT_SEC = 30.0
_RECOVERY_CLEANUP_TIMEOUT_SEC = 10.0
_RECOVERY_INITIAL_POLL_INTERVAL_SEC = 2.0
_RECOVERY_MAX_POLL_INTERVAL_SEC = 15.0


async def check_pod_terminated(
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    target_container: str | None = None,
) -> None:
    """Inspect pod status and fail fast if the pod or target container is terminated.

    Raises ``TrialContainerLostError`` when the Pod is gone, terminating, evicted,
    or in a terminal phase, or when the target container has exited. Raises a
    plain ``RuntimeError`` when the Pod is not scheduled yet (nothing was lost).
    Returns silently on transient API or network errors.
    """
    try:
        pod = await asyncio.to_thread(
            api.read_namespaced_pod,
            name=pod_name,
            namespace=namespace,
        )
    except ApiException as e:
        if _extract_api_status_code(e) == 404:
            raise TrialContainerLostError(
                f"Pod {pod_name} does not exist in cluster."
            ) from e
        return
    except (HTTPError, OSError) as e:
        logger.debug(
            "Transient network error checking pod %s termination status (%s); deferring to next poll.",
            pod_name,
            e,
        )
        return

    deletion_ts = (
        getattr(pod.metadata, "deletion_timestamp", None) if pod.metadata else None
    )
    if deletion_ts is not None and isinstance(deletion_ts, (datetime.datetime, str)):
        raise TrialContainerLostError(
            f"Pod {pod_name} is terminating (deletionTimestamp={deletion_ts})."
        )

    if pod.status and getattr(pod.status, "reason", None) in (
        "Evicted",
        "NodeLost",
        "Preempted",
        "UnexpectedAdmissionError",
        "DeadlineExceeded",
    ):
        raise TrialContainerLostError(
            f"Pod {pod_name} was evicted or lost (reason={pod.status.reason!r})."
        )

    phase = pod.status.phase if pod.status else None
    if phase in ("Failed", "Succeeded"):
        raise TrialContainerLostError(
            f"Pod {pod_name} is in terminal phase '{phase}' and cannot accept exec."
        )

    if pod.spec is None or not getattr(pod.spec, "node_name", None):
        raise RuntimeError(
            f"Pod {pod_name} does not have a host assigned (node_name is None). Cannot exec into unscheduled pod."
        )

    check_container = target_container or "main"
    if pod.status:
        all_statuses = list(pod.status.container_statuses or []) + list(
            pod.status.init_container_statuses or []
        )
        for cs in all_statuses:
            if cs.name != check_container:
                continue
            terminated = None
            if cs.state and cs.state.terminated:
                terminated = cs.state.terminated
            elif (
                cs.last_state
                and cs.last_state.terminated
                and not (cs.state and cs.state.running)
            ):
                terminated = cs.last_state.terminated
            if terminated is not None:
                reason = terminated.reason or ""
                exit_code = terminated.exit_code
                raise TrialContainerLostError(
                    f"Container '{cs.name}' in pod {pod_name} has terminated "
                    f"(reason={reason!r}, exit_code={exit_code}). Cannot exec into dead container."
                )


def _enable_tcp_keepalive(raw_sock: socket.socket) -> None:
    """Enable OS-level TCP keepalive so load balancers and NAT keep idle exec streams.

    Also lets the kernel detect a dead peer on an otherwise silent connection.
    """
    try:
        raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):  # Linux
            raw_sock.setsockopt(
                socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, _GKE_TCP_KEEPALIVE_IDLE_SEC
            )
            raw_sock.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_KEEPINTVL,
                _GKE_TCP_KEEPALIVE_INTERVAL_SEC,
            )
            raw_sock.setsockopt(
                socket.IPPROTO_TCP, socket.TCP_KEEPCNT, _GKE_TCP_KEEPALIVE_COUNT
            )
        elif hasattr(socket, "TCP_KEEPALIVE"):  # macOS
            raw_sock.setsockopt(
                socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, _GKE_TCP_KEEPALIVE_IDLE_SEC
            )
        elif hasattr(socket, "SIO_KEEPALIVE_VALS"):  # Windows
            getattr(raw_sock, "ioctl")(
                getattr(socket, "SIO_KEEPALIVE_VALS"),
                (
                    1,
                    int(_GKE_TCP_KEEPALIVE_IDLE_SEC * 1000),
                    int(_GKE_TCP_KEEPALIVE_INTERVAL_SEC * 1000),
                ),
            )
    except (OSError, ValueError, AttributeError) as e:
        logger.debug("Could not enable TCP keepalive on exec socket: %s", e)


def _isolated_exec_api(api: Any) -> Any:
    """Return a shallow clone of ``api`` with its own ``api_client`` instance.

    ``kubernetes.stream.stream`` temporarily monkey-patches
    ``api.api_client.request`` during the WebSocket handshake. Isolating
    ``api_client`` per handshake prevents concurrent handshakes or REST calls
    from observing or restoring a stale ``request`` callable.
    """
    if not _HAS_KUBERNETES or not isinstance(api, k8s_client.CoreV1Api):
        return api
    api_client = getattr(api, "api_client", None)
    if api_client is None or not isinstance(api_client, k8s_client.ApiClient):
        return api
    isolated_api = copy.copy(api)
    isolated_api.api_client = copy.copy(api_client)
    return isolated_api


_WS_HANDSHAKE_TIMEOUT_INSTALLED = False


def _ensure_bounded_ws_handshake_timeout() -> None:
    """Ensure ``kubernetes.stream.ws_client.WebSocket`` bounds its handshake socket timeout.

    ``ws_client.create_websocket`` constructs ``WebSocket`` without passing a
    ``timeout``, leaving ``sock_opt.timeout = None`` (unbounded blocking across
    TCP connect, TLS handshake, and HTTP 101 Upgrade). Once connected,
    ``ExecStream.attach`` switches the socket to non-blocking mode for the
    ``select`` reactor.
    """
    global _WS_HANDSHAKE_TIMEOUT_INSTALLED
    if _WS_HANDSHAKE_TIMEOUT_INSTALLED or not _HAS_KUBERNETES:
        return
    try:
        from kubernetes.stream import ws_client

        base_cls = getattr(ws_client, "WebSocket", None)
        if base_cls is not None and not getattr(
            base_cls, "_harbor_bounded_handshake", False
        ):

            class _BoundedHandshakeWebSocket(base_cls):  # type: ignore[valid-type,misc]
                _harbor_bounded_handshake = True

                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    if getattr(self.sock_opt, "timeout", None) is None:
                        self.sock_opt.timeout = _GKE_EXEC_HANDSHAKE_TIMEOUT_SEC

            setattr(ws_client, "WebSocket", _BoundedHandshakeWebSocket)
        _WS_HANDSHAKE_TIMEOUT_INSTALLED = True
    except Exception as e:
        logger.debug("Could not install bounded WebSocket handshake timeout: %s", e)


async def connect_exec_stream(
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    command: list[str],
    *,
    container: str | None = None,
    stderr: bool = True,
    stdin: bool = False,
    stdout: bool = True,
    tty: bool = False,
    dind_services: frozenset[str] | None = None,
    max_attempts: int = _GKE_EXEC_CONNECT_MAX_ATTEMPTS,
) -> ExecStream:
    """Establish a WebSocket exec connection to the pod, retrying transient API server/proxy errors.

    The Kubernetes client performs the handshake (URL, authentication, TLS and
    HTTP upgrade) in the dedicated ``gke-exec`` thread pool. The connected socket
    is then handed to the exec reactor (see ``exec_stream``), which owns all
    further I/O; the returned ``ExecStream`` is its asyncio handle.

    ``dind_services`` names the logical Compose services that execute *inside*
    ``dind-engine`` rather than as native Pod containers (RFC 0004, Shape B/C).
    A command targeting one of them cannot be addressed by ``container=`` at all,
    because the kubelet only knows about Pod containers -- it is reached by
    wrapping the argv in ``docker exec <service>`` against the native ``main``
    container, which proxies to the DinD daemon.

    This is passed in rather than looked up from a module-level registry keyed by
    Pod name. The Pod name is not stable: under the Job path the controller
    generates it, so anything registered before the spawn poll is filed under a
    name no caller ever queries, and the routing silently degrades to a raw exec
    against the ``docker:dind`` rootfs (which has no ``/bin/bash``). The set is a
    property of the translated Pod spec, known at build time, so the caller that
    owns the spec passes it directly and there is no key to get wrong.
    """
    _ensure_bounded_ws_handshake_timeout()
    target_service = container or "main"
    if dind_services and target_service in dind_services:
        docker_flags = ["-i"] if stdin else []
        if tty:
            docker_flags.append("-t")
        command = ["docker", "exec", *docker_flags, target_service, *command]
        container = "main"

    call_kwargs: dict[str, Any] = {
        "command": command,
        "stderr": stderr,
        "stdin": stdin,
        "stdout": stdout,
        "tty": tty,
        # The reactor parses raw frames; this only stops WSClient from decoding.
        "binary": True,
        "_preload_content": False,
        "_request_timeout": _GKE_EXEC_HANDSHAKE_TIMEOUT_SEC,
    }
    if container is not None:
        call_kwargs["container"] = container

    loop = asyncio.get_running_loop()
    label = f"{pod_name}/{container or 'default'}"
    limiter = get_control_plane_limiter()
    for attempt in range(max_attempts):
        try:
            isolated_api = _isolated_exec_api(api)
            async with limiter:
                ws_client = await loop.run_in_executor(
                    _get_exec_executor(),
                    functools.partial(
                        stream,
                        isolated_api.connect_get_namespaced_pod_exec,
                        pod_name,
                        namespace,
                        **call_kwargs,
                    ),
                )
            limiter.record_success()
            raw_sock = getattr(getattr(ws_client, "sock", None), "sock", None)
            if raw_sock is None:
                raise GKEExecStreamClosedError(
                    f"Exec handshake with pod {pod_name} returned no connected socket"
                )
            _enable_tcp_keepalive(raw_sock)
            return ExecStream.attach(raw_sock, label=label, keepalive_ref=ws_client)
        except ApiException as e:
            if is_control_plane_overload(e):
                limiter.record_overload()
            status_code = _extract_api_status_code(e)
            is_transient = status_code in (429, 500, 502, 503, 504)
            # The handshake error alone cannot tell a dead Pod from a slow one, so
            # these branches ask the Pod. A lost Pod is reported as such: Harbor's
            # retry filter matches exception class names, and a lost trial must not
            # look like an API failure. A Pod that is not scheduled lost nothing;
            # it ends the retries with the handshake error.
            if not is_transient and status_code in (400, 404, 0):
                try:
                    await check_pod_terminated(
                        api, pod_name, namespace, target_container=container
                    )
                    is_transient = True
                except TrialContainerLostError as lost:
                    raise lost from e
                except RuntimeError:
                    is_transient = False
            elif is_transient and "container not found" in str(e.body or "").lower():
                # Kubelet answers HTTP 500 "container not found" both while a container is
                # still being created and after the Pod has died. Only the former is
                # retryable, so consult the Pod's actual state instead of spinning through
                # the full retry budget against a terminal Pod.
                try:
                    await check_pod_terminated(
                        api, pod_name, namespace, target_container=container
                    )
                except TrialContainerLostError as lost:
                    raise lost from e
                except RuntimeError:
                    is_transient = False

            if is_transient and attempt < max_attempts - 1:
                wait_time = jittered_backoff_delay(attempt)
                logger.debug(
                    f"Transient exec connection error on pod {pod_name} "
                    f"(HTTP {status_code}: {e.reason}), retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_attempts})..."
                )
                await asyncio.sleep(wait_time)
                continue
            raise
        except OSError as e:
            if attempt < max_attempts - 1:
                wait_time = jittered_backoff_delay(attempt)
                logger.debug(
                    f"Network error connecting exec stream to pod {pod_name} "
                    f"({type(e).__name__}: {e}), retrying in {wait_time:.1f}s "
                    f"(attempt {attempt + 1}/{max_attempts})..."
                )
                await asyncio.sleep(wait_time)
                continue
            raise
    raise RuntimeError(f"Exec connection to pod {pod_name} was never attempted")


# Wire exit code returned by ``build_supervised_script`` when the inner command
# exits with 0. ``kube-apiserver``'s ``StreamTranslatorHandler`` fabricates a
# channel-3 ``StatusSuccess`` (exit code 0) whenever the backend SPDY stream to
# the kubelet closes with EOF on ``errorStream`` (for example, when a
# Konnectivity tunnel resets during a silent multi-minute verifier run). By
# swapping 0 and ``_SUPERVISED_ZERO_EXIT_CODE`` on supervisor exit, a genuine 0
# travels over channel 3 as ``NonZeroExitCode(242)``—which
# ``StreamTranslatorHandler`` never fabricates—while any channel-3
# ``StatusSuccess`` (0) on a supervised stream is unambiguously routed to
# ``poll_decoupled_exec`` to recover the true state from ``workdir``.
_SUPERVISED_ZERO_EXIT_CODE = 242


def unwrap_supervised_exit_code(raw_rc: int, label: str = "") -> int:
    """Decode the wire exit code of ``build_supervised_script``."""
    if raw_rc == _SUPERVISED_ZERO_EXIT_CODE:
        return 0
    if raw_rc == 0:
        target = f" {label}" if label else ""
        raise GKEExecStreamClosedError(
            f"Kubernetes exec stream{target} closed with unverified "
            "StatusSuccess (0) on a supervised command"
        )
    return raw_rc


async def read_exec_output(
    stream: ExecStream,
    output: ExecOutputAccumulator,
    *,
    finish: bool = True,
) -> None:
    """Feed a command's stdout and stderr into ``output`` until the stream ends.

    On a normal end (when ``finish=True``) the decoders are flushed. Raises
    ``GKEExecStreamClosedError`` if the stream ended before the command's exit
    status arrived; ``output`` then holds everything received so far and can be
    resumed by recovery polling.
    """
    while (item := await stream.read()) is not None:
        await output.feed_channel(*item)
    stream.raise_unless_completed(
        f"Kubernetes exec stream {stream.label} ended before the command completed"
    )
    if finish:
        await output.finish()


async def collect_exec_bytes(stream: ExecStream) -> tuple[bytes, bytes]:
    """Read a stream to its end and return raw ``(stdout, stderr)``."""
    out = bytearray()
    err = bytearray()
    while (item := await stream.read()) is not None:
        channel, data = item
        if channel == STDOUT_CHANNEL:
            out += data
        elif channel == STDERR_CHANNEL:
            err += data
    return bytes(out), bytes(err)


_TRANSIENT_EXEC_STATUS_MARKERS: tuple[str, ...] = (
    "no agent available",
    "error dialing backend",
    "error sending request",
    "failed calling webhook",
    "dial tcp",
    "i/o timeout",
    "tls handshake timeout",
    "connection refused",
)


def is_transient_exec_status_error(exc: BaseException) -> bool:
    """Return True if an ERROR_CHANNEL status failure reflects a transient proxy/tunnel error.

    When the Kubernetes API server accepts the HTTP 101 WebSocket upgrade before
    dialing the kubelet via Konnectivity (or before a webhook call completes),
    dial failures such as ``No agent available`` arrive on WebSocket channel 3
    (``ERROR_CHANNEL``) as ``status: Failure`` without an ``ExitCode`` cause.
    Because the request never reached the container runtime, the command never
    started and can be safely retried from scratch.
    """
    text = str(exc).lower()
    return any(marker in text for marker in _TRANSIENT_EXEC_STATUS_MARKERS)


async def run_exec_command(
    connect: ConnectFn,
    command: list[str],
    *,
    container: str | None = None,
    timeout_sec: float,
    max_attempts: int = 1,
) -> tuple[bytes, bytes, int]:
    """Run a short command to completion and return ``(stdout, stderr, exit_code)``.

    Raises ``TimeoutError`` if it does not finish within ``timeout_sec`` and
    ``GKEExecStreamClosedError`` if the stream ended without an exit status.
    """
    attempts = max(1, max_attempts)
    for attempt in range(attempts):
        out = b""
        err = b""
        stream = await connect(command, container=container)
        try:
            async with asyncio.timeout(timeout_sec):
                out, err = await collect_exec_bytes(stream)
            return out, err, stream.returncode()
        except GKEExecStreamClosedError as exc:
            if (
                attempt < attempts - 1
                and stream.status_received
                and not out
                and not err
                and is_transient_exec_status_error(exc)
            ):
                wait_time = jittered_backoff_delay(attempt)
                await asyncio.sleep(wait_time)
                continue
            raise
        finally:
            stream.close()
    raise RuntimeError("run_exec_command made no attempts")


def build_recovery_probe_script(
    workdir: str, stdout_offset: int, stderr_offset: int
) -> str:
    """Read-only status probe for a supervised command's workdir.

    Prints one header line, then (for ``DONE``, ``DEAD`` and ``RUNNING``) the
    command's stdout from ``stdout_offset`` on stdout and its stderr from
    ``stderr_offset`` on stderr. It never deletes anything, so running it twice
    gives the same answer; cleanup is a separate exec issued after the client
    parsed ``DONE``.

    Headers:
      ``DONE:<rc>``            exit code file present; output follows.
      ``DONE_NO_OUTPUT:<rc>``  the supervisor finished and removed the workdir;
                               only the exit code is left.
      ``LOST``                 neither workdir nor status file exists.
      ``DEAD``                 the command's process is gone without an exit
                               code; output follows.
      ``RUNNING``              still running; new output follows.

    Liveness is sampled *before* the exit code file is checked. A process that
    writes its exit code and exits between the two checks is then reported as
    ``DONE``, never as ``DEAD``.
    """
    w = shlex.quote(workdir)
    status_file = shlex.quote(f"{workdir}.status")
    tail_out = f"tail -c +{stdout_offset + 1} {w}/stdout"
    tail_err = f"tail -c +{stderr_offset + 1} {w}/stderr >&3"
    return (
        # fd 3 carries the command's stderr; the probe's own errors are discarded.
        "exec 3>&2 2>/dev/null; "
        "alive=0; "
        f'if [ -f {w}/pid ] && kill -0 "$(cat {w}/pid)"; then alive=1; fi; '
        f"if [ -f {w}/exitcode ]; then "
        f'echo "DONE:$(cat {w}/exitcode)"; {tail_out}; {tail_err}; '
        f"elif [ -f {status_file} ]; then "
        f'echo "DONE_NO_OUTPUT:$(cat {status_file})"; '
        f"elif [ ! -d {w} ]; then echo LOST; "
        f'elif [ "$alive" = 0 ] && [ -f {w}/pid ]; then echo DEAD; {tail_out}; {tail_err}; '
        f"else : > {w}/recovered; echo RUNNING; {tail_out}; {tail_err}; "
        "fi"
    )


def _build_background_launch(workdir: str, full_command: str) -> str:
    """Start ``full_command`` in the background and record its pid in the workdir.

    The command runs in a wrapper shell that writes the exit code to
    ``<workdir>/exitcode`` atomically and then exits with that same code, so the
    supervisor gets it from ``wait`` without reading any file. The wrapper
    recreates the workdir before writing the exit code: a task may empty
    ``/tmp`` while it runs, and recovery after a lost stream still needs the
    exit code. When ``setsid`` exists, the wrapper is the
    leader of a new session and process group, so ``build_kill_script`` can
    signal everything the command started, including background processes that
    were re-parented after their parent exited. ``setsid`` does not fork here: a
    background job of a non-interactive shell is never a process group leader,
    so ``$!`` is the wrapper itself. Without ``setsid``, the wrapper is a plain
    subshell and the kill script falls back to walking the process tree.
    """
    w = shlex.quote(workdir)
    wrapper = (
        f'trap "" HUP; ( {full_command} ) > {w}/stdout 2> {w}/stderr; '
        "rc=$?; "
        f"mkdir -p {w} 2>/dev/null; "
        f"echo $rc > {w}/exitcode.tmp && mv {w}/exitcode.tmp {w}/exitcode; "
        "exit $rc"
    )
    return (
        f"mkdir -p {w}; chmod 777 {w}; touch {w}/stdout {w}/stderr; "
        f"if command -v setsid >/dev/null 2>&1; then "
        f"setsid sh -c {shlex.quote(wrapper)} & "
        f"else ( {wrapper} ) & fi; "
        f"cmd_pid=$!; echo $cmd_pid > {w}/pid; "
    )


def build_decoupled_launch_script(workdir: str, full_command: str) -> str:
    """Launch ``full_command`` detached; its state is followed by ``poll_decoupled_exec``."""
    return _build_background_launch(workdir, full_command) + "true"


def build_supervised_script(workdir: str, full_command: str) -> str:
    """Launch ``full_command`` detached and stream its output until it exits.

    The supervisor tails the command's output files to its own stdout/stderr,
    waits for the wrapper, records its exit code in ``<workdir>.status``, removes
    the workdir (unless recovery polling has marked ``<workdir>/recovered`` or
    the command exited with ``_SUPERVISED_ZERO_EXIT_CODE``), and exits with
    0 and ``_SUPERVISED_ZERO_EXIT_CODE`` swapped so that a genuine 0 exit code
    cannot be confused with a ``StatusSuccess`` fabricated by
    ``kube-apiserver``'s ``StreamTranslatorHandler`` on a broken SPDY tunnel.

    The exit code comes from ``wait`` on the wrapper, which is the supervisor's
    own child, not from files: a task that empties ``/tmp`` cannot corrupt it or
    end the wait early. A command killed by ``build_kill_script`` makes ``wait``
    return 137. The status file is only written while the workdir still exists,
    so a kill that already cleaned up leaves nothing behind.
    """
    w = shlex.quote(workdir)
    status_file = shlex.quote(f"{workdir}.status")
    zero_code = _SUPERVISED_ZERO_EXIT_CODE
    return (
        _build_background_launch(workdir, full_command)
        + "if tail --pid=$$ -n0 /dev/null 2>/dev/null; then "
        f"tail --pid=$cmd_pid -n +1 -f {w}/stdout & "
        "tail_out_pid=$!; "
        f"tail --pid=$cmd_pid -n +1 -f {w}/stderr >&2 & "
        "tail_err_pid=$!; "
        "wait $cmd_pid; rc=$?; "
        "wait $tail_out_pid $tail_err_pid 2>/dev/null; "
        "else "
        # busybox tail has no --pid and polls about once a second: give it time
        # to copy the last output before stopping it.
        f"tail -n +1 -f {w}/stdout & "
        "tail_out_pid=$!; "
        f"tail -n +1 -f {w}/stderr >&2 & "
        "tail_err_pid=$!; "
        "wait $cmd_pid; rc=$?; "
        "sleep 1.1; "
        "kill $tail_out_pid $tail_err_pid 2>/dev/null; "
        "wait $tail_out_pid $tail_err_pid 2>/dev/null; "
        "fi; "
        f'[ -d {w} ] && echo "$rc" > {status_file} 2>/dev/null; '
        f'[ -f {w}/recovered ] || [ "$rc" -eq {zero_code} ] || rm -rf {w}; '
        f'if [ "$rc" -eq 0 ]; then exit {zero_code}; '
        f'elif [ "$rc" -eq {zero_code} ]; then exit 0; '
        'else exit "$rc"; fi'
    )


# Prints "<pid> <ppid>" for every process. /proc is read with shell builtins
# only, so this works in minimal images without procps; ``ps`` is the fallback
# for hosts without /proc (macOS, used by the unit tests).
_PROCESS_TABLE_FN = """\
_harbor_ptable() {
  if [ -d /proc/self ]; then
    for d in /proc/[0-9]*; do
      while read -r k v; do
        if [ "$k" = "PPid:" ]; then echo "${d#/proc/} $v"; break; fi
      done 2>/dev/null < "$d/status"
    done 2>/dev/null
  else
    ps -A -o pid= -o ppid= 2>/dev/null
  fi
}
"""

# Prints the pid in $1 and all its descendants.
_PROCESS_TREE_FN = """\
_harbor_tree() {
  _t=$(_harbor_ptable); _all=" $1 "; _front=" $1 "; _depth=0
  while [ "$_front" != " " ] && [ $_depth -lt 64 ]; do
    _next=" "
    while read -r _p _q; do
      case "$_front" in *" $_q "*)
        case "$_all" in *" $_p "*) ;; *) _next="$_next$_p "; _all="$_all$_p ";; esac;;
      esac
    done <<_HARBOR_EOF
$_t
_HARBOR_EOF
    _front=$_next; _depth=$((_depth + 1))
  done
  echo $_all
}
"""


def build_kill_script(workdir: str) -> str:
    """Kill a supervised command with everything it started, then remove its files.

    1. If the command has not finished (no exit code file), its wrapper pid is
       still valid: freeze the wrapper and all its descendants with SIGSTOP.
       The tree is listed twice so children forked before the first freeze are
       caught too.
    2. SIGKILL the wrapper's process group. When the command was launched with
       ``setsid`` this reaches background processes that were re-parented away
       from the tree. Linux never reuses a pid that is still a process group
       id, so this cannot hit an unrelated group; without ``setsid`` no such
       group exists and the call fails harmlessly.
    3. SIGKILL the frozen tree.

    A pid of 0 or 1, or a non-numeric pid file, is never signalled.
    """
    w = shlex.quote(workdir)
    status_file = shlex.quote(f"{workdir}.status")
    return (
        _PROCESS_TABLE_FN
        + _PROCESS_TREE_FN
        + f"pid=$(cat {w}/pid 2>/dev/null)\n"
        + 'case "$pid" in ""|*[!0-9]*|0|1) pid="";; esac\n'
        + 'if [ -n "$pid" ]; then\n'
        + f"  if [ ! -f {w}/exitcode ]; then\n"
        + '    procs=$(_harbor_tree "$pid"); kill -s STOP $procs 2>/dev/null\n'
        + '    procs=$(_harbor_tree "$pid"); kill -s STOP $procs 2>/dev/null\n'
        + "  else\n"
        + '    procs=""\n'
        + "  fi\n"
        + '  kill -s KILL -- "-$pid" 2>/dev/null\n'
        + '  [ -n "$procs" ] && kill -s KILL $procs 2>/dev/null\n'
        + "fi\n"
        + f"rm -rf {w} {status_file} 2>/dev/null; true\n"
    )


def build_cleanup_script(workdir: str) -> str:
    w = shlex.quote(workdir)
    status_file = shlex.quote(f"{workdir}.status")
    return f"rm -rf {w} {status_file} 2>/dev/null; true"


async def run_best_effort(
    connect: ConnectFn,
    script: str,
    *,
    container: str | None,
    timeout_sec: float,
    pod_name: str,
    purpose: str,
) -> None:
    """Run a housekeeping script; failures are logged, never raised."""
    try:
        await run_exec_command(
            connect, ["sh", "-c", script], container=container, timeout_sec=timeout_sec
        )
    except Exception as e:
        logger.debug(
            "Best-effort %s on pod %s failed (%s: %s)",
            purpose,
            pod_name,
            type(e).__name__,
            e,
        )


async def poll_decoupled_exec(
    *,
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    workdir: str,
    output: ExecOutputAccumulator,
    timeout_sec: int | None,
    start_time: float,
    connect: ConnectFn,
    container: str | None = None,
) -> ExecResult:
    """Follow a supervised command through its on-disk workdir until it completes.

    Used for decoupled mode and to recover a supervised command whose exec
    stream was lost. ``output`` already holds what was streamed so far; the probe
    resumes from its exact byte counts. ``connect`` must open raw execs as the
    same identity as the supervisor (no ``su``/``cd`` wrapping), so ``kill -0``
    and file access behave as they do for the supervisor itself.
    """
    poll_interval = _RECOVERY_INITIAL_POLL_INTERVAL_SEC
    last_state: str | None = None
    polls = 0

    async def _sleep() -> None:
        nonlocal poll_interval
        await asyncio.sleep(random.uniform(poll_interval * 0.8, poll_interval * 1.2))
        poll_interval = min(poll_interval * 1.5, _RECOVERY_MAX_POLL_INTERVAL_SEC)

    while True:
        await check_pod_terminated(api, pod_name, namespace, target_container=container)
        if timeout_sec and (time.monotonic() - start_time >= timeout_sec):
            await run_best_effort(
                connect,
                build_kill_script(workdir),
                container=container,
                timeout_sec=_RECOVERY_CLEANUP_TIMEOUT_SEC,
                pod_name=pod_name,
                purpose="kill of timed-out command",
            )
            await output.finish()
            return ExecResult(
                stdout=output.stdout,
                stderr=f"{output.stderr}\nCommand timed out after {timeout_sec} seconds".strip(),
                return_code=124,
            )

        polls += 1
        script = build_recovery_probe_script(
            workdir, output.stdout_bytes, output.stderr_bytes
        )
        try:
            probe_out, probe_err, _ = await run_exec_command(
                connect,
                ["sh", "-c", script],
                container=container,
                timeout_sec=_RECOVERY_PROBE_TIMEOUT_SEC,
            )
        except ApiException as e:
            if _extract_api_status_code(e) == 404:
                raise TrialContainerLostError(
                    f"Pod {pod_name} does not exist in cluster."
                ) from e
            logger.debug(
                "Exec status probe %d for %s on pod %s failed (ApiException %s); retrying.",
                polls,
                workdir,
                pod_name,
                _extract_api_status_code(e),
            )
            await _sleep()
            continue
        except (HTTPError, OSError, GKEExecStreamClosedError) as e:
            logger.debug(
                "Exec status probe %d for %s on pod %s failed (%s: %s); retrying.",
                polls,
                workdir,
                pod_name,
                type(e).__name__,
                e,
            )
            await _sleep()
            continue

        header_bytes, _, body = probe_out.partition(b"\n")
        header = header_bytes.decode("utf-8", "replace").strip()
        state = header.split(":", 1)[0]
        if state != last_state:
            logger.info(
                "Exec status probe %d for %s on pod %s: %s",
                polls,
                workdir,
                pod_name,
                header or "<empty>",
            )
            last_state = state

        if state in ("DONE", "DONE_NO_OUTPUT"):
            rc_text = header.split(":", 1)[1].strip() if ":" in header else ""
            try:
                return_code = int(rc_text)
            except ValueError as e:
                raise GKEExecStreamClosedError(
                    f"Command in {workdir} on pod {pod_name} finished with an "
                    f"unreadable exit code {rc_text!r}"
                ) from e
            if state == "DONE":
                await output.feed("stdout", body)
                await output.feed("stderr", probe_err)
            else:
                logger.warning(
                    "Command in %s on pod %s finished (exit %d) after its supervisor "
                    "removed the output files; output after byte %d (stdout) / %d "
                    "(stderr) is not recoverable.",
                    workdir,
                    pod_name,
                    return_code,
                    output.stdout_bytes,
                    output.stderr_bytes,
                )
            await output.finish()
            await run_best_effort(
                connect,
                build_cleanup_script(workdir),
                container=container,
                timeout_sec=_RECOVERY_CLEANUP_TIMEOUT_SEC,
                pod_name=pod_name,
                purpose="workdir cleanup",
            )
            return ExecResult(
                stdout=output.stdout, stderr=output.stderr, return_code=return_code
            )

        if state == "RUNNING":
            await output.feed("stdout", body)
            await output.feed("stderr", probe_err)
        elif state == "DEAD":
            await output.feed("stdout", body)
            await output.feed("stderr", probe_err)
            logger.warning(
                "Command in %s on pod %s exited without writing an exit code "
                "(process killed); reporting exit code 1.",
                workdir,
                pod_name,
            )
            await output.finish()
            await run_best_effort(
                connect,
                build_cleanup_script(workdir),
                container=container,
                timeout_sec=_RECOVERY_CLEANUP_TIMEOUT_SEC,
                pod_name=pod_name,
                purpose="workdir cleanup",
            )
            return ExecResult(stdout=output.stdout, stderr=output.stderr, return_code=1)
        elif state == "LOST":
            logger.warning(
                "Command in %s on pod %s cannot be recovered: neither its workdir nor "
                "its status file exists.",
                workdir,
                pod_name,
            )
            raise GKEExecStreamClosedError(
                f"Kubernetes exec stream disconnected and the status of the command in "
                f"{workdir} on pod {pod_name} could not be recovered"
            )
        else:
            logger.warning(
                "Unrecognized exec status probe output for %s on pod %s: %r",
                workdir,
                pod_name,
                probe_out[:200],
            )
        await _sleep()


_TAR_UPLOAD_CHUNK_BYTES = 256 * 1024
_TAR_UPLOAD_MIN_TIMEOUT_SEC = 30.0
_TAR_UPLOAD_SEC_PER_MIB = 5.0


async def stream_tar_to_pod(
    stream: ExecStream,
    tar_buffer: io.BytesIO,
    pod_name: str,
) -> None:
    """Stream tar bytes into a remote extraction command and wait for it to finish.

    Each chunk write waits until the kernel accepted it, which is the flow control.
    A timeout of 5 s per MiB, at least 30 s, bounds the whole transfer, including
    the remote extraction.
    """
    tar_buffer.seek(0, io.SEEK_END)
    total_bytes = tar_buffer.tell()
    tar_buffer.seek(0)

    adaptive_timeout = max(
        _TAR_UPLOAD_MIN_TIMEOUT_SEC,
        (total_bytes / (1024 * 1024)) * _TAR_UPLOAD_SEC_PER_MIB,
    )
    stderr_buf = bytearray()

    async def _drain_stderr() -> str:
        # Everything queued so far; the stream keeps receiving in the background.
        for channel, data in stream.take_buffered():
            if channel == STDERR_CHANNEL:
                stderr_buf.extend(data)
        return stderr_buf.decode("utf-8", "replace").strip()

    try:
        try:
            async with asyncio.timeout(adaptive_timeout):
                while chunk := tar_buffer.read(_TAR_UPLOAD_CHUNK_BYTES):
                    if stream.is_closed:
                        err_msg = await _drain_stderr()
                        raise RuntimeError(
                            f"Exec stream to pod {pod_name} closed prematurely during upload: "
                            f"{err_msg or stream.describe_end()}"
                        )
                    try:
                        await stream.write(chunk)
                    except GKEExecStreamClosedError as e:
                        err_msg = await _drain_stderr()
                        details = f" ({err_msg})" if err_msg else ""
                        raise RuntimeError(
                            f"Failed to write tar chunk to pod {pod_name}{details}: {e}"
                        ) from e
                _, err = await collect_exec_bytes(stream)
                stderr_buf.extend(err)
        except TimeoutError as e:
            raise TimeoutError(
                f"Timed out after {adaptive_timeout:.1f}s uploading {total_bytes} bytes "
                f"and waiting for tar extraction to complete on pod {pod_name}"
            ) from e

        stderr_msg = stderr_buf.decode("utf-8", "replace").strip()
        stream.raise_unless_completed(
            f"Kubernetes exec stream closed before tar extraction on pod {pod_name} "
            "reported a status"
        )
        rc = stream.returncode()
        if rc != 0:
            raise RuntimeError(
                f"tar extraction failed on pod {pod_name} with exit code {rc}: {stderr_msg}"
            )

        if stderr_msg and any(
            kw in stderr_msg.lower()
            for kw in ("fatal", "no space left on device", "short read", "corrupt")
        ):
            raise RuntimeError(
                f"tar extraction reported error on pod {pod_name}: {stderr_msg}"
            )
    finally:
        stream.close()


async def _upload_tar_buffer(
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    tar_buffer: io.BytesIO,
    target_dir: str,
    *,
    container: str | None = None,
    dind_services: frozenset[str] | None = None,
) -> None:
    """Create ``target_dir`` and extract ``tar_buffer`` into it in a single exec.

    ``head -c`` stops reading at the exact archive size, so the command does not
    depend on stdin EOF (which the v4 exec protocol cannot signal).
    """
    total_bytes = len(tar_buffer.getvalue())
    quoted_dir = shlex.quote(target_dir)
    exec_command = [
        "sh",
        "-c",
        f"mkdir -p {quoted_dir} && head -c {total_bytes} | tar xf - -C {quoted_dir}",
    ]
    stream = await connect_exec_stream(
        api,
        pod_name,
        namespace,
        exec_command,
        container=container,
        stderr=True,
        stdin=True,
        stdout=True,
        tty=False,
        dind_services=dind_services,
    )
    await stream_tar_to_pod(stream, tar_buffer, pod_name=pod_name)


async def upload_file(
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    source_path: Path | str,
    target_path: str,
    container: str | None = None,
    dind_services: frozenset[str] | None = None,
) -> None:
    """Upload a file to a pod container using a single-hop tar stream."""
    source_path = Path(source_path)

    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
        tar.add(str(source_path), arcname=Path(target_path).name)
    tar_buffer.seek(0)

    target_dir = str(Path(target_path).parent)
    await _upload_tar_buffer(
        api,
        pod_name,
        namespace,
        tar_buffer,
        target_dir,
        container=container,
        dind_services=dind_services,
    )


async def upload_dir(
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    source_dir: Path | str,
    target_dir: str,
    container: str | None = None,
    dind_services: frozenset[str] | None = None,
) -> None:
    """Upload a directory to a pod container using a single-hop tar stream."""
    source_dir = Path(source_dir)
    if not source_dir.is_dir():
        logger.warning(f"No files to upload from {source_dir}")
        return

    tar_buffer = pack_dir_to_bytes(source_dir)
    await _upload_tar_buffer(
        api,
        pod_name,
        namespace,
        tar_buffer,
        target_dir,
        container=container,
        dind_services=dind_services,
    )


async def _read_tar_stream(
    stream: ExecStream,
    source_description: str,
    pod_name: str,
) -> bytes:
    """Read a binary tar stream from an exec until the command completes."""
    try:
        tar_data, err = await collect_exec_bytes(stream)
    finally:
        stream.close()

    stream.raise_unless_completed(
        f"Kubernetes exec stream closed prematurely while downloading "
        f"{source_description} from pod {pod_name}"
    )
    rc = stream.returncode()
    if rc != 0:
        stderr_msg = err.decode("utf-8", "replace").strip()
        detail = f": {stderr_msg}" if stderr_msg else ""
        msg = f"Command failed with exit code {rc} while downloading {source_description} from pod {pod_name}{detail}"
        if "No such file or directory" in stderr_msg:
            raise RemoteFileNotFoundError(msg)
        raise RuntimeError(msg)
    return tar_data


async def download_file(
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    source_path: str,
    target_path: Path | str,
    container: str | None = None,
    dind_services: frozenset[str] | None = None,
) -> None:
    """Download a file from a pod container using a single-hop tar stream."""
    target_path = Path(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    exec_command = ["tar", "cf", "-", source_path]
    stream = await connect_exec_stream(
        api,
        pod_name,
        namespace,
        exec_command,
        container=container,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        dind_services=dind_services,
    )

    tar_data = await _read_tar_stream(stream, source_path, pod_name)

    if not tar_data:
        raise RuntimeError(
            f"No data received when downloading {source_path} from pod {pod_name}."
        )

    tar_buffer = io.BytesIO(tar_data)
    with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
        members = tar.getmembers()
        clean_source = source_path.removeprefix("./").lstrip("/")
        for member in members:
            member_clean = member.name.removeprefix("./").lstrip("/")
            if member_clean == clean_source:
                if member.isdir():
                    raise IsADirectoryError(
                        f"Target path {source_path} is a directory, not a regular file. "
                        "Use download_dir instead."
                    )
                if not member.isreg():
                    continue
                member.name = target_path.name
                tar.extract(member, path=str(target_path.parent), filter="data")
                return

        # Also check if directory is named with trailing slash in tar
        for member in members:
            member_clean = member.name.removeprefix("./").strip("/")
            if member_clean == clean_source.strip("/"):
                if member.isdir():
                    raise IsADirectoryError(
                        f"Target path {source_path} is a directory, not a regular file. "
                        "Use download_dir instead."
                    )

    raise RuntimeError(
        f"File {source_path} not found in tar archive from pod {pod_name}."
    )


async def download_dir(
    api: k8s_client.CoreV1Api,
    pod_name: str,
    namespace: str,
    source_dir: str,
    target_dir: Path | str,
    container: str | None = None,
    dind_services: frozenset[str] | None = None,
) -> None:
    """Download a directory from a pod container using a single-hop tar stream."""
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    exec_command = [
        "sh",
        "-c",
        f"cd {shlex.quote(source_dir)} && tar cf - .",
    ]
    stream = await connect_exec_stream(
        api,
        pod_name,
        namespace,
        exec_command,
        container=container,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        dind_services=dind_services,
    )

    tar_data = await _read_tar_stream(stream, source_dir, pod_name)

    if not tar_data:
        raise RuntimeError(
            f"No data received when downloading {source_dir} from pod {pod_name}."
        )

    try:
        extract_dir_from_bytes(tar_data, target_dir)
    except tarfile.TarError as e:
        raise RuntimeError(
            f"Failed to extract directory {source_dir} from pod {pod_name}: {e}"
        )
