"""Fake Kubernetes exec endpoint for harbor-gke unit tests.

``FakeKubelet`` stands in for the Kubernetes API server's exec endpoint. Each
``connect`` call runs the requested argv as a real local subprocess and speaks
the v4.channel.k8s.io WebSocket protocol to an ``ExecStream`` over a
``socketpair``. This exercises the real reactor, frame parser, status parsing,
supervised scripts and recovery probes without a cluster.

Fault injection: ``drop_stream_after`` makes a connection vanish (TCP close,
no CLOSE frame, no status) after it forwarded N bytes of stdout, while the
command keeps running, which is exactly what a lost exec stream looks like.
"""

from __future__ import annotations

import json
import os
import selectors
import signal
import socket
import struct
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Any, Callable

from harbor_gke_ext.exec_stream import (
    ExecStream,
    FrameParser,
)

OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


def server_frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
    """Encode an unmasked server-to-client frame."""
    b0 = (0x80 if fin else 0) | opcode
    n = len(payload)
    if n < 126:
        header = struct.pack("!BB", b0, n)
    elif n < 65536:
        header = struct.pack("!BBH", b0, 126, n)
    else:
        header = struct.pack("!BBQ", b0, 127, n)
    return header + payload


def channel_frame(channel: int, data: bytes) -> bytes:
    return server_frame(OP_BINARY, bytes([channel]) + data)


def status_payload(return_code: int) -> bytes:
    if return_code == 0:
        return json.dumps({"metadata": {}, "status": "Success"}).encode()
    return json.dumps(
        {
            "metadata": {},
            "status": "Failure",
            "message": f"command terminated with non-zero exit code: {return_code}",
            "reason": "NonZeroExitCode",
            "details": {
                "causes": [{"reason": "ExitCode", "message": str(return_code)}]
            },
        }
    ).encode()


@dataclass
class FakeConnection:
    command: list[str]
    container: str | None
    stdin: bool
    client_frames: list[tuple[int, bytes]] = field(default_factory=list)
    stdout_sent: int = 0
    dropped: bool = False
    return_code: int | None = None
    # Session and process-group leader of the exec'd command (start_new_session).
    pid: int | None = None
    done: threading.Event = field(default_factory=threading.Event)


class FakeKubelet:
    def __init__(self) -> None:
        self.connections: list[FakeConnection] = []
        # Called with the argv; returns N to drop that stream after N stdout bytes.
        self.drop_stream_after: Callable[[list[str]], int | None] = lambda argv: None
        # When True, a dropped stream sends channel-3 StatusSuccess + OP_CLOSE
        # before closing, matching kube-apiserver StreamTranslatorHandler when
        # the backend SPDY tunnel to kubelet closes on EOF.
        self.fabricate_success_on_drop: bool = False
        # Called with the argv; returns a status payload to send instead of the
        # process exit code (e.g. an exec Failure without an ExitCode cause).
        self.status_override: Callable[[list[str]], bytes | None] = lambda argv: None
        self._threads: list[threading.Thread] = []

    async def connect(
        self,
        command: list[str],
        *,
        container: str | None = None,
        stdin: bool = False,
        **_: Any,
    ) -> ExecStream:
        client_sock, server_sock = socket.socketpair()
        conn = FakeConnection(list(command), container, stdin)
        self.connections.append(conn)
        stream = ExecStream.attach(client_sock, label=f"fake/{len(self.connections)}")
        thread = threading.Thread(
            target=self._serve,
            args=(conn, server_sock, self.drop_stream_after(conn.command)),
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()
        return stream

    def _serve(
        self, conn: FakeConnection, sock: socket.socket, drop_after: int | None
    ) -> None:
        override = self.status_override(conn.command)
        if override is not None:
            try:
                sock.sendall(channel_frame(3, override))
                sock.sendall(server_frame(OP_CLOSE, struct.pack("!H", 1000)))
            except OSError:
                pass
            finally:
                try:
                    sock.close()
                except OSError:
                    pass
                conn.done.set()
            return

        proc = subprocess.Popen(
            conn.command,
            stdin=subprocess.PIPE if conn.stdin else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        conn.pid = proc.pid
        assert proc.stdout is not None and proc.stderr is not None
        sel = selectors.DefaultSelector()
        sel.register(proc.stdout, selectors.EVENT_READ, 1)
        sel.register(proc.stderr, selectors.EVENT_READ, 2)
        sel.register(sock, selectors.EVENT_READ, "sock")
        parser = FrameParser()
        open_pipes = 2
        sock_open = True
        try:
            while open_pipes and sock_open:
                for key, _ in sel.select(timeout=0.5):
                    if key.data == "sock":
                        try:
                            data = sock.recv(65536)
                        except OSError:
                            data = b""
                        if not data:
                            sock_open = False
                            break
                        for opcode, payload in parser.feed(data):
                            conn.client_frames.append((opcode, payload))
                            if opcode == OP_BINARY and payload[:1] == b"\x00":
                                if proc.stdin is not None:
                                    try:
                                        proc.stdin.write(payload[1:])
                                        proc.stdin.flush()
                                    except (BrokenPipeError, ValueError):
                                        pass
                            elif opcode == OP_PING:
                                sock.sendall(server_frame(OP_PONG, payload))
                            elif opcode == OP_CLOSE:
                                sock_open = False
                        continue
                    chunk = os.read(key.fileobj.fileno(), 65536)  # type: ignore[union-attr]
                    if not chunk:
                        sel.unregister(key.fileobj)
                        open_pipes -= 1
                        continue
                    channel = key.data
                    if drop_after is not None and channel == 1:
                        room = drop_after - conn.stdout_sent
                        if room <= len(chunk):
                            if room > 0:
                                sock.sendall(channel_frame(1, chunk[:room]))
                                conn.stdout_sent += room
                            conn.dropped = True
                            if self.fabricate_success_on_drop:
                                sock.sendall(channel_frame(3, status_payload(0)))
                                sock.sendall(
                                    server_frame(OP_CLOSE, struct.pack("!H", 1000))
                                )
                            sock.close()
                            proc.stdout.close()
                            proc.stderr.close()
                            return
                    sock.sendall(channel_frame(channel, chunk))
                    if channel == 1:
                        conn.stdout_sent += len(chunk)
            if not sock_open:
                # Client went away first; the real kubelet stops the stream too.
                return
            conn.return_code = proc.wait()
            status = status_payload(conn.return_code)
            try:
                sock.sendall(channel_frame(3, status))
                sock.sendall(server_frame(OP_CLOSE, struct.pack("!H", 1000)))
            except OSError:
                return
            # Wait briefly for the client's CLOSE reply, then hang up.
            sock.settimeout(2.0)
            try:
                while True:
                    data = sock.recv(65536)
                    if not data:
                        break
                    frames = parser.feed(data)
                    conn.client_frames.extend(frames)
                    if any(op == OP_CLOSE for op, _ in frames):
                        break
            except OSError:
                pass
        finally:
            try:
                sock.close()
            except OSError:
                pass
            if proc.stdin is not None:
                try:
                    proc.stdin.close()
                except OSError:
                    pass
            conn.done.set()

    def join(self, timeout: float = 10.0) -> None:
        for thread in self._threads:
            thread.join(timeout)

    def kill_leftovers(self) -> None:
        """SIGKILL every process group a connection started.

        Commands outlive their streams by design (dropped and timed-out
        streams), so they must be reaped when a test ends.
        """
        for conn in self.connections:
            if conn.pid is None:
                continue
            try:
                os.killpg(conn.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
