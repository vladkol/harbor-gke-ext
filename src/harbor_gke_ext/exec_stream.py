"""Kubernetes exec streams multiplexed on a single reactor thread.

The Kubernetes Python client exposes an exec session as a blocking
``WSClient``. Reading one requires a thread per open stream, and closing one
blocks for up to three seconds while it waits for the server's CLOSE frame.
With hundreds of concurrent trials, that design turns a bounded thread pool
into a queue in front of every command and stalls the event loop on close.

This module keeps the Kubernetes client only for the WebSocket handshake
(URL building, authentication, TLS and HTTP upgrade). After the handshake the
raw socket is handed to one daemon thread, the reactor, which owns every exec
socket for the rest of its life:

* Sockets are non-blocking and multiplexed with ``selectors``.
* Server frames are parsed here rather than by ``websocket-client``, whose
  frame reader blocks until a whole frame has arrived.
* Writes are queued per stream and each resolves an asyncio future once the
  kernel has accepted every byte, which gives uploads backpressure.
* Keepalive PINGs, PONG replies and CLOSE handling happen on the reactor.
* Received data is delivered to the owning event loop in batches through
  ``loop.call_soon_threadsafe``.

The asyncio side of a stream is :class:`ExecStream`.
"""

from __future__ import annotations

import asyncio
import codecs
import collections
import json
import selectors
import socket
import ssl
import struct
import threading
import time
from typing import Any, Callable

from harbor.utils.logger import logger
from harbor_gke_ext.constants import (
    _GKE_EXEC_STREAM_PING_INTERVAL_SEC,
    GKEExecStreamClosedError,
)

ABNF: Any
try:
    from websocket import ABNF as _WEBSOCKET_ABNF

    ABNF = _WEBSOCKET_ABNF
except ImportError:  # pragma: no cover - websocket-client ships with kubernetes
    ABNF = None

STDIN_CHANNEL = 0
STDOUT_CHANNEL = 1
STDERR_CHANNEL = 2
ERROR_CHANNEL = 3

_OPCODE_CONT = 0x0
_OPCODE_TEXT = 0x1
_OPCODE_BINARY = 0x2
_OPCODE_CLOSE = 0x8
_OPCODE_PING = 0x9
_OPCODE_PONG = 0xA
_DATA_OPCODES = (_OPCODE_TEXT, _OPCODE_BINARY)

# A single WebSocket message larger than this is treated as a protocol error
# rather than buffered without bound. Kubelet stream frames are a few tens of KiB.
_MAX_MESSAGE_BYTES = 256 * 1024 * 1024
_RECV_CHUNK_BYTES = 64 * 1024
# Bytes read from one socket per reactor wakeup before moving on to the others.
_READ_BUDGET_BYTES = 1024 * 1024
_IDLE_SELECT_TIMEOUT_SEC = 1.0

END_REMOTE_CLOSE = "remote_close"
END_CONNECTION_LOST = "connection_lost"
END_PROTOCOL_ERROR = "protocol_error"
END_LOCAL_CLOSE = "local_close"
END_SHUTDOWN = "reactor_shutdown"


class WebSocketProtocolError(Exception):
    """Raised when the server sends bytes that are not a valid WebSocket stream."""


def _unmask(mask_key: bytes, payload: bytes) -> bytes:
    if ABNF is None:
        return bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    return ABNF.mask(mask_key, payload)


class FrameParser:
    """Incremental parser for server-to-client WebSocket frames (RFC 6455).

    ``feed`` accepts any slice of the byte stream and returns every message and
    control frame completed by it. Fragmented messages are reassembled.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._fragment_opcode: int | None = None
        self._fragments: list[bytes] = []
        self._fragment_bytes = 0

    def feed(self, data: bytes) -> list[tuple[int, bytes]]:
        self._buffer += data
        messages: list[tuple[int, bytes]] = []
        buf = self._buffer
        while True:
            if len(buf) < 2:
                break
            b0, b1 = buf[0], buf[1]
            fin = bool(b0 & 0x80)
            if b0 & 0x70:
                raise WebSocketProtocolError("reserved bits set without an extension")
            opcode = b0 & 0x0F
            masked = bool(b1 & 0x80)
            length = b1 & 0x7F
            pos = 2
            if length == 126:
                if len(buf) < 4:
                    break
                length = struct.unpack_from("!H", buf, 2)[0]
                pos = 4
            elif length == 127:
                if len(buf) < 10:
                    break
                length = struct.unpack_from("!Q", buf, 2)[0]
                pos = 10
            mask_key = b""
            if masked:
                if len(buf) < pos + 4:
                    break
                mask_key = bytes(buf[pos : pos + 4])
                pos += 4
            if length > _MAX_MESSAGE_BYTES:
                raise WebSocketProtocolError(
                    f"frame of {length} bytes exceeds the {_MAX_MESSAGE_BYTES}-byte limit"
                )
            if len(buf) < pos + length:
                break
            payload = bytes(buf[pos : pos + length])
            del buf[: pos + length]
            if masked:
                payload = _unmask(mask_key, payload)

            if opcode >= 0x8:
                if opcode not in (_OPCODE_CLOSE, _OPCODE_PING, _OPCODE_PONG):
                    raise WebSocketProtocolError(f"unknown control opcode {opcode:#x}")
                if not fin or length > 125:
                    raise WebSocketProtocolError("invalid control frame")
                messages.append((opcode, payload))
            elif opcode == _OPCODE_CONT:
                if self._fragment_opcode is None:
                    raise WebSocketProtocolError("continuation frame without a start")
                self._fragments.append(payload)
                self._fragment_bytes += len(payload)
                if self._fragment_bytes > _MAX_MESSAGE_BYTES:
                    raise WebSocketProtocolError("fragmented message exceeds the limit")
                if fin:
                    messages.append((self._fragment_opcode, b"".join(self._fragments)))
                    self._fragment_opcode = None
                    self._fragments = []
                    self._fragment_bytes = 0
            elif opcode in _DATA_OPCODES:
                if self._fragment_opcode is not None:
                    raise WebSocketProtocolError("new message inside a fragmented one")
                if fin:
                    messages.append((opcode, payload))
                else:
                    self._fragment_opcode = opcode
                    self._fragments = [payload]
                    self._fragment_bytes = len(payload)
            else:
                raise WebSocketProtocolError(f"unknown data opcode {opcode:#x}")
        return messages


def encode_client_frame(opcode: int, payload: bytes) -> bytes:
    """Build one masked client-to-server frame."""
    if ABNF is None:  # pragma: no cover - websocket-client ships with kubernetes
        raise RuntimeError("websocket-client is required for Kubernetes exec streams")
    return ABNF.create_frame(payload, opcode).format()


def parse_exec_status(payload: bytes) -> int:
    """Return the exit code carried by an exec ERROR_CHANNEL status message.

    ``Success`` is exit code 0. ``Failure`` with an ``ExitCode`` cause carries the
    command's non-zero exit code. Any other ``Failure`` means the exec itself failed
    (for example, the runtime could not start the process), which is reported as
    ``GKEExecStreamClosedError`` with the server's message.
    """
    text = payload.decode("utf-8", "replace")
    try:
        status = json.loads(text)
    except ValueError as e:
        raise GKEExecStreamClosedError(
            f"Kubernetes exec returned an unreadable status: {text[:300]!r}"
        ) from e
    if not isinstance(status, dict):
        raise GKEExecStreamClosedError(
            f"Kubernetes exec returned an unexpected status: {text[:300]!r}"
        )
    if status.get("status") == "Success":
        return 0
    details = status.get("details") or {}
    causes = details.get("causes") if isinstance(details, dict) else None
    for cause in causes or []:
        if isinstance(cause, dict) and cause.get("reason") == "ExitCode":
            try:
                return int(cause.get("message", ""))
            except (TypeError, ValueError):
                break
    raise GKEExecStreamClosedError(
        f"Kubernetes exec failed: {status.get('message') or text[:300]}"
    )


def _describe_error(error: BaseException | None) -> str:
    if error is None:
        return ""
    message = str(error)
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


class ExecStream:
    """Asyncio handle for one Kubernetes exec WebSocket owned by the reactor.

    All methods must be called from the event loop that created the stream.
    """

    def __init__(self, label: str, loop: asyncio.AbstractEventLoop) -> None:
        self.label = label
        self._loop = loop
        self._chunks: collections.deque[tuple[int, bytes]] = collections.deque()
        self._status = bytearray()
        self._data_event = asyncio.Event()
        self._closed = False
        self._close_requested = False
        self._end_reason: str | None = None
        self._end_error: BaseException | None = None
        self._entry: _StreamEntry | None = None
        self._reactor: ExecReactor | None = None

    @classmethod
    def attach(
        cls,
        sock: socket.socket,
        *,
        label: str,
        keepalive_ref: Any = None,
    ) -> ExecStream:
        """Hand a connected WebSocket's raw socket to the reactor.

        ``sock`` must be positioned at a frame boundary (nothing read after the
        HTTP upgrade). ``keepalive_ref`` keeps the handshake object alive for as
        long as the stream exists.
        """
        stream = cls(label, asyncio.get_running_loop())
        stream._reactor = get_exec_reactor()
        stream._entry = _StreamEntry(stream, sock, stream._loop, keepalive_ref)
        stream._reactor.register(stream._entry)
        return stream

    # -- Called on the event loop by the reactor -------------------------------------

    def _on_data(self, items: list[tuple[int, bytes]]) -> None:
        for channel, data in items:
            if channel == ERROR_CHANNEL:
                self._status += data
            else:
                self._chunks.append((channel, data))
        self._data_event.set()

    def _on_closed(self, reason: str, error: BaseException | None) -> None:
        if self._closed:
            return
        self._closed = True
        self._end_reason = reason
        self._end_error = error
        self._data_event.set()

    # -- Public API -----------------------------------------------------------------

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def status_received(self) -> bool:
        return bool(self._status)

    def describe_end(self) -> str:
        """Human-readable description of how the stream ended."""
        if not self._closed:
            return "stream still open"
        detail = _describe_error(self._end_error)
        return f"{self._end_reason} ({detail})" if detail else str(self._end_reason)

    async def read(self) -> tuple[int, bytes] | None:
        """Return the next ``(channel, data)`` chunk, or ``None`` once the stream ended."""
        while True:
            if self._chunks:
                return self._chunks.popleft()
            if self._closed:
                return None
            self._data_event.clear()
            await self._data_event.wait()

    def take_buffered(self) -> list[tuple[int, bytes]]:
        """Remove and return every chunk received so far, without waiting."""
        items = list(self._chunks)
        self._chunks.clear()
        return items

    async def write(self, data: bytes, channel: int = STDIN_CHANNEL) -> None:
        """Send ``data`` on ``channel`` and wait until the kernel accepted all of it."""
        if self._closed or self._close_requested:
            raise GKEExecStreamClosedError(
                f"Exec stream {self.label} is closed ({self.describe_end()})"
            )
        if self._reactor is None or self._entry is None:
            raise RuntimeError("ExecStream is not attached to a reactor")
        frame = encode_client_frame(_OPCODE_BINARY, bytes([channel]) + data)
        future: asyncio.Future[None] = self._loop.create_future()
        self._reactor.submit_write(self._entry, frame, future)
        await future

    def close(self) -> None:
        """Close the stream without blocking. Safe to call more than once."""
        if self._closed or self._close_requested:
            return
        self._close_requested = True
        if self._reactor is not None and self._entry is not None:
            self._reactor.request_close(self._entry)

    def returncode(self) -> int:
        """Exit code of the remote command.

        Raises ``GKEExecStreamClosedError`` when no status was received, which
        means the stream ended before the command completed.
        """
        if not self._status:
            raise GKEExecStreamClosedError(
                f"Kubernetes exec stream {self.label} ended without a command status: "
                f"{self.describe_end()}"
            )
        return parse_exec_status(bytes(self._status))

    def raise_unless_completed(self, context: str) -> None:
        """Raise ``GKEExecStreamClosedError`` if the stream ended without a status."""
        if not self._status:
            raise GKEExecStreamClosedError(f"{context}: {self.describe_end()}")


class _PendingWrite:
    __slots__ = ("view", "offset", "future")

    def __init__(self, data: bytes, future: asyncio.Future[None] | None) -> None:
        self.view = memoryview(data)
        self.offset = 0
        self.future = future


class _StreamEntry:
    """Reactor-side state of one stream. Touched only by the reactor thread."""

    def __init__(
        self,
        stream: ExecStream,
        sock: socket.socket,
        loop: asyncio.AbstractEventLoop,
        keepalive_ref: Any,
    ) -> None:
        self.stream = stream
        self.sock = sock
        self.loop = loop
        self.keepalive_ref = keepalive_ref
        self.parser = FrameParser()
        self.outgoing: collections.deque[_PendingWrite] = collections.deque()
        self.pending_data: list[tuple[int, bytes]] = []
        self.next_ping = 0.0
        self.read_wants_write = False
        self.events = 0
        self.closed = False


def _resolve_future(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


def _fail_future(future: asyncio.Future[None], error: BaseException) -> None:
    if not future.done():
        future.set_exception(error)


class ExecReactor:
    """One thread that performs all socket I/O for every registered exec stream."""

    def __init__(self) -> None:
        self._selector = selectors.DefaultSelector()
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)
        self._selector.register(self._wake_r, selectors.EVENT_READ, None)
        self._commands: collections.deque[Callable[[], None]] = collections.deque()
        self._commands_lock = threading.Lock()
        self._entries: set[_StreamEntry] = set()
        self._carry_over: set[_StreamEntry] = set()
        self._stopping = False
        self._thread = threading.Thread(
            target=self._run, name="gke-exec-reactor", daemon=True
        )
        self._thread.start()

    # -- Thread-safe entry points ------------------------------------------------------

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def register(self, entry: _StreamEntry) -> None:
        self._call(lambda: self._add(entry))

    def submit_write(
        self, entry: _StreamEntry, frame: bytes, future: asyncio.Future[None]
    ) -> None:
        self._call(lambda: self._enqueue_user_write(entry, frame, future))

    def request_close(self, entry: _StreamEntry) -> None:
        self._call(lambda: self._finish(entry, END_LOCAL_CLOSE, None))

    def stop(self) -> None:
        def _stop() -> None:
            self._stopping = True

        self._call(_stop)
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=2.0)

    def _call(self, fn: Callable[[], None]) -> None:
        with self._commands_lock:
            self._commands.append(fn)
        try:
            self._wake_w.send(b"\0")
        except (BlockingIOError, InterruptedError):
            pass  # A wakeup is already pending.
        except OSError:
            pass  # Reactor already shut down; the command will never run.

    # -- Reactor thread ----------------------------------------------------------------

    def _run(self) -> None:
        while True:
            try:
                timeout = 0.0 if self._carry_over else self._select_timeout()
                events = self._selector.select(timeout)
                self._run_commands()
                if self._stopping:
                    break
                for key, mask in events:
                    entry = key.data
                    if entry is None:
                        self._drain_wakeups()
                        continue
                    if entry.closed:
                        continue
                    if mask & selectors.EVENT_WRITE:
                        if entry.read_wants_write:
                            self._read(entry)
                        if not entry.closed:
                            self._flush(entry)
                    if mask & selectors.EVENT_READ and not entry.closed:
                        self._read(entry)
                if self._carry_over:
                    for entry in list(self._carry_over):
                        self._carry_over.discard(entry)
                        if not entry.closed:
                            self._read(entry)
                self._send_due_pings()
                self._deliver_pending()
            except Exception:
                # A bug here must not silently kill every exec stream in the process.
                logger.exception("GKE exec reactor iteration failed")
                time.sleep(0.05)
        for entry in list(self._entries):
            self._finish(entry, END_SHUTDOWN, None)
        self._deliver_pending()
        try:
            self._selector.close()
        finally:
            self._wake_r.close()
            self._wake_w.close()

    def _run_commands(self) -> None:
        while True:
            with self._commands_lock:
                if not self._commands:
                    return
                fn = self._commands.popleft()
            fn()

    def _drain_wakeups(self) -> None:
        try:
            while self._wake_r.recv(4096):
                pass
        except (BlockingIOError, InterruptedError):
            pass

    def _select_timeout(self) -> float:
        if not self._entries:
            return _IDLE_SELECT_TIMEOUT_SEC
        now = time.monotonic()
        soonest = min(entry.next_ping for entry in self._entries)
        return max(0.0, min(_IDLE_SELECT_TIMEOUT_SEC, soonest - now))

    def _post(self, entry: _StreamEntry, fn: Callable[..., None], *args: Any) -> bool:
        try:
            entry.loop.call_soon_threadsafe(fn, *args)
            return True
        except RuntimeError:
            # The owning event loop is closed; nobody can consume this stream.
            if not entry.closed:
                self._finish(entry, END_LOCAL_CLOSE, None, notify=False)
            return False

    def _add(self, entry: _StreamEntry) -> None:
        if self._stopping:
            entry.closed = True
            self._post(
                entry,
                entry.stream._on_closed,
                END_SHUTDOWN,
                RuntimeError("exec reactor is shutting down"),
            )
            return
        try:
            entry.sock.setblocking(False)
            entry.events = selectors.EVENT_READ
            self._selector.register(entry.sock, entry.events, entry)
        except Exception as e:
            entry.closed = True
            self._close_socket(entry)
            self._post(entry, entry.stream._on_closed, END_CONNECTION_LOST, e)
            return
        entry.next_ping = time.monotonic() + _GKE_EXEC_STREAM_PING_INTERVAL_SEC
        self._entries.add(entry)
        # TLS may already hold decrypted bytes that select() cannot report.
        self._carry_over.add(entry)

    def _enqueue_user_write(
        self, entry: _StreamEntry, frame: bytes, future: asyncio.Future[None]
    ) -> None:
        if entry.closed:
            self._post(
                entry,
                _fail_future,
                future,
                GKEExecStreamClosedError(
                    f"Exec stream {entry.stream.label} closed before the write was sent"
                ),
            )
            return
        entry.outgoing.append(_PendingWrite(frame, future))
        self._flush(entry)

    def _enqueue_control(
        self, entry: _StreamEntry, opcode: int, payload: bytes
    ) -> None:
        entry.outgoing.append(_PendingWrite(encode_client_frame(opcode, payload), None))
        self._flush(entry)

    def _update_interest(self, entry: _StreamEntry) -> None:
        if entry.closed:
            return
        events = selectors.EVENT_READ
        if entry.outgoing or entry.read_wants_write:
            events |= selectors.EVENT_WRITE
        if events != entry.events:
            entry.events = events
            self._selector.modify(entry.sock, events, entry)

    def _flush(self, entry: _StreamEntry) -> None:
        while entry.outgoing and not entry.closed:
            item = entry.outgoing[0]
            try:
                # A partially sent item is always retried with the identical remaining
                # bytes, as OpenSSL requires after SSL_ERROR_WANT_WRITE.
                sent = entry.sock.send(item.view[item.offset :])
            except (
                ssl.SSLWantWriteError,
                ssl.SSLWantReadError,
                BlockingIOError,
                InterruptedError,
            ):
                break
            except OSError as e:
                self._finish(entry, END_CONNECTION_LOST, e)
                return
            if sent <= 0:
                break
            item.offset += sent
            if item.offset >= len(item.view):
                entry.outgoing.popleft()
                if item.future is not None:
                    self._post(entry, _resolve_future, item.future)
        self._update_interest(entry)

    def _read(self, entry: _StreamEntry) -> None:
        budget = _READ_BUDGET_BYTES
        while budget > 0:
            try:
                data = entry.sock.recv(_RECV_CHUNK_BYTES)
            except ssl.SSLWantReadError:
                if entry.read_wants_write:
                    entry.read_wants_write = False
                    self._update_interest(entry)
                return
            except ssl.SSLWantWriteError:
                entry.read_wants_write = True
                self._update_interest(entry)
                return
            except (BlockingIOError, InterruptedError):
                return
            except OSError as e:
                self._finish(entry, END_CONNECTION_LOST, e)
                return
            if not data:
                self._finish(
                    entry,
                    END_CONNECTION_LOST,
                    ConnectionError(
                        "connection closed without a WebSocket CLOSE frame"
                    ),
                )
                return
            if entry.read_wants_write:
                entry.read_wants_write = False
                self._update_interest(entry)
            budget -= len(data)
            try:
                messages = entry.parser.feed(data)
            except WebSocketProtocolError as e:
                self._finish(entry, END_PROTOCOL_ERROR, e)
                return
            for opcode, payload in messages:
                if opcode in _DATA_OPCODES:
                    # v4.channel.k8s.io: the first byte names the channel.
                    if len(payload) > 1:
                        entry.pending_data.append((payload[0], payload[1:]))
                elif opcode == _OPCODE_PING:
                    self._enqueue_control(entry, _OPCODE_PONG, payload)
                elif opcode == _OPCODE_CLOSE:
                    self._finish(entry, END_REMOTE_CLOSE, None)
                    return
                if entry.closed:
                    return
        # Budget exhausted: come back after serving the other streams.
        self._carry_over.add(entry)

    def _send_due_pings(self) -> None:
        now = time.monotonic()
        for entry in list(self._entries):
            if not entry.closed and now >= entry.next_ping:
                entry.next_ping = now + _GKE_EXEC_STREAM_PING_INTERVAL_SEC
                self._enqueue_control(entry, _OPCODE_PING, b"")

    def _deliver_pending(self) -> None:
        for entry in list(self._entries):
            if entry.pending_data:
                items, entry.pending_data = entry.pending_data, []
                self._post(entry, entry.stream._on_data, items)

    def _close_socket(self, entry: _StreamEntry) -> None:
        try:
            self._selector.unregister(entry.sock)
        except (KeyError, ValueError, OSError):
            pass
        try:
            entry.sock.close()
        except OSError:
            pass

    def _finish(
        self,
        entry: _StreamEntry,
        reason: str,
        error: BaseException | None,
        *,
        notify: bool = True,
    ) -> None:
        if entry.closed:
            return
        entry.closed = True
        if reason in (END_LOCAL_CLOSE, END_REMOTE_CLOSE, END_SHUTDOWN) and not (
            entry.outgoing and entry.outgoing[0].offset
        ):
            # Best effort: a CLOSE frame must not be spliced into a partial frame.
            try:
                entry.sock.send(
                    encode_client_frame(_OPCODE_CLOSE, struct.pack("!H", 1000))
                )
            except (OSError, ValueError):
                pass
        pending_writes = list(entry.outgoing)
        entry.outgoing.clear()
        self._close_socket(entry)
        self._entries.discard(entry)
        self._carry_over.discard(entry)
        if reason in (END_CONNECTION_LOST, END_PROTOCOL_ERROR):
            logger.debug(
                "Exec stream %s ended: %s %s",
                entry.stream.label,
                reason,
                _describe_error(error),
            )
        if not notify:
            return
        # Data first, then the close notification: call_soon_threadsafe is FIFO.
        if entry.pending_data:
            items, entry.pending_data = entry.pending_data, []
            self._post(entry, entry.stream._on_data, items)
        write_error = GKEExecStreamClosedError(
            f"Exec stream {entry.stream.label} closed before the write completed "
            f"({reason}{': ' + _describe_error(error) if error else ''})"
        )
        for item in pending_writes:
            if item.future is not None:
                self._post(entry, _fail_future, item.future, write_error)
        self._post(entry, entry.stream._on_closed, reason, error)


_REACTOR: ExecReactor | None = None
_REACTOR_LOCK = threading.Lock()


def get_exec_reactor() -> ExecReactor:
    """Return the process-wide reactor, starting it on first use."""
    global _REACTOR
    with _REACTOR_LOCK:
        if _REACTOR is None or not _REACTOR.is_alive:
            _REACTOR = ExecReactor()
        return _REACTOR


def shutdown_exec_reactor() -> None:
    """Close every exec stream and stop the reactor thread, if it is running."""
    global _REACTOR
    with _REACTOR_LOCK:
        reactor, _REACTOR = _REACTOR, None
    if reactor is not None:
        try:
            reactor.stop()
        except Exception:
            pass


class ExecOutputAccumulator:
    """Collects stdout and stderr of one command across streams and recovery polls.

    Bytes are decoded with one incremental UTF-8 decoder per stream, so a
    multi-byte character split across frames or polls is decoded correctly.
    Byte counts are kept exactly, for resuming from on-disk output files.
    """

    def __init__(self, callback: Any = None) -> None:
        self._callback = callback
        self._decoders = {
            "stdout": codecs.getincrementaldecoder("utf-8")("replace"),
            "stderr": codecs.getincrementaldecoder("utf-8")("replace"),
        }
        self._parts: dict[str, list[str]] = {"stdout": [], "stderr": []}
        self.stdout_bytes = 0
        self.stderr_bytes = 0

    async def feed(self, stream_name: str, data: bytes) -> None:
        if not data:
            return
        if stream_name == "stdout":
            self.stdout_bytes += len(data)
        else:
            self.stderr_bytes += len(data)
        text = self._decoders[stream_name].decode(data)
        await self._emit(stream_name, text)

    async def feed_channel(self, channel: int, data: bytes) -> None:
        if channel == STDOUT_CHANNEL:
            await self.feed("stdout", data)
        elif channel == STDERR_CHANNEL:
            await self.feed("stderr", data)

    async def finish(self) -> None:
        """Flush incomplete trailing characters (as U+FFFD)."""
        for name, decoder in self._decoders.items():
            await self._emit(name, decoder.decode(b"", final=True))

    async def _emit(self, stream_name: str, text: str) -> None:
        if not text:
            return
        self._parts[stream_name].append(text)
        if self._callback is not None:
            await self._callback(text, stream_name)

    @property
    def stdout(self) -> str:
        return "".join(self._parts["stdout"])

    @property
    def stderr(self) -> str:
        return "".join(self._parts["stderr"])
