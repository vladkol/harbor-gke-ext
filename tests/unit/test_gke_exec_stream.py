"""Tests for the exec reactor, frame parser and ExecStream (exec_stream.py)."""

from __future__ import annotations

import asyncio
import json
import os
import random
import socket
import struct
import threading
import time

import pytest

from harbor_gke_ext.constants import GKEExecStreamClosedError
from harbor_gke_ext.exec_stream import (
    END_CONNECTION_LOST,
    END_LOCAL_CLOSE,
    END_REMOTE_CLOSE,
    ExecOutputAccumulator,
    ExecReactor,
    ExecStream,
    FrameParser,
    WebSocketProtocolError,
    encode_client_frame,
    parse_exec_status,
    shutdown_exec_reactor,
)

from fake_kubelet import (
    OP_BINARY,
    OP_CLOSE,
    OP_PING,
    OP_PONG,
    OP_TEXT,
    channel_frame,
    server_frame,
    status_payload,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _fresh_reactor():
    shutdown_exec_reactor()
    yield
    shutdown_exec_reactor()


# ─── Frame parser ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("size", [0, 1, 125, 126, 65535, 65536, 200_000])
def test_parser_payload_lengths(size):
    payload = os.urandom(size)
    frames = FrameParser().feed(server_frame(OP_BINARY, payload))
    assert frames == [(OP_BINARY, payload)]


def test_parser_byte_by_byte_and_multiple_frames():
    wire = (
        server_frame(OP_BINARY, b"\x01hello")
        + server_frame(OP_PING, b"p")
        + server_frame(OP_BINARY, b"\x02" + b"x" * 70000)
    )
    parser = FrameParser()
    out = []
    for i in range(len(wire)):
        out.extend(parser.feed(wire[i : i + 1]))
    assert out == [
        (OP_BINARY, b"\x01hello"),
        (OP_PING, b"p"),
        (OP_BINARY, b"\x02" + b"x" * 70000),
    ]


def test_parser_reassembles_fragments_with_interleaved_control():
    wire = (
        server_frame(OP_TEXT, b"\x01ab", fin=False)
        + server_frame(OP_PING, b"")
        + server_frame(0x0, b"cd", fin=False)
        + server_frame(0x0, b"ef", fin=True)
    )
    assert FrameParser().feed(wire) == [(OP_PING, b""), (OP_TEXT, b"\x01abcdef")]


def test_parser_unmasks_masked_frames():
    # Client frames are masked; the parser handles both directions.
    frame = encode_client_frame(OP_BINARY, b"\x00payload")
    assert FrameParser().feed(frame) == [(OP_BINARY, b"\x00payload")]


@pytest.mark.parametrize(
    "wire",
    [
        bytes([0x80 | 0x40 | OP_BINARY, 0]),  # RSV1 set
        server_frame(0x0, b"orphan"),  # continuation without a start
        server_frame(OP_PING, b"x" * 126),  # control frame too long
        server_frame(OP_PING, b"", fin=False),  # fragmented control frame
        server_frame(0x3, b""),  # reserved data opcode
        server_frame(OP_TEXT, b"a", fin=False) + server_frame(OP_TEXT, b"b"),
    ],
)
def test_parser_rejects_protocol_violations(wire):
    with pytest.raises(WebSocketProtocolError):
        FrameParser().feed(wire)


def test_parser_rejects_oversized_frames_before_buffering():
    header = struct.pack("!BBQ", 0x80 | OP_BINARY, 127, 1 << 40)
    with pytest.raises(WebSocketProtocolError):
        FrameParser(max_message_bytes=1024).feed(header)


# ─── Status parsing ────────────────────────────────────────────────────────────


def test_parse_exec_status_success_and_exit_codes():
    assert parse_exec_status(status_payload(0)) == 0
    assert parse_exec_status(status_payload(7)) == 7
    assert parse_exec_status(status_payload(137)) == 137


def test_parse_exec_status_failure_without_exit_code_reports_server_message():
    payload = json.dumps(
        {
            "status": "Failure",
            "message": 'OCI runtime exec failed: exec: "bash": not found',
            "reason": "InternalError",
        }
    ).encode()
    with pytest.raises(GKEExecStreamClosedError, match="bash.*not found"):
        parse_exec_status(payload)


@pytest.mark.parametrize("payload", [b"not json", b"[1, 2]", b""])
def test_parse_exec_status_unreadable(payload):
    with pytest.raises(GKEExecStreamClosedError):
        parse_exec_status(payload)


# ─── ExecStream over a socketpair ──────────────────────────────────────────────


class ScriptedServer:
    """Plays a scripted server side on one end of a socketpair."""

    def __init__(self) -> None:
        self.client_sock, self.sock = socket.socketpair()
        self.parser = FrameParser()
        self.received: list[tuple[int, bytes]] = []

    def send(self, data: bytes) -> None:
        self.sock.sendall(data)

    def recv_frames(self, until_opcode: int | None = None, timeout: float = 5.0):
        self.sock.settimeout(timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = self.sock.recv(65536)
            except TimeoutError:
                break
            if not data:
                break
            frames = self.parser.feed(data)
            self.received.extend(frames)
            if until_opcode is not None and any(op == until_opcode for op, _ in frames):
                break
        return self.received


async def _read_all(stream: ExecStream) -> tuple[bytes, bytes]:
    out, err = bytearray(), bytearray()
    while (item := await stream.read()) is not None:
        channel, data = item
        (out if channel == 1 else err).extend(data)
    return bytes(out), bytes(err)


async def test_stream_delivers_channels_status_and_replies_to_close():
    server = ScriptedServer()
    stream = ExecStream.attach(server.client_sock, label="t")
    server.send(channel_frame(1, b"out-1 "))
    server.send(channel_frame(2, b"err-1"))
    server.send(channel_frame(1, b"out-2"))
    server.send(channel_frame(3, status_payload(3)))
    server.send(server_frame(OP_CLOSE, struct.pack("!H", 1000)))

    out, err = await asyncio.wait_for(_read_all(stream), 5)
    assert (out, err) == (b"out-1 out-2", b"err-1")
    assert stream.returncode() == 3
    assert stream.end_reason == END_REMOTE_CLOSE
    frames = await asyncio.to_thread(server.recv_frames, OP_CLOSE)
    assert frames[-1][0] == OP_CLOSE


async def test_stream_lost_without_status_raises_with_cause():
    server = ScriptedServer()
    stream = ExecStream.attach(server.client_sock, label="t")
    server.send(channel_frame(1, b"partial"))
    server.sock.close()

    out, _ = await asyncio.wait_for(_read_all(stream), 5)
    assert out == b"partial"
    assert stream.end_reason == END_CONNECTION_LOST
    with pytest.raises(GKEExecStreamClosedError, match="ConnectionError"):
        stream.raise_unless_completed("ctx")
    with pytest.raises(GKEExecStreamClosedError):
        stream.returncode()


async def test_status_then_eof_without_close_frame_counts_as_completed():
    server = ScriptedServer()
    stream = ExecStream.attach(server.client_sock, label="t")
    server.send(channel_frame(3, status_payload(0)))
    server.sock.close()
    await asyncio.wait_for(stream.wait_closed(), 5)
    stream.raise_unless_completed("ctx")
    assert stream.returncode() == 0


async def test_server_ping_gets_pong_with_same_payload():
    server = ScriptedServer()
    stream = ExecStream.attach(server.client_sock, label="t")
    server.send(server_frame(OP_PING, b"are-you-there"))
    frames = await asyncio.to_thread(server.recv_frames, OP_PONG)
    assert (OP_PONG, b"are-you-there") in frames
    stream.close()


async def test_reactor_sends_keepalive_pings():
    reactor = ExecReactor(ping_interval_sec=0.05)
    try:
        server = ScriptedServer()
        stream = ExecStream.attach(server.client_sock, label="t", reactor=reactor)
        frames = await asyncio.to_thread(server.recv_frames, OP_PING, 3.0)
        assert any(op == OP_PING for op, _ in frames)
        stream.close()
    finally:
        reactor.stop()


async def test_local_close_is_non_blocking_and_fails_later_writes():
    server = ScriptedServer()
    stream = ExecStream.attach(server.client_sock, label="t")
    started = time.monotonic()
    stream.close()
    assert time.monotonic() - started < 0.05
    assert await stream.wait_closed(5)
    assert stream.end_reason == END_LOCAL_CLOSE
    frames = await asyncio.to_thread(server.recv_frames, OP_CLOSE)
    assert frames and frames[-1][0] == OP_CLOSE
    with pytest.raises(GKEExecStreamClosedError):
        await stream.write(b"late")


async def test_write_backpressure_delivers_exact_bytes_in_order():
    server = ScriptedServer()
    server.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    stream = ExecStream.attach(server.client_sock, label="t")
    chunks = [os.urandom(random.randint(1, 300_000)) for _ in range(12)]
    expected = b"".join(chunks)
    received = bytearray()
    parser = FrameParser()

    def slow_reader():
        server.sock.settimeout(10)
        while len(received) < len(expected):
            data = server.sock.recv(1024)
            if not data:
                break
            for opcode, payload in parser.feed(data):
                if opcode == OP_BINARY:
                    assert payload[:1] == b"\x00"
                    received.extend(payload[1:])
            time.sleep(0.0005)

    reader = threading.Thread(target=slow_reader, daemon=True)
    reader.start()
    for chunk in chunks:
        await asyncio.wait_for(stream.write(chunk), 30)
    await asyncio.to_thread(reader.join, 30)
    assert bytes(received) == expected
    stream.close()


async def test_pending_write_fails_when_connection_drops():
    server = ScriptedServer()
    server.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    stream = ExecStream.attach(server.client_sock, label="t")
    write = asyncio.create_task(stream.write(os.urandom(8 * 1024 * 1024)))
    await asyncio.sleep(0.2)
    assert not write.done()
    server.sock.close()
    with pytest.raises(GKEExecStreamClosedError):
        await asyncio.wait_for(write, 5)


async def test_protocol_error_ends_stream_without_status():
    server = ScriptedServer()
    stream = ExecStream.attach(server.client_sock, label="t")
    server.send(server_frame(0x0, b"orphan continuation"))
    assert await stream.wait_closed(5)
    with pytest.raises(GKEExecStreamClosedError, match="protocol_error"):
        stream.raise_unless_completed("ctx")


async def test_many_concurrent_streams_are_byte_exact():
    """200 streams on one reactor thread, random frame sizes and split points."""
    n_streams = 200
    servers = [ScriptedServer() for _ in range(n_streams)]
    streams = [
        ExecStream.attach(s.client_sock, label=f"s{i}") for i, s in enumerate(servers)
    ]
    payloads = [
        (os.urandom(random.randint(0, 150_000)), os.urandom(random.randint(0, 20_000)))
        for _ in range(n_streams)
    ]

    def serve(server: ScriptedServer, out: bytes, err: bytes, rc: int) -> None:
        wire = bytearray()
        for data, channel in ((out, 1), (err, 2)):
            pos = 0
            while pos < len(data):
                step = random.randint(1, 40_000)
                wire += channel_frame(channel, data[pos : pos + step])
                pos += step
        wire += channel_frame(3, status_payload(rc))
        wire += server_frame(OP_CLOSE, struct.pack("!H", 1000))
        pos = 0
        while pos < len(wire):
            step = random.randint(1, 9000)
            server.sock.sendall(wire[pos : pos + step])
            pos += step

    threads = [
        threading.Thread(target=serve, args=(s, o, e, i % 256), daemon=True)
        for i, (s, (o, e)) in enumerate(zip(servers, payloads))
    ]
    for t in threads:
        t.start()
    async with asyncio.timeout(60):
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(_read_all(s)) for s in streams]
    results = [t.result() for t in tasks]
    for i, ((out, err), (exp_out, exp_err)) in enumerate(zip(results, payloads)):
        assert out == exp_out, f"stream {i} stdout mismatch"
        assert err == exp_err, f"stream {i} stderr mismatch"
        assert streams[i].returncode() == i % 256
    for t in threads:
        t.join(5)


async def test_streams_on_a_closed_loop_do_not_break_the_reactor():
    server_a = ScriptedServer()

    def other_loop():
        async def attach():
            return ExecStream.attach(server_a.client_sock, label="dead-loop")

        asyncio.run(attach())  # loop closes with the stream still registered

    await asyncio.to_thread(other_loop)
    server_a.send(channel_frame(1, b"nobody listens"))

    server_b = ScriptedServer()
    stream_b = ExecStream.attach(server_b.client_sock, label="alive")
    server_b.send(channel_frame(1, b"ok"))
    server_b.send(channel_frame(3, status_payload(0)))
    server_b.send(server_frame(OP_CLOSE, b""))
    out, _ = await asyncio.wait_for(_read_all(stream_b), 5)
    assert out == b"ok"


# ─── Output accumulator ────────────────────────────────────────────────────────


async def test_accumulator_decodes_characters_split_across_chunks():
    seen: list[tuple[str, str]] = []

    async def callback(text: str, stream_name: str) -> None:
        seen.append((stream_name, text))

    acc = ExecOutputAccumulator(callback)
    data = "naïve € 日本 ✓".encode()
    for i in range(len(data)):
        await acc.feed("stdout", data[i : i + 1])
    await acc.feed("stderr", b"\xe2\x82")  # truncated "€"
    await acc.finish()
    assert acc.stdout == "naïve € 日本 ✓"
    assert acc.stdout_bytes == len(data)
    assert acc.stderr == "\ufffd"
    assert acc.stderr_bytes == 2
    assert "".join(t for s, t in seen if s == "stdout") == acc.stdout


async def test_accumulator_propagates_callback_errors():
    async def callback(text: str, stream_name: str) -> None:
        raise RuntimeError("consumer failed")

    acc = ExecOutputAccumulator(callback)
    with pytest.raises(RuntimeError, match="consumer failed"):
        await acc.feed("stdout", b"x")
