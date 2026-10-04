"""Unit tests for harbor_gke_ext.control_plane (overload classifier, jitter, AIMD limiter)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException
from limiter_probe import free_slots

from harbor_gke_ext import control_plane
from harbor_gke_ext.constants import (
    _GKE_CONTROL_PLANE_LIMIT_MAX,
    _GKE_EXEC_SEMAPHORE_LIMIT,
)
from harbor_gke_ext.control_plane import (
    AdaptiveConcurrencyLimiter,
    get_control_plane_limiter,
    is_control_plane_overload,
    jittered_backoff_delay,
)

_WARDEN_BODY = (
    '{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",'
    '"message":"Internal error occurred: failed calling webhook '
    '\\"warden-validating.common-webhooks.networking.gke.io\\": failed to call '
    'webhook: Post \\"https://localhost:5443/webhook/warden-validating?timeout=10s\\": '
    'context deadline exceeded","reason":"InternalError","code":500}'
)


def _api_error(status: int, reason: str = "", body: str | None = None) -> ApiException:
    err = ApiException(status=status, reason=reason)
    err.body = body
    return err


class _FakeClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch) -> _FakeClock:
    """Drive the limiter's cooldowns without touching the event loop's clock."""
    fake = _FakeClock()
    monkeypatch.setattr(control_plane, "time", fake)
    return fake


async def _fill(limiter: AdaptiveConcurrencyLimiter) -> None:
    for _ in range(_GKE_CONTROL_PLANE_LIMIT_MAX):
        await limiter.acquire()


def _release(limiter: AdaptiveConcurrencyLimiter, count: int) -> None:
    for _ in range(count):
        limiter.release()


# ─────────────────────────────────────────────────────────────────────────────
# is_control_plane_overload
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_overload_rest_webhook_call_failure():
    assert is_control_plane_overload(
        _api_error(500, "Internal Server Error", _WARDEN_BODY)
    )


@pytest.mark.unit
def test_overload_exec_handshake_webhook_failure_in_reason():
    # WebSocket exec handshakes report status=0 and put everything in `reason`.
    reason = (
        "Handshake status 500 Internal Server Error -+-+- {'audit-id': 'x'} -+-+- b'"
        + _WARDEN_BODY
        + "'"
    )
    assert is_control_plane_overload(_api_error(0, reason))


@pytest.mark.unit
@pytest.mark.parametrize("status", [429, 503])
def test_overload_throttling_statuses(status):
    assert is_control_plane_overload(_api_error(status, "Too Many Requests"))


@pytest.mark.unit
def test_overload_handshake_429():
    assert is_control_plane_overload(_api_error(0, "Handshake status 429 Too Many"))


@pytest.mark.unit
@pytest.mark.parametrize(
    "err",
    [
        # Plain 500 without the webhook marker (for example kubelet "container not found").
        _api_error(500, "Internal Server Error", '{"message":"container not found"}'),
        # Konnectivity tunnel down: transient, but not control-plane load.
        _api_error(
            503,
            "Service Unavailable",
            '{"message":"error dialing backend: No agent available"}',
        ),
        # A webhook *denial* is a policy decision, not overload.
        _api_error(
            400,
            "Bad Request",
            '{"message":"admission webhook \\"x\\" denied the request: failed calling webhook"}',
        ),
        _api_error(404, "Not Found"),
        _api_error(409, "Conflict"),
    ],
)
def test_not_overload(err):
    assert not is_control_plane_overload(err)


@pytest.mark.unit
def test_non_api_exceptions_are_not_overload():
    assert not is_control_plane_overload(TimeoutError("timed out"))
    assert not is_control_plane_overload(OSError("connection reset"))


# ─────────────────────────────────────────────────────────────────────────────
# jittered_backoff_delay
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    ("attempt", "nominal"),
    [(0, 2.0), (1, 4.0), (2, 8.0), (3, 16.0), (4, 20.0), (10, 20.0)],
)
@pytest.mark.parametrize(
    ("draw", "factor"),
    [
        (lambda a, b: a, 0.8),
        (lambda a, b: (a + b) / 2, 1.0),
        (lambda a, b: b, 1.2),
    ],
    ids=["low", "mid", "high"],
)
def test_jitter_follows_capped_exponential_schedule(
    monkeypatch, attempt, nominal, draw, factor
):
    monkeypatch.setattr(control_plane, "random", SimpleNamespace(uniform=draw))
    assert jittered_backoff_delay(attempt) == pytest.approx(nominal * factor)


@pytest.mark.unit
def test_jitter_default_rng_stays_in_bounds():
    for _ in range(200):
        assert 1.6 <= jittered_backoff_delay(0) <= 2.4


# ─────────────────────────────────────────────────────────────────────────────
# AdaptiveConcurrencyLimiter
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_starts_at_exec_pool_size_and_is_process_wide():
    assert await free_slots(AdaptiveConcurrencyLimiter()) == _GKE_EXEC_SEMAPHORE_LIMIT
    assert get_control_plane_limiter() is get_control_plane_limiter()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_overload_halves_once_per_cooldown_and_respects_minimum(clock):
    lim = AdaptiveConcurrencyLimiter()
    lim.record_overload()
    assert await free_slots(lim) == 64
    # A burst of failures inside the cooldown is one overload episode.
    clock.now += 4.9
    lim.record_overload()
    lim.record_overload()
    assert await free_slots(lim) == 64
    for expected in (32, 16, 8, 4, 4):
        clock.now += 5.0
        lim.record_overload()
        assert await free_slots(lim) == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_success_increases_additively_after_cooldown(clock):
    lim = AdaptiveConcurrencyLimiter()
    lim.record_overload()
    # No growth during the cooldown after a decrease.
    clock.now += 4.0
    lim.record_success()
    assert await free_slots(lim) == 64
    clock.now += 1.0
    lim.record_success()
    assert await free_slots(lim) == 65
    # At most one slot per increase interval, however many successes.
    lim.record_success()
    lim.record_success()
    assert await free_slots(lim) == 65
    for _ in range(_GKE_CONTROL_PLANE_LIMIT_MAX):
        clock.now += 1.0
        lim.record_success()
    assert await free_slots(lim) == _GKE_CONTROL_PLANE_LIMIT_MAX


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_bounds_concurrency_and_serves_waiters_fifo():
    lim = AdaptiveConcurrencyLimiter()
    await _fill(lim)
    order: list[str] = []

    async def waiter(name: str) -> None:
        async with lim:
            order.append(name)

    t1 = asyncio.create_task(waiter("a"))
    await asyncio.sleep(0)
    t2 = asyncio.create_task(waiter("b"))
    await asyncio.sleep(0)
    assert order == []
    _release(lim, 2)
    await asyncio.gather(t1, t2)
    assert order == ["a", "b"]
    _release(lim, _GKE_CONTROL_PLANE_LIMIT_MAX - 2)
    assert await free_slots(lim) == _GKE_CONTROL_PLANE_LIMIT_MAX


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_shrink_does_not_interrupt_in_flight_and_growth_wakes_waiters(
    clock,
):
    lim = AdaptiveConcurrencyLimiter()
    await _fill(lim)
    lim.record_overload()

    acquired = asyncio.Event()

    async def waiter() -> None:
        await lim.acquire()
        acquired.set()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)
    # Releasing down to the new limit must not admit the waiter yet.
    _release(lim, 64)
    await asyncio.sleep(0)
    assert not acquired.is_set()
    # Recovery raises the limit and admits the waiter immediately.
    clock.now += 5.0
    lim.record_success()
    await asyncio.wait_for(task, timeout=1.0)
    assert await free_slots(lim) == 0
    _release(lim, 65)
    assert await free_slots(lim) == 65


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "grant_before_cancel",
    [False, True],
    ids=["cancelled-while-waiting", "granted-then-cancelled-same-tick"],
)
async def test_limiter_cancelled_waiter_does_not_leak_a_slot(grant_before_cancel):
    lim = AdaptiveConcurrencyLimiter()
    await _fill(lim)
    task = asyncio.create_task(lim.acquire())
    await asyncio.sleep(0)
    if grant_before_cancel:
        lim.release()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    if not grant_before_cancel:
        lim.release()
    assert await free_slots(lim) == 1


@pytest.mark.unit
def test_limiter_release_without_acquire_is_an_error():
    with pytest.raises(RuntimeError, match="released more than acquired"):
        AdaptiveConcurrencyLimiter().release()
