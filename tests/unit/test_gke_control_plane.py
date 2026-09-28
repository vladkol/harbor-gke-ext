"""Unit tests for harbor_gke_ext.control_plane (overload classifier, jitter, AIMD limiter)."""

from __future__ import annotations

import asyncio

import pytest
from kubernetes.client.rest import ApiException

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

    def __call__(self) -> float:
        return self.now


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
def test_jitter_bounds_follow_capped_exponential_schedule(attempt, nominal):
    low = jittered_backoff_delay(attempt, uniform=lambda a, b: a)
    high = jittered_backoff_delay(attempt, uniform=lambda a, b: b)
    mid = jittered_backoff_delay(attempt, uniform=lambda a, b: (a + b) / 2)
    assert low == pytest.approx(nominal * 0.8)
    assert high == pytest.approx(nominal * 1.2)
    assert mid == pytest.approx(nominal)


@pytest.mark.unit
def test_jitter_default_rng_stays_in_bounds():
    for _ in range(200):
        assert 1.6 <= jittered_backoff_delay(0) <= 2.4


# ─────────────────────────────────────────────────────────────────────────────
# AdaptiveConcurrencyLimiter
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_limiter_rejects_invalid_configuration():
    with pytest.raises(ValueError):
        AdaptiveConcurrencyLimiter(minimum=0, maximum=4)
    with pytest.raises(ValueError):
        AdaptiveConcurrencyLimiter(minimum=8, maximum=4)
    with pytest.raises(ValueError):
        AdaptiveConcurrencyLimiter(minimum=1, maximum=4, decrease_factor=1.0)


@pytest.mark.unit
def test_limiter_starts_at_maximum_and_default_matches_exec_pool():
    assert AdaptiveConcurrencyLimiter(minimum=2, maximum=16).limit == 16
    assert get_control_plane_limiter().limit == 128
    assert get_control_plane_limiter() is get_control_plane_limiter()


@pytest.mark.unit
def test_limiter_overload_halves_once_per_cooldown_and_respects_minimum():
    clock = _FakeClock()
    lim = AdaptiveConcurrencyLimiter(
        minimum=4, maximum=128, decrease_cooldown_sec=5.0, clock=clock
    )
    lim.record_overload()
    assert lim.limit == 64
    # A burst of failures inside the cooldown is one overload episode.
    clock.now += 4.9
    lim.record_overload()
    lim.record_overload()
    assert lim.limit == 64
    for expected in (32, 16, 8, 4, 4):
        clock.now += 5.0
        lim.record_overload()
        assert lim.limit == expected


@pytest.mark.unit
def test_limiter_success_increases_additively_after_cooldown():
    clock = _FakeClock()
    lim = AdaptiveConcurrencyLimiter(
        minimum=4,
        maximum=10,
        decrease_cooldown_sec=5.0,
        increase_interval_sec=1.0,
        clock=clock,
    )
    lim.record_overload()
    assert lim.limit == 5
    # No growth during the cooldown after a decrease.
    clock.now += 4.0
    lim.record_success()
    assert lim.limit == 5
    clock.now += 1.0
    lim.record_success()
    assert lim.limit == 6
    # At most one slot per increase interval, however many successes.
    lim.record_success()
    lim.record_success()
    assert lim.limit == 6
    for _ in range(10):
        clock.now += 1.0
        lim.record_success()
    assert lim.limit == 10  # capped at maximum


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_bounds_concurrency_and_serves_waiters_fifo():
    lim = AdaptiveConcurrencyLimiter(minimum=1, maximum=2)
    await lim.acquire()
    await lim.acquire()
    order: list[str] = []

    async def waiter(name: str) -> None:
        async with lim:
            order.append(name)

    t1 = asyncio.create_task(waiter("a"))
    await asyncio.sleep(0)
    t2 = asyncio.create_task(waiter("b"))
    await asyncio.sleep(0)
    assert lim.in_flight == 2
    assert lim.waiting == 2
    lim.release()
    lim.release()
    await asyncio.gather(t1, t2)
    assert order == ["a", "b"]
    assert lim.in_flight == 0
    assert lim.waiting == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_shrink_does_not_interrupt_in_flight_and_growth_wakes_waiters():
    clock = _FakeClock()
    lim = AdaptiveConcurrencyLimiter(
        minimum=1,
        maximum=4,
        decrease_cooldown_sec=5.0,
        increase_interval_sec=1.0,
        clock=clock,
    )
    for _ in range(4):
        await lim.acquire()
    lim.record_overload()
    assert lim.limit == 2
    assert lim.in_flight == 4

    acquired = asyncio.Event()

    async def waiter() -> None:
        await lim.acquire()
        acquired.set()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)
    # Releasing down to the new limit must not admit the waiter yet.
    lim.release()
    lim.release()
    await asyncio.sleep(0)
    assert not acquired.is_set()
    assert lim.in_flight == 2
    # Recovery raises the limit and admits the waiter immediately.
    clock.now += 5.0
    lim.record_success()
    await asyncio.wait_for(task, timeout=1.0)
    assert acquired.is_set()
    assert lim.limit == 3
    assert lim.in_flight == 3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_cancelled_waiter_does_not_leak_a_slot():
    lim = AdaptiveConcurrencyLimiter(minimum=1, maximum=1)
    await lim.acquire()
    task = asyncio.create_task(lim.acquire())
    await asyncio.sleep(0)
    assert lim.waiting == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert lim.waiting == 0
    lim.release()
    assert lim.in_flight == 0
    await asyncio.wait_for(lim.acquire(), timeout=1.0)
    assert lim.in_flight == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limiter_slot_granted_then_cancelled_is_returned():
    lim = AdaptiveConcurrencyLimiter(minimum=1, maximum=1)
    await lim.acquire()
    task = asyncio.create_task(lim.acquire())
    await asyncio.sleep(0)
    # Grant the slot and cancel in the same tick, before the waiter resumes.
    lim.release()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert lim.in_flight == 0
    assert lim.waiting == 0


@pytest.mark.unit
def test_limiter_release_without_acquire_is_an_error():
    lim = AdaptiveConcurrencyLimiter(minimum=1, maximum=1)
    with pytest.raises(RuntimeError, match="released more than acquired"):
        lim.release()
