"""Adaptive back-pressure for Kubernetes control-plane calls.

On a Standard cluster the API server absorbs Harbor's peak exec and create rate,
and the static handshake bound (``_GKE_EXEC_SEMAPHORE_LIMIT``) is never the
bottleneck. On Autopilot every ``pods/exec`` CONNECT and every Pod/Job write is
also validated by Google-managed, fail-closed admission webhooks (for example
``warden-validating.common-webhooks.networking.gke.io``). Under a burst those
webhook calls time out and the API server answers HTTP 500 "failed calling
webhook". A fixed limit and fixed backoff keep offering the same load, so the
overload persists until retry budgets run out.

This module supplies the three pieces used on those paths:

- ``is_control_plane_overload`` classifies an error as control-plane overload.
- ``AdaptiveConcurrencyLimiter`` is an AIMD concurrency limit shared by exec
  handshakes and Job/Pod creates. It shrinks on overload and grows back on
  success, and it never exceeds the previous static limit.
- ``jittered_backoff_delay`` is the existing exponential schedule with a
  mean-preserving uniform spread, so clients that failed together spread out.
"""

from __future__ import annotations

import asyncio
import collections
import functools
import math
import random
import time

from harbor_gke_ext.client import _extract_api_status_code
from harbor_gke_ext.constants import (
    _GKE_CONTROL_PLANE_LIMIT_DECREASE_COOLDOWN_SEC,
    _GKE_CONTROL_PLANE_LIMIT_DECREASE_FACTOR,
    _GKE_CONTROL_PLANE_LIMIT_INCREASE_INTERVAL_SEC,
    _GKE_CONTROL_PLANE_LIMIT_MAX,
    _GKE_CONTROL_PLANE_LIMIT_MIN,
    _GKE_EXEC_CONNECT_EXPONENTIAL_BASE,
    _GKE_EXEC_CONNECT_INITIAL_DELAY_SEC,
    _GKE_EXEC_CONNECT_MAX_DELAY_SEC,
    _GKE_KONNECTIVITY_NO_AGENT_MARKER,
    _GKE_RETRY_JITTER_RATIO,
    _GKE_WEBHOOK_CALL_FAILURE_MARKER,
)
from harbor.utils.logger import logger

try:
    from kubernetes.client.rest import ApiException

    _HAS_KUBERNETES = True
except ImportError:
    _HAS_KUBERNETES = False


def is_control_plane_overload(exc: BaseException) -> bool:
    """Return True when ``exc`` means the Kubernetes control plane is overloaded.

    Overload is:

    - HTTP 429 (API Priority and Fairness throttling) or 503;
    - any 5xx whose message says a fail-closed admission webhook could not be
      called, which is how GKE Autopilot's Warden webhook fails under load.

    Status is read with ``_extract_api_status_code`` because WebSocket exec
    handshakes report ``status=0`` and carry the code in "Handshake status NNN".

    Not overload: a konnectivity "No agent available" error (a node tunnel
    problem), a webhook *denial* (a policy decision, HTTP 400/403), and any
    non-``ApiException`` such as socket timeouts, which cannot be told apart
    from ordinary network loss.
    """
    if not _HAS_KUBERNETES or not isinstance(exc, ApiException):
        return False
    text = f"{exc.reason or ''} {exc.body or ''}".lower()
    if _GKE_KONNECTIVITY_NO_AGENT_MARKER in text:
        return False
    status = _extract_api_status_code(exc)
    if status in (429, 503):
        return True
    return 500 <= status <= 599 and _GKE_WEBHOOK_CALL_FAILURE_MARKER in text


def jittered_backoff_delay(attempt: int) -> float:
    """Return the delay before retry ``attempt`` (0-based), with uniform jitter.

    The capped exponential value ``min(initial * base**attempt, max)`` is scaled
    by a factor drawn uniformly from ``[1 - jitter_ratio, 1 + jitter_ratio]``.
    The mean equals the unjittered schedule, so the expected total retry window
    is unchanged. Only the synchronisation between clients is removed.
    """
    nominal = min(
        _GKE_EXEC_CONNECT_INITIAL_DELAY_SEC
        * (_GKE_EXEC_CONNECT_EXPONENTIAL_BASE**attempt),
        _GKE_EXEC_CONNECT_MAX_DELAY_SEC,
    )
    return nominal * random.uniform(
        1.0 - _GKE_RETRY_JITTER_RATIO, 1.0 + _GKE_RETRY_JITTER_RATIO
    )


class AdaptiveConcurrencyLimiter:
    """An asyncio concurrency limit with additive-increase/multiplicative-decrease.

    Use it like a semaphore (``async with limiter:``) and report outcomes with
    ``record_success`` and ``record_overload``. The limit:

    - starts at ``_GKE_CONTROL_PLANE_LIMIT_MAX`` and never leaves
      ``[_GKE_CONTROL_PLANE_LIMIT_MIN, _GKE_CONTROL_PLANE_LIMIT_MAX]``;
    - is multiplied by ``_GKE_CONTROL_PLANE_LIMIT_DECREASE_FACTOR`` on overload,
      at most once per ``_GKE_CONTROL_PLANE_LIMIT_DECREASE_COOLDOWN_SEC``,
      because a burst of calls failing together reflects one overload episode;
    - grows by one slot per ``_GKE_CONTROL_PLANE_LIMIT_INCREASE_INTERVAL_SEC``
      in which a success is recorded, and not during the cooldown after a
      decrease.

    When the limit shrinks, in-flight calls are not interrupted; new calls wait
    until the in-flight count drops below the new limit. Waiters are served in
    FIFO order.

    All methods must be called from the event loop thread. There is no locking
    because each method runs to completion without awaiting.
    """

    def __init__(self) -> None:
        self._limit = _GKE_CONTROL_PLANE_LIMIT_MAX
        self._in_flight = 0
        self._waiters: collections.deque[asyncio.Future[None]] = collections.deque()
        self._last_decrease: float | None = None
        self._last_increase: float | None = None

    async def acquire(self) -> None:
        if self._in_flight < self._limit and not self._waiters:
            self._in_flight += 1
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                # A slot was granted in the same tick the waiter was cancelled.
                # Hand it back so it is not leaked.
                self.release()
            raise
        finally:
            try:
                self._waiters.remove(fut)
            except ValueError:
                pass

    def release(self) -> None:
        if self._in_flight <= 0:
            raise RuntimeError("AdaptiveConcurrencyLimiter released more than acquired")
        self._in_flight -= 1
        self._wake_waiters()

    async def __aenter__(self) -> AdaptiveConcurrencyLimiter:
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.release()

    def record_overload(self) -> None:
        now = time.monotonic()
        if (
            self._last_decrease is not None
            and now - self._last_decrease
            < _GKE_CONTROL_PLANE_LIMIT_DECREASE_COOLDOWN_SEC
        ):
            return
        previous = self._limit
        self._limit = max(
            _GKE_CONTROL_PLANE_LIMIT_MIN,
            math.floor(self._limit * _GKE_CONTROL_PLANE_LIMIT_DECREASE_FACTOR),
        )
        self._last_decrease = now
        self._last_increase = now
        if self._limit != previous:
            logger.info(
                "Kubernetes control plane is overloaded; lowering concurrent "
                "exec/create limit from %d to %d.",
                previous,
                self._limit,
            )

    def record_success(self) -> None:
        if self._limit >= _GKE_CONTROL_PLANE_LIMIT_MAX:
            return
        now = time.monotonic()
        if (
            self._last_decrease is not None
            and now - self._last_decrease
            < _GKE_CONTROL_PLANE_LIMIT_DECREASE_COOLDOWN_SEC
        ):
            return
        if (
            self._last_increase is not None
            and now - self._last_increase
            < _GKE_CONTROL_PLANE_LIMIT_INCREASE_INTERVAL_SEC
        ):
            return
        self._limit += 1
        self._last_increase = now
        if self._limit == _GKE_CONTROL_PLANE_LIMIT_MAX:
            logger.debug("Kubernetes control-plane limit recovered to %d.", self._limit)
        self._wake_waiters()

    def _wake_waiters(self) -> None:
        while self._waiters and self._in_flight < self._limit:
            fut = self._waiters.popleft()
            if fut.done():
                continue
            self._in_flight += 1
            fut.set_result(None)


@functools.cache
def get_control_plane_limiter() -> AdaptiveConcurrencyLimiter:
    """Return the process-wide limiter for exec handshakes and Job/Pod creates."""
    return AdaptiveConcurrencyLimiter()
