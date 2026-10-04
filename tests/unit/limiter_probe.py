"""Observe the control-plane limiter through its public semaphore API only."""

from __future__ import annotations

import asyncio

from harbor_gke_ext.constants import _GKE_CONTROL_PLANE_LIMIT_MAX
from harbor_gke_ext.control_plane import (
    AdaptiveConcurrencyLimiter,
    get_control_plane_limiter,
)


async def free_slots(limiter: AdaptiveConcurrencyLimiter | None = None) -> int:
    """Return how many ``acquire()`` calls the limiter admits right now.

    Every probe is handed back before returning, so the limiter's state is
    unchanged. With nothing in flight, the result is the current limit.
    """
    limiter = limiter or get_control_plane_limiter()
    probes = [
        asyncio.create_task(limiter.acquire())
        for _ in range(_GKE_CONTROL_PLANE_LIMIT_MAX + 1)
    ]
    # One loop pass lets every probe take the uncontended fast path or queue.
    await asyncio.sleep(0)
    admitted = [t for t in probes if t.done()]
    queued = [t for t in probes if not t.done()]
    for task in queued:
        task.cancel()
    await asyncio.gather(*queued, return_exceptions=True)
    for _ in admitted:
        limiter.release()
    return len(admitted)
