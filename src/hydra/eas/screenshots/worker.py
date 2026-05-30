"""ScreenshotWorker — Redis-backed async loop (Design §3.3, §6.2, R6.4).

One :class:`ScreenshotWorker` instance owns N coroutines (``N =
EASSettings.screenshot.max_concurrency``). Each coroutine:

1. ``BLPOP hydra:eas:screenshot:queue 1`` — block for up to 1 s for an
   entry. The short timeout lets :meth:`stop` make rapid progress without
   requiring explicit cancellation at every worker.
2. Parse the JSON envelope ``{url, tenant_id?, source, asset_id?}``.
3. Acquire a per-host semaphore via
   ``INCR hydra:eas:screenshot:sem:{host}`` + ``EXPIRE 30``. If the
   counter exceeds ``per_host_concurrency``, the worker decrements and
   re-queues the entry with a 30-second delay.
4. Delegate to :class:`ScreenshotAdapter.render`.
5. Release the semaphore.

Cancellation semantics: :meth:`stop` sets ``_running = False`` and
cancels every worker task. Outstanding ``BLPOP`` calls receive a
``CancelledError`` on cancellation; the worker swallows it and exits
cleanly. An exception inside the render path is logged and the loop
continues — one bad URL does not bring down the pool.

The Redis interface this worker relies on is intentionally minimal:
``blpop``, ``rpush``, ``incr``, ``expire``, ``decr``, and
``set``/``setex`` (for delayed re-queue). The tests inject a fake
client that implements those methods.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from hydra.eas.screenshots.adapter import ScreenshotAdapter

logger = logging.getLogger(__name__)

__all__ = ["ScreenshotWorker", "SCREENSHOT_QUEUE_KEY", "SCREENSHOT_SEMAPHORE_KEY"]


# Redis key patterns. Kept as module-level constants so callers
# (e.g. the images router that enqueues on POST) can reuse them
# without hard-coding the literals.
SCREENSHOT_QUEUE_KEY: str = "hydra:eas:screenshot:queue"
SCREENSHOT_SEMAPHORE_KEY: str = "hydra:eas:screenshot:sem:{host}"

# How long to back off when a host is at its per-host concurrency limit.
_PER_HOST_BACKOFF_SECONDS: int = 30


class ScreenshotWorker:
    """Async worker pool that drains the screenshot queue."""

    def __init__(
        self,
        redis: Any,
        adapter: "ScreenshotAdapter",
        concurrency: int,
        per_host_concurrency: int,
        queue_key: str = SCREENSHOT_QUEUE_KEY,
    ) -> None:
        self._redis = redis
        self._adapter = adapter
        self._concurrency = max(1, int(concurrency))
        self._per_host_concurrency = max(1, int(per_host_concurrency))
        self._queue_key = queue_key
        self._running: bool = False
        self._tasks: list[asyncio.Task[None]] = []

    # ---- lifecycle ----------------------------------------------------

    async def start(self) -> None:
        """Spawn ``concurrency`` worker coroutines.

        Idempotent: a second call with the pool already running is a
        no-op. This makes the method safe to wire into
        :func:`hydra.eas.setup.setup_eas` which itself is called from
        the FastAPI startup event and may be invoked multiple times in
        test fixtures.
        """

        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._loop(i), name=f"eas.screenshot_worker.{i}")
            for i in range(self._concurrency)
        ]
        logger.info(
            "eas.screenshot_worker.started",
            extra={"concurrency": self._concurrency},
        )

    async def stop(self) -> None:
        """Stop the pool. Cancels outstanding BLPOP calls."""

        if not self._running:
            return
        self._running = False
        for task in self._tasks:
            task.cancel()
        # Gather with return_exceptions so a cancelled task's
        # CancelledError does not propagate into the shutdown path.
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        logger.info("eas.screenshot_worker.stopped")

    # ---- main loop ----------------------------------------------------

    async def _loop(self, worker_index: int) -> None:
        """Single worker coroutine body."""

        log = logger.getChild(f"w{worker_index}")
        while self._running:
            try:
                entry = await self._pop_next()
            except asyncio.CancelledError:
                return
            except Exception as exc:  # noqa: BLE001 — keep the loop alive
                log.warning(
                    "eas.screenshot_worker.pop_failed",
                    extra={"error": str(exc)},
                )
                await asyncio.sleep(1.0)
                continue

            if entry is None:
                continue

            try:
                await self._process(entry)
            except asyncio.CancelledError:
                return
            except Exception as exc:  # noqa: BLE001 — keep the loop alive
                log.warning(
                    "eas.screenshot_worker.process_failed",
                    extra={"error": str(exc), "entry": entry},
                )

    async def _pop_next(self) -> dict[str, Any] | None:
        """BLPOP one entry from the screenshot queue, or return None on timeout.

        The BLPOP response format is ``(queue_name, value)`` for real
        Redis clients. Some test fakes return just the value — we
        tolerate both shapes.
        """

        popped = await self._redis.blpop(self._queue_key, timeout=1)
        if popped is None:
            return None
        # redis-py returns (bytes, bytes); normalise to str then JSON-decode.
        if isinstance(popped, (tuple, list)):
            if len(popped) < 2:
                return None
            raw = popped[1]
        else:
            raw = popped
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "eas.screenshot_worker.bad_payload",
                extra={"payload": str(raw)[:200], "error": str(exc)},
            )
            return None

    async def _process(self, entry: dict[str, Any]) -> None:
        """Handle a single queue entry."""

        url = entry.get("url")
        if not isinstance(url, str) or not url:
            logger.warning("eas.screenshot_worker.missing_url", extra={"entry": entry})
            return

        host = urlparse(url).hostname or "unknown"
        sem_key = SCREENSHOT_SEMAPHORE_KEY.format(host=host)

        # Acquire per-host semaphore. ``INCR`` + ``EXPIRE`` keeps the
        # key alive even if the worker is killed mid-render (the
        # counter self-heals after 30 s).
        try:
            current = int(await self._redis.incr(sem_key))
            await self._redis.expire(sem_key, 30)
        except Exception as exc:  # noqa: BLE001 — fail open on Redis errors
            logger.warning(
                "eas.screenshot_worker.sem_acquire_failed",
                extra={"host": host, "error": str(exc)},
            )
            await self._adapter.render(url)
            return

        if current > self._per_host_concurrency:
            # Another worker is already at the host's cap. Give back
            # the slot we just claimed and re-queue with backoff so the
            # next BLPOP picks up a different host first.
            try:
                await self._redis.decr(sem_key)
            except Exception:  # noqa: BLE001 — decr failure is benign
                pass
            await self._requeue_with_delay(entry, delay_seconds=_PER_HOST_BACKOFF_SECONDS)
            return

        try:
            await self._adapter.render(url)
        finally:
            try:
                await self._redis.decr(sem_key)
            except Exception:  # noqa: BLE001 — decr failure is benign
                pass

    async def _requeue_with_delay(
        self, entry: dict[str, Any], delay_seconds: int
    ) -> None:
        """Re-push ``entry`` to the queue after ``delay_seconds``.

        Uses :func:`asyncio.sleep` rather than a dedicated delay queue
        because the MVP screenshot volume is low enough that holding
        the worker for 30 s is acceptable — upgrading to a sorted-set
        delay queue is a post-MVP task when the screenshot throughput
        justifies it.
        """

        await asyncio.sleep(delay_seconds)
        try:
            await self._redis.rpush(self._queue_key, json.dumps(entry))
        except Exception as exc:  # noqa: BLE001 — log and drop
            logger.warning(
                "eas.screenshot_worker.requeue_failed",
                extra={"entry": entry, "error": str(exc)},
            )
