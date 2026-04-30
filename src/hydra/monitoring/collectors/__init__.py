"""Collector base class and registration module (P12 — Component 3/4).

All concrete collectors (scheduler, storage, API, pipeline) derive from
:class:`BaseCollector`, which provides the shared async background loop
pattern with error isolation.

The loop (see :meth:`BaseCollector._loop`) follows Algorithm 1 from the
design document:

1. While ``self._running`` is ``True``, invoke :meth:`~BaseCollector.collect`.
2. Catch **any** exception raised by ``collect()``, log it, and increment
   the :data:`~hydra.monitoring.metrics.COLLECTOR_ERRORS` counter with the
   concrete class name as the ``collector`` label.
3. Sleep ``self._interval`` seconds between iterations, regardless of
   whether the previous iteration succeeded or failed.

This guarantees that a transient upstream failure (Redis unreachable,
PostgreSQL query timeout, etc.) cannot crash the background task or leak
into the FastAPI application lifecycle. See Requirement 4.2 and 22.1.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod

from hydra.monitoring.metrics import COLLECTOR_ERRORS

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base class for periodic metric collectors.

    Subclasses implement :meth:`collect` with the actual metric-scraping
    logic. The base class handles the background loop, error isolation,
    and lifecycle management.

    Typical usage::

        class MyCollector(BaseCollector):
            async def collect(self) -> None:
                ...  # scrape upstream, update metrics

        collector = MyCollector(interval=30.0)
        task = await collector.start()
        ...
        await collector.stop()
        task.cancel()

    Attributes:
        _interval: Seconds to sleep between collection cycles.
        _running: Loop guard. Set to ``True`` in :meth:`start` and ``False``
            in :meth:`stop`. Checked at the top of each loop iteration.
        _task: Handle to the background task created by :meth:`start`,
            or ``None`` before ``start()`` has been called.
    """

    def __init__(self, interval: float = 60.0) -> None:
        """Initialize the collector.

        Args:
            interval: Seconds between collection cycles. Must be positive.
                Concrete collectors typically override the default via
                values sourced from :class:`~hydra.config.MonitoringSettings`.
        """
        self._interval: float = interval
        self._running: bool = False
        self._task: asyncio.Task[None] | None = None

    @abstractmethod
    async def collect(self) -> None:
        """Perform one collection cycle.

        Subclasses implement this with their metric-scraping logic. The
        method MAY raise any exception — the base class loop catches,
        logs, and counts every exception without propagating it.

        Preconditions:
            Upstream dependencies supplied to the constructor are
            available (or the method must tolerate their unavailability
            by raising, which the loop will handle).
        """

    async def start(self) -> asyncio.Task[None]:
        """Start the background collection loop.

        Creates an :class:`asyncio.Task` running :meth:`_loop` and stores
        a reference to it. Calling ``start()`` on an already-running
        collector returns the existing task rather than creating a
        duplicate.

        Returns:
            The ``asyncio.Task`` executing the loop. Callers typically
            track this in a :class:`~hydra.monitoring.MonitoringContext`
            for graceful shutdown.
        """
        if self._task is not None and not self._task.done():
            return self._task

        self._running = True
        self._task = asyncio.create_task(
            self._loop(),
            name=f"collector:{self.__class__.__name__}",
        )
        return self._task

    async def stop(self) -> None:
        """Signal the loop to exit after the current sleep completes.

        This sets ``_running`` to ``False``; the loop checks this flag at
        the top of each iteration and exits cleanly. The caller is
        responsible for cancelling the task if it needs to be torn down
        before the current ``asyncio.sleep`` resolves (see
        :meth:`~hydra.monitoring.MonitoringContext.shutdown`).
        """
        self._running = False

    async def _loop(self) -> None:
        """Run ``collect()`` on ``self._interval`` seconds with error isolation.

        Implements Algorithm 1 from the design document. Every exception
        raised by :meth:`collect` — including :class:`BaseException`
        subclasses other than :class:`asyncio.CancelledError` — is caught,
        logged, and counted against
        :data:`~hydra.monitoring.metrics.COLLECTOR_ERRORS`. The loop then
        sleeps and continues.

        ``asyncio.CancelledError`` is re-raised so that task cancellation
        (e.g., from ``MonitoringContext.shutdown``) terminates the loop
        immediately rather than being swallowed.
        """
        collector_name = self.__class__.__name__
        while self._running:
            try:
                await self.collect()
            except asyncio.CancelledError:
                # Cooperative cancellation: surface to the task runner.
                raise
            except Exception as exc:  # noqa: BLE001 — error isolation is the whole point
                logger.error(
                    "Collector %s failed: %s",
                    collector_name,
                    exc,
                    exc_info=True,
                )
                COLLECTOR_ERRORS.labels(collector=collector_name).inc()
            await asyncio.sleep(self._interval)


from hydra.monitoring.collectors.api import APICollector
from hydra.monitoring.collectors.pipeline import PipelineCollector
from hydra.monitoring.collectors.scheduler import SchedulerCollector
from hydra.monitoring.collectors.storage import StorageCollector

__all__ = [
    "BaseCollector",
    "APICollector",
    "PipelineCollector",
    "SchedulerCollector",
    "StorageCollector",
]
