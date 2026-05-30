"""Screenshot_Adapter — orchestrates SSRF guard → render → MinIO → ES → record
(Design §3.3, §8.2, R6.1–R6.5, Property 10).

:class:`ScreenshotAdapter` is the boundary between the screenshot worker
(task 8.7) and the storage / record emission path. Each ``render(url)``
call:

1. **SSRF guard** (R6.1) — :func:`is_safe_url` rejects private, loopback,
   link-local, multicast, reserved, and CGNAT addresses. A rejection
   short-circuits with a failed :class:`NormalizedRecord` carrying
   ``payload.error = "SSRF_BLOCKED"``.
2. **Backpressure check** (R6.4) — :meth:`BackpressureMonitor.check_engine`
   on the ``minio`` engine. ``BLOCKED`` emits a failed record with
   ``payload.error = "BACKPRESSURE_BLOCKED"`` so the caller can decide
   whether to re-queue. Design §3.3 has the worker re-push with a 30s
   delay — the adapter itself stays pure.
3. **Render** — :class:`PlaywrightRenderer.render` with Chromium pinned
   to the SSRF-vetted IP via ``--host-resolver-rules``. TLS errors and
   navigation errors surface as :attr:`RenderResult.error_class`.
4. **Persist + index (success path)** — compute the SHA-256 content
   hash and the pHash, upload the PNG to MinIO at
   ``hydra-screenshots/{yyyy}/{mm}/{dd}/{sha256(url)}.png``, and index
   the metadata (record_hash, phash, phash_bits, content_hash,
   rendered_at, viewport, etc.) into the ``hydra-screenshots``
   Elasticsearch index. OCR runs last so an OCR failure does not abort
   the primary capture.
5. **Emit a NormalizedRecord** — always. Even a failed render gets a
   record so downstream tooling sees the attempt (R6.3). On success the
   payload carries the full metadata triple; on failure only
   ``url, error`` (plus ``rendered_at`` for audit).

Determinism (Property 10). The output record satisfies:

* ``content_hash == sha256(png_bytes)`` — byte-identical inputs produce
  byte-identical hashes.
* ``phash == imagehash.phash(png_bytes)`` — byte-identical inputs
  produce byte-identical perceptual hashes.
* ``minio_key == f"hydra-screenshots/{yyyy}/{mm}/{dd}/{sha256(url)}.png"``
  — the key is a pure function of the URL and the render day.
* ``raw_hash`` — ``xxhash64(f"screenshot:{sha256(url)}:{content_hash or 'error'}")``.
  A fixed outcome for a fixed URL produces the same raw_hash.

The ``rendered_at`` wall-clock field is the only non-deterministic
attribute per R6.5 / Property 10.
"""

from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from hydra.eas.metrics import (
    hydra_eas_screenshot_bytes_total,
    hydra_eas_screenshot_captures_total,
)
from hydra.eas.screenshots.phash import compute_phash
from hydra.eas.screenshots.ssrf_guard import host_resolver_rules, is_safe_url
from hydra.eas.storage.es_mappings import HYDRA_SCREENSHOTS_INDEX
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier
from hydra.utils.hashing import compute_raw_hash

if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from hydra.eas.screenshots.ocr import OCRProcessor
    from hydra.eas.screenshots.renderer import PlaywrightRenderer
    from hydra.eas.settings import EASSettings

logger = logging.getLogger(__name__)

__all__ = ["ScreenshotAdapter"]


# ``source_meta.adapter_type`` string for screenshot records — kept as a
# module constant so external consumers (lookup assembler, observatory
# aggregation) can filter by it without hard-coding the literal.
SCREENSHOT_ADAPTER_TYPE = "screenshot"


@dataclass(slots=True, frozen=True)
class _RenderOutput:
    """Internal aggregate of render + hash data used to build the record."""

    png_bytes: bytes
    content_hash: str
    phash: str
    http_status: int
    title: str
    error_class: str | None
    rendered_at: datetime


class ScreenshotAdapter:
    """Render a URL, persist the PNG to MinIO, index metadata to ES.

    Dependency-injected so the same class can be reused by the worker
    (task 8.7) and by the ``POST /api/v1/assets/{id}/screenshot`` router
    (task 8.8, via :class:`JobManager`). The router schedules a background
    job that eventually calls :meth:`render` on the same adapter instance.
    """

    def __init__(
        self,
        settings: "EASSettings",
        renderer: "PlaywrightRenderer",
        minio_client: Any,
        es_client: Any,
        backpressure_monitor: Any,
        ocr: "OCRProcessor | None" = None,
    ) -> None:
        self._settings = settings
        self._renderer = renderer
        self._minio = minio_client
        self._es = es_client
        self._bp = backpressure_monitor
        self._ocr = ocr

    # ---- public API ---------------------------------------------------

    async def render(self, url: str) -> NormalizedRecord:
        """Render ``url`` and return the resulting :class:`NormalizedRecord`.

        The record is always returned — failures surface via
        ``record.payload["error"]`` rather than exceptions. The caller
        decides whether to retry, re-queue with delay, or treat as
        terminal.
        """

        url_sha256 = hashlib.sha256(url.encode("utf-8")).hexdigest()
        rendered_at = datetime.now(timezone.utc)

        # ---------------- R6.1 SSRF guard ----------------
        safe, resolved_ip, reason = await is_safe_url(url)
        if not safe:
            logger.info(
                "eas.screenshot.ssrf_blocked",
                extra={"url": url, "reason": reason},
            )
            try:
                hydra_eas_screenshot_captures_total.labels(status="failed").inc()
            except Exception:  # noqa: BLE001 — metrics must never break the adapter
                pass
            return self._build_error_record(
                url=url,
                url_sha256=url_sha256,
                rendered_at=rendered_at,
                error_class="SSRF_BLOCKED",
                error_detail=reason,
            )

        # ---------------- R6.4 Backpressure check ----------------
        if await self._minio_blocked():
            logger.info(
                "eas.screenshot.backpressure_blocked", extra={"url": url}
            )
            try:
                hydra_eas_screenshot_captures_total.labels(status="failed").inc()
            except Exception:  # noqa: BLE001
                pass
            return self._build_error_record(
                url=url,
                url_sha256=url_sha256,
                rendered_at=rendered_at,
                error_class="BACKPRESSURE_BLOCKED",
                error_detail="minio",
            )

        # ---------------- Render ----------------
        host = urlparse(url).hostname or ""
        rules = (
            host_resolver_rules(host, resolved_ip)
            if resolved_ip and host
            else None
        )
        render_result = await self._renderer.render(
            url=url,
            viewport=tuple(self._settings.screenshot.viewport),  # type: ignore[arg-type]
            timeout_seconds=self._settings.screenshot.timeout_seconds,
            user_agent=self._settings.screenshot.user_agent,
            host_resolver_rules=rules,
        )

        if render_result.error_class is not None or not render_result.png_bytes:
            logger.info(
                "eas.screenshot.render_failed",
                extra={"url": url, "error_class": render_result.error_class},
            )
            try:
                hydra_eas_screenshot_captures_total.labels(status="failed").inc()
            except Exception:  # noqa: BLE001
                pass
            return self._build_error_record(
                url=url,
                url_sha256=url_sha256,
                rendered_at=rendered_at,
                error_class=render_result.error_class or "RenderFailed",
                error_detail=None,
                http_status=render_result.http_status or None,
                title=render_result.title or None,
            )

        # ---------------- Success path ----------------
        png_bytes = render_result.png_bytes
        content_hash = hashlib.sha256(png_bytes).hexdigest()
        try:
            phash_hex = compute_phash(png_bytes)
        except Exception as exc:  # noqa: BLE001 — any phash failure is terminal
            logger.warning(
                "eas.screenshot.phash_failed",
                extra={"url": url, "error": str(exc)},
            )
            try:
                hydra_eas_screenshot_captures_total.labels(status="failed").inc()
            except Exception:  # noqa: BLE001
                pass
            return self._build_error_record(
                url=url,
                url_sha256=url_sha256,
                rendered_at=rendered_at,
                error_class="PHashFailed",
                error_detail=str(exc),
                http_status=render_result.http_status,
                title=render_result.title,
            )

        minio_key = self._minio_key(url_sha256, rendered_at)

        # Persist the PNG. A MinIO failure degrades to a failed record
        # (no blob, no ES index) rather than a silent drop.
        try:
            await self._put_png(minio_key, png_bytes)
        except Exception as exc:  # noqa: BLE001 — classify as a render-adjacent error
            logger.warning(
                "eas.screenshot.minio_failed",
                extra={"url": url, "minio_key": minio_key, "error": str(exc)},
            )
            try:
                hydra_eas_screenshot_captures_total.labels(status="failed").inc()
            except Exception:  # noqa: BLE001
                pass
            return self._build_error_record(
                url=url,
                url_sha256=url_sha256,
                rendered_at=rendered_at,
                error_class="MinIOWriteFailed",
                error_detail=str(exc),
                http_status=render_result.http_status,
                title=render_result.title,
            )

        # OCR (best-effort — a failure here does not abort the capture).
        ocr_text = ""
        if self._ocr is not None and self._settings.screenshot.ocr_enabled:
            try:
                ocr_text = await self._ocr.extract_text(
                    png_bytes, max_chars=self._settings.screenshot.ocr_max_chars
                )
            except Exception as exc:  # noqa: BLE001
                logger.info(
                    "eas.screenshot.ocr_failed",
                    extra={"url": url, "error": str(exc)},
                )
                ocr_text = ""

        output = _RenderOutput(
            png_bytes=png_bytes,
            content_hash=content_hash,
            phash=phash_hex,
            http_status=render_result.http_status,
            title=render_result.title,
            error_class=None,
            rendered_at=rendered_at,
        )

        # Assemble the record BEFORE indexing so the ES doc carries the
        # same record_hash the rest of HYDRA will see.
        record = self._build_success_record(
            url=url,
            url_sha256=url_sha256,
            minio_key=minio_key,
            output=output,
            ocr_text=ocr_text,
        )

        # Index metadata. Indexing failures are logged but do not
        # invalidate the capture — the MinIO blob already exists and the
        # record has been emitted.
        try:
            await self._index_es_metadata(
                record_hash=record.raw_hash,
                url=url,
                output=output,
                minio_key=minio_key,
                ocr_text=ocr_text,
                tags=list(record.tags),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "eas.screenshot.es_index_failed",
                extra={"url": url, "error": str(exc)},
            )

        try:
            hydra_eas_screenshot_captures_total.labels(status="success").inc()
            hydra_eas_screenshot_bytes_total.inc(len(png_bytes))
        except Exception:  # noqa: BLE001
            pass

        return record

    # ---- helpers ------------------------------------------------------

    async def _minio_blocked(self) -> bool:
        """Return ``True`` when the MinIO engine's backpressure state is BLOCKED.

        We deliberately fail open (``False``) when the monitor is missing
        or raises — backpressure is a soft signal, not a correctness
        guard, and we already have the blob-write failure path to catch
        storage outages.
        """

        if self._bp is None:
            return False
        try:
            engine_state = await self._bp.check_engine("minio")
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "eas.screenshot.bp_check_failed", extra={"error": str(exc)}
            )
            return False
        state = getattr(engine_state, "state", None)
        return state == "BLOCKED"

    def _minio_key(self, url_sha256: str, rendered_at: datetime) -> str:
        """Return ``hydra-screenshots/{yyyy}/{mm}/{dd}/{sha256(url)}.png``."""

        return (
            f"hydra-screenshots/{rendered_at:%Y}/{rendered_at:%m}/"
            f"{rendered_at:%d}/{url_sha256}.png"
        )

    async def _put_png(self, minio_key: str, png_bytes: bytes) -> None:
        """Upload ``png_bytes`` to the ``hydra-screenshots`` bucket.

        The MinIO client interface used here is intentionally minimal:
        we need a ``put_object`` callable (async or sync). ``boto3`` is
        synchronous — the adapter offloads the upload to a thread when
        the client exposes a sync method. Tests pass in an async fake
        that returns ``None`` immediately.
        """

        put = self._minio.put_object

        # Most test doubles (and the eventual production wrapper) will
        # expose an async ``put_object``; fall back to running a sync
        # client in a worker thread so boto3 works unchanged.
        import asyncio
        import inspect

        bucket = "hydra-screenshots"
        # Strip the bucket prefix from the key so the MinIO-level key
        # matches S3 conventions — the design refers to the full
        # ``hydra-screenshots/<path>.png`` as the MinIO key for
        # addressability in records, but the actual S3 object key is
        # the portion **after** the bucket name.
        object_key = minio_key
        if object_key.startswith(f"{bucket}/"):
            object_key = object_key[len(bucket) + 1 :]

        kwargs = {
            "Bucket": bucket,
            "Key": object_key,
            "Body": io.BytesIO(png_bytes),
            "ContentType": "image/png",
        }

        if inspect.iscoroutinefunction(put):
            await put(**kwargs)
        else:
            # ``put_object`` is synchronous (boto3). Offload to a thread
            # so the event loop stays responsive.
            await asyncio.to_thread(put, **kwargs)

    async def _index_es_metadata(
        self,
        *,
        record_hash: str,
        url: str,
        output: _RenderOutput,
        minio_key: str,
        ocr_text: str,
        tags: list[str],
    ) -> None:
        """Index the screenshot metadata into ``hydra-screenshots``.

        Uses ``record_hash`` as the ES ``_id`` so repeated captures of
        the same URL on the same day overwrite the previous doc — this
        matches the MinIO key scheme (same URL + same day → same blob).
        """

        # Extract url_host for tenant-agnostic filtering on the lookup
        # path (§3.7). Falling back to the raw URL is safe because
        # ``keyword`` fields accept any string.
        url_host = urlparse(url).hostname or ""

        # ``phash_bits`` is the raw 8-byte form of the perceptual hash,
        # stored under the ``binary`` ES mapping so that script_score
        # similarity queries can pop-count the XOR.
        try:
            phash_bytes = bytes.fromhex(output.phash)
        except ValueError:
            phash_bytes = b""

        doc: dict[str, Any] = {
            "record_hash": record_hash,
            "url": url,
            "url_host": url_host,
            "http_status": output.http_status,
            "title": output.title,
            "phash": output.phash,
            "phash_bits": phash_bytes,
            "content_hash": output.content_hash,
            "rendered_at": output.rendered_at.isoformat(),
            "viewport_w": int(self._settings.screenshot.viewport[0]),
            "viewport_h": int(self._settings.screenshot.viewport[1]),
            "tier": int(Tier.VULNERABILITY_INTELLIGENCE),
            "minio_key": minio_key,
            "tags": tags,
        }
        if ocr_text:
            # ES ``ocr_excerpt`` is a keyword with ignore_above=1024, so
            # we truncate the excerpt conservatively to avoid silent
            # drops on the mapping side.
            doc["ocr_text"] = ocr_text
            doc["ocr_excerpt"] = ocr_text[:512]

        await self._es.index(
            index=HYDRA_SCREENSHOTS_INDEX,
            id=record_hash,
            document=doc,
        )

    # ---- record builders ---------------------------------------------

    def _build_success_record(
        self,
        *,
        url: str,
        url_sha256: str,
        minio_key: str,
        output: _RenderOutput,
        ocr_text: str,
    ) -> NormalizedRecord:
        payload: dict[str, Any] = {
            "url": url,
            "http_status": output.http_status,
            "title": output.title,
            "content_hash": output.content_hash,
            "phash": output.phash,
            "minio_key": minio_key,
            "rendered_at": output.rendered_at.isoformat(),
            "viewport": list(self._settings.screenshot.viewport),
        }
        if ocr_text:
            payload["ocr_excerpt"] = ocr_text[:512]
            payload["has_ocr"] = True

        raw_hash = compute_raw_hash(
            f"screenshot:{url_sha256}:{output.content_hash}".encode("utf-8")
        )
        stream_id = f"screenshot:{url_sha256[:32]}"

        return NormalizedRecord(
            stream_id=stream_id,
            tier=Tier.VULNERABILITY_INTELLIGENCE,
            timestamp=output.rendered_at,
            geo=None,
            payload=payload,
            source_meta=SourceMeta(
                source_name="screenshot",
                source_url=url,
                adapter_type=SCREENSHOT_ADAPTER_TYPE,
                fetch_timestamp=output.rendered_at,
                raw_format="png",
            ),
            raw_hash=raw_hash,
            confidence=1.0,
            tags=["screenshot"],
        )

    def _build_error_record(
        self,
        *,
        url: str,
        url_sha256: str,
        rendered_at: datetime,
        error_class: str,
        error_detail: str | None,
        http_status: int | None = None,
        title: str | None = None,
    ) -> NormalizedRecord:
        payload: dict[str, Any] = {
            "url": url,
            "error": error_class,
            "rendered_at": rendered_at.isoformat(),
        }
        if error_detail is not None:
            payload["error_detail"] = error_detail
        if http_status is not None:
            payload["http_status"] = http_status
        if title:
            payload["title"] = title

        raw_hash = compute_raw_hash(
            f"screenshot:{url_sha256}:error".encode("utf-8")
        )
        stream_id = f"screenshot:{url_sha256[:32]}"

        return NormalizedRecord(
            stream_id=stream_id,
            tier=Tier.VULNERABILITY_INTELLIGENCE,
            timestamp=rendered_at,
            geo=None,
            payload=payload,
            source_meta=SourceMeta(
                source_name="screenshot",
                source_url=url,
                adapter_type=SCREENSHOT_ADAPTER_TYPE,
                fetch_timestamp=rendered_at,
                raw_format="",
            ),
            raw_hash=raw_hash,
            confidence=0.0,
            tags=["screenshot", "screenshot_failed"],
        )
