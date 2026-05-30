"""Shared base class for Tier 29 (Vulnerability Intelligence) REST adapters.

:class:`Tier29RestAdapter` subclasses :class:`~hydra.adapters.rest_json.RestJsonAdapter`
and leaves ``fetch()`` / ``parse()`` / ``validate()`` untouched so that every
Tier 29 stream behaves like any other REST/JSON source for ingestion,
validation, pagination, and conditional requests.

It overrides :meth:`normalize` to emit records with the Tier 29
(``VULNERABILITY_INTELLIGENCE``) enum value, a per-source ``raw_hash``
scheme, and the source-specific payload shape from R9.2–R9.6. Concrete
subclasses supply three hooks:

* :meth:`_compute_raw_hash` — stable per-record hash scheme (R9.2–R9.6).
* :meth:`_build_payload` — the exact field set required by the requirement.
* :meth:`_extract_timestamp` — the record timestamp to persist on
  :class:`~hydra.models.normalized.NormalizedRecord`.

Subclasses also set ``source_label`` (e.g. ``"nvd"``, ``"epss"``) which
populates:

* the ``source`` label on ``hydra_eas_cve_records_total`` (R9.7),
* the record's ``tags`` list, and
* the ``source`` field injected into each payload so downstream consumers can
  filter without re-inspecting the stream identifier.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar

import xxhash

from hydra.eas.metrics import hydra_eas_cve_records_total
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier

from ..rest_json import RestJsonAdapter


class Tier29RestAdapter(RestJsonAdapter):
    """Shared base for every Tier 29 REST/JSON adapter.

    Subclasses MUST set :attr:`source_label` to a short stable identifier
    (``"nvd"``, ``"epss"``, ``"kev"``, ``"exploitdb"``, ``"metasploit"``).
    """

    #: Short identifier used for the ``source`` metric label and the
    #: ``source`` field in the emitted payload. Must be overridden by
    #: subclasses.
    source_label: ClassVar[str] = ""

    #: Kept as ``"rest_json"`` on purpose — the Tier 29 source identity lives
    #: in :attr:`source_label` and in the payload, not in the adapter_type.
    adapter_type: ClassVar[str] = "rest_json"

    # -- hooks that subclasses MUST implement ------------------------------

    def _compute_raw_hash(self, record: dict[str, Any]) -> str:
        """Return a 16-char xxhash64 hex digest for ``record``.

        Schemes per R9.2–R9.6 (see concrete subclasses for details).
        """

        raise NotImplementedError

    def _build_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        """Project ``record`` into the exact payload shape required by R9.*.

        Subclasses SHOULD NOT include the ``source`` key; the base class
        injects it after calling this hook so every payload gets a uniform
        ``source`` field without duplicating the string in every subclass.
        """

        raise NotImplementedError

    def _extract_timestamp(self, record: dict[str, Any]) -> datetime:
        """Return the ``timestamp`` for the emitted NormalizedRecord.

        Subclasses should return ``datetime.now(timezone.utc)`` when no
        natural timestamp is available in ``record`` (e.g. KEV entries that
        only carry a date_added value the caller chose to omit).
        """

        raise NotImplementedError

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _xxhash64(value: str) -> str:
        """xxhash64 hex digest of *value* as UTF-8 bytes (16 lowercase hex)."""

        return xxhash.xxh64(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime | None:
        """Parse ``value`` into a timezone-aware UTC ``datetime``.

        Accepts ``datetime`` instances (naive ones are treated as UTC) and
        ISO 8601 strings. Returns ``None`` when ``value`` is ``None`` or
        cannot be parsed.
        """

        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    # -- normalize ---------------------------------------------------------

    def normalize(self, records: list[dict[str, Any]]) -> list[NormalizedRecord]:
        """Emit :class:`NormalizedRecord` instances for Tier 29.

        * ``tier = Tier.VULNERABILITY_INTELLIGENCE``.
        * ``source_meta.adapter_type = "rest_json"`` (inherited from the
          parent — only the ``source`` label and payload expose the per-source
          identity).
        * ``raw_hash`` comes from :meth:`_compute_raw_hash`.
        * ``payload`` comes from :meth:`_build_payload` with a uniform
          ``source`` key injected for downstream convenience.
        * ``timestamp`` comes from :meth:`_extract_timestamp`, falling back to
          the current UTC wall clock when the hook returns ``None``.
        * ``tags = [source_label]`` so downstream consumers can filter by
          source even when the payload is opaque to them.

        After building the batch, increments
        ``hydra_eas_cve_records_total{source=self.source_label}`` by the
        number of records emitted (R9.7). The metric is a no-op when
        ``prometheus_client`` is unavailable; see
        :mod:`hydra.eas.metrics`.
        """

        if not self.source_label:
            raise RuntimeError(
                f"{type(self).__name__} must set a non-empty `source_label` "
                "class attribute before normalize() is called."
            )

        source_meta_name = self._stream_meta.get("source")
        source_name = (
            source_meta_name.name if source_meta_name is not None else self.stream_id
        )
        source_url = source_meta_name.url if source_meta_name is not None else ""

        normalized: list[NormalizedRecord] = []
        for record in records:
            payload = dict(self._build_payload(record))
            payload.setdefault("source", self.source_label)

            ts = self._extract_timestamp(record) or datetime.now(timezone.utc)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            nr = NormalizedRecord(
                stream_id=self.stream_id,
                tier=Tier.VULNERABILITY_INTELLIGENCE,
                timestamp=ts,
                geo=None,
                payload=payload,
                source_meta=SourceMeta(
                    source_name=source_name,
                    source_url=source_url,
                    adapter_type=self.adapter_type,
                ),
                raw_hash=self._compute_raw_hash(record),
                confidence=1.0,
                tags=[self.source_label],
            )
            normalized.append(nr)

        if normalized:
            hydra_eas_cve_records_total.labels(source=self.source_label).inc(
                len(normalized)
            )

        return normalized


__all__ = ["Tier29RestAdapter"]
