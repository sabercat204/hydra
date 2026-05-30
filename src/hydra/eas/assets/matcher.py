"""Indicator → asset matching (Design §3.2, Property 7).

``AssetMatcher.is_match(indicator, asset)`` is a **pure, deterministic**
function that decides whether a string indicator extracted from a
:class:`hydra.models.normalized.NormalizedRecord` payload matches a
registered :class:`hydra.eas.assets.models.Asset`. Determinism is the
contract: identical inputs produce identical booleans across runs and
worker processes (R3.1 / R27.7 / Property 7).

The per-type semantics mirror Design §3.2:

+--------------+--------------------------------------------------------+
| ``IP``       | exact equality of the compressed IP form               |
+--------------+--------------------------------------------------------+
| ``CIDR``     | ``indicator ∈ ip_network(asset.normalized_value)``     |
+--------------+--------------------------------------------------------+
| ``DOMAIN``   | case-insensitive exact match **or** suffix match       |
|              | (``indicator`` ends with ``.`` + normalized_value)     |
+--------------+--------------------------------------------------------+
| ``HOSTNAME`` | case-insensitive exact match only                      |
+--------------+--------------------------------------------------------+
| ``ASN``      | ``pyasn`` IP→ASN resolution, integer equality          |
+--------------+--------------------------------------------------------+

Error handling is **silent rejection** for the IP/CIDR/ASN branches: if
``indicator`` is not a parseable IP, the match returns ``False`` rather
than raising. The extractor in ``hydra.eas.assets.extractor`` pre-classifies
values, but we still guard here because:

1. The matcher is called by external components (including the CVE
   pipeline via ``record_exposure_from_correlation``) that may pass
   strings classified by different logic.
2. The alternative — propagating exceptions — would destabilize the
   ingestion path (R3.1). A ``False`` return is the conservative choice.

ASN lookups require a ``pyasn`` dataset at
``EASSettings.asn_database_path``. When the file is missing the matcher
increments ``hydra_eas_asn_lookup_failure_total`` (registered in
:mod:`hydra.eas.metrics`) and returns ``False`` without raising.
"""

from __future__ import annotations

import ipaddress
import logging
from pathlib import Path
from typing import Any

from hydra.eas.assets.models import Asset
from hydra.eas.metrics import hydra_eas_asn_lookup_failure_total
from hydra.eas.schemas.assets import AssetType

logger = logging.getLogger(__name__)

__all__ = ["AssetMatcher"]


class AssetMatcher:
    """Pure matching function with an optional ``pyasn`` database (Design §3.2).

    Instances are cheap to construct and hold a single optional reference to a
    loaded ``pyasn.pyasn`` object. Tests inject a mock via the
    ``asn_database`` constructor argument to avoid filesystem coupling.

    Typical production wiring is a module-level singleton created by
    ``setup_eas`` (task 17.1). The convenience ``_default_matcher`` instance
    at the bottom of this module exists so that tasks that land before
    ``setup_eas`` (e.g. the monitor) can import a working matcher without
    an explicit DI step.
    """

    def __init__(
        self,
        asn_database_path: Path | str | None = None,
        asn_database: Any | None = None,
    ) -> None:
        """Create a matcher.

        Parameters
        ----------
        asn_database_path:
            Path to the on-disk ``pyasn`` IP→ASN dataset (typically
            seeded from MaxMind GeoLite2-ASN). When ``None``, ASN matches
            short-circuit to ``False``. The constructor does not eagerly
            load the database; it defers to the first ASN lookup so that
            a missing file only penalises ASN-type assets.
        asn_database:
            An already-loaded ``pyasn`` instance (or a duck-typed object
            exposing ``lookup(ip) -> (asn, prefix)``). Used by tests to
            inject a deterministic database without touching disk. When
            provided, ``asn_database_path`` is ignored.
        """

        self._asn_database_path: Path | None = (
            Path(asn_database_path) if asn_database_path is not None else None
        )
        self._asn_database: Any | None = asn_database
        # Sentinel distinguishing "never attempted" from "attempted and
        # failed". A failed load should not be retried on every call — the
        # weekly refresh DAG is responsible for restoring the file.
        self._asn_load_attempted: bool = asn_database is not None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_match(self, indicator: str, asset: Asset) -> bool:
        """Return ``True`` when ``indicator`` matches ``asset`` per §3.2."""

        if asset.asset_type == AssetType.IP.value:
            return self._match_ip(indicator, asset.normalized_value)
        if asset.asset_type == AssetType.CIDR.value:
            return self._match_cidr(indicator, asset.normalized_value)
        if asset.asset_type == AssetType.DOMAIN.value:
            return self._match_domain(indicator, asset.normalized_value)
        if asset.asset_type == AssetType.HOSTNAME.value:
            return self._match_hostname(indicator, asset.normalized_value)
        if asset.asset_type == AssetType.ASN.value:
            return self._match_asn(indicator, asset.normalized_value)
        # Unknown asset_type — should never happen in production because
        # the CHECK constraint in the ``assets`` table enforces the enum.
        return False

    # ------------------------------------------------------------------
    # Per-type matchers
    # ------------------------------------------------------------------

    @staticmethod
    def _match_ip(indicator: str, normalized_value: str) -> bool:
        try:
            left = ipaddress.ip_address(indicator)
            right = ipaddress.ip_address(normalized_value)
        except (ValueError, TypeError):
            return False
        return left == right

    @staticmethod
    def _match_cidr(indicator: str, normalized_value: str) -> bool:
        try:
            ip = ipaddress.ip_address(indicator)
            network = ipaddress.ip_network(normalized_value, strict=False)
        except (ValueError, TypeError):
            return False
        # ``in`` does version-aware containment and naturally returns
        # ``False`` when the address family doesn't match the network.
        try:
            return ip in network
        except TypeError:
            return False

    @staticmethod
    def _match_domain(indicator: str, normalized_value: str) -> bool:
        # Both sides are lowered before comparison because an indicator
        # pulled from an arbitrary payload may have preserved casing even
        # after extraction; the stored ``normalized_value`` is already
        # lowered by ``normalize_asset_value`` but we fold again for a
        # second layer of defence.
        i = indicator.lower()
        nv = normalized_value.lower()
        return i == nv or i.endswith("." + nv)

    @staticmethod
    def _match_hostname(indicator: str, normalized_value: str) -> bool:
        return indicator.lower() == normalized_value.lower()

    def _match_asn(self, indicator: str, normalized_value: str) -> bool:
        # ``normalized_value`` is stored without the "AS" prefix (see
        # ``normalize_asset_value``) but older rows or future migrations
        # could still carry the prefix — tolerate both.
        nv = normalized_value.removeprefix("AS")
        try:
            expected = int(nv)
        except ValueError:
            return False

        origin_asn = self._asn_of(indicator)
        if origin_asn is None:
            return False
        return origin_asn == expected

    # ------------------------------------------------------------------
    # ASN lookup helper
    # ------------------------------------------------------------------

    def _asn_of(self, indicator: str) -> int | None:
        """Resolve ``indicator`` (an IP string) to its origin ASN.

        Returns ``None`` on any failure: non-IP indicator, missing
        database, pyasn returning a null ASN for the prefix. All failure
        paths that trace back to a missing database bump
        ``hydra_eas_asn_lookup_failure_total`` so the weekly refresh
        DAG's absence is visible via metrics.
        """

        try:
            # Reject non-IP strings up-front — pyasn accepts only
            # IP-shaped inputs and would otherwise raise.
            ipaddress.ip_address(indicator)
        except (ValueError, TypeError):
            return None

        db = self._load_asn_database()
        if db is None:
            return None

        try:
            # pyasn.lookup returns ``(asn, prefix)`` where ``asn`` may be
            # ``None`` for unknown prefixes. Non-pyasn duck-typed doubles
            # (e.g. test fakes) use the same shape.
            result = db.lookup(indicator)
        except Exception:  # pragma: no cover - defensive against library bugs
            logger.warning("asn_lookup_failed", extra={"indicator": indicator})
            return None

        if not result:
            return None
        asn = result[0]
        if asn is None:
            return None
        try:
            return int(asn)
        except (TypeError, ValueError):
            return None

    def _load_asn_database(self) -> Any | None:
        """Lazy-load the pyasn database, caching success and failure."""

        if self._asn_database is not None:
            return self._asn_database

        # Only attempt the (expensive) load once per process.
        if self._asn_load_attempted:
            return None
        self._asn_load_attempted = True

        if self._asn_database_path is None or not self._asn_database_path.exists():
            hydra_eas_asn_lookup_failure_total.inc()
            logger.warning(
                "asn_database_missing",
                extra={
                    "path": str(self._asn_database_path)
                    if self._asn_database_path is not None
                    else None
                },
            )
            return None

        try:
            import pyasn  # type: ignore[import-untyped]
        except ImportError:
            hydra_eas_asn_lookup_failure_total.inc()
            logger.warning("pyasn_import_failed")
            return None

        try:
            self._asn_database = pyasn.pyasn(str(self._asn_database_path))
        except Exception as exc:  # pragma: no cover - filesystem-specific
            hydra_eas_asn_lookup_failure_total.inc()
            logger.warning(
                "asn_database_load_failed",
                extra={"path": str(self._asn_database_path), "error": str(exc)},
            )
            self._asn_database = None
        return self._asn_database


# Module-level convenience matcher for callers that don't own their own
# wiring. ``setup_eas`` (task 17.1) will replace this with a DI-managed
# singleton that knows the real ``EASSettings.asn_database_path``. Tests
# should construct their own matcher with injected ``asn_database``.
_default_matcher = AssetMatcher()
