"""Tier 29 (Vulnerability Intelligence) REST adapters.

One adapter per stream registered under Tier 29 in
``src/hydra/registry/stream_registry.yaml``:

* ``NVDCVEAdapter`` → ``nvd-cve`` stream (NIST NVD CVE feed, R9.2).
* ``FirstEPSSAdapter`` → ``first-epss`` stream (FIRST EPSS daily scores, R9.3).
* ``CISAKEVAdapter`` → ``cisa-kev`` stream (CISA Known Exploited
  Vulnerabilities catalog, R9.4).
* ``ExploitDBAdapter`` → ``exploitdb`` stream (ExploitDB public exploits,
  R9.5).
* ``MetasploitAdapter`` → ``metasploit-modules`` stream (Metasploit module
  metadata, R9.6).

Each adapter emits :class:`hydra.models.normalized.NormalizedRecord` with
``tier = Tier.VULNERABILITY_INTELLIGENCE`` and a source-specific
``raw_hash`` scheme documented on the concrete subclass.

The shared :class:`~hydra.adapters.tier29.base.Tier29RestAdapter` base class
inherits from :class:`~hydra.adapters.rest_json.RestJsonAdapter`, so
``fetch()`` / ``parse()`` / ``validate()`` behave identically to any other
config-driven REST/JSON stream; only ``normalize()`` is overridden to shape
the payload per R9.2–R9.6 and to bump the
``hydra_eas_cve_records_total{source=...}`` counter per R9.7.

The :func:`build_tier29_adapter` helper dispatches from a ``stream_id`` to
the correct subclass. It is intentionally minimal for now; task 17.1 wires it
into the wider ``setup_eas`` plumbing.
"""

from __future__ import annotations

from typing import Any

from hydra.config import HydraSettings

from .base import Tier29RestAdapter
from .epss import FirstEPSSAdapter
from .exploitdb import ExploitDBAdapter
from .kev import CISAKEVAdapter
from .metasploit import MetasploitAdapter
from .nvd import NVDCVEAdapter

_STREAM_TO_ADAPTER: dict[str, type[Tier29RestAdapter]] = {
    "nvd-cve": NVDCVEAdapter,
    "first-epss": FirstEPSSAdapter,
    "cisa-kev": CISAKEVAdapter,
    "exploitdb": ExploitDBAdapter,
    "metasploit-modules": MetasploitAdapter,
}


def build_tier29_adapter(
    stream_id: str,
    settings: HydraSettings,
    *,
    stream_config: dict[str, Any] | None = None,
) -> Tier29RestAdapter:
    """Dispatch from a Tier 29 ``stream_id`` to the matching adapter subclass.

    Raises
    ------
    ValueError
        If ``stream_id`` is not one of the five Tier 29 streams defined in
        ``stream_registry.yaml``.
    """

    try:
        adapter_cls = _STREAM_TO_ADAPTER[stream_id]
    except KeyError as exc:
        known = ", ".join(sorted(_STREAM_TO_ADAPTER))
        raise ValueError(
            f"Unknown Tier 29 stream_id {stream_id!r}; expected one of: {known}"
        ) from exc
    return adapter_cls(stream_id, settings, stream_config=stream_config)


__all__ = [
    "CISAKEVAdapter",
    "ExploitDBAdapter",
    "FirstEPSSAdapter",
    "MetasploitAdapter",
    "NVDCVEAdapter",
    "Tier29RestAdapter",
    "build_tier29_adapter",
]
