"""Shared pytest fixtures and helpers for EAS tests (task 7.1).

The helpers below are used by ``test_assets.py`` and ``test_asset_matcher.py``
so they live here rather than being duplicated across files. A separate
``tests/eas`` conftest keeps them out of the general test namespace.

Two small factories:

* ``make_asset`` — build an :class:`hydra.eas.assets.models.Asset` with sane
  defaults.
* ``make_normalized_record`` — build a
  :class:`hydra.models.normalized.NormalizedRecord` with sane defaults.

Both are plain factory functions, not ``@pytest.fixture``s, because the
call sites want to parameterize across many shapes per test (property tests
especially). They are also exposed as pytest fixtures (``make_asset`` and
``make_normalized_record``) for tests that want injection-style access.

We also install defensive ``sys.modules`` stubs for ``pandasdmx`` *before*
any ``hydra.*`` import fires, because the installed ``pandasdmx`` package
has a module-level ``from pydantic import DictError`` that explodes under
pydantic v2. None of the tests in this directory need ``pandasdmx``, but a
transitive import through ``hydra.adapters.sdmx`` (via a wildcard import
elsewhere) could still trigger the failure. The stub makes the module
look importable and empty.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Defensive pandasdmx shim (must run BEFORE any hydra.* import)
# ---------------------------------------------------------------------------

import sys
import types
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4


def _install_pandasdmx_stubs() -> None:
    """Install empty ``pandasdmx`` stubs if the real package cannot import.

    The vendored ``pandasdmx`` package on common 3.12 installs fails to import
    because of a removed ``DictError`` symbol in ``pydantic`` v2. Since none of
    the EAS tests touch SDMX adapters, a pair of empty modules is enough to
    satisfy any transitive ``import pandasdmx`` that might fire during
    ``hydra.*`` collection.
    """

    # Short-circuit if somebody already imported pandasdmx cleanly.
    if "pandasdmx" in sys.modules:
        return

    # Probe without importing so we do not trigger the pydantic-v2 error path.
    try:
        import importlib.util

        spec = importlib.util.find_spec("pandasdmx")
    except (ImportError, ValueError):
        spec = None

    if spec is None:
        return  # nothing to stub out; real package is absent

    # The real package would fail at import time. Preempt that with a minimal
    # stub so any transitive ``import pandasdmx`` at hydra-module-load time
    # lands on a no-op.
    stub = types.ModuleType("pandasdmx")
    stub.Request = object  # type: ignore[attr-defined]
    stub.list_sources = lambda: []  # type: ignore[attr-defined]
    stub.add_source = lambda *a, **kw: None  # type: ignore[attr-defined]
    stub.to_pandas = lambda *a, **kw: None  # type: ignore[attr-defined]
    sys.modules["pandasdmx"] = stub


_install_pandasdmx_stubs()


# ---------------------------------------------------------------------------
# Now it is safe to import hydra.*.
# ---------------------------------------------------------------------------

import pytest  # noqa: E402

from hydra.eas.assets.models import Asset  # noqa: E402
from hydra.models.normalized import NormalizedRecord, SourceMeta, Tier  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_asset(
    tenant_id: UUID | None = None,
    asset_type: str = "ip",
    normalized_value: str = "192.0.2.1",
    **kwargs: Any,
) -> Asset:
    """Build an :class:`Asset` with sane defaults.

    Overriding any defaulted field via ``**kwargs`` is supported; this is the
    cheap way to test e.g. ``is_active=False`` or a deactivated_at timestamp.
    """

    defaults: dict[str, Any] = {
        "asset_id": uuid4(),
        "tenant_id": tenant_id if tenant_id is not None else uuid4(),
        "asset_type": asset_type,
        "normalized_value": normalized_value,
        "raw_value": kwargs.pop("raw_value", normalized_value),
        "is_active": True,
        "capture_screenshots": False,
        "created_at": datetime.now(timezone.utc),
        "deactivated_at": None,
        "notes": None,
    }
    defaults.update(kwargs)
    return Asset(**defaults)


def make_normalized_record(
    tier: int = 16,
    raw_hash: str | None = None,
    payload: dict[str, Any] | None = None,
    **kwargs: Any,
) -> NormalizedRecord:
    """Build a :class:`NormalizedRecord` with sane defaults.

    ``raw_hash`` defaults to a valid 16-char lowercase hex. The
    ``NormalizedRecord`` validator enforces that shape, so any override must
    satisfy it as well.
    """

    source_meta = kwargs.pop(
        "source_meta",
        SourceMeta(
            source_name="test-source",
            source_url="",
            adapter_type="rest_json",
            access_level="green",
        ),
    )
    defaults: dict[str, Any] = {
        "stream_id": kwargs.pop("stream_id", "test-stream"),
        "tier": Tier(tier),
        "timestamp": kwargs.pop("timestamp", datetime.now(timezone.utc)),
        "geo": kwargs.pop("geo", None),
        "payload": payload if payload is not None else {},
        "source_meta": source_meta,
        "raw_hash": raw_hash if raw_hash is not None else "0123456789abcdef",
    }
    # confidence/tags/ingested_at can still be overridden if the caller
    # wants to exercise them.
    defaults.update(kwargs)
    return NormalizedRecord(**defaults)


# ---------------------------------------------------------------------------
# Pytest-fixture-style access for tests that want dependency-injection
# ---------------------------------------------------------------------------


@pytest.fixture
def asset_factory():
    """Injection-style fixture wrapping :func:`make_asset`."""
    return make_asset


@pytest.fixture
def record_factory():
    """Injection-style fixture wrapping :func:`make_normalized_record`."""
    return make_normalized_record
