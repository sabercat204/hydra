"""Tenant auth plumbing tests (task 4.4).

Covers three pieces of R20 / R20.2 wiring:

1. :class:`hydra.api.dependencies.APIKeyRecord` accepts a ``tenant_id`` kwarg
   and the default factory mints a fresh UUID per instance.
2. :func:`hydra.api.dependencies.get_current_tenant_id` returns the tenant_id
   of an ``APIKeyRecord`` passed via ``Depends``. We call the coroutine
   directly — no FastAPI app is spun up here because Design §3.1 describes
   this as a pure pass-through dependency.
3. ``scripts/create_api_key.py`` surfaces ``--tenant-id`` in its ``--help``
   output (Design §3.1 provisioning).

_(satisfies R20.1, R20.2)_
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path
from uuid import UUID, uuid4

from hydra.api.dependencies import APIKeyRecord, get_current_tenant_id


# ---------------------------------------------------------------------------
# APIKeyRecord — tenant_id kwarg and default factory
# ---------------------------------------------------------------------------


def test_api_key_record_accepts_tenant_id_kwarg() -> None:
    """R20.1 — an explicit tenant_id kwarg round-trips unchanged."""
    tenant = uuid4()
    record = APIKeyRecord(key_id="k1", name="acme-1", tenant_id=tenant)
    assert record.tenant_id == tenant
    assert isinstance(record.tenant_id, UUID)


def test_api_key_record_default_factory_generates_unique_uuids() -> None:
    """R20.1 — two records without an explicit tenant_id must differ."""
    r1 = APIKeyRecord(key_id="k1", name="a")
    r2 = APIKeyRecord(key_id="k2", name="b")
    # Both UUIDs are real v4 UUIDs and they are not equal. A collision here
    # is astronomically unlikely; if this test flakes, the RNG is broken.
    assert isinstance(r1.tenant_id, UUID)
    assert isinstance(r2.tenant_id, UUID)
    assert r1.tenant_id != r2.tenant_id


def test_api_key_record_default_scopes_preserved() -> None:
    """Sanity — adding tenant_id didn't accidentally clear the scopes default."""
    record = APIKeyRecord(key_id="k1", name="a")
    assert record.scopes == ["read", "search", "write"]


# ---------------------------------------------------------------------------
# get_current_tenant_id — pure pass-through dependency (R20.2)
# ---------------------------------------------------------------------------


def test_get_current_tenant_id_returns_record_tenant() -> None:
    """R20.2 — the dependency returns ``api_key.tenant_id`` verbatim.

    We invoke the coroutine directly rather than running FastAPI's dependency
    resolver because the function under test is a pure projection: it reads
    one attribute off its argument. Spinning up an ASGI app would test
    FastAPI's injector, not our code.
    """
    tenant = uuid4()
    record = APIKeyRecord(key_id="k1", name="acme-1", tenant_id=tenant)
    returned = asyncio.run(get_current_tenant_id(api_key=record))
    assert returned == tenant


def test_get_current_tenant_id_preserves_type() -> None:
    """The dependency must return a ``uuid.UUID`` (not str, not bytes)."""
    record = APIKeyRecord(key_id="k1", name="a")
    returned = asyncio.run(get_current_tenant_id(api_key=record))
    assert isinstance(returned, UUID)
    assert returned == record.tenant_id


# ---------------------------------------------------------------------------
# create_api_key.py --help advertises --tenant-id (Design §3.1)
# ---------------------------------------------------------------------------


def test_create_api_key_cli_help_mentions_tenant_id() -> None:
    """The CLI ``--help`` output must surface the ``--tenant-id`` flag.

    Spawned in a subprocess so argparse actually parses ``--help`` and emits
    text the way a real operator would see it. No database is touched
    because argparse exits before ``create_key()`` runs.
    """
    script = Path(__file__).resolve().parents[2] / "scripts" / "create_api_key.py"
    assert script.exists(), f"expected {script} to exist"

    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # argparse exits 0 on --help on POSIX and Windows alike.
    assert result.returncode == 0, (
        f"--help exited {result.returncode}; stderr={result.stderr!r}"
    )
    help_text = result.stdout
    assert "--tenant-id" in help_text, (
        f"expected '--tenant-id' in --help output; got:\n{help_text}"
    )
