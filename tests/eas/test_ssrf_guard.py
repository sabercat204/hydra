"""Unit tests for the Screenshot_Adapter SSRF guard (task 8.9).

Exercises :func:`hydra.eas.screenshots.ssrf_guard.is_safe_url` and
:func:`hydra.eas.screenshots.ssrf_guard.host_resolver_rules`.

Real DNS is never contacted — every hostname-based test monkeypatches
:func:`hydra.eas.screenshots.ssrf_guard._resolve_host` to a dictionary
lookup so the event loop's ``getaddrinfo`` is not involved. IP-literal
tests skip the DNS branch entirely because the guard short-circuits on
:func:`ipaddress.ip_address`.

Validates: R6.1, Design §13.2.
"""

from __future__ import annotations

import pytest

from hydra.eas.screenshots import ssrf_guard
from hydra.eas.screenshots.ssrf_guard import host_resolver_rules, is_safe_url


# ---------------------------------------------------------------------------
# Scheme rejection — parsed before any resolution call, so no DNS needed
# ---------------------------------------------------------------------------


async def test_rejects_file_scheme() -> None:
    """``file://`` is neither http nor https → unsupported_scheme."""
    safe, ip, reason = await is_safe_url("file:///etc/passwd")
    assert safe is False
    assert ip is None
    assert reason is not None and reason.startswith("unsupported_scheme")


async def test_rejects_ftp_scheme() -> None:
    safe, ip, reason = await is_safe_url("ftp://example.com/")
    assert safe is False
    assert ip is None
    assert reason is not None and reason.startswith("unsupported_scheme")


async def test_rejects_javascript_scheme() -> None:
    safe, ip, reason = await is_safe_url("javascript:alert(1)")
    assert safe is False
    assert ip is None
    assert reason is not None and reason.startswith("unsupported_scheme")


async def test_rejects_gopher_scheme() -> None:
    safe, ip, reason = await is_safe_url("gopher://example.com")
    assert safe is False
    assert ip is None
    assert reason is not None and reason.startswith("unsupported_scheme")


# ---------------------------------------------------------------------------
# Missing host
# ---------------------------------------------------------------------------


async def test_rejects_missing_host() -> None:
    """``http://`` with no host produces ``missing_host``."""
    safe, ip, reason = await is_safe_url("http://")
    assert safe is False
    assert ip is None
    assert reason == "missing_host"


# ---------------------------------------------------------------------------
# IPv4 literal classification — no DNS call expected
# ---------------------------------------------------------------------------


async def test_rejects_ipv4_loopback_literal() -> None:
    safe, ip, reason = await is_safe_url("http://127.0.0.1/")
    assert safe is False
    assert ip == "127.0.0.1"
    assert reason == "loopback"


async def test_rejects_ipv4_private_10() -> None:
    safe, ip, reason = await is_safe_url("http://10.0.0.1/")
    assert safe is False
    assert ip == "10.0.0.1"
    assert reason == "private"


async def test_rejects_ipv4_private_172() -> None:
    safe, ip, reason = await is_safe_url("http://172.16.5.5/")
    assert safe is False
    assert ip == "172.16.5.5"
    assert reason == "private"


async def test_rejects_ipv4_private_192() -> None:
    safe, ip, reason = await is_safe_url("http://192.168.1.1/")
    assert safe is False
    assert ip == "192.168.1.1"
    assert reason == "private"


async def test_rejects_ipv4_link_local() -> None:
    """169.254.169.254 is the classic cloud-metadata endpoint."""
    safe, ip, reason = await is_safe_url("http://169.254.169.254/")
    assert safe is False
    assert ip == "169.254.169.254"
    assert reason == "link_local"


async def test_rejects_ipv4_cgnat() -> None:
    """100.64.0.0/10 is routable-but-not-public; rejected explicitly."""
    safe, ip, reason = await is_safe_url("http://100.64.0.1/")
    assert safe is False
    assert ip == "100.64.0.1"
    assert reason == "cgnat"


async def test_rejects_ipv4_unspecified() -> None:
    """``0.0.0.0`` resolves to the host's own interfaces — unsafe."""
    safe, ip, reason = await is_safe_url("http://0.0.0.0/")
    assert safe is False
    assert ip == "0.0.0.0"
    assert reason == "unspecified"


async def test_rejects_ipv4_multicast() -> None:
    """224.0.0.0/4 is multicast; never a valid HTTP target."""
    safe, ip, reason = await is_safe_url("http://224.0.0.1/")
    assert safe is False
    assert ip == "224.0.0.1"
    assert reason == "multicast"


# ---------------------------------------------------------------------------
# IPv6 literal classification — ``urlparse`` strips brackets from hostname
# ---------------------------------------------------------------------------


async def test_rejects_ipv6_loopback_literal() -> None:
    safe, ip, reason = await is_safe_url("http://[::1]/")
    assert safe is False
    assert ip == "::1"
    assert reason == "loopback"


async def test_rejects_ipv6_link_local() -> None:
    safe, ip, reason = await is_safe_url("http://[fe80::1]/")
    assert safe is False
    assert ip == "fe80::1"
    assert reason == "link_local"


async def test_rejects_ipv6_ula_private() -> None:
    """fc00::/7 is RFC 4193 ULA — ``is_private`` returns True."""
    safe, ip, reason = await is_safe_url("http://[fc00::1]/")
    assert safe is False
    assert ip == "fc00::1"
    assert reason == "private"


# ---------------------------------------------------------------------------
# Safe IP literal — public routable address
# ---------------------------------------------------------------------------


async def test_accepts_public_ipv4_literal() -> None:
    """8.8.8.8 is public and routable; the guard lets it through."""
    safe, ip, reason = await is_safe_url("http://8.8.8.8/")
    assert safe is True
    assert ip == "8.8.8.8"
    assert reason is None


# ---------------------------------------------------------------------------
# Hostname with monkeypatched DNS
# ---------------------------------------------------------------------------


# Stable fixture mapping used by every test in this block. ``None`` marks
# a hostname that should behave as NXDOMAIN when resolved.
_DNS_FIXTURE: dict[str, str | None] = {
    "private.example.com": "10.0.0.5",
    "public.example.com": "8.8.8.8",
    "rebinds.example.com": "127.0.0.1",
    "broken.example.com": None,
}


async def _fake_resolve(host: str) -> str | None:
    """Stand-in for ``ssrf_guard._resolve_host`` — pure lookup, no I/O."""
    return _DNS_FIXTURE.get(host)


@pytest.fixture
def patch_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Swap the module-level ``_resolve_host`` for the fixture lookup."""
    monkeypatch.setattr(ssrf_guard, "_resolve_host", _fake_resolve)


async def test_rejects_hostname_resolving_to_private_ip(
    patch_resolver: None,
) -> None:
    """Hostname resolves to RFC 1918 → rejected with ``private``."""
    safe, ip, reason = await is_safe_url("http://private.example.com")
    assert safe is False
    assert ip == "10.0.0.5"
    assert reason == "private"


async def test_accepts_hostname_resolving_to_public_ip(
    patch_resolver: None,
) -> None:
    """Hostname resolves to 8.8.8.8 → safe with the resolved IP returned."""
    safe, ip, reason = await is_safe_url("http://public.example.com")
    assert safe is True
    assert ip == "8.8.8.8"
    assert reason is None


async def test_rejects_dns_rebinding_to_loopback(
    patch_resolver: None,
) -> None:
    """DNS rebinding defence: resolved loopback is rejected, not pinned."""
    safe, ip, reason = await is_safe_url("http://rebinds.example.com")
    assert safe is False
    assert ip == "127.0.0.1"
    assert reason == "loopback"


async def test_rejects_nxdomain_hostname(patch_resolver: None) -> None:
    """Resolution failure propagates as ``dns_failed`` with no IP."""
    safe, ip, reason = await is_safe_url("http://broken.example.com")
    assert safe is False
    assert ip is None
    assert reason == "dns_failed"


# ---------------------------------------------------------------------------
# host_resolver_rules — sync pure function, case-preserving
# ---------------------------------------------------------------------------


def test_host_resolver_rules_basic() -> None:
    """Simple hostname + IPv4 pair produces the ``MAP`` rule verbatim."""
    assert (
        host_resolver_rules("example.com", "8.8.8.8")
        == "MAP example.com 8.8.8.8"
    )


def test_host_resolver_rules_preserves_case() -> None:
    """Mixed-case hostnames are not lower-cased (Chromium ignores case,
    but keeping the original form makes launch arguments easier to audit).
    """
    assert (
        host_resolver_rules("Example.COM", "8.8.8.8")
        == "MAP Example.COM 8.8.8.8"
    )


def test_host_resolver_rules_ipv6_brackets_passthrough() -> None:
    """Bracketed IPv6 literals are passed through unchanged."""
    assert host_resolver_rules("host", "[::1]") == "MAP host [::1]"
