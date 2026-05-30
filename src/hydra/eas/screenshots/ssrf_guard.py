"""SSRF defences for the Screenshot_Adapter (Design §13.2, R6.1).

Screenshot capture is the single EAS capability that makes outbound HTTP
requests on behalf of tenants. This module provides the two primitives the
adapter and renderer need to refuse dangerous URLs and pin Chromium's DNS
resolution so that DNS-rebinding attacks cannot swap the vetted IP mid
session:

* :func:`is_safe_url` — validates the scheme, resolves the hostname via
  ``getaddrinfo`` (offloaded to the running event loop), and rejects any
  resolution that lands inside a private, loopback, link-local, multicast,
  reserved, or CGNAT range.
* :func:`host_resolver_rules` — produces the
  ``--host-resolver-rules="MAP <host> <ip>"`` argument for Chromium, so the
  browser itself is pinned to the exact address that :func:`is_safe_url`
  vetted, preventing a rebinding race between the SSRF check and the render.

``ipaddress.IPv4Address.is_private`` covers ``10.0.0.0/8``,
``172.16.0.0/12``, and ``192.168.0.0/16`` but **not** CGNAT
(``100.64.0.0/10``) — that range is explicitly checked here. The IPv6
equivalents (``fc00::/7``, ``fe80::/10``, ``::1``, multicast) are all
already captured by ``is_private``/``is_loopback``/``is_link_local``/
``is_multicast`` on ``IPv6Address``.

All rejections produce a structured reason string rather than raising an
exception so the adapter can record the failure as a ``NormalizedRecord``
with ``payload.error = "SSRF_BLOCKED"`` (R6.3 pattern extended to SSRF
blocks — see adapter.py).
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
from urllib.parse import urlparse

__all__ = [
    "is_safe_url",
    "host_resolver_rules",
    "CGNAT_IPV4_NETWORK",
]

logger = logging.getLogger(__name__)


# ``ipaddress.IPv4Address.is_private`` does not include the CGNAT range; we
# keep it as a module-level constant so the rejection reason can reference
# the canonical network notation.
CGNAT_IPV4_NETWORK = ipaddress.IPv4Network("100.64.0.0/10")


def _classify_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str | None:
    """Return a human-readable rejection reason if ``ip`` is unsafe.

    Returns ``None`` when the address is safe (public / routable).
    """

    if ip.is_loopback:
        return "loopback"
    if ip.is_link_local:
        return "link_local"
    if ip.is_multicast:
        return "multicast"
    if ip.is_reserved:
        return "reserved"
    if ip.is_unspecified:
        return "unspecified"
    if ip.is_private:
        # covers RFC 1918 for IPv4 and ULA (fc00::/7) for IPv6
        return "private"
    if isinstance(ip, ipaddress.IPv4Address) and ip in CGNAT_IPV4_NETWORK:
        # CGNAT is routable for ISPs but not public — treat as unsafe.
        return "cgnat"
    return None


async def _resolve_host(host: str) -> str | None:
    """Resolve ``host`` to a single textual IP address.

    Uses ``loop.getaddrinfo`` so the blocking DNS call does not stall the
    event loop. Returns ``None`` when resolution fails for any reason
    (NXDOMAIN, timeout, unknown family). The caller treats a ``None`` result
    as an unsafe URL — refusing to render is the correct behaviour when we
    cannot independently verify the resolved IP.
    """

    loop = asyncio.get_event_loop()
    try:
        # ``AF_UNSPEC`` lets us accept both IPv4 and IPv6 answers; we pick
        # the first result so the pin is deterministic.
        infos = await loop.getaddrinfo(
            host,
            None,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as exc:
        logger.debug(
            "eas.ssrf.dns_failed", extra={"host": host, "error": str(exc)}
        )
        return None
    except Exception as exc:  # noqa: BLE001 — any DNS-layer failure is fatal
        logger.debug(
            "eas.ssrf.dns_error", extra={"host": host, "error": str(exc)}
        )
        return None

    if not infos:
        return None

    # ``sockaddr`` is a 2-tuple for IPv4 and a 4-tuple for IPv6; the IP is
    # always at index 0.
    sockaddr = infos[0][4]
    return sockaddr[0]


async def is_safe_url(url: str) -> tuple[bool, str | None, str | None]:
    """Return ``(safe, resolved_ip, reason_if_unsafe)``.

    The tuple layout is intentional: callers almost always want the
    resolved IP in the success case (to pass to
    :func:`host_resolver_rules`) and a rejection reason in the failure
    case. Keeping them in a single call avoids two round trips through
    ``getaddrinfo``.

    ``safe == True`` implies ``resolved_ip`` is a non-empty textual IP.
    ``safe == False`` implies ``reason_if_unsafe`` is a non-empty string
    (e.g. ``"unsupported_scheme"``, ``"missing_host"``, ``"dns_failed"``,
    ``"private"``, ``"loopback"``, ``"cgnat"``).
    """

    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return (False, None, f"unsupported_scheme:{scheme or 'none'}")

    host = parsed.hostname  # lower-cased, strips IPv6 brackets
    if not host:
        return (False, None, "missing_host")

    # If the caller passed a literal IP, skip the DNS step and classify
    # the address directly — otherwise resolve.
    try:
        ip_obj = ipaddress.ip_address(host)
        resolved = host
    except ValueError:
        resolved = await _resolve_host(host)
        if resolved is None:
            return (False, None, "dns_failed")
        try:
            ip_obj = ipaddress.ip_address(resolved)
        except ValueError:
            return (False, resolved, "invalid_resolved_ip")

    reason = _classify_ip(ip_obj)
    if reason is not None:
        return (False, resolved, reason)

    return (True, resolved, None)


def host_resolver_rules(host: str, resolved_ip: str) -> str:
    """Return Chromium's ``--host-resolver-rules`` argument value.

    Chromium's flag accepts comma-separated rules; for the SSRF-pin use
    case we only need one ``MAP`` rule so the browser cannot re-resolve
    the host to a different address mid-session. The returned string is
    the **value** of the flag — the caller is responsible for wrapping
    it in ``--host-resolver-rules=<value>``.

    ``host`` is passed through unchanged so that the rule matches the
    exact hostname in the URL (including case); Chromium's matching is
    case-insensitive for hostnames but keeping the original form makes
    the launch-argument list easier to audit.
    """

    return f"MAP {host} {resolved_ip}"
