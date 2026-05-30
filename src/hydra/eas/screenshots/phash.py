"""Perceptual-hash helpers for the Screenshot_Adapter (R6.2, R8.1).

Two small functions exposed:

* :func:`compute_phash` — produces a 16-character lowercase hexadecimal
  string from raw PNG bytes via :func:`imagehash.phash`. The string form
  is what lands in Elasticsearch's ``phash`` field and the
  :class:`hydra.eas.schemas.images.ImageMetadataResponse` schema.
* :func:`hamming_similarity` — returns
  ``1.0 - (popcount(a ^ b) / 64.0)`` for two 16-char hex strings.
  Satisfies the property-test bounds (``[0.0, 1.0]`` inclusive, symmetry,
  ``H(a, a) == 1.0``) required by Property 11.

Import-safety: :mod:`imagehash` and :mod:`PIL` are optional dependencies
(``[eas]`` extra). The imports are lazy so this module can be loaded in
test environments without either library — the failure surfaces only when
:func:`compute_phash` is actually called. :func:`hamming_similarity` is
pure Python (int ^ int + ``bit_count``) so it never requires either
library at runtime.
"""

from __future__ import annotations

import re
from io import BytesIO

__all__ = [
    "compute_phash",
    "hamming_similarity",
]


_HEX16_RE = re.compile(r"^[0-9a-f]{16}$")


def compute_phash(png_bytes: bytes) -> str:
    """Return the 16-char lowercase hex pHash of ``png_bytes``.

    Lazy-imports :mod:`imagehash` / :mod:`PIL` so that this module can be
    loaded in environments without the ``[eas]`` extra installed. The
    first call without those libraries raises :class:`ImportError` with a
    pointer to the extra.
    """

    try:
        import imagehash  # type: ignore[import-not-found]
        from PIL import Image  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - import guard
        raise ImportError(
            "compute_phash() requires the `[eas]` extras (imagehash, Pillow). "
            "Install with `pip install 'hydra[eas]'`."
        ) from exc

    # ``imagehash.phash`` always returns a 64-bit hash — the ``__str__``
    # form is a 16-character lowercase hex string.
    image = Image.open(BytesIO(png_bytes))
    try:
        return str(imagehash.phash(image))
    finally:
        try:
            image.close()
        except Exception:  # noqa: BLE001 — teardown is best-effort
            pass


def hamming_similarity(a: str, b: str) -> float:
    """Return ``1.0 - (popcount(int(a,16) ^ int(b,16)) / 64.0)``.

    The inputs must be 16-character lowercase hexadecimal strings (the
    canonical output of :func:`compute_phash`). Returns a float in
    ``[0.0, 1.0]``. Property 11 invariants:

    * **Symmetry** — ``hamming_similarity(a, b) == hamming_similarity(b, a)``
      because ``a ^ b == b ^ a``.
    * **Identity** — ``hamming_similarity(a, a) == 1.0`` because
      ``a ^ a == 0``.
    * **Bounds** — the popcount is in ``[0, 64]`` so the quotient is in
      ``[0.0, 1.0]``.

    We validate the input format here rather than trusting the caller
    because a silently-truncated or mis-cased input would otherwise
    produce a similarity in the wrong range.
    """

    if not _HEX16_RE.match(a) or not _HEX16_RE.match(b):
        raise ValueError(
            "hamming_similarity requires 16-character lowercase hex strings"
        )

    xor_int = int(a, 16) ^ int(b, 16)
    # ``int.bit_count`` is a 3.10+ builtin with C-accelerated popcount.
    # We require Python 3.12 so it is always present; keep an explicit
    # branch as a safety net for the type checker.
    if hasattr(xor_int, "bit_count"):
        differing_bits = xor_int.bit_count()
    else:  # pragma: no cover - unreachable on 3.12+
        differing_bits = bin(xor_int).count("1")

    return 1.0 - (differing_bits / 64.0)
