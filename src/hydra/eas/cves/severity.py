"""CVE severity classification (Design §3.4, R10.4).

Exposes :func:`severity_for`, a pure function that folds CVSS score,
KEV listing, and EPSS score into one of the four :class:`ExposureSeverity`
labels (``"low"`` / ``"medium"`` / ``"high"`` / ``"critical"``). The
CVE_Pipeline calls this when handing a correlated exposure to the
:class:`AssetMonitor` so that the corresponding ``asset_exposures``
row carries an accurate severity (R10.4).

Design §3.4 table (reproduced verbatim):

+----------------------------------------------+-----------+
| condition                                    | severity  |
+==============================================+===========+
| kev_listed AND cvss_v3_score >= 9.0          | critical  |
+----------------------------------------------+-----------+
| kev_listed OR (cvss >= 7.0 AND epss >= 0.7)  | high      |
+----------------------------------------------+-----------+
| cvss_v3_score >= 7.0                         | medium    |
+----------------------------------------------+-----------+
| otherwise                                    | low       |
+----------------------------------------------+-----------+

The function evaluates the rows top-to-bottom and returns on the first
match. ``epss_score`` defaults to ``0.0`` when ``None`` so the
``epss >= 0.7`` branch only fires on evidence of elevated exploit
probability.
"""

from __future__ import annotations

from typing import Literal

Severity = Literal["low", "medium", "high", "critical"]

__all__ = ["severity_for", "Severity"]


def severity_for(
    cvss_v3_score: float | None,
    kev_listed: bool,
    epss_score: float | None,
) -> Severity:
    """Classify a CVE observation into a severity label (R10.4).

    ``cvss_v3_score`` of ``None`` is treated as ``0.0`` — a missing
    score cannot upgrade the severity. ``epss_score`` of ``None`` is
    likewise treated as ``0.0``.
    """

    cvss = float(cvss_v3_score) if cvss_v3_score is not None else 0.0
    epss = float(epss_score) if epss_score is not None else 0.0

    if kev_listed and cvss >= 9.0:
        return "critical"
    if kev_listed or (cvss >= 7.0 and epss >= 0.7):
        return "high"
    if cvss >= 7.0:
        return "medium"
    return "low"
