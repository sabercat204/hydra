"""Mil-Int Public Information surface — military / defense / intelligence
document repositories aggregated from public sources (tiers 100-107).

Mirrors the EAS module shape: a self-contained subsystem with its own
settings, schemas, routers, classification gate, dedup resolver, and
standards cross-reference engine. Exposes the ``/api/v1/mil-int/*``
namespace once :func:`setup_mil_int` is wired into the FastAPI app.
"""

from hydra.mil_int.settings import MilIntSettings

__all__ = ["MilIntSettings"]
