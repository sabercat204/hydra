"""FastAPI dependency wiring for the mil_int surface.

Holds module-level singletons populated by :func:`setup_mil_int`. Each
``get_*`` function below is a dependency callable suitable for
``Depends(...)`` in a router.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status

from hydra.config import HydraSettings, settings as default_settings
from hydra.mil_int.settings import MilIntSettings
from hydra.mil_int.xref.resolver import XrefResolver
from hydra.registry.stream_registry import StreamRegistry, get_registry


_state: dict[str, Any] = {
    "settings": None,
    "xref_resolver": None,
    "search_backend": None,
}


def set_mil_int_components(
    *,
    settings: HydraSettings | None = None,
    xref_resolver: XrefResolver | None = None,
    search_backend: Any | None = None,
) -> None:
    """Install runtime singletons. Only non-None args overwrite state."""
    if settings is not None:
        _state["settings"] = settings
    if xref_resolver is not None:
        _state["xref_resolver"] = xref_resolver
    if search_backend is not None:
        _state["search_backend"] = search_backend


def _settings() -> HydraSettings:
    return _state.get("settings") or default_settings


def get_mil_int_settings() -> MilIntSettings:
    return _settings().mil_int


def get_stream_registry() -> StreamRegistry:
    return get_registry()


def get_xref_resolver() -> XrefResolver:
    resolver = _state.get("xref_resolver")
    if resolver is None:
        # Lazy-build from the seed path so the surface still functions
        # even when the deployment-bootstrap hasn't called setup_mil_int.
        resolver = XrefResolver.from_path(_settings().mil_int.xref_seed_path)
        _state["xref_resolver"] = resolver
    return resolver


def get_search_backend() -> Any:
    backend = _state.get("search_backend")
    if backend is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="mil_int search backend not configured",
        )
    return backend


__all__ = [
    "get_mil_int_settings",
    "get_search_backend",
    "get_stream_registry",
    "get_xref_resolver",
    "set_mil_int_components",
]
