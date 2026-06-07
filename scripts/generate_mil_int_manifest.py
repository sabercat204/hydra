"""Generate specs/mil-int-surface/source_manifest.md from the registry.

Run from the repo root:

    python scripts/generate_mil_int_manifest.py > specs/mil-int-surface/source_manifest.md
"""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from hydra.registry.stream_registry import load_registry

    registry = load_registry("src/hydra/registry/stream_registry.yaml")
    print("# Mil-Int Surface — Source Manifest")
    print()
    print(
        "Generated from `src/hydra/registry/stream_registry.yaml` for tiers "
        "100–107. Re-generate via `python scripts/generate_mil_int_manifest.py`."
    )
    print()

    total = 0
    ingestable = 0
    for tid in sorted(t for t in registry.tiers if 100 <= t <= 107):
        tier = registry.tiers[tid]
        print(f"## Tier {tid} — {tier.name}")
        print()
        print(
            f"- cadence: `{tier.cadence}`  ·  adapter: `{tier.adapter}`  ·  "
            f"sources: {len(tier.sources)}"
        )
        print()
        print("| Source | Access | URL | Notes |")
        print("|---|---|---|---|")
        for src in tier.sources:
            total += 1
            if src.access_policy in ("open", "registration"):
                ingestable += 1
            notes = src.notes
            if len(notes) > 60:
                notes = notes[:60] + "…"
            print(f"| {src.name} | `{src.access_policy}` | <{src.url}> | {notes} |")
        print()

    print("## Totals")
    print()
    print(f"- Total sources: **{total}**")
    print(f"- Auto-ingestable (open / registration): **{ingestable}**")
    print(
        f"- Subscription / restricted / archived / monitor_only: "
        f"**{total - ingestable}**"
    )


if __name__ == "__main__":
    main()
