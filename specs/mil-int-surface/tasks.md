# Mil-Int Surface — Implementation Tasks

| # | Task | Status |
|---|---|---|
| 1 | Add `Tier` enum entries 29 and 100–107 in `models/normalized.py`. | ✅ |
| 2 | Append tiers 100–107 + `doc_repo` adapter + `biweekly`/`quarterly`/`on_change` cadences to `stream_registry.yaml`. | ✅ |
| 3 | Extend `StreamSource` with `access_policy` and add `get_sources_by_access_policy()` on `StreamRegistry`. | ✅ |
| 4 | Implement `adapters/doc_repo.py` (fetch / parse / validate / normalize / blob downloader). | ✅ |
| 5 | Register `doc_repo` in `adapters/__init__.py` and `scheduler/task_runner._ADAPTER_TYPE_MAP`. | ✅ |
| 6 | Add `biweekly`, `quarterly`, `on_change` to `dag_factory.CADENCE_CONFIG`. | ✅ |
| 7 | Add `dags/cadence_biweekly.py`, `cadence_quarterly.py`, `cadence_on_change.py`. | ✅ |
| 8 | Build `src/hydra/mil_int/` (settings, classification, dedup, metrics, schemas, xref, dependencies, routers, setup). | ✅ |
| 9 | Wire `MilIntSettings` into `HydraSettings`; mount routers in `api/app.create_app`. | ✅ |
| 10 | Add `config/mil_int_xref.yaml` seed and `mil_int.*` defaults to `config/settings.yaml`. | ✅ |
| 11 | Author `specs/mil-int-surface/{requirements,design,tasks,source_manifest}.md` and update `README.md`. | ✅ |
| 12 | Tests: `test_doc_repo_adapter.py`, `test_mil_int_classification.py`, `test_mil_int_xref.py`, `test_mil_int_dedup.py`, `test_mil_int_routers.py`; extend `test_registry.py` and `test_storage_router.py`. | ✅ |

## Verification

1. `python3 -c "from hydra.registry.stream_registry import load_registry; r = load_registry('src/hydra/registry/stream_registry.yaml'); assert sorted(t for t in r.tiers if t >= 100) == [100,101,102,103,104,105,106,107]; print('ok', sum(len(r.tiers[t].sources) for t in r.tiers if t >= 100), 'mil_int sources')"`
2. `pytest tests/test_registry.py tests/test_storage_router.py tests/test_mil_int_*.py tests/test_doc_repo_adapter.py -v`
3. `curl http://localhost:8000/api/v1/mil-int/manifest` after `docker-compose up -d`.
4. `curl 'http://localhost:8000/api/v1/mil-int/standards/xref?from_id=MIL-STD-461'` returns the seeded NIST SP 800-53 mapping.
5. `ruff check src/hydra/mil_int src/hydra/adapters/doc_repo.py tests/test_mil_int_*.py`.
