"""Tests for YamlCredentialStore — 12 tests."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
import yaml

from hydra.auth.credential_store import YamlCredentialStore
from hydra.auth.exceptions import CredentialNotFoundError, CredentialStoreError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_yaml(path: Path, data: dict | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        yaml.safe_dump(data, fh)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_yaml_store_loads_valid_file(tmp_path: Path) -> None:
    creds = {"stream_a": {"api_key": "abc123"}, "stream_b": {"username": "u", "password": "p"}}
    p = tmp_path / "creds.yaml"
    _write_yaml(p, creds)

    store = YamlCredentialStore(path=p)
    result = store.get("stream_a")
    assert result == {"api_key": "abc123"}


def test_yaml_store_get_missing_stream(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "creds.yaml", {"stream_a": {"api_key": "x"}})
    store = YamlCredentialStore(path=tmp_path / "creds.yaml")

    with pytest.raises(CredentialNotFoundError):
        store.get("nonexistent")


def test_yaml_store_file_not_found() -> None:
    with pytest.raises(CredentialStoreError, match="not found"):
        YamlCredentialStore(path=Path("/tmp/does_not_exist_hydra_test.yaml"))


def test_yaml_store_malformed_yaml(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text("{{{{invalid yaml: [")

    with pytest.raises(CredentialStoreError, match="Malformed YAML"):
        YamlCredentialStore(path=p)


def test_yaml_store_reload(tmp_path: Path) -> None:
    p = tmp_path / "creds.yaml"
    _write_yaml(p, {"stream_a": {"api_key": "old"}})
    store = YamlCredentialStore(path=p)
    assert store.get("stream_a")["api_key"] == "old"

    _write_yaml(p, {"stream_a": {"api_key": "new"}})
    store.reload()
    assert store.get("stream_a")["api_key"] == "new"


def test_yaml_store_reload_thread_safety(tmp_path: Path) -> None:
    p = tmp_path / "creds.yaml"
    _write_yaml(p, {"s": {"k": "v"}})
    store = YamlCredentialStore(path=p)

    errors: list[Exception] = []

    def worker() -> None:
        try:
            for _ in range(50):
                store.reload()
                store.get("s")
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread-safety errors: {errors}"


def test_yaml_store_list_streams(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "creds.yaml", {"z_stream": {}, "a_stream": {}, "m_stream": {}})
    store = YamlCredentialStore(path=tmp_path / "creds.yaml")
    assert store.list_streams() == ["a_stream", "m_stream", "z_stream"]


def test_yaml_store_empty_file(tmp_path: Path) -> None:
    p = tmp_path / "creds.yaml"
    p.write_text("")  # empty YAML → None
    store = YamlCredentialStore(path=p)

    with pytest.raises(CredentialNotFoundError):
        store.get("anything")
    assert store.list_streams() == []


def test_yaml_store_get_returns_copy(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "creds.yaml", {"s": {"api_key": "original"}})
    store = YamlCredentialStore(path=tmp_path / "creds.yaml")

    result = store.get("s")
    result["api_key"] = "mutated"

    assert store.get("s")["api_key"] == "original"


def test_yaml_store_unicode_stream_ids(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "creds.yaml", {"données_météo": {"clé": "valeur"}, "数据流": {"密钥": "值"}})
    store = YamlCredentialStore(path=tmp_path / "creds.yaml")

    assert store.get("données_météo") == {"clé": "valeur"}
    assert store.get("数据流") == {"密钥": "值"}


def test_yaml_store_nested_credentials(tmp_path: Path) -> None:
    creds = {
        "earthdata_jwst": {
            "client_id": "cid",
            "client_secret": "csec",
            "username": "user",
            "password": "pass",
        }
    }
    _write_yaml(tmp_path / "creds.yaml", creds)
    store = YamlCredentialStore(path=tmp_path / "creds.yaml")

    result = store.get("earthdata_jwst")
    assert result == creds["earthdata_jwst"]


def test_yaml_store_reload_file_deleted(tmp_path: Path) -> None:
    p = tmp_path / "creds.yaml"
    _write_yaml(p, {"s": {"k": "v"}})
    store = YamlCredentialStore(path=p)

    p.unlink()

    with pytest.raises(CredentialStoreError, match="not found"):
        store.reload()
