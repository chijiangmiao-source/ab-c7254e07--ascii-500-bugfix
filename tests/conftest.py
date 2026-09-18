"""Pytest fixtures: isolated DATA_DIR per test and fresh app instances.

A second app instance against the same on-disk data simulates an API restart
(the real-process version lives in scripts/verify.py).
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import config, db, storage
from app.main import create_app


@pytest.fixture
def data_env(tmp_path, monkeypatch):
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_DIR", data)
    monkeypatch.setattr(config, "DB_PATH", data / "registry.db")
    monkeypatch.setattr(config, "CHUNKS_DIR", data / "chunks")
    monkeypatch.setattr(config, "FILES_DIR", data / "files")
    monkeypatch.setattr(config, "TEMP_DIR", data / "tmp")
    storage.ensure_dirs()
    db.init_db()
    return data


@pytest.fixture
def client(data_env):
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def restart_client(data_env):
    """A brand-new application/connection set over the same persisted data."""
    with TestClient(create_app()) as c:
        yield c
