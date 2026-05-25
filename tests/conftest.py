import json
import sys
import types

import pytest
from fastapi.testclient import TestClient


if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")

    class IntegrityError(Exception):
        pass

    def connect(*args, **kwargs):
        raise RuntimeError("Database is not available during unit tests")

    psycopg2_stub.IntegrityError = IntegrityError
    psycopg2_stub.connect = connect

    extras_stub = types.ModuleType("psycopg2.extras")
    extras_stub.RealDictCursor = object

    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extras"] = extras_stub

from backend.app import app
from backend import data, routes, task_manager as task_manager_module


@pytest.fixture
def client(tmp_path, monkeypatch):
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(data, "ACCOUNTS_FILE", accounts_file)
    monkeypatch.setattr(routes, "USE_DATABASE", False)
    monkeypatch.setattr(task_manager_module, "USE_DATABASE", False)
    app.state.token_map = {}
    routes.task_manager.tasks.clear()
    routes.task_manager.active_connections.clear()
    routes.task_manager.cancel_flags.clear()
    routes.task_manager.pause_flags.clear()

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def saved_accounts(tmp_path, monkeypatch):
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(data, "ACCOUNTS_FILE", accounts_file)

    def read_accounts():
        return json.loads(accounts_file.read_text(encoding="utf-8"))

    return read_accounts
