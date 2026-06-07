from __future__ import annotations
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _no_db(monkeypatch):
    import simulator.cloud.db as db
    monkeypatch.setattr(db, "make_engine", lambda *a, **k: None)
    yield