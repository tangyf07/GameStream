"""Validate event schema file and sample generated events."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SCHEMA_PATH = ROOT / "simulator" / "schemas" / "events.json"
REQUIRED_TYPES = {
    "login",
    "create_role",
    "enter_dungeon",
    "clear_dungeon",
    "death",
    "equip",
    "enhance",
    "recharge",
    "gacha",
    "friend",
    "logout",
}


def test_schema_file_exists_and_loads():
    assert SCHEMA_PATH.exists()
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert schema["type"] == "object"
    assert set(schema["required"]) >= {
        "event_id",
        "event_type",
        "event_time",
        "player_id",
        "role_id",
        "server_id",
        "session_id",
    }
    enum = schema["properties"]["event_type"]["enum"]
    assert set(enum) == REQUIRED_TYPES


def test_generated_events_validate():
    jsonschema = pytest.importorskip("jsonschema")
    from simulator.generate_events import generate

    out = ROOT / "data" / "test_schema_events.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    generate(players=50, events=200, servers=2, days=3, seed=1, out_path=out)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    # Draft202012Validator if available
    Validator = getattr(jsonschema, "Draft202012Validator", jsonschema.Draft7Validator)
    validator = Validator(schema)
    types_seen = set()
    with out.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            validator.validate(obj)
            types_seen.add(obj["event_type"])
    # probabilistic — with 200 events we expect most types; require core set
    assert {"login", "logout", "enter_dungeon"} <= types_seen
