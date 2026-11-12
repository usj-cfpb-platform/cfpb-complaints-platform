"""The documented examples must satisfy the machine-readable contracts, and bad data must fail."""

import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

ROOT = Path(__file__).resolve().parents[2]


def _load(rel: str) -> dict:
    return json.loads((ROOT / rel).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def run_schema() -> dict:
    schema = _load("config/schemas/run_metadata.schema.json")
    Draft202012Validator.check_schema(schema)
    return schema


@pytest.fixture(scope="module")
def manifest_schema() -> dict:
    schema = _load("config/schemas/manifest.schema.json")
    Draft202012Validator.check_schema(schema)
    return schema


def test_run_metadata_example_is_valid(run_schema: dict) -> None:
    Draft202012Validator(run_schema).validate(_load("docs/examples/run_metadata.example.json"))


def test_manifest_example_is_valid(manifest_schema: dict) -> None:
    Draft202012Validator(manifest_schema).validate(_load("docs/examples/manifest.example.json"))


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("outcome", "ok"),
        ("run_id", "run-1"),
        ("trigger", "cron"),
        ("record_count", -1),
    ],
)
def test_run_metadata_rejects_bad_values(run_schema: dict, field: str, bad_value: object) -> None:
    doc = copy.deepcopy(_load("docs/examples/run_metadata.example.json"))
    doc[field] = bad_value
    with pytest.raises(ValidationError):
        Draft202012Validator(run_schema).validate(doc)


def test_raw_object_path_must_follow_layout(run_schema: dict) -> None:
    doc = copy.deepcopy(_load("docs/examples/run_metadata.example.json"))
    doc["raw_objects"][0]["path"] = "raw/api/page1.json"
    with pytest.raises(ValidationError):
        Draft202012Validator(run_schema).validate(doc)
