"""Validate manifests and run metadata against the committed JSON schemas.

The schemas in ``config/schemas/`` are the machine-readable form of ``docs/data_contracts.md``.
A document that does not satisfy them is never written as a completion marker.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from common.config import REPO_ROOT

SCHEMA_DIR = REPO_ROOT / "config" / "schemas"


class ContractError(ValueError):
    """A document does not satisfy its data contract."""


@cache
def _validator(schema_file: str) -> Draft202012Validator:
    schema = json.loads((SCHEMA_DIR / schema_file).read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def _validate(schema_file: str, document: dict[str, Any], label: str) -> None:
    errors = sorted(_validator(schema_file).iter_errors(document), key=lambda e: list(e.path))
    if errors:
        details = "; ".join(
            f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors
        )
        raise ContractError(f"{label} violates its contract: {details}")


def validate_manifest(document: dict[str, Any]) -> None:
    """Historical load manifest (``config/schemas/manifest.schema.json``)."""
    _validate("manifest.schema.json", document, "manifest.json")


def validate_run_metadata(document: dict[str, Any]) -> None:
    """Incremental run metadata (``config/schemas/run_metadata.schema.json``)."""
    _validate("run_metadata.schema.json", document, "run_metadata.json")


def schema_path(name: str) -> Path:
    return SCHEMA_DIR / name
