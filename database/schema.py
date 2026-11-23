"""MongoDB serving-layer model: collection names, JSON Schema validator and indexes.

Design (details in docs/mongodb.md):
* one document per complaint in ``complaints`` (embedded, flat: every dashboard query reads one
  complaint at a time, so nothing is referenced across collections);
* ``complaint_id`` is the business key, enforced by a unique index;
* dates are stored as BSON Date (UTC midnight) so range filters and ``$dateTrunc`` work;
* one ``agg_<name>`` collection per Spark aggregate, replaced on every load;
* ``pipeline_runs`` keeps one document per load (freshness + reconciliation for the dashboard).
"""

from __future__ import annotations

from typing import Any

COMPLAINTS = "complaints"
PIPELINE_RUNS = "pipeline_runs"
AGGREGATE_NAMES = (
    "monthly_counts",
    "issue_breakdown",
    "geography_channel",
    "company_response",
    "timely_response",
    "routing_lag",
)


def aggregate_collection(name: str) -> str:
    return f"agg_{name}"


def _nullable(bson_type: str) -> dict[str, Any]:
    return {"bsonType": [bson_type, "null"]}


# Fields every curated document must carry (written by database.documents.to_document).
REQUIRED_FIELDS = (
    "complaint_id",
    "date_received",
    "product",
    "option",
    "segment",
    "year",
    "month",
    "year_month",
    "record_hash",
    "source",
    "source_run_id",
    "load_run_id",
)

COMPLAINT_VALIDATOR: dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": list(REQUIRED_FIELDS),
        "properties": {
            "complaint_id": {"bsonType": "string", "minLength": 1},
            "date_received": {"bsonType": "date"},
            "date_sent_to_company": _nullable("date"),
            "product": {"bsonType": "string"},
            "sub_product": _nullable("string"),
            "issue": _nullable("string"),
            "sub_issue": _nullable("string"),
            "company": _nullable("string"),
            "company_public_response": _nullable("string"),
            "company_response": _nullable("string"),
            "state": _nullable("string"),
            "state_is_valid": _nullable("bool"),
            "zip_code": _nullable("string"),
            "tags": {"bsonType": ["array", "null"], "items": {"bsonType": "string"}},
            "submitted_via": _nullable("string"),
            "timely": _nullable("bool"),
            "option": {"bsonType": "int"},
            "segment": {"bsonType": "string"},
            "year": {"bsonType": "int", "minimum": 2011},
            "month": {"bsonType": "int", "minimum": 1, "maximum": 12},
            "year_month": {"bsonType": "string", "pattern": "^[0-9]{4}-[0-9]{2}$"},
            "routing_lag_days": _nullable("int"),
            "routing_lag_status": _nullable("string"),
            "quality_flags": {"bsonType": ["array", "null"], "items": {"bsonType": "string"}},
            "record_hash": {"bsonType": "string", "minLength": 1},
            "source": {"enum": ["full_load", "api"]},
            "source_run_id": {"bsonType": "string"},
            "ingested_at": _nullable("date"),
            "load_run_id": {"bsonType": "string"},
            "loaded_at": _nullable("date"),
            "first_loaded_at": _nullable("date"),
        },
    }
}

# The unique business key. Not dropped by the index benchmark.
UNIQUE_INDEX = ("ux_complaint_id", [("complaint_id", 1)])

# Compound indexes derived from the dashboard filters (equality fields first, date range last:
# the Equality-Sort-Range rule). Every dashboard query filters on segment and a date range.
DASHBOARD_INDEXES: tuple[tuple[str, list[tuple[str, int]]], ...] = (
    ("ix_segment_date", [("segment", 1), ("date_received", 1)]),
    ("ix_segment_product_date", [("segment", 1), ("product", 1), ("date_received", 1)]),
    ("ix_segment_issue_date", [("segment", 1), ("issue", 1), ("date_received", 1)]),
    ("ix_segment_state_date", [("segment", 1), ("state", 1), ("date_received", 1)]),
    ("ix_segment_company_date", [("segment", 1), ("company", 1), ("date_received", 1)]),
    ("ix_segment_channel_date", [("segment", 1), ("submitted_via", 1), ("date_received", 1)]),
)

AGGREGATE_INDEXES: dict[str, list[tuple[str, int]]] = {
    "monthly_counts": [("segment", 1), ("year_month", 1)],
    "issue_breakdown": [("segment", 1), ("issue", 1)],
    "geography_channel": [("segment", 1), ("state", 1)],
    "company_response": [("segment", 1), ("year", 1)],
    "timely_response": [("segment", 1), ("year_month", 1)],
    "routing_lag": [("segment", 1), ("year_month", 1)],
}


def ensure_complaints(db: Any, *, dashboard_indexes: bool = True) -> None:
    """Create or update the validated ``complaints`` collection and its indexes."""
    if COMPLAINTS in db.list_collection_names():
        db.command(
            "collMod",
            COMPLAINTS,
            validator=COMPLAINT_VALIDATOR,
            validationLevel="strict",
            validationAction="error",
        )
    else:
        db.create_collection(
            COMPLAINTS,
            validator=COMPLAINT_VALIDATOR,
            validationLevel="strict",
            validationAction="error",
        )
    coll = db[COMPLAINTS]
    name, keys = UNIQUE_INDEX
    coll.create_index(keys, name=name, unique=True)
    if dashboard_indexes:
        create_dashboard_indexes(db)
    db[PIPELINE_RUNS].create_index([("finished_at", -1)], name="ix_finished_at")


def create_dashboard_indexes(db: Any) -> list[str]:
    coll = db[COMPLAINTS]
    return [coll.create_index(keys, name=name) for name, keys in DASHBOARD_INDEXES]


def drop_dashboard_indexes(db: Any) -> list[str]:
    """Drop only the dashboard indexes (used by the before/after benchmark)."""
    existing = set(db[COMPLAINTS].index_information())
    dropped = []
    for name, _ in DASHBOARD_INDEXES:
        if name in existing:
            db[COMPLAINTS].drop_index(name)
            dropped.append(name)
    return dropped
