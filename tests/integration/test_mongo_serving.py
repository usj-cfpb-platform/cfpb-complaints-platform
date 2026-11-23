"""MongoDB behaviour against a real server (CI service container, or your local compose).

Skipped unless pymongo is installed and MONGO_URI points to a reachable server.
Uses a throwaway database so it never touches the real ``cfpb`` data.
"""

import os
import secrets
from datetime import UTC, date, datetime

import pytest

pymongo = pytest.importorskip("pymongo")

from database import queries, schema  # noqa: E402
from database.documents import to_document  # noqa: E402
from database.load import replace_aggregate, upsert_documents  # noqa: E402

NOW = datetime(2026, 11, 23, tzinfo=UTC)


@pytest.fixture()
def db():
    uri = os.environ.get("MONGO_URI")
    if not uri:
        pytest.skip("MONGO_URI not set")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000, tz_aware=True)
    try:
        client.admin.command("ping")
    except pymongo.errors.PyMongoError:
        pytest.skip("MongoDB not reachable")
    name = f"cfpb_test_{secrets.token_hex(4)}"
    database = client[name]
    schema.ensure_complaints(database)
    yield database
    client.drop_database(name)


def _docs(n: int, version: str = "v1", segment: str = "deposit_accounts") -> list[dict]:
    rows = []
    for i in range(n):
        rows.append(
            to_document(
                {
                    "complaint_id": str(1000 + i),
                    "date_received": date(2024, 1 + i % 12, 1 + i % 27),
                    "date_sent_to_company": date(2024, 1 + i % 12, 1 + i % 27),
                    "product": "Checking or savings account",
                    "issue": f"Issue {i % 3}",
                    "state": ["CA", "NY", None][i % 3],
                    "submitted_via": "Web",
                    "company_response": "Closed with explanation",
                    "timely": [True, True, False, None][i % 4],
                    "routing_lag_days": i % 5,
                    "routing_lag_status": "ok",
                    "option": 15,
                    "segment": segment,
                    "year": 2024,
                    "month": 1 + i % 12,
                    "year_month": f"2024-{1 + i % 12:02d}",
                    "record_hash": f"{version}-{i}",
                    "source": "full_load",
                    "source_run_id": "full_test",
                    "ingested_at": datetime(2026, 11, 22),
                },
                "load_test",
                NOW,
            )
        )
    return rows


def test_repeated_load_is_idempotent(db) -> None:
    first = upsert_documents(db, iter(_docs(50)), batch_size=7)
    second = upsert_documents(db, iter(_docs(50)), batch_size=7)
    assert (first["inserted"], first["updated"], first["unchanged"]) == (50, 0, 0)
    assert (second["inserted"], second["updated"], second["unchanged"]) == (0, 0, 50)
    assert db[schema.COMPLAINTS].count_documents({}) == 50  # not inflated


def test_changed_record_is_updated_not_duplicated(db) -> None:
    upsert_documents(db, iter(_docs(10)))
    docs = _docs(10)
    docs[3]["record_hash"] = "changed"
    docs[3]["company_response"] = "Closed with monetary relief"
    result = upsert_documents(db, iter(docs))
    assert (result["inserted"], result["updated"], result["unchanged"]) == (0, 1, 9)
    stored = db[schema.COMPLAINTS].find_one({"complaint_id": docs[3]["complaint_id"]})
    assert stored["company_response"] == "Closed with monetary relief"
    assert db[schema.COMPLAINTS].count_documents({}) == 10


def test_unique_index_rejects_a_duplicate_complaint_id(db) -> None:
    doc = _docs(1)[0]
    db[schema.COMPLAINTS].insert_one(dict(doc))
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        db[schema.COMPLAINTS].insert_one(dict(doc))


def test_validator_rejects_a_document_without_date(db) -> None:
    bad = _docs(1)[0]
    bad["date_received"] = "2024-01-01"  # string, not a BSON date
    with pytest.raises(pymongo.errors.WriteError):
        db[schema.COMPLAINTS].insert_one(bad)


def test_dashboard_indexes_exist(db) -> None:
    names = set(db[schema.COMPLAINTS].index_information())
    assert {n for n, _ in schema.DASHBOARD_INDEXES} <= names
    assert schema.UNIQUE_INDEX[0] in names


def test_every_catalogue_query_runs_and_timely_pct_is_correct(db) -> None:
    upsert_documents(db, iter(_docs(40)))
    for name, builder in queries.CATALOGUE.items():
        rows = queries.run(db, builder({"segment": "deposit_accounts"}))
        assert rows, name
    timely = queries.run(db, queries.timely_rate())[0]
    # pattern True, True, False, None over 40 docs -> 20 yes, 10 no, 10 missing
    assert (timely["timely_yes"], timely["timely_no"], timely["timely_missing_or_invalid"]) == (
        20,
        10,
        10,
    )
    assert timely["timely_pct"] == pytest.approx(66.67)


def test_date_filter_uses_the_compound_index(db) -> None:
    upsert_documents(db, iter(_docs(30)))
    from database.benchmark import explain, summarize_explain

    pipeline = queries.monthly_trend(
        {"segment": "deposit_accounts", "date_from": "2024-02-01", "date_to": "2024-03-31"}
    )
    summary = summarize_explain(explain(db, pipeline))
    assert summary["access"] == "IXSCAN"


def test_aggregate_replacement_is_complete(db) -> None:
    replace_aggregate(db, "monthly_counts", [{"segment": "a", "complaints": 1}] * 3, "load_1")
    replace_aggregate(db, "monthly_counts", [{"segment": "a", "complaints": 2}], "load_2")
    coll = db[schema.aggregate_collection("monthly_counts")]
    assert coll.count_documents({}) == 1
    assert coll.find_one()["load_run_id"] == "load_2"
