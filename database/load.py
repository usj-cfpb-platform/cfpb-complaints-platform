"""Load curated Spark output into MongoDB (Turn 4).

    docker compose run --rm spark python -m database.load           # MinIO -> MongoDB
    docker compose run --rm spark python -m database.load --sample  # local sample (CI, demo)

Steps:
1. read the newest Spark quality report and refuse to load if its gate FAILED;
2. read curated Parquet with Spark and upsert it in batches by ``complaint_id``:
   new id -> insert, different ``record_hash`` -> update, same hash -> untouched (idempotent);
3. delete documents whose id is no longer in curated (the curated layer is the full truth);
4. replace every ``agg_<name>`` collection atomically (write a temp collection, then rename);
5. write ``reports/reconciliation/run_id=<id>/reconciliation.json`` and a ``pipeline_runs``
   document (freshness and counts for the dashboard health view).

Exit codes: 0 loaded and balanced, 1 refused or unbalanced, 2 configuration error.
"""

from __future__ import annotations

import argparse
import json
import logging
import secrets
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from common.config import ConfigError, load_settings
from database import schema
from database.documents import (
    LoadRefused,
    build_reconciliation,
    check_quality_gate,
    classify,
    latest_report_key,
    to_document,
)

LOG = logging.getLogger("database.load")
DEFAULT_BATCH = 5000


def new_load_run_id(now: datetime) -> str:
    return f"load_{now.strftime('%Y%m%dT%H%M%SZ')}_{secrets.token_hex(3)}"


# --------------------------------------------------------------------------- inputs


class Source:
    """Where Spark wrote its outputs: MinIO (s3a://bucket) or the local sample folder."""

    def __init__(self, sample: bool) -> None:
        self.sample = sample
        if sample:
            from ingestion.storage import LocalStore
            from processing.job import SAMPLE_OUT, SAMPLE_SETTINGS

            SAMPLE_OUT.mkdir(parents=True, exist_ok=True)
            self.store = LocalStore(SAMPLE_OUT)
            self.base_uri = SAMPLE_OUT.resolve().as_uri()
            self.option = int(SAMPLE_SETTINGS["domain"]["option"])
        else:
            from common.config import env
            from ingestion.storage import MinioStore

            self.store = MinioStore.from_env()
            self.base_uri = f"s3a://{env('MINIO_BUCKET')}"
            self.option = int(load_settings()["domain"]["option"])

    def uri(self, key: str) -> str:
        return f"{self.base_uri.rstrip('/')}/{key}"

    def latest_quality_report(self) -> dict[str, Any]:
        keys = [obj.key for obj in self.store.list_objects("quality/")]
        key = latest_report_key(keys)
        if key is None:
            raise LoadRefused("no quality report found: run processing.job first")
        LOG.info("Quality report: %s", key)
        return json.loads(self.store.get_bytes(key))

    def write_json(self, key: str, document: dict[str, Any]) -> None:
        data = json.dumps(document, indent=2, default=str).encode("utf-8")
        self.store.put_bytes(key, data, content_type="application/json")


def _spark(sample: bool) -> Any:
    if sample:
        from processing.job import _local_spark

        return _local_spark()
    from common.spark import get_spark

    return get_spark("cfpb-mongodb-load")


def _rows(df: Any) -> Iterator[dict[str, Any]]:
    for row in df.toLocalIterator(prefetchPartitions=True):
        yield row.asDict(recursive=True)


def _batches(items: Iterator[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


# --------------------------------------------------------------------------- MongoDB writes


def upsert_documents(
    db: Any,
    docs: Iterator[dict[str, Any]],
    *,
    batch_size: int = DEFAULT_BATCH,
    force_update: bool = False,
) -> dict[str, Any]:
    """Idempotent load of documents keyed by complaint_id. Returns counts and the id set."""
    from pymongo import InsertOne, UpdateOne

    coll = db[schema.COMPLAINTS]
    inserted = updated = unchanged = 0
    seen: set[str] = set()
    for batch in _batches(docs, batch_size):
        ids = [d["complaint_id"] for d in batch]
        dup = seen.intersection(ids)
        if dup:
            raise LoadRefused(f"duplicate complaint_id across batches: {sorted(dup)[:5]}")
        seen.update(ids)
        existing = {
            d["complaint_id"]: d.get("record_hash")
            for d in coll.find(
                {"complaint_id": {"$in": ids}}, {"complaint_id": 1, "record_hash": 1}
            )
        }
        split = classify(batch, existing, force_update=force_update)
        ops: list[Any] = []
        for doc in split.inserts:
            ops.append(InsertOne({**doc, "first_loaded_at": doc["loaded_at"]}))
        for doc in split.updates:
            ops.append(UpdateOne({"complaint_id": doc["complaint_id"]}, {"$set": doc}))
        if ops:
            coll.bulk_write(ops, ordered=False)
        inserted += len(split.inserts)
        updated += len(split.updates)
        unchanged += split.unchanged
        LOG.info(
            "batch of %d: +%d inserted, %d updated, %d unchanged",
            len(batch),
            len(split.inserts),
            len(split.updates),
            split.unchanged,
        )
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged, "ids": seen}


def prune_stale(db: Any, option: int, curated_ids: set[str], delete: bool) -> tuple[int, int]:
    """Documents of this option whose id is no longer curated -> (found, deleted)."""
    coll = db[schema.COMPLAINTS]
    stale = [
        d["complaint_id"]
        for d in coll.find({"option": option}, {"complaint_id": 1, "_id": 0})
        if d["complaint_id"] not in curated_ids
    ]
    deleted = 0
    if delete and stale:
        for start in range(0, len(stale), DEFAULT_BATCH):
            chunk = stale[start : start + DEFAULT_BATCH]
            deleted += coll.delete_many({"complaint_id": {"$in": chunk}}).deleted_count
    return len(stale), deleted


def replace_aggregate(db: Any, name: str, rows: list[dict[str, Any]], load_run_id: str) -> int:
    """Swap in a new aggregate collection: readers see the old or the new one, never half."""
    target = schema.aggregate_collection(name)
    tmp = f"{target}__tmp_{load_run_id}"
    db.drop_collection(tmp)
    if rows:
        db[tmp].insert_many([{**r, "load_run_id": load_run_id} for r in rows])
    else:
        db.create_collection(tmp)
    keys = schema.AGGREGATE_INDEXES.get(name)
    if keys:
        db[tmp].create_index(keys, name=f"ix_{name}")
    db[tmp].rename(target, dropTarget=True)
    return len(rows)


# --------------------------------------------------------------------------- the load


def run_load(
    source: Source,
    db: Any,
    spark: Any,
    *,
    batch_size: int = DEFAULT_BATCH,
    force_update: bool = False,
    prune: bool = True,
    with_aggregates: bool = True,
) -> dict[str, Any]:
    started = datetime.now(UTC)
    load_run_id = new_load_run_id(started)
    report = source.latest_quality_report()
    check_quality_gate(report)
    option = int(report.get("option", source.option))

    schema.ensure_complaints(db)
    curated = spark.read.parquet(source.uri(f"curated/complaints/option={option}"))
    curated_rows = curated.count()
    LOG.info("Curated rows to load: %d (Spark reported %s)", curated_rows, report.get("run_id"))

    docs = (to_document(row, load_run_id, started) for row in _rows(curated))
    counts = upsert_documents(db, docs, batch_size=batch_size, force_update=force_update)
    curated_ids: set[str] = counts.pop("ids")

    # Prune only when the read matches what Spark reported (never delete on a partial read).
    safe_to_prune = prune and curated_rows == (report.get("reconciliation") or {}).get("curated")
    stale_found, stale_deleted = prune_stale(db, option, curated_ids, delete=safe_to_prune)

    aggregates: dict[str, int] = {}
    if with_aggregates:
        for name in schema.AGGREGATE_NAMES:
            path = source.uri(f"curated/aggregates/option={option}/{name}")
            rows = [r.asDict(recursive=True) for r in spark.read.parquet(path).collect()]
            aggregates[name] = replace_aggregate(db, name, rows, load_run_id)

    mongo_documents = db[schema.COMPLAINTS].count_documents({"option": option})
    finished = datetime.now(UTC)
    reconciliation = build_reconciliation(
        load_run_id=load_run_id,
        report=report,
        curated_rows_read=curated_rows,
        distinct_ids_read=len(curated_ids),
        stale_found=stale_found,
        stale_deleted=stale_deleted,
        mongo_documents=mongo_documents,
        started_at=started,
        finished_at=finished,
        **counts,
    )
    reconciliation["aggregate_rows"] = aggregates

    freshness = _freshness(db, option)
    source.write_json(
        f"reports/reconciliation/run_id={load_run_id}/reconciliation.json", reconciliation
    )
    db[schema.PIPELINE_RUNS].insert_one(
        {
            **reconciliation,
            "started_at": started,
            "finished_at": finished,
            "freshness": freshness,
            "quality_dimensions": {
                dim: [c.get("status") for c in checks]
                for dim, checks in (report.get("dimensions") or {}).items()
            },
            "quality_warned_checks": (report.get("gate") or {}).get("warned_checks"),
        }
    )
    reconciliation["freshness"] = freshness
    return reconciliation


def _freshness(db: Any, option: int) -> dict[str, Any]:
    pipeline = [
        {"$match": {"option": option}},
        {
            "$group": {
                "_id": None,
                "latest_date_received": {"$max": "$date_received"},
                "latest_ingested_at": {"$max": "$ingested_at"},
            }
        },
    ]
    row = next(iter(db[schema.COMPLAINTS].aggregate(pipeline)), {})
    return {
        "latest_date_received": row.get("latest_date_received"),
        "latest_ingested_at": row.get("latest_ingested_at"),
        "serving_refreshed_at": datetime.now(UTC),
    }


def _print(rec: dict[str, Any]) -> None:
    m = rec["mongodb"]
    print(f"\nLoad {rec['load_run_id']}  option={rec['option']}  quality={rec['quality_gate']}")
    print(f"  raw rows (Spark input)    : {rec['raw']['raw_rows']}")
    print(f"  curated rows (Spark)      : {rec['curated']['reported_by_spark']}")
    print(f"  curated rows read         : {rec['curated']['read_by_loader']}")
    print(f"  inserted/updated/unchanged: {m['inserted']} / {m['updated']} / {m['unchanged']}")
    print(f"  stale found/deleted       : {m['stale_found']} / {m['stale_deleted']}")
    print(f"  MongoDB documents         : {m['documents_after_load']}")
    for name, ok in rec["checks"].items():
        print(f"  [{'OK' if ok else 'FAIL'}] {name}")
    print(f"  BALANCED: {rec['balanced']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sample", action="store_true", help="load data/processing_out (local)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    parser.add_argument("--force-update", action="store_true", help="rewrite every document")
    parser.add_argument("--no-prune", action="store_true", help="keep documents not in curated")
    parser.add_argument("--skip-aggregates", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    try:
        source = Source(sample=args.sample)
        from database.mongo import get_db

        db = get_db()
    except (ConfigError, KeyError, ValueError) as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 2

    spark = _spark(args.sample)
    try:
        rec = run_load(
            source,
            db,
            spark,
            batch_size=args.batch_size,
            force_update=args.force_update,
            prune=not args.no_prune,
            with_aggregates=not args.skip_aggregates,
        )
    except LoadRefused as exc:
        print(f"LOAD REFUSED: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    _print(rec)
    return 0 if rec["balanced"] else 1


if __name__ == "__main__":
    sys.exit(main())
