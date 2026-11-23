# Turn 4 report - MongoDB serving layer and integration

Author: Mhamad Naser Dine. Branch `feat/turn4-mongodb` (PR #20). Option 15.
Builds on Turn 2 (PR #18) and Turn 3 (PR #19). Evidence: `docs/evidence/turn4_real_run.md`.

## 1. What this turn delivers

| Brief V2 6.4 requirement | Where | Evidence |
|---|---|---|
| One curated complaint collection + at least one aggregate collection | `complaints` + six `agg_*` + `pipeline_runs` | real load: 706,024 documents |
| Complaint ID as business key, uniqueness enforced | unique index `ux_complaint_id` | CI test: duplicate insert rejected |
| Schema and data types documented, date type chosen | `$jsonSchema` validator, BSON Date | `docs/mongodb.md` section 3; CI test rejects string dates |
| Bulk loading / upserts, safe reruns | `database/load.py`, `record_hash` classification | replay: 0 / 0 / 706,024 |
| Aggregates refreshed when inputs change | atomic swap (temp collection + rename) | CI test |
| Indexes for the dashboard's actual filters | 6 compound ESR indexes | `docs/mongodb.md` section 4 |
| At least five meaningful aggregation queries | `database/queries.py`, Q1-Q8 | section 6 of the evidence |
| Before/after index comparison, same data and conditions | `database/benchmark.py` | `docs/evidence/index_benchmark.md` |
| Raw -> curated -> MongoDB reconciliation | `reports/reconciliation/run_id=<id>/reconciliation.json` | 4/4 checks, balanced |
| Load refused when quality fails | `check_quality_gate` | unit tests; demo with `--inject-defect` |

Tests: 33 new unit tests, 8 MongoDB integration tests and 1 end-to-end test (sample raw -> Spark ->
MongoDB, loaded twice) in the new CI job `mongodb-e2e` against a real MongoDB 7.

## 2. Design decisions (D-019 to D-023, `docs/architecture.md`)

* **pymongo bulk writes, not the MongoDB Spark connector**: no Docker image rebuild on our slow
  network, and exact insert / update / unchanged counts.
* **Flat embedded documents, BSON Date** after the Session 4.1 embed-vs-reference check.
* **Compound indexes in Equality-Sort-Range order** (`segment`, category, `date_received`).
* **The loader is a guard**: it refuses a failed quality gate, and it deletes ids that left curated
  only after a full read that matches Spark's count.
* **Aggregates swapped atomically**, so the dashboard never reads a half-written collection.

## 3. Results on real data (23 Nov 2026)

19,378,282 raw rows -> 706,024 curated (option 15) -> 706,024 MongoDB documents; the replay changed
nothing. Every benchmark query moved from COLLSCAN to IXSCAN; speed-ups from 2.1x (broad multi-year
views reading 16 % of the documents) to 23.7x (state filter reading 1 %).

## 4. Limitations

* Single MongoDB node: no replica set or sharding (discussed in `docs/mongodb.md` section 7).
* The load reads all curated rows each time (about 2 minutes); fine at 706k, would need incremental
  partitions at larger scale.
* The benchmark measures one laptop with warm caches; absolute times vary, the scan counts do not.
* No API batch was processed yet at the time of the real run; the first scheduled runs are now
  collecting and enter MongoDB through the same `processing.job` -> `database.load` path.

## 5. Hand-off to Turn 5 (Maroun, analysis) and Turn 6 (Ali, dashboard)

Start from `main` after PR #20 is merged. Rebuild the data on your laptop:

```
docker compose up -d
docker tag cfpb-storage-init cfpb-ingestion        # avoids a 1.5 h rebuild; never use --build
docker compose run --rm ingestion python -m ingestion.historical
docker compose run --rm spark python -m processing.job
docker compose run --rm spark python -m database.load
```

Read MongoDB only through `database.queries` (or the `agg_*` collections). Contract:
`docs/data_contracts.md` section 8.

```python
from database.mongo import get_db
from database import queries

db = get_db()
f = {"segment": ["deposit_accounts", "transfers_payments"], "date_from": "2024-01-01"}
rows = queries.run(db, queries.issue_mix(f))          # counts + share_pct within segment
timely = queries.run(db, queries.timely_rate(f))      # Yes / (Yes + No), missing separate
freshness = db.pipeline_runs.find_one(sort=[("finished_at", -1)])["freshness"]
```

Rules to keep: show counts and shares within each segment; flag `below_min_cell`; never average
monthly percentages; the routing interval is CFPB referral time, not resolution time; company shares
are not quality rankings.
