# Banking Complaints Data Platform

Course project for **020IABDM2 Big Data Frameworks** (USJ / ESIB, coordinator Dr. Yehia Taher).

A reproducible batch platform that collects public CFPB consumer-complaint data
(historical CSV + scheduled incremental API), preserves it in a raw zone, cleans and
validates it with Spark, serves it from MongoDB and presents it in a Streamlit dashboard.

> Complaint counts reflect complaints submitted to and published by the CFPB. They do not
> measure customers, market share, harm or service quality, and they are never a company ranking.

## Team

| Member | Turns | Ownership |
|---|---|---|
| Mhamad Naser Dine (lead) | 1, 4 | Architecture and foundation; MongoDB serving layer and pipeline integration |
| Maroun Mashaalany | 2, 5 | Historical and incremental acquisition, raw layer; analysis |
| Ali Srour | 3, 6 | Spark processing and data quality; dashboard |

Domain: **to be assigned** (configured in `config/settings.yaml`, never hard-coded).

## Architecture

`CFPB CSV ZIP / CFPB API` → Python collector → **MinIO** raw zone → **Spark** (validate, clean,
deduplicate) → curated Parquet → **MongoDB** (curated + aggregates) → **Streamlit**.
Full diagram and justification: `docs/architecture.md` (in progress).

## Repository structure

```
ingestion/    historical and incremental collectors
processing/   Spark jobs and data-quality rules
database/     MongoDB load scripts, indexes and queries
analytics/    reproducible analysis notebooks or scripts
dashboard/    Streamlit application
common/       shared configuration and helpers
tests/        unit, integration and end-to-end tests
config/       non-secret configuration and examples
docs/         architecture, runbook, data dictionary, contribution logs
scripts/      operational helper scripts
docker-compose.yml   local service orchestration (in progress)
```

## Quick start

Prerequisites: Docker Desktop (Compose v2+), Git, about 6 GB free RAM for Docker.

```bash
cp .env.example .env              # Windows PowerShell: Copy-Item .env.example .env
docker compose up -d --build      # first build downloads Java, PySpark and S3A jars (several minutes)
docker compose ps                 # minio, mongodb, dashboard running; storage-init and spark exited (0)
docker compose logs spark         # expect: "Smoke test PASSED"
```

| Service | URL | Purpose |
|---|---|---|
| MinIO console | http://localhost:9001 | Raw, curated and quality zones (log in with `.env` credentials) |
| Streamlit | http://localhost:8501 | Dashboard |
| MongoDB | `mongodb://localhost:27017` | Serving layer (MongoDB Compass) |
| Spark UI | http://localhost:4040 | Only while a Spark job runs |

```bash
docker compose run --rm spark python -m processing.smoke_test   # rerun the Spark <-> MinIO check
docker compose down               # stop (data kept)
docker compose down -v            # reset: stop and DELETE all local data
python -m pip install -r requirements-dev.txt && python -m pytest   # unit tests
```

## Data acquisition (Turn 2)

Collectors run inside the Compose network and read credentials from `.env`. Start the stack first
(`docker compose up -d --build`). Local scratch space: about 1.3x the size of the CSV plus the ZIP.

```
# 0. Cheap look at the source: ZIP size, real CSV header and date formats (reads ~256 KB)
docker compose run --rm ingestion python -m ingestion.historical --inspect

# 1. Historical load: ZIP + extracted CSV + manifest.json into MinIO (safe to rerun; skips if unchanged)
docker compose run --rm ingestion python -m ingestion.historical
docker compose run --rm ingestion python -m ingestion.historical --dry-run   # plan only

# 2. Incremental collection (needs domain.products in config/settings.yaml, or --product "<label>")
docker compose run --rm ingestion python -m ingestion.incremental --dry-run --product "<label>"
docker compose run --rm ingestion python -m ingestion.incremental

# 3. Bring runs collected by the scheduled GitHub workflow into MinIO (needs GITHUB_TOKEN)
docker compose run --rm -e GITHUB_TOKEN ingestion python -m ingestion.sync_artifacts

# 4. List completed batches (what Spark may read)
docker compose run --rm ingestion python -m ingestion.discovery
```

Details, troubleshooting and the Spark hand-off contract: `docs/MAROUN_TURN2_REPORT.md`.

## Spark processing and data quality (Turn 3)

Option 15 (checking and savings accounts + money transfers and digital payments). The job reads
every complete raw batch, cleans and filters it, removes duplicate Complaint IDs, checks six quality
dimensions against `config/quality_rules.yaml` (tag `dq-thresholds-v1`) and writes curated Parquet
only if the quality gate does not fail.

```
# Product labels in the raw data (historical label timeline)
docker compose run --rm spark python -m processing.job --list-labels

# Measure everything, write nothing
docker compose run --rm spark python -m processing.job --dry-run

# Full run: curated/, aggregates, quarantine/ and quality/ in MinIO
docker compose run --rm spark python -m processing.job

# Quality-gate demonstration: inject N duplicate records; the gate must FAIL and write nothing
docker compose run --rm spark python -m processing.job --inject-defect 100

# Development without MinIO (committed sample, writes to data/processing_out/)
docker compose run --rm spark python -m processing.job --sample

# Spark tests (also run in CI)
docker compose run --rm spark sh -c "pip install -q pytest==8.3.4 && python -m pytest -q tests/unit/test_processing_rules.py tests/unit/test_processing_spark.py"
```

Results of the first real run, decisions and the hand-off to Turn 4: `docs/ALI_TURN3_REPORT.md`.

## MongoDB serving layer (Turn 4)

Curated complaints and the six Spark aggregates are loaded into MongoDB (database `cfpb`). The
loader refuses a failed quality gate, upserts by `complaint_id` (a replay changes nothing) and
writes a raw -> curated -> MongoDB reconciliation report. Design: `docs/mongodb.md`.

```
# Load (after processing.job): complaints + agg_* collections + reconciliation report
docker compose run --rm spark python -m database.load

# Replay the same data: expect inserted 0 / updated 0, same document count (demo step 5)
docker compose run --rm spark python -m database.load

# Index evidence: explain() and timings before/after (writes docs/evidence/index_benchmark.md)
docker compose run --rm spark python -m database.benchmark

# Print the query catalogue (Q1-Q8); add --show-pipeline to see the stages
docker compose run --rm spark python -m database.queries --segment deposit_accounts --date-from 2024-01-01

# Sample end to end without MinIO (also in CI with a real MongoDB)
docker compose run --rm spark python -m processing.job --sample
docker compose run --rm spark python -m database.load --sample
```

Reset only the serving layer: `docker compose exec mongodb mongosh -u <user> -p <password> --eval "db.getSiblingDB('cfpb').dropDatabase()"`, then load again.

## Status

| Layer | Owner | Status |
|---|---|---|
| Foundation | Mhamad | Done (Turn 1) |
| Acquisition + raw | Maroun | Done (Turn 2, PR #18) |
| Spark + data quality | Ali | Done (Turn 3, PR #19) |
| MongoDB + integration | Mhamad | In progress (Turn 4) |
| Analysis | Maroun | Not started |
| Dashboard | Ali | Not started |

## Data source

CFPB Consumer Complaint Database (public). See `docs/` for the data dictionary,
limitations and usage basis.
