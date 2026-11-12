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

## Status

| Layer | Owner | Status |
|---|---|---|
| Foundation | Mhamad | In progress |
| Acquisition + raw | Maroun | Not started |
| Spark + data quality | Ali | Not started |
| MongoDB + integration | Mhamad | Not started |
| Analysis | Maroun | Not started |
| Dashboard | Ali | Not started |

## Data source

CFPB Consumer Complaint Database (public). See `docs/` for the data dictionary,
limitations and usage basis.
