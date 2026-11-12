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

> Work in progress: Docker Compose services are added in the next PR.

```bash
cp .env.example .env          # then edit the placeholders
python -m pip install -r requirements-dev.txt
python -m pytest
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
