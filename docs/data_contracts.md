# Data contracts

Status: v0.1 (Turn 1). Every layer must follow these contracts. Changes need a PR reviewed by the
owners of the affected layers. Machine-readable versions: `config/schemas/*.schema.json`
(checked by `tests/unit/test_contracts.py`).

## 1. MinIO layout (bucket `datalake`)

| Prefix | Content | Written by | Mutability |
|---|---|---|---|
| `raw/cfpb/full_load/ingest_date=YYYY-MM-DD/run_id=<id>/complaints.csv.zip` | Original archive, unmodified | Historical loader | Write-once |
| `raw/cfpb/full_load/ingest_date=YYYY-MM-DD/run_id=<id>/extracted/complaints.csv` | Extracted copy (ZIP is not splittable, Spark reads this) | Historical loader | Write-once |
| `raw/cfpb/full_load/ingest_date=YYYY-MM-DD/run_id=<id>/manifest.json` | Manifest (section 3) | Historical loader | Write-once |
| `raw/cfpb/api/ingest_date=YYYY-MM-DD/run_id=<id>/response_NNNN.json` | One raw API page per file, unmodified, `NNNN` from 0001 | Incremental collector | Write-once |
| `raw/cfpb/api/ingest_date=YYYY-MM-DD/run_id=<id>/run_metadata.json` | Run metadata (section 4) | Incremental collector | Write-once |
| `state/high_water_mark.json` | Current high-water mark for the next run | Incremental collector | Overwritten only after a successful run |
| `curated/complaints/domain=<slug>/year=YYYY/month=MM/` | Curated records, Snappy Parquet | Spark | Rewritten per load |
| `quarantine/run_id=<id>/` | Rejected records with a `reject_reason` column | Spark | Write-once |
| `quality/run_id=<id>/quality_report.json` | Quality results per run | Spark | Write-once |
| `reports/reconciliation/run_id=<id>/reconciliation.json` | Raw -> curated -> MongoDB counts | Loader | Write-once |

Rules: raw objects are never edited or deleted; a rerun gets a new `run_id`.
Dates in paths are UTC.

## 2. Run identifiers

`run_id = <source>_<UTC timestamp>_<6 hex chars>`, e.g. `api_20261113T031700Z_a1b2c3`,
`full_20261113T090000Z_9f8e7d`. `source` is `api` or `full`. The same `run_id` is stored on every
curated record (`source_run_id`) and every MongoDB document, for lineage.

## 3. Historical load manifest (`manifest.json`)

| Field | Type | Meaning |
|---|---|---|
| `run_id` | string | Run identifier |
| `source_url` | string | URL of the CSV ZIP |
| `download_started_at`, `download_finished_at` | ISO 8601 UTC | Download timing |
| `archive_path` | string | MinIO path of the ZIP |
| `archive_bytes` | integer | Size of the ZIP |
| `archive_sha256` | string | Checksum of the ZIP |
| `extracted_path` | string | MinIO path of the CSV |
| `extracted_bytes` | integer | Size of the CSV |
| `extracted_sha256` | string | Checksum of the CSV |
| `record_count` | integer | Data rows in the CSV (header excluded) |
| `header` | array of strings | CSV column names as found |
| `code_version` | string | Git commit of the loader |

## 4. Incremental run metadata (`run_metadata.json`)

Covers every item of brief section 5.2.

| Field | Type | Meaning |
|---|---|---|
| `run_id` | string | Run identifier |
| `trigger` | `schedule` / `manual` / `test` | What started the run |
| `scheduled_for` | ISO 8601 or null | Planned time (schedule only), to measure delay |
| `started_at`, `finished_at` | ISO 8601 UTC | Run timing |
| `endpoint` | string | API base URL |
| `request_params` | object | Non-secret parameters (dates, size, sort, product filter) |
| `window.date_received_min`, `window.date_received_max` | date | Requested window |
| `window.lookback_days` | integer | Overlap with the previous run |
| `pagination.page_size` | integer | Records per page |
| `pagination.pages_fetched` | integer | Pages saved |
| `pagination.last_search_after` | string or null | Last pagination key used |
| `http.final_status` | integer or null | Last HTTP status |
| `http.retry_count` | integer | Total retries across pages |
| `outcome` | `success` / `partial` / `failed` | Final result |
| `raw_objects` | array | One entry per page: `path`, `bytes`, `sha256`, `records` |
| `record_count` | integer | Records saved in this run |
| `high_water_mark.previous` / `.next` | object | `date_received` (date) and `complaint_id` (string) |
| `source_meta` | object | API `_meta`: `last_updated`, `last_indexed`, `total_record_count`, `is_data_stale`, `has_data_issue` |
| `code_version` | string | Git commit of the collector |

The high-water mark advances **only** when `outcome = success`.

## 5. Definition of an incremental run (cohort standard)

One execution of the collector that:
1. requests `date_received` in `[high_water_mark.date_received - lookback_days, today]`,
   filtered server-side to the domain products in `config/settings.yaml`;
2. saves every page as raw JSON in MinIO **before** any transformation;
3. writes `run_metadata.json`;
4. advances the high-water mark only on success.

Overlap between runs is intended: complaints are published with a delay, so recent dates keep
receiving new records. Duplicates are removed downstream by `complaint_id`.
