# Data contracts

Status: v0.3 (Turn 3: Spark outputs, section 7). Every layer must follow these contracts. Changes need a PR reviewed by the
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
| `curated/complaints/option=<n>/year=YYYY/month=MM/` | Curated records, Snappy Parquet (`option` = brief option number, 15 for us; path of brief V2) | Spark | Rewritten per run |
| `curated/aggregates/option=<n>/<name>/` | Dashboard aggregates, Snappy Parquet (section 7) | Spark | Rewritten per run |
| `quarantine/run_id=<id>/` | Rejected records with a `reject_reason` column | Spark | Write-once |
| `quality/run_id=<id>/quality_report.json` | Quality results per run | Spark | Write-once |
| `reports/reconciliation/run_id=<id>/reconciliation.json` | Raw -> curated -> MongoDB counts | Loader | Write-once |

Rules: raw objects are never edited or deleted; a rerun gets a new `run_id`.
Dates in paths are UTC.

## 2. Run identifiers

`run_id = <source>_<UTC timestamp>_<6 hex chars>`, e.g. `api_20261113T031700Z_a1b2c3`,
`full_20261113T090000Z_9f8e7d`. `source` is `api` or `full`; Spark processing runs use `spark`
(`spark_20261122T170908Z_8b65fe`) for `quarantine/` and `quality/`. The same `run_id` is stored on every
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

## 6. Completed batches and additive fields (Turn 2)

**Completion rule (for Spark and every other reader).** Process a raw batch only if it is complete:

| Source | Complete when |
|---|---|
| Historical load | `manifest.json` exists and is valid, and the ZIP and CSV it names exist with the recorded sizes |
| API run | `run_metadata.json` exists and is valid, `outcome == "success"`, and every page in `raw_objects` exists with the recorded size |

The marker (`manifest.json` / `run_metadata.json`) is always written **last**, so a batch without it
(crash, outage) is incomplete and must be ignored. `partial` and `failed` runs keep their metadata
for provenance but are not complete. The reference implementation is `ingestion/discovery.py`
(`python -m ingestion.discovery --json`); Spark jobs should call it, or apply the same rule.

**Optional fields written by the collectors.** The schemas allow extra properties; these were added
without changing any required field:

* `manifest.json`: `source_etag`, `source_last_modified`, `source_content_length` (used to skip a
  reload when the CFPB file is unchanged), `http_retry_count`, `download_resumed`.
* `run_metadata.json`: `pagination.cursor_sources` (how each `search_after` cursor was obtained),
  `completeness` (`expected_total`, `expected_total_is_exact`, `unique_records_fetched`: the
  collector compares unique records fetched with the API's own total and marks the run `partial`
  if records are missing), and `error` (`type`, `message`) when `outcome != "success"`.

**API limits that shape the collector** (CFPB OpenAPI spec, checked 2026-11-14): `size` is at most
100; `date_received_max` is exclusive (the collector sends `max + 1 day`); `frm` is capped at 100000,
so deep paging uses `search_after`; the API filters on `date_received` and `date_sent_to_company`
but not on publication or update date, hence the lookback window. Authentication: none. Rate limits:
not documented.

## 7. Spark processing outputs (Turn 3)

Job: `python -m processing.job` (`processing/`). Inputs: the **newest** complete historical load
plus **every** complete API run (section 6). Each run reprocesses all of them and rewrites the
curated layer, so a replay of the same input never inflates counts.

**Steps and counts.** Every raw row ends in exactly one bucket, and the run checks the equation
`raw_rows = rejected_identity + filtered_out_of_scope + rejected_date + duplicates_removed
+ filtered_out_after_dedup + curated` (`reconciliation.unbalanced_rows` must be 0).

| Step | Rows that leave | Where they go |
|---|---|---|
| Identity | malformed CSV row, missing/non-numeric `complaint_id`, missing `product` | `quarantine/` (`reject_reason`) |
| Scope | product not in `domain.products` (and optional `sub_products` / `issues`) | counted only |
| Receipt date | missing or invalid `date_received` (impossible date, before 2011-07-01, in the future) | `quarantine/` |
| Deduplication | older versions of a Complaint ID | counted; versions with different content also in `quarantine/` as `duplicate_conflict_superseded` |

**Deduplication rule (decided, D-013).** Keep the version with the most recent `ingested_at`; on a
tie prefer `api` over `full_load`; on a remaining tie keep the highest `record_hash`.

**Quality gate.** Thresholds in `config/quality_rules.yaml` (version 1, git tag `dq-thresholds-v1`).
FAIL on any check: quarantine and quality report are written, curated data and aggregates are
**not** (the previous curated layer stays in place). WARN: reported, the run continues.
`quality_report.json` holds: `gate`, `dimensions` (every check with value, thresholds, status),
`reconciliation`, `reject_reasons`, `label_timeline`, `completeness_by_field`, `freshness`,
`top_combinations`, `invalid_examples`, `inputs`, `code_version`, `rules_version`.

**Aggregates** (`curated/aggregates/option=<n>/<name>/`; every row has `option`, `segment`,
`complaints` = distinct Complaint IDs, `below_min_cell` = `complaints < privacy.min_cell_size`):

| Name | Grain | Extra columns |
|---|---|---|
| `monthly_counts` | segment, product, year, month, year_month | |
| `issue_breakdown` | segment, product, issue, sub_issue | |
| `geography_channel` | segment, state (`Missing` / `Invalid` when not a valid code), submitted_via | |
| `company_response` | segment, year, company_response | |
| `timely_response` | segment, year_month | `timely_yes`, `timely_no`, `timely_missing_or_invalid`, `timely_pct` = Yes / (Yes + No) x 100 |
| `routing_lag` | segment, year_month | `lag_valid`, `lag_negative`, `lag_missing`, `lag_invalid`, `lag_mean_days`, `lag_median_days`, `lag_p90_days` (valid intervals only) |

## 8. MongoDB serving layer (Turn 4) - contract for analysis and dashboard

Database `MONGO_DB` (default `cfpb`). Design and rationale: `docs/mongodb.md`.

| Collection | Key / grain | Notes for readers |
|---|---|---|
| `complaints` | one document per `complaint_id` (unique index) | all curated fields; dates are BSON `Date` (UTC) |
| `agg_monthly_counts` | segment, product, year, month, year_month | from Spark, replaced each load |
| `agg_issue_breakdown` | segment, product, issue, sub_issue | |
| `agg_geography_channel` | segment, state, submitted_via | `state` is `Missing` / `Invalid` when not a valid code |
| `agg_company_response` | segment, year, company_response | |
| `agg_timely_response` | segment, year_month | multi-month timely % must be recomputed from summed `timely_yes` / `timely_no`, never averaged |
| `agg_routing_lag` | segment, year_month | routing time (CFPB receipt to referral), not company resolution time |
| `pipeline_runs` | one document per load, newest `finished_at` first | `freshness.latest_date_received`, `freshness.serving_refreshed_at`, `balanced`, `quality_gate`, counts |

Rules for Turns 5 and 6:
* Use `database.queries` (filters dict: `segment`, `date_from`, `date_to`, `product`, `issue`,
  `state`, `company`, `channel`) for filtered views; the `agg_*` collections for unfiltered overviews.
* Never read MinIO or raw data from the dashboard (brief 5).
* Show counts **and** shares within segment; hide or grey out rows with `below_min_cell = true`.
* Freshness indicator: newest `pipeline_runs` document.
