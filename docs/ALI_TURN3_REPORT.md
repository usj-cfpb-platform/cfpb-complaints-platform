# Turn 3 report - Spark processing and data quality

Author: Ali Srour. Branch: `feat/turn3-spark-processing`. Builds on Turn 2 (PR #18, merged).
Option: **15 - Access to deposited and transferred funds** (checking and savings accounts + money
transfers and digital payments).

## 1. What this turn delivers

| Brief 6.3 requirement | Where |
|---|---|
| Read only complete raw batches | `processing/readers.py` uses `ingestion.discovery` (Turn 2 completion rule) |
| Explicit schema, CSV quoting and multi-line fields | `processing/schema.py`, `read_csv_batch` (`multiLine`, `escape='"'`, `_corrupt_record`) |
| Standardise names and types, parse dates, flag invalid dates | `processing/transform.py` `standardise`, `parse` |
| Yes/No, missing values, empty strings | `parse` (`timely`, `consumer_disputed`, empty -> null) |
| Product, sub-product and issue filters for the option | `Scope` + `in_scope`, labels in `config/settings.yaml` |
| Deduplicate by Complaint ID with a documented rule | `rank_versions` (D-013) |
| Derived fields (year, month, routing lag, lineage) | `derive` |
| Quarantine rejected and suspicious records | `quarantine/run_id=<id>/` with `reject_reason`; `quality_flags` on kept records |
| Curated Parquet, partitioned | `curated/complaints/option=15/year=/month=` |
| Dashboard-ready aggregates | `processing/aggregates.py`, `curated/aggregates/option=15/<name>/` |
| Quality report, six dimensions, pre-registered thresholds | `processing/quality.py`, `config/quality_rules.yaml`, tag `dq-thresholds-v1` |
| Raw-to-curated reconciliation | `reconciliation` section of every quality report (exact equation) |
| A deliberate defect caught by the gate | `--inject-defect N` (section 6) |
| Tests: invalid dates, missing values, filters, historical labels, duplicates | `tests/unit/test_processing_spark.py`, `tests/unit/test_processing_rules.py` (29 tests) |

## 2. How to run

```
docker compose run --rm spark python -m processing.job --list-labels   # product labels found
docker compose run --rm spark python -m processing.job --dry-run       # measure, write nothing
docker compose run --rm spark python -m processing.job                 # full run
docker compose run --rm spark python -m processing.job --inject-defect 100   # gate demo
docker compose run --rm spark python -m processing.job --sample        # no MinIO needed
```

Exit code 0 = pass or warn, 1 = quality gate failed, 2 = configuration or nothing to process.

## 3. Design

**Order of steps.** Every raw row ends in exactly one bucket, so the run can prove that nothing
was lost: identity rejects -> out of scope -> receipt-date rejects -> duplicates -> curated.

**Columns by name, all strings first (D-014).** The live CSV has 15 columns, older files and the
committed sample 17. The schema is built from the header recorded in the manifest, unknown or
missing required columns stop the job, and every value is read as text. Typing happens afterwards,
so an impossible date such as `2026-13-45` is *detected and reported* instead of silently becoming
null at read time.

**Scope with historical labels (D-012).** CFPB renamed its products in April 2017. The option's
scope lists the old and the new labels, and the added `segment` column groups them for the focus
question (deposit accounts vs transfers) while the original `product` label is never changed.

**Deduplication (D-013).** Newest `ingested_at` wins; on a tie `api` beats `full_load`; then the
highest `record_hash`. Superseded versions whose content differs are written to quarantine as
`duplicate_conflict_superseded`, so changed complaints stay visible.

**Kept but flagged (D-017).** A complaint with an invalid state code or a negative routing lag is
still a real complaint, so it is kept with a `quality_flags` entry; the bad interval is excluded
from lag statistics.

**Laptop-sized (D-015).** The 19.4M raw rows are never cached: one aggregate pass gives every
raw-level count (rows per batch, reject reasons, label timeline), then only the in-scope rows
(about 0.7M) are kept in memory. The full run takes about 3 minutes on a 16 GB laptop.

**Quality gate (D-016).** Thresholds were committed and tagged `dq-thresholds-v1` *before* the
first real run. Any FAIL keeps the previous curated layer in place and writes only quarantine and
the quality report; a check that could not be measured counts as FAIL.

## 4. First real run

Input: historical load `full_20261122T132122Z_9c8cbf` (19,371,701 records, loaded by me from an
empty environment using only the README, which also closes the "second member repeats the load"
item of PR #18). Run `spark_20261122T170908Z_8b65fe`, code version `6ed3f73`, rules version 1.

| Reconciliation | Rows |
|---|---|
| raw rows read (= manifest `record_count`) | 19,371,701 |
| rejected (identity) | 0 |
| filtered out (other options) | 18,665,722 |
| rejected (receipt date) | 0 |
| duplicates removed | 0 |
| **curated (option 15)** | **705,979** |
| unbalanced | 0 |

Segments: `deposit_accounts` 501,221, `transfers_payments` 204,758.

| Dimension | Check | Value | Status |
|---|---|---|---|
| Uniqueness | duplicate / missing Complaint IDs in curated | 0 / 0 | pass |
| Completeness | highest missing rate among required fields | 0.0007 % (5 missing `issue`) | **warn** |
| Completeness | state / ZIP / company response missing | 2.93 % / 0.18 % / 0 % | pass |
| Validity | invalid receipt dates, invalid IDs, malformed rows | 0 / 0 / 0 | pass |
| Validity | invalid state codes | 0.011 % (75) | pass |
| Validity | negative routing lag | 0.068 % (480) | **warn** |
| Consistency | curated products without a segment | 0 | pass |
| Freshness | days since latest receipt date / latest ingestion | 2 / 0 | pass |
| Reconciliation | unbalanced rows / Spark rows vs manifest | 0 / 0 | pass |

**Gate: WARN** (passed with two warnings); curated data and aggregates written.

Fields reported but not judged: `sub_issue` missing 41 % and `company_public_response` 63 %
(not always applicable), `tags` 85 % (only some consumers have a tag), `consumer_consent_provided`
and `consumer_disputed` 100 % (no longer published by CFPB).

## 5. Findings about the data

1. **No duplicate Complaint IDs and no malformed rows** in the historical file: Spark read exactly
   the manifest's record count with `multiLine` enabled.
2. **480 complaints were "sent to the company" one day before CFPB "received" them**, all in
   2013 (e.g. complaint 386255: received 2013-04-19, sent 2013-04-18). Likely a time-zone or
   data-entry effect in early records; kept, flagged, excluded from lag statistics.
3. **All 75 invalid state values are the same text**: `UNITED STATES MINOR OUTLYING ISLANDS`,
   written in full instead of the code `UM`. Kept and flagged; the dashboard groups them as
   `Invalid`.
4. **The April 2017 product renaming is visible in the data**: old labels stop on 21-22 April
   2017, new ones start on 21-24 April 2017 (table in `docs/data_dictionary.md`).
5. Largest combinations (top of `top_combinations`): checking account deposits and withdrawals
   (71,212), domestic money transfer "other transaction problem" (60,503), debit/ATM card problems
   (48,421).

## 6. Quality-gate demonstration

`--inject-defect N` simulates a broken deduplication step by appending N copies of curated
records. The gate must fail on two independent checks (duplicate Complaint IDs and the
reconciliation equation), write nothing to `curated/`, and still write the quality report.
Covered by `test_injected_duplicate_defect_is_caught_and_nothing_is_written`.

Live run on the real data (run `spark_20261122T172858Z_9a2657`, `--inject-defect 100`):

| Check | Value | Status |
|---|---|---|
| `uniqueness.curated_duplicate_ids` | 100 | **fail** |
| `reconciliation.unbalanced_rows` | 100 (curated 706,079 vs 705,979 explained by the raw data) | **fail** |
| all other checks | same as the real run | pass / warn |

**Gate: FAIL** - `Outputs written: False`, exit code 1. The curated layer of run
`spark_20261122T170908Z_8b65fe` stayed in place; the failed run's quality report and quarantine
were still written under `quality/` and `quarantine/run_id=spark_20261122T172858Z_9a2657/`.
## 7. Limits and follow-ups

* No API run has been processed yet: the scheduled collector starts once `domain.products` is on
  `main` (this PR). The API path is covered by tests with sample pages.
* The CSV is read with `multiLine`, which Spark cannot split: reading uses one core (about 1 minute
  on the test laptop). Acceptable for a daily batch.
* If the same Complaint ID appears twice in the *same* batch with different content, the tie is
  broken by `record_hash` (deterministic but arbitrary); none in the real data.

## 8. Hand-off to Turn 4 (MongoDB, Mhamad)

* Read `s3a://datalake/curated/complaints/option=15/` (Parquet, 705,979 rows, one per
  `complaint_id`; columns in `docs/data_dictionary.md`). Upsert by `complaint_id`; `record_hash`
  tells you whether a document changed.
* Aggregates for the dashboard: `s3a://datalake/curated/aggregates/option=15/<name>/` (six tables,
  columns in `docs/data_contracts.md` section 7). Use `below_min_cell` for small-cell suppression.
* Latest quality report: newest `quality/run_id=*/quality_report.json`. Its
  `reconciliation.curated` is the number MongoDB must match for the raw -> curated -> MongoDB
  reconciliation.
* Only write MongoDB after a run whose `gate.status` is not `fail` (`outputs_written: true`).
