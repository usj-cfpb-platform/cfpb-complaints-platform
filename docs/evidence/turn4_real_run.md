# Turn 4 evidence: real data, 2026-11-23 (Mhamad's laptop, Docker Compose)

All commands run with `docker compose run --rm ...` on a fresh MinIO and MongoDB.

## 1. Historical load (Turn 2 code)

| Item | Value |
|---|---|
| Run | `full_20261123T092410Z_e778e4` |
| Source | `https://files.consumerfinance.gov/ccdb/complaints.csv.zip` (Last-Modified Mon, 23 Nov 2026 10:18:03 GMT) |
| Archive | 370,848,637 bytes, SHA-256 `c4140912f12012f2e8b90b3c86bcf5676d39304256ed1a4e5f4b3818ca7e8025` |
| Records | 19,378,282 |
| Duration | about 30 min (09:24 to 09:54 UTC) on a slow connection |
| Manifest | `raw/cfpb/full_load/ingest_date=2026-11-23/run_id=full_20261123T092410Z_e778e4/manifest.json` |
| Discovery | `COMPLETE full ... records=19378282`, 1 complete, 0 incomplete |

## 2. Spark processing (Turn 3 code), run `spark_20261123T100218Z_abfd01`

| Stage | Rows |
|---|---|
| raw_rows | 19,378,282 |
| rejected_identity | 0 |
| filtered_out_of_scope | 18,672,258 |
| rejected_date | 0 |
| duplicates_removed | 0 |
| curated | **706,024** |
| unbalanced_rows | 0 |

Quality gate **WARN** (thresholds tag `dq-thresholds-v1`): `completeness.required_missing_rate = 7e-06`
(a handful of complaints without `issue`) and `validity.negative_routing_lag_rate = 0.00068`
(decision D-017). All 16 other checks PASS, including freshness (0 days) and reconciliation.
Duration about 5 min.

## 3. MongoDB load (Turn 4), run `load_20261123T100933Z_5d0299`

```
Load load_20261123T100933Z_5d0299  option=15  quality=warn
  raw rows (Spark input)    : 19378282
  curated rows (Spark)      : 706024
  curated rows read         : 706024
  inserted/updated/unchanged: 706024 / 0 / 0
  stale found/deleted       : 0 / 0
  MongoDB documents         : 706024
  [OK] curated_read_equals_reported
  [OK] curated_ids_unique
  [OK] loaded_operations_balance
  [OK] mongo_equals_curated
  BALANCED: True
```
142 batches of 5,000 in 1 min 46 s (about 6,600 documents per second).

## 4. Replay of the same curated data, run `load_20261123T101242Z_67d73a` (demo step 5)

```
Load load_20261123T101242Z_67d73a  option=15  quality=warn
  inserted/updated/unchanged: 0 / 0 / 706024
  stale found/deleted       : 0 / 0
  MongoDB documents         : 706024
  BALANCED: True
```
No document inserted or rewritten; the count is not inflated. 45 s (reads only).

## 5. Index benchmark

See `index_benchmark.md` (regenerated after the winning-index fix). First run, 706,024 documents,
median of 5 runs:

| Query | Before | Docs examined before | After | Docs examined after | Before ms | After ms | Speed-up |
|---|---|---|---|---|---|---|---|
| Monthly trend, transfers, 2023-2025 | COLLSCAN | 706,024 | IXSCAN | 115,119 | 907 | 427 | 2.1x |
| Issue mix, deposits, California, 2024 | COLLSCAN | 706,024 | IXSCAN | 7,973 | 855 | 36 | 23.7x |
| Company response, deposits, one product, 2025 | COLLSCAN | 706,024 | IXSCAN | 84,195 | 943 | 311 | 3.0x |
| Timely %, transfers, Web, 2022-2025 | COLLSCAN | 706,024 | IXSCAN | 122,813 | 1,001 | 478 | 2.1x |
| Routing lag, transfers, last 12 months | COLLSCAN | 706,024 | IXSCAN | 39,257 | 898 | 192 | 4.7x |

Reading: every query switches from a full scan to an index range scan. The gain grows with
selectivity: the state filter (1 % of the collection) is 24x faster; broad multi-year queries
still read 16-17 % of the documents, so the gain is about 2x. Keys examined equal documents
examined, so the index range matches the result exactly (no wasted reads).

## 6. Query catalogue on both segments since 2024-01-01 (demo step 6)

| Measure | Deposit accounts | Transfers and payments |
|---|---|---|
| Complaints | 218,085 | 136,154 |
| Timely % (Yes / Yes+No) | 99.32 % (216,604 / 218,085) | 98.43 % (134,018 / 136,154) |
| Submitted via Web | 88.72 % | 96.91 % |
| Leading issue | Managing an account, 56.20 % | (see output) |
| Receipt-to-referral days: median / mean / p90 | 0 / 1.11 / 1 | 0 / 1.15 / 1 |
| Company response "Closed with monetary relief" | 12.43 % | (see output) |

Top companies by share of deposit-account complaints (not a quality ranking): JPMorgan Chase 11.23 %,
Wells Fargo 11.02 %, Bank of America 9.22 %, Navy Federal Credit Union 7.61 %, Chime 7.15 %.

## 7. Scheduled incremental collection

`Incremental collection` workflow run #3 (manual, on `main` after PR #19): success in 4 min 30 s.
Scheduled runs #1 and #2 (before PR #19) failed fast by design: no product filter configured,
so the collector refused to download unfiltered data.
