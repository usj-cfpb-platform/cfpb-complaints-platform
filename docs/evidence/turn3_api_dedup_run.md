# Turn 3 evidence: historical + API batches, deduplication on real data (2026-11-24)

Ali's laptop, Docker Compose. Run `spark_20261124T162014Z_f507b1` (code `85dca90` + fix
`fix/turn3-api-dedup-evidence`), quality thresholds `dq-thresholds-v1` (rules version 1).
Quality report: `quality/run_id=spark_20261124T162014Z_f507b1/quality_report.json` in MinIO.

## 1. Inputs (all complete batches, `ingestion.discovery`)

The 4 scheduled API runs were collected by GitHub Actions and brought into MinIO with
`python -m ingestion.sync_artifacts` (4 uploaded, 0 rejected).

| Batch | Source | Ingested at (UTC) | Rows read by Spark (= metadata `record_count`) |
|---|---|---|---|
| `full_20261122T132122Z_9c8cbf` | historical CSV | 2026-11-22 13:39 | 19,371,701 |
| `api_20261123T110725Z_409bef` | API, 143 pages | 2026-11-23 11:11 | 14,201 |
| `api_20261123T125849Z_ece7c9` | API, 143 pages | 2026-11-23 12:59 | 14,201 |
| `api_20261124T021642Z_34c868` | API, 138 pages | 2026-11-24 02:20 | 13,727 |
| `api_20261124T135136Z_6b7568` | API, 144 pages | 2026-11-24 13:55 | 14,339 |
| **Total** | | | **19,428,169** |

## 2. Reconciliation

| Stage | Rows |
|---|---|
| raw rows | 19,428,169 |
| rejected (identity) | 0 |
| filtered out (other options) | 18,665,722 |
| rejected (receipt date) | 0 |
| valid in scope | 762,447 |
| **duplicates removed** | **55,803** |
| of which a different version of the same complaint (`duplicate_conflict_superseded`, in quarantine) | 14,706 |
| filtered out after dedup | 0 |
| **curated** | **706,644** |
| unbalanced | 0 |

All 56,468 API records are in scope (the collector filters by product on the server). 55,803 of
them are Complaint IDs already present in the CSV or in an earlier API run (the 45-day lookback
windows overlap on purpose); 665 are complaints the 22 November CSV did not contain yet. Curated
grew from 705,979 (run `spark_20261122T170908Z_8b65fe`, CSV only) to 706,644 (+665).

For every duplicated Complaint ID the rule D-013 kept the version with the newest `ingested_at`
(here always the latest API run that returned it). 14,706 superseded versions had different
content (for example a company response that changed after the CSV snapshot); they are kept in
`quarantine/run_id=spark_20261124T162014Z_f507b1/` as evidence.

## 3. Quality gate

**WARN**, same two warnings as the first run: `completeness.required_missing_rate = 7e-06`
(5 complaints without `issue`) and `validity.negative_routing_lag_rate = 0.000679`. All other
checks PASS, including `uniqueness.curated_duplicate_ids = 0` after deduplication,
`reconciliation.unbalanced_rows = 0`, `raw_vs_manifest_difference = 0`, freshness
(latest receipt date 2026-11-23, 1 day; latest ingestion 0 days).

## 4. How to reproduce

```
docker compose run --rm -e GITHUB_TOKEN ingestion python -m ingestion.sync_artifacts
docker compose run --rm ingestion python -m ingestion.discovery      # 1 full + 4 api, all COMPLETE
docker compose run --rm spark python -m processing.job
```
