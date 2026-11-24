# Turn 4 evidence: MongoDB re-load with the API batches, and DEF-08 (2026-11-24)

Mhamad's laptop, Docker Compose, same MinIO and MongoDB as `turn4_real_run.md` (CSV-only state:
706,024 documents). This closes the end-to-end path historical CSV + scheduled API -> Spark -> MongoDB
on one machine.

## 1. Sync and discovery (Turn 2 code)

```
docker compose run --rm -e GITHUB_TOKEN ingestion python -m ingestion.sync_artifacts
  uploaded: api_20261123T110725Z_409bef, api_20261123T125849Z_ece7c9,
            api_20261124T021642Z_34c868, api_20261124T135136Z_6b7568   rejected: 0
docker compose run --rm ingestion python -m ingestion.discovery
COMPLETE   full full_20261123T092410Z_e778e4  records=19378282
COMPLETE   api  api_20261123T110725Z_409bef  records=14201
COMPLETE   api  api_20261123T125849Z_ece7c9  records=14201
COMPLETE   api  api_20261124T021642Z_34c868  records=13727
COMPLETE   api  api_20261124T135136Z_6b7568  records=14339
5 complete, 0 incomplete
```

## 2. First pass: Spark `spark_20261124T170948Z_dde90a` -> load `load_20261124T171529Z_50ba49`

| Stage | Rows |
|---|---|
| raw rows (19,378,282 CSV + 56,468 API) | 19,434,750 |
| filtered out (other options) | 18,672,258 |
| duplicates removed | 55,847 |
| curated | 706,645 (+621 vs CSV only) |
| quality gate | WARN (same two warnings as 23 Nov) |

```
inserted/updated/unchanged: 621 / 14201 / 691823     BALANCED: True
replay  load_20261124T171653Z_242c65: 0 / 0 / 706645  BALANCED: True
```

**621 inserted** = complaints received after the 23 Nov CSV snapshot. **14,201 updated** was
suspicious: 621 + 14,201 = 14,822 = every complaint the API returned. A real day does not change
2 % of the domain.

## 3. Diagnosis (`python -m scripts.diagnose_versions`, read-only)

The newest API version of each complaint also in the CSV, compared field by field:

| | Complaints |
|---|---|
| in the CSV and in an API batch | 14,201 |
| differ in at least one **raw** field | 14,201 |
| differ in at least one **parsed** field (real change) | **183** |

| Field | Raw diff | Parsed diff | Example (CSV -> API) |
|---|---|---|---|
| `date_received` | 14,201 | 0 | `2026-10-09` -> `2026-10-09T00:20:56.000Z` |
| `date_sent_to_company` | 14,201 | 0 | `2026-10-09` -> `2026-10-09T00:33:21.000Z` |
| `company_response` | 183 | 183 | `In progress` -> `Closed with explanation` |
| `company_public_response` | 84 | 84 | empty -> `Company has responded to the consumer ...` |
| all 12 other fields | 0 | 0 | |

Cause (DEF-08): `record_hash` hashed the raw date text. Both sources describe the same day in a
different format, so every API version looked changed: false MongoDB updates and false
`duplicate_conflict_superseded` rows in quarantine.

## 4. Fix and second pass

Fix (`processing/transform.py`): dates are hashed as parsed `YYYY-MM-DD`; raw text is used only
when a date does not parse. CSV hashes are unchanged (their raw text already is `YYYY-MM-DD`).
Regression test: `test_same_complaint_with_api_timestamps_is_not_a_conflict`.

| Spark run | duplicates removed | of which content changed |
|---|---|---|
| Ali, 24 Nov, before the fix (`spark_20261124T162014Z_f507b1`) | 55,803 | 14,706 |
| Mhamad, after the fix (`spark_20261124T172824Z_2acc04`) | 55,847 | **732** |

The 732 real conflicts are the 183 CSV -> API changes above plus versions that changed between two
API runs (a company response updated during the 45-day lookback). Curated rows are identical
(706,645); only the classification of the losing versions changed.

```
load_20261124T173338Z_b1aa20  inserted/updated/unchanged: 0 / 14822 / 691823   BALANCED: True
load_20261124T173513Z_30feed  inserted/updated/unchanged: 0 / 0 / 706645       BALANCED: True
```

The 14,822 updates are a one-time migration: every document that came from the API (621 + 14,201)
gets the new hash; the 691,823 CSV documents keep theirs. The replay then changes nothing.
From now on, `updated` in a load counts real changes only.

## 5. Reproduce

```
docker compose run --rm -e GITHUB_TOKEN ingestion python -m ingestion.sync_artifacts
docker compose run --rm ingestion python -m ingestion.discovery
docker compose run --rm spark python -m processing.job
docker compose run --rm spark python -m database.load        # twice: second run = 0 / 0 / all
docker compose run --rm spark python -m scripts.diagnose_versions
```
