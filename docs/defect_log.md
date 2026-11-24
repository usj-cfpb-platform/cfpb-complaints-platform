# Defect log

Brief V2 section 10: "a brief account of defects found and corrected". One row per real defect,
newest last. Each owner adds the defects found in their own layer.

| ID | Date found | Turn / layer | Found by | Defect | Impact | Correction | Evidence | Regression test |
|---|---|---|---|---|---|---|---|---|
| DEF-01 | 2026-11-12 | 1 / infra | manual check (`docker pull`) | Official MinIO images no longer published; `quay.io/minio/minio` returned 401 UNAUTHORIZED | Platform could not start its raw zone | Chainguard MinIO build pinned by digest (decision D-003) | `docker-compose.yml`, `docs/architecture.md` D-003 | Spark smoke test writes and reads Parquet on MinIO |
| DEF-02 | 2026-11-12 | 1 / infra | first `docker compose up --build` | Two services built the same image name (`cfpb-python:0.1`); the second export failed with "already exists" and cancelled the Spark build | 1 h of build lost on a slow network | Removed the shared `image:` from the compose anchor; pip cache mount added to the Spark image | PR #17 | CI `docker compose config`; later builds succeed from cache |
| DEF-03 | 2026-11-18 | 2 / ingestion | code review against the Turn 1 API probe | `_meta.break_points` values are lists (`[1794435841000, "32548220"]`) but the collector accepted only strings | Pagination would skip the documented cursor source and could stop after page 1 | Accept list-shaped cursors, joined with `_` | commit `a7f364d` | unit test with the real list shape |
| DEF-04 | 2026-11-21 | 2 / ci | CI (`lint-and-test`) | Code used a pattern flagged by the pinned ruff 0.8.4 (UP038); `requests` missing from dev requirements | PR blocked by CI | Code and `requirements-dev.txt` corrected | commit `e21eddb` | CI lint job |
| DEF-05 | 2026-11-22 | 3 / ci | CI (`lint-and-test`) | Spark 3.5 tests failed on the runner's newer default Java; a later edit pasted shell lines into the workflow | Spark tests could not run in CI | Java 17 pinned with `actions/setup-java` (same as the Spark image); stray lines removed | commits `969f197`, `1375322` | Spark unit tests run in CI |
| DEF-06 | 2026-11-23 | 4 / database | review of the first real benchmark output | Benchmark reported every index the planner tried (from `rejectedPlans`) as used | Misleading index evidence | Read index names from the winning plan only | commit `92bbaca`, `docs/evidence/index_benchmark.md` | `test_summarize_explain_reports_only_the_winning_index` |
| DEF-07 | 2026-11-24 | 3 / processing | code review before the first API processing run | A successful API run whose window had no new complaints stores no page; Spark was given an empty path list and would stop the whole job | One empty scheduled run would block all processing | Empty API runs are skipped and logged (`processing/readers.py`) | branch `fix/turn3-api-dedup-evidence` | `test_empty_api_run_is_skipped` |
| DEF-08 | 2026-11-24 | 3-4 / processing + database | Mhamad, from the numbers of the first MongoDB load after the API batches (`load_20261124T171529Z_50ba49`: 14,201 updated in one day) | `record_hash` was computed on the raw date text. The CSV writes `2026-10-09`, the API `2026-10-09T00:20:56.000Z` | Every complaint returned again by the API counted as a changed version: 14,201 MongoDB updates and false `duplicate_conflict_superseded` rows in quarantine. `scripts/diagnose_versions.py` showed that only 183 really changed (`company_response`, `company_public_response`) | Hash dates as parsed `YYYY-MM-DD` (raw text only when a date does not parse) | `docs/evidence/turn4_api_reload.md` | `test_same_complaint_with_api_timestamps_is_not_a_conflict` |

## Deliberate defects (tests that must fail)

| Test | Injected defect | Expected result | Evidence |
|---|---|---|---|
| Quality gate | `processing.job --inject-defect 100` adds 100 duplicate curated IDs | Gate FAIL, nothing written | `docs/ALI_TURN3_REPORT.md` section 6, run `spark_20261122T172858Z_9a2657` |
| Loader guard | Newest quality report has `gate.status = fail` | `database.load` refuses (exit 1), MongoDB unchanged | `tests/unit/test_database_logic.py::test_gate_refuses` |
| MongoDB validator | Document with `date_received` as a string | Insert rejected | `tests/integration/test_mongo_serving.py` |
| Unique index | Second insert of the same `complaint_id` | DuplicateKeyError | `tests/integration/test_mongo_serving.py` |

Not defects (expected behaviour): scheduled collection runs #1 and #2 (22 Nov) failed fast because
no product filter was configured before PR #19; the collector refuses to download unfiltered data.
