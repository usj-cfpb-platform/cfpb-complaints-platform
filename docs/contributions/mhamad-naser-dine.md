# Contribution log - Mhamad Naser Dine

Turns owned: 1 (foundation), 4 (MongoDB + integration)

| Date | What I did | PRs authored | PRs reviewed | Decisions / learning | Blockers |
|---|---|---|---|---|---|
| 2026-11-11 | Planned project, created GitHub organisation and protected repository | - | - | Public repo so contributions are visible; PR-only workflow on `main` | - |
| 2026-11-12 | Repository skeleton, configuration loader and tests | #1 | - | Domain kept as config, never hard-coded | - |
| 2026-11-23 | Turn 4: reviewed PR #19; MongoDB serving layer (validated collection, idempotent upserts, aggregates, indexes, 8 queries, benchmark, reconciliation), CI job with real MongoDB | Turn 4 PR | #19 | Load refuses failed gate; ESR compound indexes; pymongo instead of Spark connector (no rebuild) | Local data rebuild needs ~1 h download |
| 2026-11-23 | Real run: historical load 19,378,282 rows, Spark 706,024 curated, MongoDB load balanced, replay 0/0/706,024, index benchmark (2.1x-23.7x), fixed benchmark winning-index parsing | #20 | #19 | Gains scale with selectivity | - |
| 2026-11-23 | Audit of Turns 1-4 against brief V2; closing PR: CODEOWNERS (#5), CONTRIBUTING review rule, defect log | closing PR | - | Gaps assigned per owner (API path end to end, CSV vs Parquet benchmark) | - |
| 2026-11-24 | Re-ran sync, Spark and MongoDB load with the 4 API batches (621 inserted, balanced, replay 0/0/all); found from the load numbers that 14,018 of 14,201 updates were date-format only; diagnostic script and fix DEF-08 | fix/turn4-record-hash-dates | #24, #25 | Hash parsed dates, raw text only when unparseable; one-time re-hash accepted | Spark tests only run in CI |
