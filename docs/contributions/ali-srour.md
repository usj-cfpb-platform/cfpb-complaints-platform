# Contribution log - Ali Srour

Turns owned: 3 (Spark + data quality), 6 (dashboard)

| Date | What I did | PRs authored | PRs reviewed | Decisions / learning | Blockers |
|---|---|---|---|---|---|
| 2026-11-21 | Reviewed Turn 2 PR #18: found the two CI failures (missing `requests` in dev requirements, ruff UP038) | - | #18 | Reproduced the CI failure locally before commenting | - |
| 2026-11-22 | Reproduced the historical load from an empty environment using only the README (19,371,701 records). Turn 3: Spark job (schemas, readers, cleaning, option 15 filter with historical labels, dedup, derived fields), quarantine, quality gate with six dimensions and pre-registered thresholds (`dq-thresholds-v1`), aggregates, defect demo, 29 tests, docs and report | #19 | - | D-012 to D-018; the real data had 0 duplicates and 0 malformed rows, but 480 negative routing lags and 75 non-standard state values | Low free disk on my laptop: job designed to cache only in-scope rows |
| 2026-11-22 | Fixed CI for the Spark tests (Java 17 pinned with `actions/setup-java`, DEF-05); ran the `--inject-defect 100` gate demo on real data (gate FAIL, nothing written); wrote the Turn 3 → Turn 4 hand-off for Mhamad | #19 | - | Spark 3.5 needs Java 17; a quality gate is only credible once it has been seen to fail | Pasting into an editor broke the CI file once; rewritten with a script |
