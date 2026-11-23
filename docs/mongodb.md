# MongoDB serving layer (Turn 4)

Owner: Mhamad. Code: `database/`. Contract for Turns 5 and 6: `docs/data_contracts.md` section 8.

## 1. Why a document store here

The dashboard and the analysis read **aggregates over filtered complaints**: one segment, a date
range and one or two categories (product, issue, state, company, channel), grouped by month, issue,
state or response. Every complaint is read on its own, never joined to another record. A document
per complaint, with compound indexes on exactly those filters, serves that access pattern directly.
We chose MongoDB for the access pattern (Session 4.1), not because it is "modern"; a relational
star schema would also work but needs joins we do not use.

## 2. Document model and the embed-vs-reference check

| Question (Session 4.1 check) | Answer for a complaint | Decision |
|---|---|---|
| Is the related data read together with the complaint? | Product, issue, company, state, response are always shown with it | Embed |
| Does the related data change independently and often? | No: labels are fixed values copied from CFPB | Embed |
| Is it shared by many documents and large? | Company names repeat, but they are short strings with no other attributes | Embed (duplication accepted) |
| Would embedding make a document grow without bound? | No: fixed set of fields | Embed |

Result: **flat, fully embedded documents**, no references. Trade-off accepted: a company or label
renamed by CFPB is rewritten on many documents, which the loader does anyway (`record_hash` changes).

Collections:

| Collection | Content | Refresh |
|---|---|---|
| `complaints` | one document per curated complaint (all curated columns + `load_run_id`, `loaded_at`, `first_loaded_at`) | upsert by `complaint_id` |
| `agg_monthly_counts`, `agg_issue_breakdown`, `agg_geography_channel`, `agg_company_response`, `agg_timely_response`, `agg_routing_lag` | Spark aggregates, one row per grain | replaced atomically on every load |
| `pipeline_runs` | one document per load: reconciliation, freshness, quality status | append |

## 3. Types and validation

* **Dates are BSON `Date`** (UTC midnight for `date_received`, `date_sent_to_company`), not strings:
  range filters use the index and `$dateTrunc`, `$max` work natively. A string date such as
  `"2024-01-01"` is rejected by the validator.
* `complaint_id` is a string (CFPB ids are numeric today, but they are identifiers, not numbers).
* `$jsonSchema` validator, `validationLevel: strict`, `validationAction: error`
  (`database/schema.py`): required lineage and partition fields, enum on `source`, `YYYY-MM` pattern.

## 4. Indexes, derived from the dashboard filters

| Index | Keys | Serves |
|---|---|---|
| `ux_complaint_id` (unique) | `complaint_id` | upserts, uniqueness guarantee (brief 6.4) |
| `ix_segment_date` | `segment, date_received` | every view with only segment + period |
| `ix_segment_product_date` | `segment, product, date_received` | product filter |
| `ix_segment_issue_date` | `segment, issue, date_received` | issue filter |
| `ix_segment_state_date` | `segment, state, date_received` | state filter, map |
| `ix_segment_company_date` | `segment, company, date_received` | company filter |
| `ix_segment_channel_date` | `segment, submitted_via, date_received` | channel filter |

Key order follows **Equality, Sort, Range**: equality filters first, the date range last, so one
index range scan returns exactly the selected complaints. Evidence: `docs/evidence/index_benchmark.md`
(`python -m database.benchmark`: same queries and data, before and after the indexes).

## 5. Loading safely (idempotency)

`python -m database.load`:
1. refuses to load if the newest Spark quality report has `gate.status = fail`;
2. upserts in batches of 5,000: looks up the stored `record_hash` per id, then
   insert (new id) / update (hash changed) / skip (same hash), with unordered bulk writes;
3. deletes documents whose id disappeared from curated, **only** when the rows read equal the
   count Spark reported (never deletes on a partial read);
4. swaps each aggregate collection (temp collection + `rename(dropTarget=True)`), so the dashboard
   never sees a half-written aggregate;
5. writes `reports/reconciliation/run_id=<load id>/reconciliation.json` and a `pipeline_runs` document.

A replay of the same curated data gives `inserted 0 / updated 0 / unchanged N` and the same counts:
this is demo step 5. CI proves it on the sample (`tests/e2e/test_sample_to_mongodb.py`).

## 6. Query catalogue (`database/queries.py`, at least five required)

| # | Query | Main stages |
|---|---|---|
| Q1 | Monthly trend per segment | `$match` -> `$group(segment, year_month)` -> `$sort` |
| Q2 | Issue mix, counts and shares within segment | `$match` -> `$group` -> `$setWindowFields` (segment total) -> share |
| Q3 | State distribution (Missing kept) | same as Q2 on `state` |
| Q4 | Submission channel mix | same as Q2 on `submitted_via` |
| Q5 | Company response categories | same as Q2 on `company_response` |
| Q6 | Timely % = Yes / (Yes + No), missing separate | `$group` with conditional sums -> `$project` |
| Q7 | Receipt-to-referral days: mean, median, p90, negatives | `$group` with `$avg`, `$median`, `$percentile` |
| Q8 | Companies with the largest share (not a ranking of quality) | Q2 on `company` -> `$rank` |

All results carry `below_min_cell` (fewer than 10 complaints) for privacy and noise.
`python -m database.queries` prints them all for the demo (`--show-pipeline` prints the stages).

## 7. Growth: replication and sharding (discussed, not deployed)

* **Replication:** a 3-member replica set would give automatic failover for the dashboard; reads of
  aggregates could go to secondaries. Under a network partition MongoDB favours consistency for
  writes (only the primary accepts them): the CP side of CAP for writes.
* **Sharding key:** if complaints grew far beyond one server, a compound key
  `{segment: 1, date_received: 1}` would keep each dashboard query on few shards (the filters always
  contain segment and a date range). A hashed `complaint_id` would spread writes evenly but make every
  dashboard query scatter to all shards, so it fits the upsert path, not our read path.
* **Redis cache:** not needed at today's latency (see the benchmark); listed as future work.
