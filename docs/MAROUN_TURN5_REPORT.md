# Turn 5 report - Analysis (option 15: access to deposited and transferred funds)

Author: Maroun Machaalany. Status: **code and tests done; real-data results are produced by running
the script on the laptop (section 5) and committed as evidence.** No number in this report is
invented: every figure comes from `docs/evidence/turn5_analysis_results.md`.

## 1. Question

From `config/settings.yaml`: how do reported delays and problems accessing funds differ between
**deposit accounts** (`deposit_accounts`) and **transfers / payments** (`transfers_payments`)?

## 2. Assumption to confirm (honest note)

The brief's section 9 ("minimum measures") is not in the repository, so the measures below were
**chosen by me from the fields available in the serving layer** and documented here. If the brief
lists different mandatory measures, they must be added; the pipeline is built so that adding one is
a new function in `analytics/measures.py` plus one line in `collect()`.

## 3. Measures

| # | Measure | Why it answers the question | Source (only `database.queries`) |
|---|---|---|---|
| M1 | Volume per segment | Context for every share | `monthly_trend` |
| M2 | Timely company response % = Yes / (Yes + No), Wilson 95 % CI, gap between segments with CI and z-test p-value | "Delay": the company's own timeliness flag | `timely_rate` |
| M3 | M2 per year, counts re-summed (never averaged) | Is the gap stable or a one-year effect? | `timely_rate` with year filters |
| M4 | Routing lag (mean, median, P90) per segment | Delay inside CFPB routing. **Not** company resolution time | `routing_lag_summary` |
| M5 | Issue-share gaps (pp) within each segment | "Problems accessing funds": which issues weigh more on one side. Data-driven, no issue names hard-coded | `issue_mix` |
| M6 | Company response mix gaps (pp) | Do outcomes (relief, explanation) differ? | `company_response_mix` |
| M7 | Submission channel gaps (pp) | Different customers may use different channels | `channel_mix` |

Rules kept (from `docs/data_contracts.md` section 8): shares are computed within each segment;
missing/invalid values are reported separately and never counted as No; cells under 10 complaints
are marked unreliable; timely percentages are always recomputed from summed counts.

## 4. How to read the results (limits to state in the defence)

* A complaint is not an incident: more complaints in one segment does not mean worse service
  (customer base and complaint habits differ). Shares inside a segment are therefore the main view.
* With hundreds of thousands of rows almost any gap is "significant". The effect size (gap in
  percentage points and its CI) is what matters, not the p-value alone.
* Timely % is the company's flag as published by CFPB, not an independent measurement.
* Routing lag is CFPB receipt to referral, not the time to resolve the customer's problem.
* Association only: no causal claim from this data.
* Product labels were renamed by CFPB in April 2017; the two segments group old and new labels
  (`config/settings.yaml`), so year-level comparisons around 2017 deserve caution.

## 5. How to reproduce

```
docker compose up -d
docker compose run --rm spark python -m analytics.run_analysis
# optional window: --date-from 2018-01-01 --date-to 2025-12-31
```

Outputs `docs/evidence/turn5_analysis_results.{json,md}` (also printed). Pure-logic tests need no
database: `python -m pytest tests/unit/test_analytics.py`.

## 6. Results on real data

*To be filled from `docs/evidence/turn5_analysis_results.md` after the run (freshness line, table
M2, one-sentence reading per measure).* Placeholder deliberately left empty to avoid unverified
figures.

## 7. Files

`analytics/measures.py` (statistics), `analytics/run_analysis.py` (collection and rendering),
`tests/unit/test_analytics.py` (7 tests), this report.
