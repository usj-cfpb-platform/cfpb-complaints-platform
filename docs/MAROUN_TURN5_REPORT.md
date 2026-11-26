# Turn 5 report - Analysis (option 15: access to deposited and transferred funds)

Author: Maroun Machaalany. Status: **code and tests done; real-data results are produced by running
the script on the laptop (section 5) and committed as evidence.** No number in this report is
invented: every figure comes from `docs/analysis/turn5_analysis_results.md`.

## 1. Question

From `config/settings.yaml`: how do reported delays and problems accessing funds differ between
**deposit accounts** (`deposit_accounts`) and **transfers / payments** (`transfers_payments`)?

## 2. Source of the requirements

Brief V2 sections 7 and 9 as summarised in Mhamad's hand-off (`TURN5_HANDOFF_FOR_MAROUN.md`):
five analytical questions (two pre-declared for the demo), a reproducible script writing to
`docs/analysis/`, metric definitions, a findings write-up with limitations, tests, and a hand-off
to Ali. The brief file itself is not in the repository.

## 3. The five questions and their measures

| # | Question | Demo | Source (only `database.queries`) |
|---|---|---|---|
| Q1 | How did monthly complaint volumes change for deposit accounts vs transfers (labels changed in April 2017)? | | `monthly_trend` |
| Q2 | Which issues dominate each segment, and how large is the share of fund-access problems in each? | yes | `issue_mix` (+ `--fund-access-issue`) |
| Q3 | Is the timely-response rate different between the segments, and has it changed over time? | yes | `timely_rate`, overall and per year |
| Q4 | How do company responses differ (e.g. share closed with monetary relief)? | | `company_response_mix` |
| Q5 | Do routing interval, channel or state mix differ? | | `routing_lag_summary`, `channel_mix`, `state_distribution` |

The fund-access issue labels are NOT hard-coded: after reading the real labels in the Q2 table, they
are passed explicitly (`--fund-access-issue "<exact label>"`, repeatable) and recorded in the output.

### Metric definitions

* Total = distinct curated Complaint IDs (one document per ID).
* Share = complaints in the category / complaints in the same segment (never across segments).
* Timely % = Yes / (Yes + No); missing or invalid reported separately; Wilson 95 % CI; yearly or
  monthly values are re-summed from counts, never averaged.
* Gap = segment A minus segment B in percentage points, with 95 % CI and z-test p-value.
* Interval = CFPB receipt to referral (routing), with missing/invalid/negative reported; not the
  company's resolution time.
* Cells under 10 complaints are marked unreliable. The newest month (Nov 2026) is partial.

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
# optional: --date-from 2024-01-01 --fund-access-issue "<exact issue label>"
```

Outputs `docs/analysis/turn5_analysis_results.{json,md}` (also printed). Pure-logic tests need no
database: `python -m pytest tests/unit/test_analytics.py`.

## 6. Results on real data

*To be filled from `docs/analysis/turn5_analysis_results.md` after the run (freshness line, table
M2, one-sentence reading per measure).* Placeholder deliberately left empty to avoid unverified
figures.

## 7. Hand-off to Ali (dashboard, Turn 6)

Views (all from `database.queries`, same filter dict): (1) monthly trend by segment (`monthly_trend`);
(2) issue mix within segment (`issue_mix`); (3) timely % by segment and year (`timely_rate`);
(4) company response mix (`company_response_mix`); (5) routing lag, channel and state mix.
Filters: date range plus segment, product, issue, state, channel, company. Demo questions: Q2 and Q3.
Always show counts and shares within segment, grey out `below_min_cell`, show freshness from the
newest `pipeline_runs` document.

## 8. Files

`analytics/measures.py`, `analytics/run_analysis.py`, `tests/unit/test_analytics.py`, this report.
Findings (`docs/analysis.md`) come in the second PR after the real-data run.
