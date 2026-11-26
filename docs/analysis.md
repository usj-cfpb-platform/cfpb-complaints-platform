# Turn 5 findings - option 15, access to deposited and transferred funds

Author: Maroun Machaalany. Data: MongoDB load `load_20261126T170954Z_1a407d` (706,017 curated
complaints, balanced), latest complaint received 2026-11-23, so **2026 is a partial year**.
All numbers come from `docs/analysis/turn5_full_history.md` (all dates) and
`docs/analysis/turn5_analysis_results.md` (from 2024-01-01), produced by
`python -m analytics.run_analysis` (see `docs/MAROUN_TURN5_REPORT.md`).

Question: how do reported delays and problems accessing funds differ between deposit accounts
(`deposit_accounts`) and transfers/payments (`transfers_payments`)?

Reading rules. Counts are distinct Complaint IDs. Shares are inside each segment. No causal claim and
no company quality ranking is made: a complaint count is not an incident count and depends on the
customer base and on how easily people complain. Definitions are in the report, section 3.

## Q1 - Volume over time

| Year | deposit_accounts | transfers_payments |
|---|---:|---:|
| 2012 | 12,210 | 0 |
| 2013 | 13,388 | 559 |
| 2014 | 14,662 | 1,170 |
| 2015 | 17,137 | 1,626 |
| 2016 | 21,846 | 1,574 |
| 2017 | 19,718 | 3,708 |
| 2018 | 21,207 | 5,430 |
| 2019 | 21,734 | 5,082 |
| 2020 | 24,238 | 8,306 |
| 2021 | 29,556 | 13,896 |
| 2022 | 37,582 | 13,534 |
| 2023 | 49,873 | 13,749 |
| 2024 | 52,821 | 16,751 |
| 2025 | 84,195 | 84,619 |
| 2026 (partial, to 23 Nov) | 81,070 | 34,776 |
| **Total** | **501,237** | **204,780** |

* Deposit accounts rise in almost every year; transfers start in 2013 and stay below deposits until
  2025, when both are about 84,000 (transfers 16,751 in 2024, 84,619 in 2025).
* The 2025 jump is not explained by this analysis. Which companies or issues drive it was not
  examined; `top_companies` would be the next step.
* Limitations: CFPB renamed its products in April 2017 and the two segments group old and new labels,
  so 2016-2018 comparisons are not like for like. The serving layer has no 2011 rows. Counts are
  complaints, not incidents.

## Q2 (demo) - Which issues dominate, and how large are fund-access problems?

Since 2024-01-01 (share within segment):

| deposit_accounts | % | transfers_payments | % |
|---|---:|---|---:|
| Managing an account | 56.2 | Other transaction problem | 45.76 |
| Closing an account | 12.98 | Fraud or scam | 20.29 |
| Problem with a lender or other company charging your account | 12.62 | Unauthorized transactions or other transaction problem | 10.21 |
| Problem caused by your funds being low | 9.12 | Trouble accessing funds in your mobile or digital wallet | 5.96 |
| Opening an account | 8.36 | Money was not available when promised | 4.9 |

Fund-access issue labels chosen after reading the real labels (exact CFPB text, declared on the
command line, not hard-coded): "Money was not available when promised" and "Trouble accessing funds
in your mobile or digital wallet". Together they are **10.86 % of transfers/payments complaints**
since 2024 (5.96 + 4.90). Over the full history "Money was not available when promised" alone is
7.05 % of transfers/payments complaints.

* **Important limitation: the two segments use different CFPB issue lists.** Not one top issue is
  shared. The issue-gap table (for example "Managing an account" 56.2 pp higher in deposits,
  "Other transaction problem" 45.76 pp higher in transfers) therefore mostly reflects different
  label sets, not different customer problems.
* For deposits the share of the two fund-access labels is 0 because those labels do not exist for
  that product, **not** because deposit customers have no access problems. Access problems on deposit
  accounts sit inside broader labels such as "Managing an account" and would need sub-issue analysis,
  which was not done. No ranking of the two segments on fund-access problems is possible from the
  issue field alone.

## Q3 (demo) - Is the timely-response rate different, and has it changed?

Timely % = Yes / (Yes + No); there were 0 missing or invalid values in both segments.

| Window | deposit_accounts | transfers_payments | Gap (pp), 95 % CI |
|---|---:|---:|---|
| Since 2024 | 99.32 % (99.29-99.36) | 98.43 % (98.37-98.50) | 0.89 (0.81-0.96) |
| All years | 98.78 % (98.75-98.81) | 98.29 % (98.24-98.35) | 0.49 (0.42-0.55) |

By year (gap = deposits minus transfers): 2013 0.27, 2014 1.17, 2015 1.84, **2016 -1.63,
2017 -1.16**, 2018 1.02, 2019 0.52, 2020 0.58, 2021 0.78, 2022 0.19, 2023 0.96, 2024 1.69,
2025 0.49, 2026 1.39. Deposits' lowest values are 95.07 % (2016) and 96.66 % (2017). The 2012 transfers
cell is empty (no transfers complaints that year).

* Deposit accounts are on average slightly more often timely (about 0.5 to 0.9 points). The gap is
  statistically clear (very large samples) but small in practice: both rates are above 98 %.
* The gap is not stable: it changes sign in 2016-2017 and ranges from 0.19 to 1.84 points in other
  years.
* Limitations: "timely" is the company's own flag as published by CFPB, not an independent
  measure of delay. It does not measure how long a customer waited for their money. 2026 is
  partial. Yearly rates are recomputed from counts, never averaged.

## Q4 - Company responses

| Response (share within segment) | Since 2024 deposits | Since 2024 transfers | Gap (pp) |
|---|---:|---:|---:|
| Closed with explanation | 79.9 | 90.84 | -10.94 |
| Closed with monetary relief | 12.41 | 4.66 | 7.75 |
| Closed with non-monetary relief | 5.44 | 3.22 | 2.22 |
| In progress | 2.15 | 1.0 | 1.15 |
| Untimely response | 0.11 | 0.28 | -0.17 |

Over all years: monetary relief 15.83 % (deposits) vs 6.27 % (transfers); explanation only 76.7 %
vs 88.85 %.

* Deposit complaints end with monetary relief more than twice as often as transfers/payments ones.
* Limitations: the share closed with relief is an outcome category chosen by the company; it says
  nothing about whether the complaint was justified. "In progress" is higher for deposits and the
  newest complaints are not closed yet. In the full history the legacy values "Closed",
  "Closed without relief" and "Closed with relief" (0.24 to 0.48 % of deposits) come from older
  CFPB vocabulary and cannot be compared with transfers (0.0 to 0.03 %).

## Q5 - Routing interval, channel and state

**Receipt to referral (CFPB routing, not resolution time).**

| Window | deposits mean / median / P90 | transfers mean / median / P90 |
|---|---|---|
| Since 2024 | 1.11 / 0 / 1 day | 1.15 / 0 / 1 day |
| All years | 1.65 / 0 / 4 days | 1.36 / 0 / 1 day |

* Since 2024 there is no practical difference. Over all years deposits have a longer tail
  (P90 4 days vs 1). Missing/invalid/negative intervals over all years: 400 for deposits
  (500,837 valid of 501,237) and 80 for transfers (204,700 valid of 204,780).
* This is the time before CFPB forwards the complaint, not the time to get the money back.

**Channel (since 2024).** Web 88.72 % (deposits) vs 96.91 % (transfers); Referral 5.89 % vs 0.69 %;
Phone 4.63 % vs 2.13 %; Postal mail 0.76 % vs 0.27 %. Deposits arrive through more varied
channels. Over all years Web is 72.29 % vs 93.4 % and Referral 17.16 % vs 1.54 %. Email (0.02 % of
deposits, small cell) is not reliable.

**State (since 2024).** The largest share gaps are small: CA 13.75 % vs 10.97 % (+2.78 pp),
IL 3.98 % vs 6.06 % (-2.08 pp), FL 9.81 % vs 8.2 % (+1.61 pp). A missing state is 1.02 % of deposits
and 2.45 % of transfers (full history 2.09 % vs 5.0 %). Shares are not adjusted for state population,
so they show where complaints come from, not where problems are more frequent.

## Summary for the defence

1. Both segments are complained about increasingly; transfers caught up with deposits in 2025.
2. The company-reported timely rate is high (above 98 %) in both; deposits are slightly higher, the
   gap varies by year and reversed in 2016-2017.
3. Fund-access problems are visible by label only for transfers (10.86 % since 2024); the issue
   fields of the two segments cannot be compared directly.
4. Deposit complaints more often end with monetary relief; transfers more often with explanation only.
5. Routing time is about one day or less for both; no meaningful difference since 2024.
