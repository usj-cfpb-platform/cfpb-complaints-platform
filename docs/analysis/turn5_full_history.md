# Turn 5 analysis results: deposit_accounts vs transfers_payments

Data freshness: latest complaint received 2026-11-23 00:00:00+00:00, serving layer refreshed 2026-11-26 17:10:48.376000+00:00. The latest month is partial.

Filters: none (all dates)

## Q1 Volume

| Segment | Complaints |
|---|---:|
| deposit_accounts | 501,237 |
| transfers_payments | 204,780 |

### Complaints per year (the newest year is partial)

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
| 2026 | 81,070 | 34,776 |

### Top issues in deposit_accounts (share within segment)

| Issue | Complaints | Share % |
|---|---:|---:|
| Managing an account | 242,749 | 48.43 |
| Closing an account | 53,515 | 10.68 |
| Problem with a lender or other company charging your account | 46,854 | 9.35 |
| Account opening, closing, or management | 37,959 | 7.57 |
| Opening an account | 37,748 | 7.53 |

### Top issues in transfers_payments (share within segment)

| Issue | Complaints | Share % |
|---|---:|---:|
| Other transaction problem | 72,910 | 35.6 |
| Fraud or scam | 51,148 | 24.98 |
| Unauthorized transactions or other transaction problem | 19,702 | 9.62 |
| Money was not available when promised | 14,427 | 7.05 |
| Managing, opening, or closing your mobile wallet account | 12,943 | 6.32 |

## Q3 Timely company response (Yes / (Yes + No))

| Segment | Valid | Timely % | 95 % CI | Missing/invalid |
|---|---:|---:|---|---:|
| deposit_accounts | 501,237 | 98.78 | (98.75, 98.81) | 0 |
| transfers_payments | 204,780 | 98.29 | (98.24, 98.35) | 0 |

Gap deposit_accounts minus transfers_payments: 0.49 pp, 95 % CI (0.42, 0.55), p = 1e-57.

### By year (counts re-summed, never averaged)

| Year | deposit_accounts % | transfers_payments % | Gap (pp) |
|---|---:|---:|---:|
| 2012 | 97.57 | n/a | n/a |
| 2013 | 99.38 | 99.11 | 0.27 |
| 2014 | 99.21 | 98.03 | 1.17 |
| 2015 | 99.25 | 97.42 | 1.84 |
| 2016 | 95.07 | 96.7 | -1.63 |
| 2017 | 96.66 | 97.82 | -1.16 |
| 2018 | 99.34 | 98.32 | 1.02 |
| 2019 | 98.02 | 97.5 | 0.52 |
| 2020 | 98.97 | 98.39 | 0.58 |
| 2021 | 98.37 | 97.59 | 0.78 |
| 2022 | 98.58 | 98.39 | 0.19 |
| 2023 | 99.12 | 98.16 | 0.96 |
| 2024 | 99.38 | 97.7 | 1.69 |
| 2025 | 99.28 | 98.79 | 0.49 |
| 2026 | 99.33 | 97.94 | 1.39 |

## Q5 Routing lag (CFPB receipt to referral, not resolution time)

| Segment | Valid | Mean | Median | P90 |
|---|---:|---:|---:|---:|
| deposit_accounts | 500,837 | 1.65 | 0.0 | 4.0 |
| transfers_payments | 204,700 | 1.36 | 0.0 | 1.0 |

## Q2 Largest issue-share gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| issue | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| Managing an account | 48.43 | 0.0 | 48.43 | yes |
| Other transaction problem | 0.0 | 35.6 | -35.6 | yes |
| Fraud or scam | 0.0 | 24.98 | -24.98 | yes |
| Closing an account | 10.68 | 0.0 | 10.68 | yes |
| Unauthorized transactions or other transaction problem | 0.0 | 9.62 | -9.62 | yes |
| Problem with a lender or other company charging your account | 9.35 | 0.0 | 9.35 | yes |
| Account opening, closing, or management | 7.57 | 0.0 | 7.57 | yes |
| Opening an account | 7.53 | 0.0 | 7.53 | yes |
| Money was not available when promised | 0.0 | 7.05 | -7.05 | yes |
| Managing, opening, or closing your mobile wallet account | 0.0 | 6.32 | -6.32 | yes |

## Q4 Company response mix gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| company_response | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| Closed with explanation | 76.7 | 88.85 | -12.15 | yes |
| Closed with monetary relief | 15.83 | 6.27 | 9.56 | yes |
| Closed with non-monetary relief | 5.35 | 3.92 | 1.43 | yes |
| Closed | 0.48 | 0.03 | 0.45 | yes |
| Closed without relief | 0.4 | 0.0 | 0.4 | yes |
| In progress | 0.93 | 0.66 | 0.27 | yes |
| Closed with relief | 0.24 | 0.0 | 0.24 | yes |
| Untimely response | 0.07 | 0.27 | -0.2 | yes |

## Q5 Submission channel gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| channel | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| Web | 72.29 | 93.4 | -21.11 | yes |
| Referral | 17.16 | 1.54 | 15.62 | yes |
| Phone | 8.48 | 4.53 | 3.95 | yes |
| Postal mail | 1.71 | 0.46 | 1.25 | yes |
| Fax | 0.35 | 0.06 | 0.29 | yes |
| Email | 0.02 | 0.0 | 0.02 | no (small cell) |

## Q5 Largest state-share gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| state | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| Missing | 2.09 | 5.0 | -2.91 | yes |
| CA | 14.15 | 11.98 | 2.17 | yes |
| NY | 8.23 | 6.8 | 1.43 | yes |
| FL | 9.43 | 8.07 | 1.36 | yes |
| IL | 4.11 | 5.18 | -1.07 | yes |
| TX | 8.07 | 9.07 | -1.0 | yes |
| NJ | 3.78 | 2.99 | 0.79 | yes |
| PA | 3.87 | 3.21 | 0.66 | yes |
| MS | 0.55 | 1.02 | -0.47 | yes |
| MD | 2.69 | 2.23 | 0.46 | yes |
