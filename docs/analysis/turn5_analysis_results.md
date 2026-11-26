# Turn 5 analysis results: deposit_accounts vs transfers_payments

Data freshness: latest complaint received 2026-11-23 00:00:00+00:00, serving layer refreshed 2026-11-26 17:10:48.376000+00:00. The latest month is partial.

Filters: {'date_from': '2024-01-01'}

## Q1 Volume

| Segment | Complaints |
|---|---:|
| deposit_accounts | 218,086 |
| transfers_payments | 136,146 |

### Complaints per year (the newest year is partial)

| Year | deposit_accounts | transfers_payments |
|---|---:|---:|
| 2024 | 52,821 | 16,751 |
| 2025 | 84,195 | 84,619 |
| 2026 | 81,070 | 34,776 |

### Top issues in deposit_accounts (share within segment)

| Issue | Complaints | Share % |
|---|---:|---:|
| Managing an account | 122,564 | 56.2 |
| Closing an account | 28,304 | 12.98 |
| Problem with a lender or other company charging your account | 27,526 | 12.62 |
| Problem caused by your funds being low | 19,880 | 9.12 |
| Opening an account | 18,238 | 8.36 |

### Top issues in transfers_payments (share within segment)

| Issue | Complaints | Share % |
|---|---:|---:|
| Other transaction problem | 62,305 | 45.76 |
| Fraud or scam | 27,627 | 20.29 |
| Unauthorized transactions or other transaction problem | 13,897 | 10.21 |
| Trouble accessing funds in your mobile or digital wallet | 8,108 | 5.96 |
| Money was not available when promised | 6,671 | 4.9 |

Fund-access issues ['Money was not available when promised', 'Trouble accessing funds in your mobile or digital wallet']: share within segment {'deposit_accounts': 0, 'transfers_payments': 10.86}.

## Q3 Timely company response (Yes / (Yes + No))

| Segment | Valid | Timely % | 95 % CI | Missing/invalid |
|---|---:|---:|---|---:|
| deposit_accounts | 218,086 | 99.32 | (99.29, 99.36) | 0 |
| transfers_payments | 136,146 | 98.43 | (98.37, 98.5) | 0 |

Gap deposit_accounts minus transfers_payments: 0.89 pp, 95 % CI (0.81, 0.96), p = 5.84e-145.

### By year (counts re-summed, never averaged)

| Year | deposit_accounts % | transfers_payments % | Gap (pp) |
|---|---:|---:|---:|
| 2024 | 99.38 | 97.7 | 1.69 |
| 2025 | 99.28 | 98.79 | 0.49 |
| 2026 | 99.33 | 97.94 | 1.39 |

## Q5 Routing lag (CFPB receipt to referral, not resolution time)

| Segment | Valid | Mean | Median | P90 |
|---|---:|---:|---:|---:|
| deposit_accounts | 218,086 | 1.11 | 0.0 | 1.0 |
| transfers_payments | 136,146 | 1.15 | 0.0 | 1.0 |

## Q2 Largest issue-share gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| issue | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| Managing an account | 56.2 | 0.0 | 56.2 | yes |
| Other transaction problem | 0.0 | 45.76 | -45.76 | yes |
| Fraud or scam | 0.0 | 20.29 | -20.29 | yes |
| Closing an account | 12.98 | 0.0 | 12.98 | yes |
| Problem with a lender or other company charging your account | 12.62 | 0.0 | 12.62 | yes |
| Unauthorized transactions or other transaction problem | 0.0 | 10.21 | -10.21 | yes |
| Problem caused by your funds being low | 9.12 | 0.0 | 9.12 | yes |
| Opening an account | 8.36 | 0.0 | 8.36 | yes |
| Trouble accessing funds in your mobile or digital wallet | 0.0 | 5.96 | -5.96 | yes |
| Money was not available when promised | 0.0 | 4.9 | -4.9 | yes |

## Q4 Company response mix gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| company_response | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| Closed with explanation | 79.9 | 90.84 | -10.94 | yes |
| Closed with monetary relief | 12.41 | 4.66 | 7.75 | yes |
| Closed with non-monetary relief | 5.44 | 3.22 | 2.22 | yes |
| In progress | 2.15 | 1.0 | 1.15 | yes |
| Untimely response | 0.11 | 0.28 | -0.17 | yes |

## Q5 Submission channel gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| channel | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| Web | 88.72 | 96.91 | -8.19 | yes |
| Referral | 5.89 | 0.69 | 5.2 | yes |
| Phone | 4.63 | 2.13 | 2.5 | yes |
| Postal mail | 0.76 | 0.27 | 0.49 | yes |

## Q5 Largest state-share gaps (share within segment, pp = deposit_accounts minus transfers_payments)

| state | deposit_accounts % | transfers_payments % | Gap (pp) | Reliable |
|---|---:|---:|---:|---|
| CA | 13.75 | 10.97 | 2.78 | yes |
| IL | 3.98 | 6.06 | -2.08 | yes |
| FL | 9.81 | 8.2 | 1.61 | yes |
| Missing | 1.02 | 2.45 | -1.43 | yes |
| NY | 7.84 | 6.47 | 1.37 | yes |
| TX | 9.38 | 10.11 | -0.73 | yes |
| NJ | 3.51 | 2.8 | 0.71 | yes |
| AL | 1.22 | 1.84 | -0.62 | yes |
| MS | 0.69 | 1.31 | -0.62 | yes |
| SC | 1.64 | 2.21 | -0.57 | yes |
