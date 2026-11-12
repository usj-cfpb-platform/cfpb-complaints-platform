# Data dictionary - curated complaint record

Status: v0.1 (Turn 1), from the API probe of 2026-11-12. Column names in the CSV are to be
confirmed against the real file header in Turn 2. Field names follow the cohort standard:
snake_case CFPB API names.

## Source fields

| Curated field | CSV column (to verify) | API field | Type | Required | Notes |
|---|---|---|---|---|---|
| `complaint_id` | Complaint ID | `complaint_id` | string | yes | Business key; unique in curated data |
| `date_received` | Date received | `date_received` | date | yes | CFPB receipt date; API may return a timestamp |
| `date_sent_to_company` | Date sent to company | `date_sent_to_company` | date | no | |
| `product` | Product | `product` | string | yes | Domain filter field; labels changed over time |
| `sub_product` | Sub-product | `sub_product` | string | no | |
| `issue` | Issue | `issue` | string | yes | |
| `sub_issue` | Sub-issue | `sub_issue` | string | no | |
| `company` | Company | `company` | string | yes | Never presented as a quality ranking |
| `company_public_response` | Company public response | `company_public_response` | string | no | Optional statement chosen by the company |
| `company_response` | Company response to consumer | `company_response` | string | no | "In progress" is common for recent complaints |
| `state` | State | `state` | string | no | Validated against `config/reference/states.csv` (Turn 3) |
| `zip_code` | ZIP code | `zip_code` | string | no | Partially suppressed by CFPB; not used below state level |
| `tags` | Tags | `tags` | array of strings | no | e.g. Older American, Servicemember |
| `submitted_via` | Submitted via | `submitted_via` | string | no | Channel |
| `timely` | Timely response? | `timely` | boolean | no | Yes/No normalised; not customer satisfaction |
| `consumer_consent_provided` | Consumer consent provided? | not returned | string | no | CSV only, if present |
| `consumer_disputed` | Consumer disputed? | not returned | boolean | no | Legacy field, if present |

Narratives are not in the published data since September 2026 and are out of scope.

## Derived and lineage fields

| Field | Type | Definition |
|---|---|---|
| `year`, `month` | integer | From `date_received` |
| `year_month` | string `YYYY-MM` | From `date_received` |
| `routing_lag_days` | integer | `date_sent_to_company - date_received`. CFPB routing time, **not** company resolution time |
| `source` | `full_load` / `api` | Path that produced the record |
| `source_run_id` | string | `run_id` of the raw data used |
| `ingested_at` | timestamp UTC | When the raw data was collected |
| `record_hash` | string | Hash of the source fields, to detect records that changed between runs |

Deduplication tie-break (proposal, decided in Turn 3): keep the record from the most recent
`ingested_at`; if equal, prefer `api` over `full_load`.
