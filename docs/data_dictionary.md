# Data dictionary - curated complaint record

Status: v0.2. Based on the API probe of 2026-11-12 (Turn 1) and the real CSV header, confirmed on
2026-11-19 with `python -m ingestion.historical --inspect` (Turn 2; see the live results below). Field names follow the cohort standard:
snake_case CFPB API names.

## Source fields

| Curated field | CSV column (confirmed 2026-11-19) | API field | Type | Required | Notes |
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
| `consumer_consent_provided` | not in the CSV | not returned | string | no | Confirmed absent from the 2026-11-19 file; keep optional |
| `consumer_disputed` | not in the CSV | not returned | boolean | no | Confirmed absent from the 2026-11-19 file; keep optional |

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

## Turn 2 verification status (2026-11-14)

Checked against the official CFPB OpenAPI spec (cfpb/ccdb5-api `swagger-config.yaml`):

| Item | Finding |
|---|---|
| API `complaint_id` | integer in the spec; the contract stores it as a string (collectors convert) |
| API `date_received` | typed `date` in the spec; collectors also accept ISO datetimes and epoch values |
| API `date_sent_to_company` | typed string in the spec |
| API `timely` | string filter (`Yes`/`No`) |
| Date filters | `date_received_min` inclusive, `date_received_max` exclusive, format `YYYY-MM-DD` |

**Confirmed on 2026-11-19 (live CSV, `--inspect` and a full load)**

| Item | Value |
|---|---|
| ZIP size | 369,245,274 bytes (about 0.37 GB) |
| Extracted CSV size | 5,849,435,564 bytes (about 5.4 GiB) |
| Records | 19,281,923 (header excluded) |
| CSV header (15 columns, in order) | Date received, Product, Sub-product, Issue, Sub-issue, Company public response, Company, State, ZIP code, Tags, Submitted via, Date sent to company, Company response to consumer, Timely response?, Complaint ID |
| Date format (`Date received`, `Date sent to company`) | `YYYY-MM-DD` |
| HTTP | ETag and Last-Modified sent; `Accept-Ranges: bytes` (resumable download works) |
| First load | run `full_20261119T132939Z_439355`, about one hour on the author's connection |

**Still to confirm against the live API** (needs the product label assigned to the team):

```
python -m ingestion.incremental --dry-run --product "<label>"   # real API date format, _meta, cursors
```

