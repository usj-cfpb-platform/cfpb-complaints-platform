# Index benchmark (before / after dashboard indexes)

Generated 2026-11-23T10:19:53+00:00 on 706024 documents in `complaints`, median of 5 runs per query. Same data and same query conditions in both columns; only the dashboard indexes differ (`complaint_id` unique index kept).

| Query | Before: plan | Docs examined | Median ms | After: plan (index) | Docs examined | Keys examined | Median ms | Speed-up |
|---|---|---|---|---|---|---|---|---|
| Monthly trend, transfers, 2023-2025 | COLLSCAN | 706024 | 957.2 | IXSCAN (ix_segment_channel_date, ix_segment_company_date, ix_segment_date, ix_segment_issue_date, ix_segment_product_date, ix_segment_state_date) | 115119 | 115119 | 401.6 | 2.4x |
| Issue mix, deposits, California, 2024 | COLLSCAN | 706024 | 850.0 | IXSCAN (ix_segment_channel_date, ix_segment_company_date, ix_segment_date, ix_segment_issue_date, ix_segment_product_date, ix_segment_state_date) | 7973 | 7973 | 36.9 | 23.0x |
| Company response, deposits, one product, 2025 | COLLSCAN | 706024 | 998.0 | IXSCAN (ix_segment_channel_date, ix_segment_company_date, ix_segment_date, ix_segment_issue_date, ix_segment_product_date, ix_segment_state_date) | 84195 | 84195 | 304.7 | 3.3x |
| Timely %, transfers, Web channel, 2022-2025 | COLLSCAN | 706024 | 1005.6 | IXSCAN (ix_segment_channel_date, ix_segment_company_date, ix_segment_date, ix_segment_issue_date, ix_segment_product_date, ix_segment_state_date) | 122813 | 122813 | 474.9 | 2.1x |
| Routing lag, transfers, last 12 months | COLLSCAN | 706024 | 849.8 | IXSCAN (ix_segment_channel_date, ix_segment_company_date, ix_segment_date, ix_segment_issue_date, ix_segment_product_date, ix_segment_state_date) | 39257 | 39257 | 165.0 | 5.2x |

Reading: without the indexes MongoDB scans every complaint (COLLSCAN) for each request. With the compound index (segment + category + date_received, Equality-Sort-Range order) it reads only the index range of the selected segment and period, so documents examined drop to the matching complaints.
