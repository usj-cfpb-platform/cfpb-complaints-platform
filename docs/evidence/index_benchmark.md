# Index benchmark (before / after dashboard indexes)

Generated 2026-11-23T10:25:00+00:00 on 706024 documents in `complaints`, median of 5 runs per query. Same data and same query conditions in both columns; only the dashboard indexes differ (`complaint_id` unique index kept).

| Query | Before: plan | Docs examined | Median ms | After: plan (index) | Docs examined | Keys examined | Median ms | Speed-up |
|---|---|---|---|---|---|---|---|---|
| Monthly trend, transfers, 2023-2025 | COLLSCAN | 706024 | 922.4 | IXSCAN (ix_segment_date) | 115119 | 115119 | 412.1 | 2.2x |
| Issue mix, deposits, California, 2024 | COLLSCAN | 706024 | 908.9 | IXSCAN (ix_segment_state_date) | 7973 | 7973 | 33.7 | 27.0x |
| Company response, deposits, one product, 2025 | COLLSCAN | 706024 | 931.4 | IXSCAN (ix_segment_product_date) | 84195 | 84195 | 330.0 | 2.8x |
| Timely %, transfers, Web channel, 2022-2025 | COLLSCAN | 706024 | 1064.3 | IXSCAN (ix_segment_channel_date) | 122813 | 122813 | 499.8 | 2.1x |
| Routing lag, transfers, last 12 months | COLLSCAN | 706024 | 847.0 | IXSCAN (ix_segment_date) | 39257 | 39257 | 184.1 | 4.6x |

Reading: without the indexes MongoDB scans every complaint (COLLSCAN) for each request. With the compound index (segment + category + date_received, Equality-Sort-Range order) it reads only the index range of the selected segment and period, so documents examined drop to the matching complaints.
