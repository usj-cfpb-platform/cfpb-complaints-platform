# CSV vs Parquet benchmark (option 15 curated scope)

Created 2026-11-24T16:38:03Z with `python -m processing.benchmark`. Input: `s3a://datalake/curated/complaints/option=15` (706,644 rows). Same records in both formats (array columns joined as text). Each query run 3 times, median shown. CSV read with an explicit schema and `multiLine` (as the real CFPB file requires).

| | CSV (uncompressed) | Parquet (Snappy) | CSV / Parquet |
|---|---|---|---|
| Size | 281.8 MB | 54.3 MB | 5.19x |
| Read all rows (count) | 1.00 s | 0.19 s | 5.19x |
| Complaints per segment and year (2 columns) | 1.10 s | 0.40 s | 2.74x |
| Filter `transfers_payments`, year 2025 (count) | 0.96 s | 0.28 s | 3.39x |

Both formats returned the same counts: **True**.

Why (Session 3.1): Parquet is columnar and compressed, so it stores repeated labels (product, issue, company) compactly, reads only the columns a query needs, and skips row groups whose min/max statistics exclude a filter. CSV is row-based text: every query reads and parses every byte. Timings come from one laptop and vary between runs; the size difference does not.
