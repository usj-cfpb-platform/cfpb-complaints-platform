"""Column names and explicit Spark schemas for the raw inputs and the curated record.

One place maps both raw layouts onto the curated field names of ``docs/data_dictionary.md``:

* the historical CSV (column titles such as ``Date received``). The live file has 15 columns; older
  files, and the committed sample, also have ``Consumer consent provided?`` and
  ``Consumer disputed?``. Columns are matched **by name, never by position**, so both layouts work.
* the API (``hits.hits[]._source``), already in snake_case.

Every raw field is read as a string. Typing happens in ``processing.transform``, where an invalid
value can be detected and reported instead of silently becoming null at read time.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

# CSV column title -> curated field name (cohort standard: snake_case API names).
CSV_TO_FIELD: dict[str, str] = {
    "Date received": "date_received",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Consumer complaint narrative": "complaint_narrative",
    "Company public response": "company_public_response",
    "Company": "company",
    "State": "state",
    "ZIP code": "zip_code",
    "Tags": "tags",
    "Consumer consent provided?": "consumer_consent_provided",
    "Submitted via": "submitted_via",
    "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response",
    "Timely response?": "timely",
    "Consumer disputed?": "consumer_disputed",
    "Complaint ID": "complaint_id",
}

# Source fields of the curated record, in output order. Narratives are out of scope (not published
# since September 2026), so `complaint_narrative` is read if present but never kept.
SOURCE_FIELDS: tuple[str, ...] = (
    "complaint_id",
    "date_received",
    "date_sent_to_company",
    "product",
    "sub_product",
    "issue",
    "sub_issue",
    "company",
    "company_public_response",
    "company_response",
    "state",
    "zip_code",
    "tags",
    "submitted_via",
    "timely",
    "consumer_consent_provided",
    "consumer_disputed",
)

# Fields that must be present in the raw input for the job to start.
REQUIRED_CSV_COLUMNS: tuple[str, ...] = ("Complaint ID", "Date received", "Product")

# Fields known to be absent from the current source (data dictionary, confirmed 2026-11-19).
NOT_IN_CURRENT_SOURCE: tuple[str, ...] = ("consumer_consent_provided", "consumer_disputed")

# Lineage columns added by the readers.
LINEAGE_FIELDS: tuple[str, ...] = ("source", "source_run_id", "ingested_at")


class SchemaError(ValueError):
    """Raised when a raw input does not have the expected columns."""


def validate_csv_header(header: Sequence[str]) -> dict[str, str]:
    """Check a CSV header and return the rename map ``{csv column: curated field}``.

    Raises ``SchemaError`` if a required column is missing, a column is unknown (CFPB added or
    renamed one: the data dictionary must be updated first), or a column appears twice.
    """
    cleaned = [col.strip().lstrip("﻿") for col in header]
    if len(set(cleaned)) != len(cleaned):
        raise SchemaError(f"Duplicate column names in CSV header: {cleaned}")
    missing = [col for col in REQUIRED_CSV_COLUMNS if col not in cleaned]
    if missing:
        raise SchemaError(f"CSV header is missing required column(s): {missing}")
    unknown = [col for col in cleaned if col not in CSV_TO_FIELD]
    if unknown:
        raise SchemaError(
            f"Unknown CSV column(s) {unknown}: update processing/schema.py and the data dictionary"
        )
    return {col: CSV_TO_FIELD[col] for col in cleaned}


def csv_struct(header: Iterable[str]):
    """Explicit all-string Spark schema for a CSV with this header, in file order."""
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(col.strip().lstrip("﻿"), StringType(), True) for col in header])


def api_struct():
    """Explicit Spark schema for one raw API page (only the parts the job uses).

    Numbers (``complaint_id`` is an integer in the API spec) are read as their text, so the cast
    to string is explicit and lossless.
    """
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    source = StructType(
        [StructField(name, StringType(), True) for name in SOURCE_FIELDS if name != "tags"]
        + [StructField("tags", StringType(), True)]
    )
    hit = StructType([StructField("_source", source, True)])
    hits = StructType([StructField("hits", ArrayType(hit), True)])
    return StructType([StructField("hits", hits, True)])
