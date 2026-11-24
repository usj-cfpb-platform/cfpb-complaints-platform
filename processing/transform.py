"""Cleaning, validation, domain filter, deduplication and derived fields (brief section 6.3).

Pipeline, in order (every step is counted, so the run reconciles exactly, see ``job.py``):

1. ``standardise``   trim every string, empty -> null, upper-case state codes.
2. ``parse``         typed dates, Yes/No -> boolean, tags -> array, validity flags.
3. reject            rows that cannot be identified or classified (no valid Complaint ID, no
                     product, malformed CSV row) -> quarantine with a reason.
4. scope             keep every version of a Complaint ID that is in the option's scope in at least
                     one version (so a re-labelled complaint is deduplicated before filtering).
5. reject            in-scope rows without a valid receipt date -> quarantine (they cannot be
                     placed in a year/month partition).
6. ``deduplicate``   one row per Complaint ID; documented rule below.
7. final scope       the retained version must itself be in scope.
8. ``derive``        year, month, year_month, routing_lag_days and its status, segment, flags.

Deduplication rule (the same Complaint ID appears in overlapping API windows and in the
historical file): keep the version with the most recent ``ingested_at``; on a tie prefer ``api``
over ``full_load`` (the API is the newer publication path); on a remaining tie keep the highest
``record_hash`` so the result is deterministic. Original source labels are never rewritten; the
analytical ``segment`` is an added column.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from processing.schema import SOURCE_FIELDS

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

# CFPB started accepting complaints on 2011-07-21; earlier receipt dates are invalid.
MIN_VALID_DATE = date(2011, 7, 1)

# Reject reasons (quarantine) - the record is not curated.
REJECT_MISSING_ID = "missing_complaint_id"
REJECT_INVALID_ID = "invalid_complaint_id"
REJECT_MISSING_PRODUCT = "missing_product"
REJECT_MALFORMED_ROW = "malformed_csv_row"
REJECT_MISSING_DATE = "missing_date_received"
REJECT_INVALID_DATE = "invalid_date_received"
REJECT_DUPLICATE_CONFLICT = "duplicate_conflict_superseded"

# Quality flags - the record is curated, the flag explains what is wrong with one field.
FLAG_INVALID_STATE = "invalid_state"
FLAG_INVALID_TIMELY = "invalid_timely"
FLAG_MISSING_SENT_DATE = "missing_date_sent_to_company"
FLAG_INVALID_SENT_DATE = "invalid_date_sent_to_company"
FLAG_NEGATIVE_LAG = "negative_routing_lag"

# routing_lag_status values
LAG_OK = "ok"
LAG_NEGATIVE = "negative"
LAG_MISSING = "missing_sent_date"
LAG_INVALID = "invalid_sent_date"


@dataclass(frozen=True)
class Scope:
    """The option's filters, read from ``config/settings.yaml``.

    ``segments`` maps each analytical segment to the original CFPB product labels it groups
    (historical labels included). Empty ``sub_products`` / ``issues`` mean "no filter".
    """

    option: int
    slug: str
    segments: Mapping[str, tuple[str, ...]]
    sub_products: tuple[str, ...] = ()
    issues: tuple[str, ...] = ()

    @property
    def products(self) -> tuple[str, ...]:
        return tuple(label for labels in self.segments.values() for label in labels)

    @classmethod
    def from_settings(cls, settings: Mapping) -> Scope:
        domain = settings.get("domain") or {}
        option = domain.get("option")
        segments_raw = domain.get("segments") or {}
        products = [str(p).strip() for p in domain.get("products") or []]
        if not isinstance(option, int) or not segments_raw or not products:
            raise ValueError(
                "domain.option, domain.products and domain.segments must be set in "
                "config/settings.yaml before Spark processing can run"
            )
        segments = {
            str(name): tuple(str(label).strip() for label in labels)
            for name, labels in segments_raw.items()
        }
        mapped = [label for labels in segments.values() for label in labels]
        if len(set(mapped)) != len(mapped):
            raise ValueError("A product label appears in more than one segment")
        if set(mapped) != set(products):
            raise ValueError(
                "domain.segments must map exactly the labels in domain.products; "
                f"unmapped={sorted(set(products) - set(mapped))}, "
                f"unknown={sorted(set(mapped) - set(products))}"
            )
        return cls(
            option=option,
            slug=str(domain.get("slug", "")),
            segments=segments,
            sub_products=tuple(str(s).strip() for s in domain.get("sub_products") or []),
            issues=tuple(str(s).strip() for s in domain.get("issues") or []),
        )


# --------------------------------------------------------------------------- 1. standardise


def standardise(df: DataFrame) -> DataFrame:
    """Trim strings and turn empty values into null; labels are otherwise kept as published."""
    from pyspark.sql import functions as F

    out = df
    for name in SOURCE_FIELDS:
        trimmed = F.trim(F.col(name))
        out = out.withColumn(name, F.when(trimmed == "", None).otherwise(trimmed))
    return out.withColumn("state", F.upper(F.col("state")))


# --------------------------------------------------------------------------- 2. parse


def _parse_date(column: str) -> Column:
    """Date from ``YYYY-MM-DD`` (also an ISO timestamp prefix), ``MM/DD/YYYY`` or epoch seconds/ms.

    Anything else, and impossible dates such as 2026-13-45, give null; the caller compares with the
    raw value to tell *missing* from *invalid*.
    """
    from pyspark.sql import functions as F

    raw = F.col(column)
    iso = F.when(raw.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(F.substring(raw, 1, 10), "yyyy-MM-dd"))
    us = F.when(raw.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(raw, "MM/dd/yyyy"))
    epoch = F.when(
        raw.rlike(r"^\d{10,13}$"),
        F.to_date(
            F.from_unixtime(
                F.when(F.length(raw) == 13, raw.cast("long") / 1000).otherwise(raw.cast("long"))
            )
        ),
    )
    return F.coalesce(iso, us, epoch)


def _yes_no(column: str) -> Column:
    from pyspark.sql import functions as F

    value = F.lower(F.col(column))
    return F.when(value == "yes", True).when(value == "no", False)


def parse(df: DataFrame, valid_states: Iterable[str], today: date) -> DataFrame:
    """Add typed columns and validity information. Raw strings are kept for evidence."""
    from pyspark.sql import functions as F

    states = sorted({s.strip().upper() for s in valid_states if s.strip()})
    received = _parse_date("date_received")
    sent = _parse_date("date_sent_to_company")
    in_range = lambda d: d.between(F.lit(MIN_VALID_DATE), F.lit(today))  # noqa: E731

    tags_array = F.when(
        F.col("tags").startswith("["), F.from_json(F.col("tags"), "array<string>")
    ).otherwise(F.split(F.col("tags"), r"\s*,\s*"))

    return (
        df.withColumn("date_received_raw", F.col("date_received"))
        .withColumn("date_sent_to_company_raw", F.col("date_sent_to_company"))
        .withColumn("date_received", F.when(in_range(received), received))
        .withColumn("date_sent_to_company", F.when(in_range(sent), sent))
        .withColumn("timely_raw", F.col("timely"))
        .withColumn("timely", _yes_no("timely"))
        .withColumn("consumer_disputed", _yes_no("consumer_disputed"))
        .withColumn("tags", F.when(F.col("tags").isNotNull(), tags_array))
        .withColumn(
            "state_is_valid",
            F.when(F.col("state").isNull(), None).otherwise(F.col("state").isin(states)),
        )
        .withColumn(
            "complaint_id",
            # The API may give an integer that was serialised as 123.0 by some tools.
            F.regexp_replace(F.col("complaint_id"), r"\.0$", ""),
        )
    )


def reject_reason_identity(df: DataFrame) -> Column:
    """Step 3: why a row cannot be identified or classified (null = keep)."""
    from pyspark.sql import functions as F

    return (
        F.when(F.col("_corrupt_record").isNotNull(), F.lit(REJECT_MALFORMED_ROW))
        .when(F.col("complaint_id").isNull(), F.lit(REJECT_MISSING_ID))
        .when(~F.col("complaint_id").rlike(r"^\d+$"), F.lit(REJECT_INVALID_ID))
        .when(F.col("product").isNull(), F.lit(REJECT_MISSING_PRODUCT))
    )


def reject_reason_date() -> Column:
    """Step 5: in-scope rows need a valid receipt date."""
    from pyspark.sql import functions as F

    return F.when(F.col("date_received_raw").isNull(), F.lit(REJECT_MISSING_DATE)).when(
        F.col("date_received").isNull(), F.lit(REJECT_INVALID_DATE)
    )


# --------------------------------------------------------------------------- 4. scope


def in_scope(scope: Scope) -> Column:
    """Row-level filter: product (all historical labels), then optional sub-product and issue."""
    from pyspark.sql import functions as F

    condition = F.col("product").isin(list(scope.products))
    if scope.sub_products:
        condition = condition & F.col("sub_product").isin(list(scope.sub_products))
    if scope.issues:
        condition = condition & F.col("issue").isin(list(scope.issues))
    return condition


# --------------------------------------------------------------------------- 6. deduplicate


def with_record_hash(df: DataFrame) -> DataFrame:
    """SHA-256 of the source fields: detects when the same Complaint ID changed between versions.

    Dates are hashed as parsed ``YYYY-MM-DD`` values, not as received: the CSV writes
    ``2026-10-09`` and the API ``2026-10-09T00:20:56.000Z`` for the same day, and hashing the raw
    text made every API version look changed (DEF-08). A date that does not parse falls back to
    its raw text, so an invalid value still counts as a difference.
    """
    from pyspark.sql import functions as F

    def value(name: str) -> Column:
        if name in _DATE_FIELDS:
            return F.coalesce(F.col(name).cast("string"), F.col(f"{name}_raw"))
        return F.col(f"{name}_raw" if name in _RAW_KEPT else name).cast("string")

    parts = [F.coalesce(value(name), F.lit("")) for name in SOURCE_FIELDS if name != "tags"] + [
        F.coalesce(F.array_join(F.col("tags"), "|"), F.lit(""))
    ]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("\u0001", *parts), 256))


_DATE_FIELDS = {"date_received", "date_sent_to_company"}
_RAW_KEPT = {"timely"}


def rank_versions(df: DataFrame) -> DataFrame:
    """Add ``version_rank`` (1 = retained) and ``winner_hash`` per Complaint ID."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    by_id = Window.partitionBy("complaint_id")
    ordered = by_id.orderBy(
        F.col("ingested_at").desc_nulls_last(),
        F.when(F.col("source") == "api", 0).otherwise(1),
        F.col("record_hash").desc(),
    )
    return df.withColumn("version_rank", F.row_number().over(ordered)).withColumn(
        "winner_hash", F.first("record_hash").over(ordered)
    )


# --------------------------------------------------------------------------- 8. derive


def derive(df: DataFrame, scope: Scope) -> DataFrame:
    """Derived fields of the data dictionary, segment and the per-record quality flags."""
    from pyspark.sql import functions as F

    segment = None
    for name, labels in scope.segments.items():
        test = F.col("product").isin(list(labels))
        segment = F.when(test, F.lit(name)) if segment is None else segment.when(test, F.lit(name))

    lag = F.datediff(F.col("date_sent_to_company"), F.col("date_received"))
    lag_status = (
        F.when(F.col("date_sent_to_company_raw").isNull(), F.lit(LAG_MISSING))
        .when(F.col("date_sent_to_company").isNull(), F.lit(LAG_INVALID))
        .when(lag < 0, F.lit(LAG_NEGATIVE))
        .otherwise(F.lit(LAG_OK))
    )
    flags = F.array_compact(
        F.array(
            F.when(F.col("state_is_valid") == F.lit(False), F.lit(FLAG_INVALID_STATE)),
            F.when(
                F.col("timely").isNull() & F.col("timely_raw").isNotNull(),
                F.lit(FLAG_INVALID_TIMELY),
            ),
            F.when(lag_status == LAG_MISSING, F.lit(FLAG_MISSING_SENT_DATE)),
            F.when(lag_status == LAG_INVALID, F.lit(FLAG_INVALID_SENT_DATE)),
            F.when(lag_status == LAG_NEGATIVE, F.lit(FLAG_NEGATIVE_LAG)),
        )
    )
    return (
        df.withColumn("option", F.lit(scope.option))
        .withColumn("segment", segment)
        .withColumn("year", F.year("date_received"))
        .withColumn("month", F.month("date_received"))
        .withColumn("year_month", F.date_format("date_received", "yyyy-MM"))
        .withColumn("routing_lag_days", F.when(F.col("date_sent_to_company").isNotNull(), lag))
        .withColumn("routing_lag_status", lag_status)
        .withColumn("quality_flags", flags)
    )


# Columns of the curated record, in order (docs/data_dictionary.md).
CURATED_COLUMNS: tuple[str, ...] = (
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
    "state_is_valid",
    "zip_code",
    "tags",
    "submitted_via",
    "timely",
    "consumer_consent_provided",
    "consumer_disputed",
    "option",
    "segment",
    "year",
    "month",
    "year_month",
    "routing_lag_days",
    "routing_lag_status",
    "quality_flags",
    "source",
    "source_run_id",
    "ingested_at",
    "record_hash",
)

# Columns kept in quarantine: the raw evidence plus the reason.
QUARANTINE_COLUMNS: tuple[str, ...] = (
    "reject_reason",
    "complaint_id",
    "date_received_raw",
    "date_sent_to_company_raw",
    "product",
    "sub_product",
    "issue",
    "company",
    "state",
    "timely_raw",
    "_corrupt_record",
    "source",
    "source_run_id",
    "ingested_at",
)
