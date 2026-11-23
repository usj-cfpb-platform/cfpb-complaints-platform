"""Dashboard-ready aggregates computed from the curated records (brief section 7 measures).

Every aggregate is split by ``segment`` (deposit accounts vs transfers and payments) so the
option-15 question, "how do delays and problems accessing funds differ between deposit accounts
and transfers?", can be answered directly. Counts are distinct Complaint IDs (curated data is
already unique per ID). Cells below ``privacy.min_cell_size`` are marked, not dropped, so the
dashboard decides how to show them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _mark_small(df: DataFrame, min_cell: int) -> DataFrame:
    from pyspark.sql import functions as F

    return df.withColumn("below_min_cell", F.col("complaints") < F.lit(min_cell))


def build(curated: DataFrame, min_cell: int) -> dict[str, DataFrame]:
    """Name -> DataFrame. Names become folder and MongoDB collection names."""
    from pyspark.sql import functions as F

    count = F.countDistinct("complaint_id").alias("complaints")
    state_label = (
        F.when(F.col("state").isNull(), F.lit("Missing"))
        .when(~F.col("state_is_valid"), F.lit("Invalid"))
        .otherwise(F.col("state"))
    )

    monthly = curated.groupBy("option", "segment", "product", "year", "month", "year_month").agg(
        count
    )
    issues = curated.groupBy("option", "segment", "product", "issue", "sub_issue").agg(count)
    geography = curated.groupBy(
        "option",
        "segment",
        state_label.alias("state"),
        F.coalesce("submitted_via", F.lit("Missing")).alias("submitted_via"),
    ).agg(count)
    responses = curated.groupBy(
        "option",
        "segment",
        "year",
        F.coalesce("company_response", F.lit("Missing")).alias("company_response"),
    ).agg(count)

    timely = curated.groupBy("option", "segment", "year_month").agg(
        F.sum(F.when(F.col("timely") == F.lit(True), 1).otherwise(0)).alias("timely_yes"),
        F.sum(F.when(F.col("timely") == F.lit(False), 1).otherwise(0)).alias("timely_no"),
        F.sum(F.when(F.col("timely").isNull(), 1).otherwise(0)).alias("timely_missing_or_invalid"),
        count,
    )
    # Brief 7: valid Yes / (valid Yes + valid No); missing and invalid reported separately.
    timely = timely.withColumn(
        "timely_pct",
        F.when(
            (F.col("timely_yes") + F.col("timely_no")) > 0,
            F.round(100 * F.col("timely_yes") / (F.col("timely_yes") + F.col("timely_no")), 2),
        ),
    )

    ok = F.col("routing_lag_status") == F.lit("ok")
    lag = curated.groupBy("option", "segment", "year_month").agg(
        F.sum(F.when(ok, 1).otherwise(0)).alias("lag_valid"),
        F.sum(F.when(F.col("routing_lag_status") == "negative", 1).otherwise(0)).alias(
            "lag_negative"
        ),
        F.sum(F.when(F.col("routing_lag_status") == "missing_sent_date", 1).otherwise(0)).alias(
            "lag_missing"
        ),
        F.sum(F.when(F.col("routing_lag_status") == "invalid_sent_date", 1).otherwise(0)).alias(
            "lag_invalid"
        ),
        F.round(F.avg(F.when(ok, F.col("routing_lag_days"))), 2).alias("lag_mean_days"),
        F.percentile_approx(F.when(ok, F.col("routing_lag_days")), 0.5).alias("lag_median_days"),
        F.percentile_approx(F.when(ok, F.col("routing_lag_days")), 0.9).alias("lag_p90_days"),
        count,
    )

    return {
        "monthly_counts": _mark_small(monthly, min_cell),
        "issue_breakdown": _mark_small(issues, min_cell),
        "geography_channel": _mark_small(geography, min_cell),
        "company_response": _mark_small(responses, min_cell),
        "timely_response": _mark_small(timely, min_cell),
        "routing_lag": _mark_small(lag, min_cell),
    }
