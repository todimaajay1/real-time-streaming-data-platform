"""
Delta Live Tables Pipeline for Lakehouse Medallion Architecture.

This module implements a three-layer medallion architecture:
- Bronze: Raw ingested data
- Silver: Cleaned and validated data
- Gold: Aggregated metrics for analytics and ML
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, count, sum as _sum,
    avg, window, to_date
)


# ============================================================================
# BRONZE LAYER - Raw Data Ingestion
# ============================================================================


@dlt.table(
    name="bronze_events",
    comment="Raw event data ingested from Auto Loader with full schema rescue",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_events() -> DataFrame:
    """
    Bronze layer: Raw event data with all fields preserved.

    Reads from the Auto Loader ingestion target. This layer maintains
    complete data fidelity including rescued columns from schema evolution.

    Returns:
        DataFrame containing raw event data with metadata
    """
    return (
        dlt.read_stream("main.bronze.raw_events")
        .withColumn("_bronze_timestamp", current_timestamp())
    )


# ============================================================================
# SILVER LAYER - Cleaned and Validated Data
# ============================================================================

@dlt.table(
    name="silver_events",
    comment="Cleaned event data with data quality constraints enforced",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL AND user_id != ''")
@dlt.expect("valid_event_type",
            "event_type IN ('click', 'view', 'purchase', 'signup')")
def silver_events() -> DataFrame:
    """
    Silver layer: Cleaned events with data quality rules enforced.

    Data Quality Rules:
    - event_id must not be null (DROP if violated)
    - event_timestamp must not be null (DROP if violated)
    - user_id must not be null or empty (DROP if violated)
    - event_type should be a known type (WARN if violated)

    Returns:
        DataFrame containing validated event data
    """
    return (
        dlt.read_stream("bronze_events")
        .select(
            col("event_id"),
            col("event_type"),
            col("user_id"),
            col("event_timestamp").cast("timestamp"),
            col("event_properties"),
            col("user_email"),
            col("ip_address"),
            col("revenue").cast("double"),
            col("_source_file"),
            current_timestamp().alias("_silver_timestamp")
        )
        .dropDuplicates(["event_id"])  # Ensure idempotency
    )


# ============================================================================
# GOLD LAYER - Aggregated Metrics for AI/ML
# ============================================================================

@dlt.table(name="gold_user_daily_metrics",
           comment="Daily aggregated user engagement metrics for ML feature engineering",
           table_properties={"quality": "gold",
                             "pipelines.autoOptimize.managed": "true"})
def gold_user_daily_metrics() -> DataFrame:
    """
    Gold layer: Daily user engagement metrics for AI/ML consumption.

    Aggregates:
    - Total events per user per day
    - Total revenue per user per day
    - Average events per hour
    - Distinct event types per day

    Returns:
        DataFrame containing daily user metrics suitable for ML features
    """
    return (
        dlt.read_stream("silver_events")
        .withColumn("event_date", to_date(col("event_timestamp")))
        .groupBy("user_id", "event_date")
        .agg(
            count("*").alias("total_events"),
            _sum("revenue").alias("total_revenue"),
            count(col("event_id").isNotNull()).alias("event_count"),
            count(col("event_type")).alias("distinct_event_types"),
            avg("revenue").alias("avg_revenue_per_event")
        )
        .withColumn("_gold_timestamp", current_timestamp())
    )


@dlt.table(
    name="gold_hourly_event_summary",
    comment="Hourly event aggregations for real-time monitoring and alerting",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_hourly_event_summary() -> DataFrame:
    """
    Gold layer: Hourly event summaries for operational monitoring.

    Provides time-windowed aggregations for:
    - Event volume trends
    - Revenue patterns
    - Event type distribution

    Returns:
        DataFrame containing hourly event metrics
    """
    return (
        dlt.read_stream("silver_events")
        .withWatermark("event_timestamp", "1 hour")
        .groupBy(
            window("event_timestamp", "1 hour"),
            "event_type"
        )
        .agg(
            count("*").alias("event_count"),
            _sum("revenue").alias("total_revenue"),
            count("user_id").alias("unique_users")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("event_count"),
            col("total_revenue"),
            col("unique_users"),
            current_timestamp().alias("_gold_timestamp")
        )
    )


# ============================================================================
# DATA QUALITY MONITORING
# ============================================================================

@dlt.table(
    name="dq_metrics",
    comment="Data quality metrics and expectations tracking"
)
def dq_metrics() -> DataFrame:
    """
    Track data quality metrics across the pipeline.

    Monitors expectation violations and data quality trends
    for continuous improvement and alerting.

    Returns:
        DataFrame containing DQ metrics
    """
    return (
        dlt.read("event_expectations")
        .groupBy("dataset", "name")
        .agg(
            count("*").alias("total_records"),
            _sum("passed_records").alias("passed_records"),
            _sum("failed_records").alias("failed_records")
        )
    )
