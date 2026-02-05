"""
Raw Data Ingestion Module using Databricks Auto Loader.

This module provides functionality to ingest streaming data from cloud storage
using Auto Loader with schema evolution capabilities.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataIngestionError(Exception):
    """Custom exception for data ingestion failures."""
    pass


def ingest_raw_data(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    checkpoint_location: str,
    schema_location: str,
    source_format: str = "json",
    schema_hints: Optional[StructType] = None
) -> None:
    """
    Ingest raw data using Databricks Auto Loader with schema evolution.

    Auto Loader automatically detects new files as they arrive in cloud storage
    and handles schema evolution without breaking the pipeline.

    Args:
        spark: Active SparkSession instance
        source_path: Cloud storage path containing source files (e.g., s3://bucket/raw/)
        target_table: Fully qualified Delta table name (e.g., catalog.schema.bronze_table)
        checkpoint_location: Path for streaming checkpoint (e.g., s3://bucket/checkpoints/)
        schema_location: Path for inferred schema storage (e.g., s3://bucket/schemas/)
        source_format: Source file format (default: json)
        schema_hints: Optional initial schema hints for Auto Loader

    Raises:
        DataIngestionError: If ingestion fails

    Example:
        >>> ingest_raw_data(
        ...     spark=spark,
        ...     source_path="s3://my-bucket/raw/events/",
        ...     target_table="main.bronze.raw_events",
        ...     checkpoint_location="s3://my-bucket/checkpoints/bronze/",
        ...     schema_location="s3://my-bucket/schemas/bronze/"
        ... )
    """
    try:
        logger.info(f"Starting Auto Loader ingestion from {source_path}")
        logger.info(f"Target table: {target_table}")

        # Configure Auto Loader stream
        stream_df: DataFrame = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", source_format)
            .option("cloudFiles.schemaLocation", schema_location)
            # Handle schema changes
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.inferColumnTypes", "true")  # Auto-infer types
            .option("cloudFiles.schemaHints", schema_hints if schema_hints else "")
            .load(source_path)
        )

        # Add metadata columns for lineage and debugging
        enriched_df: DataFrame = (
            stream_df .withColumn(
                "_ingestion_timestamp",
                spark.sql.functions.current_timestamp()) .withColumn(
                "_source_file",
                spark.sql.functions.input_file_name()))

        # Write to Delta table with Auto Loader
        query = (
            enriched_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")  # Enable schema evolution
            # Process all available files then stop
            .trigger(availableNow=True)
            .toTable(target_table)
        )

        query.awaitTermination()
        logger.info(f"Successfully ingested data to {target_table}")

    except Exception as e:
        error_msg = f"Failed to ingest data from {source_path}: {str(e)}"
        logger.error(error_msg)
        raise DataIngestionError(error_msg) from e


def validate_ingestion_paths(
    source_path: str,
    checkpoint_location: str,
    schema_location: str
) -> bool:
    """
    Validate that ingestion paths are properly formatted.

    Args:
        source_path: Source data path
        checkpoint_location: Checkpoint path
        schema_location: Schema inference path

    Returns:
        True if all paths are valid

    Raises:
        ValueError: If any path is invalid
    """
    cloud_prefixes = ("s3://", "abfss://", "gs://", "/dbfs/")

    for path_name, path in [
        ("source_path", source_path),
        ("checkpoint_location", checkpoint_location),
        ("schema_location", schema_location)
    ]:
        if not any(path.startswith(prefix) for prefix in cloud_prefixes):
            raise ValueError(
                f"{path_name} must use cloud storage or DBFS: {path}"
            )

    return True


if __name__ == "__main__":
    # Example usage
    spark = SparkSession.builder.appName("AutoLoaderIngestion").getOrCreate()

    ingest_raw_data(
        spark=spark,
        source_path="s3://lakehouse-raw/events/",
        target_table="main.bronze.raw_events",
        checkpoint_location="s3://lakehouse-checkpoints/bronze_events/",
        schema_location="s3://lakehouse-schemas/bronze_events/"
    )
