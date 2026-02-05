"""
Unit Tests for Data Quality Validation.

Tests the data quality logic that would be enforced in the DLT pipeline,
ensuring null handling and validation rules work correctly.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col
from datetime import datetime
from typing import List, Dict, Any


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create a Spark session for testing.
    
    Returns:
        SparkSession configured for local testing
    """
    return (
        SparkSession.builder
        .appName("DataQualityTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_schema() -> StructType:
    """
    Define the schema for test data.
    
    Returns:
        StructType matching the silver_events table schema
    """
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("revenue", DoubleType(), True),
    ])


@pytest.fixture
def valid_event_data() -> List[Dict[str, Any]]:
    """
    Create valid test event data.
    
    Returns:
        List of valid event dictionaries
    """
    return [
        {
            "event_id": "evt_001",
            "event_type": "purchase",
            "user_id": "user_123",
            "event_timestamp": datetime(2025, 1, 15, 10, 30, 0),
            "revenue": 99.99
        },
        {
            "event_id": "evt_002",
            "event_type": "click",
            "user_id": "user_456",
            "event_timestamp": datetime(2025, 1, 15, 11, 0, 0),
            "revenue": 0.0
        },
    ]


@pytest.fixture
def invalid_event_data() -> List[Dict[str, Any]]:
    """
    Create invalid test event data with various quality issues.
    
    Returns:
        List of invalid event dictionaries
    """
    return [
        {
            "event_id": None,  # Null event_id (should be dropped)
            "event_type": "purchase",
            "user_id": "user_789",
            "event_timestamp": datetime(2025, 1, 15, 12, 0, 0),
            "revenue": 149.99
        },
        {
            "event_id": "evt_003",
            "event_type": "signup",
            "user_id": None,  # Null user_id (should be dropped)
            "event_timestamp": datetime(2025, 1, 15, 13, 0, 0),
            "revenue": 0.0
        },
        {
            "event_id": "evt_004",
            "event_type": "view",
            "user_id": "",  # Empty user_id (should be dropped)
            "event_timestamp": datetime(2025, 1, 15, 14, 0, 0),
            "revenue": 0.0
        },
        {
            "event_id": "evt_005",
            "event_type": "purchase",
            "user_id": "user_999",
            "event_timestamp": None,  # Null timestamp (should be dropped)
            "revenue": 249.99
        },
    ]


class TestDataQuality:
    """Test suite for data quality validation rules."""
    
    def test_valid_events_pass_quality_checks(
        self, 
        spark: SparkSession, 
        sample_schema: StructType,
        valid_event_data: List[Dict[str, Any]]
    ) -> None:
        """
        Test that valid events pass all quality checks.
        
        Args:
            spark: Spark session fixture
            sample_schema: Schema fixture
            valid_event_data: Valid test data fixture
        """
        # Create DataFrame from valid data
        df = spark.createDataFrame(valid_event_data, schema=sample_schema)
        
        # Apply quality checks (simulating DLT expectations)
        filtered_df = df.filter(
            (col("event_id").isNotNull()) &
            (col("event_timestamp").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("user_id") != "")
        )
        
        # Assert all valid records pass
        assert filtered_df.count() == len(valid_event_data), \
            "All valid events should pass quality checks"
    
    
    def test_null_event_id_is_dropped(
        self,
        spark: SparkSession,
        sample_schema: StructType,
        invalid_event_data: List[Dict[str, Any]]
    ) -> None:
        """
        Test that events with null event_id are correctly flagged and dropped.
        
        Args:
            spark: Spark session fixture
            sample_schema: Schema fixture
            invalid_event_data: Invalid test data fixture
        """
        # Create DataFrame with null event_id
        df = spark.createDataFrame(invalid_event_data, schema=sample_schema)
        
        # Apply quality check
        filtered_df = df.filter(col("event_id").isNotNull())
        
        # Assert null event_id records are dropped
        assert filtered_df.count() < df.count(), \
            "Events with null event_id should be dropped"
        
        # Verify the specific null event_id record is gone
        null_event_id_records = df.filter(col("event_id").isNull())
        assert null_event_id_records.count() == 1, \
            "Should have exactly one record with null event_id"
    
    
    def test_null_user_id_is_dropped(
        self,
        spark: SparkSession,
        sample_schema: StructType,
        invalid_event_data: List[Dict[str, Any]]
    ) -> None:
        """
        Test that events with null or empty user_id are dropped.
        
        Args:
            spark: Spark session fixture
            sample_schema: Schema fixture
            invalid_event_data: Invalid test data fixture
        """
        df = spark.createDataFrame(invalid_event_data, schema=sample_schema)
        
        # Apply quality check
        filtered_df = df.filter(
            (col("user_id").isNotNull()) & 
            (col("user_id") != "")
        )
        
        # Count invalid user_id records
        invalid_user_ids = df.filter(
            (col("user_id").isNull()) | 
            (col("user_id") == "")
        )
        
        assert invalid_user_ids.count() == 2, \
            "Should have 2 records with null/empty user_id"
        
        assert filtered_df.count() == (df.count() - 2), \
            "Null and empty user_id records should be dropped"
    
    
    def test_null_timestamp_is_dropped(
        self,
        spark: SparkSession,
        sample_schema: StructType,
        invalid_event_data: List[Dict[str, Any]]
    ) -> None:
        """
        Test that events with null timestamp are dropped.
        
        Args:
            spark: Spark session fixture
            sample_schema: Schema fixture
            invalid_event_data: Invalid test data fixture
        """
        df = spark.createDataFrame(invalid_event_data, schema=sample_schema)
        
        # Apply quality check
        filtered_df = df.filter(col("event_timestamp").isNotNull())
        
        # Count null timestamp records
        null_timestamps = df.filter(col("event_timestamp").isNull())
        
        assert null_timestamps.count() == 1, \
            "Should have exactly one record with null timestamp"
        
        assert filtered_df.count() == (df.count() - 1), \
            "Null timestamp records should be dropped"
    
    
    def test_all_quality_rules_combined(
        self,
        spark: SparkSession,
        sample_schema: StructType,
        invalid_event_data: List[Dict[str, Any]]
    ) -> None:
        """
        Test that all quality rules work together correctly.
        
        Args:
            spark: Spark session fixture
            sample_schema: Schema fixture
            invalid_event_data: Invalid test data fixture
        """
        df = spark.createDataFrame(invalid_event_data, schema=sample_schema)
        
        # Apply all quality checks (matching DLT expectations)
        filtered_df = df.filter(
            (col("event_id").isNotNull()) &
            (col("event_timestamp").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("user_id") != "")
        )
        
        # All 4 records in invalid_event_data should be dropped
        assert filtered_df.count() == 0, \
            "All invalid events should be dropped by quality checks"
        
        assert df.count() == 4, \
            "Should have 4 total invalid records in test data"
    
    
    def test_deduplication_logic(
        self,
        spark: SparkSession,
        sample_schema: StructType
    ) -> None:
        """
        Test that duplicate events are properly deduplicated by event_id.
        
        Args:
            spark: Spark session fixture
            sample_schema: Schema fixture
        """
        # Create data with duplicates
        duplicate_data = [
            {
                "event_id": "evt_001",
                "event_type": "purchase",
                "user_id": "user_123",
                "event_timestamp": datetime(2025, 1, 15, 10, 30, 0),
                "revenue": 99.99
            },
            {
                "event_id": "evt_001",  # Duplicate event_id
                "event_type": "purchase",
                "user_id": "user_123",
                "event_timestamp": datetime(2025, 1, 15, 10, 30, 1),
                "revenue": 99.99
            },
        ]
        
        df = spark.createDataFrame(duplicate_data, schema=sample_schema)
        
        # Apply deduplication
        deduplicated_df = df.dropDuplicates(["event_id"])
        
        assert deduplicated_df.count() == 1, \
            "Duplicate event_ids should result in single record"


class TestRevenueValidation:
    """Test suite for revenue-specific validation."""
    
    def test_revenue_null_handling(
        self,
        spark: SparkSession,
        sample_schema: StructType
    ) -> None:
        """
        Test that null revenue values are handled correctly.
        
        Args:
            spark: Spark session fixture
            sample_schema: Schema fixture
        """
        data = [
            {
                "event_id": "evt_001",
                "event_type": "view",
                "user_id": "user_123",
                "event_timestamp": datetime(2025, 1, 15, 10, 30, 0),
                "revenue": None  # Null revenue is acceptable for non-purchase events
            },
        ]
        
        df = spark.createDataFrame(data, schema=sample_schema)
        
        # Verify null revenue is preserved (not dropped)
        assert df.filter(col("revenue").isNull()).count() == 1, \
            "Null revenue should be preserved for valid events"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])