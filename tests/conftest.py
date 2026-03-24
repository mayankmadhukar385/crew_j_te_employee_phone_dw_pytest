"""Shared test fixtures for all test modules.

Sets up a local SparkSession with Delta Lake and seeds the config
table with test configs. No YAML files — config table is the sole
source even in tests.

Source data is registered as temp views so SourceReader can read
it using type: "test_dataframe" without needing JDBC drivers.
"""

import json

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a test SparkSession with Delta Lake support."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("datastage-etl-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def test_catalog_schema(spark):
    """Create test catalog and schema in local Spark.

    In local mode there's no Unity Catalog, so we use the default
    catalog and create a test database/schema.
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS test_schema")
    return {"catalog": "spark_catalog", "schema": "test_schema"}


@pytest.fixture
def sample_config(test_catalog_schema):
    """Standard test config dict matching the production config_json schema.

    This is what gets stored in the etl_job_config Delta table.
    Every test that needs config should use this fixture.
    """
    return {
        "pipeline": {
            "job_name": "test_job",
            "job_id": "TEST_001",
            "workflow_name": "test_workflow",
            "run_mode": "full_refresh",
            "catalog": test_catalog_schema["catalog"],
            "schema": test_catalog_schema["schema"],
        },
        "source": {
            "type": "test_dataframe",
            "table": "test_source_table",
            "test_view_name": "test_source_data",
        },
        "target": {
            "table": "test_target_table",
            "scd_type": 1,
            "business_keys": ["id"],
            "tracked_columns": ["name", "value"],
        },
        "error": {
            "enabled": True,
            "table": "test_error_table",
        },
        "delta": {
            "optimize_after_write": False,
            "vacuum_retention_hours": 168,
        },
        "logging": {
            "level": "DEBUG",
            "buffer_limit": 100,
        },
        "dq_checks": [],
    }


@pytest.fixture
def seeded_config(spark, sample_config, test_catalog_schema):
    """Seed the config into a local Delta table and return the config dict.

    This mimics production: config lives in the Delta table, not YAML.
    Tests read it via ConfigManager.get_config().
    """
    catalog = test_catalog_schema["catalog"]
    schema = test_catalog_schema["schema"]
    config_table = f"{catalog}.{schema}.etl_job_config"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config_table} (
            job_name STRING NOT NULL,
            config_json STRING NOT NULL,
            version INT NOT NULL,
            is_active STRING NOT NULL,
            created_by STRING,
            created_ts TIMESTAMP,
            description STRING
        ) USING DELTA
    """)

    # Clear any previous test config
    spark.sql(f"DELETE FROM {config_table} WHERE job_name = 'test_job'")

    # Insert test config
    config_json = json.dumps(sample_config)
    row = spark.createDataFrame([{
        "job_name": "test_job",
        "config_json": config_json,
        "version": 1,
        "is_active": "Y",
        "created_by": "pytest",
        "created_ts": None,
        "description": "Test fixture config",
    }])
    row.write.format("delta").mode("append").saveAsTable(config_table)

    return sample_config


@pytest.fixture
def register_test_source(spark):
    """Register synthetic source data as a temp view.

    SourceReader with type="test_dataframe" reads from this view.
    Individual tests can override by registering their own view
    with the same name before calling SourceReader.read().
    """
    data = [
        (1, "Alice", "active", 100),
        (2, "Bob", "active", 200),
        (3, "Charlie", "inactive", 300),
        (None, "NullId", "active", 400),
        (5, "", "active", 0),
    ]
    df = spark.createDataFrame(data, ["id", "name", "status", "value"])
    df.createOrReplaceTempView("test_source_data")
    return df
