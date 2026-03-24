"""Shared pytest fixtures for the output/project test suite.

Sets up a local SparkSession with Delta Lake support and provides a minimal
pipeline config suitable for framework-level integration tests.

All framework tests that exercise Delta writes (DeltaManager, AuditLogger,
WatermarkManager) require Delta Lake extensions — hence the full Delta
configuration here, unlike the job-level conftest which runs without Delta.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# SparkSession fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession with Delta Lake support for framework tests.

    The warehouse dir uses a distinct path to avoid conflicts with other
    test sessions running simultaneously.

    Returns:
        A local-mode SparkSession with Delta Lake catalog extensions enabled.
    """
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("crew_dw_project_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-project-test")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Pipeline config fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def framework_test_config() -> dict[str, Any]:
    """Return a minimal pipeline config for framework unit tests.

    The config mirrors the structure stored in etl_job_config but uses
    test_catalog / test_schema so Delta tables are written to isolated paths.

    Returns:
        Dict matching the structure expected by DeltaManager, AuditLogger,
        WatermarkManager, and DQValidator constructors.
    """
    return {
        "pipeline": {
            "job_name": "TEST_FRAMEWORK_JOB",
            "workflow_name": "TEST_FRAMEWORK_WORKFLOW",
            "job_id": "test-job-001",
            "run_mode": "full_refresh",
            "catalog": "spark_catalog",
            "schema": "default",
        },
        "source": {
            "type": "test_dataframe",
            "table": "TEST_SOURCE",
            "test_view_name": "test_source_view",
        },
        "target": {
            "table": "TEST_TARGET",
            "write_mode": "truncate_load",
            "business_keys": ["id"],
        },
        "targets": {
            "main": {
                "table": "TEST_TARGET",
                "write_mode": "truncate_load",
                "business_keys": ["id"],
            },
        },
        "delta": {
            "optimize_after_write": False,
            "vacuum_retention_hours": 168,
        },
        "logging": {
            "level": "INFO",
            "buffer_limit": 100,
        },
        "dq_checks": [],
    }
