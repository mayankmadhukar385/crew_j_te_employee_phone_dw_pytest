"""Pytest configuration and fixtures for CREW_J_TE_EMPLOYEE_PHONE_DW tests.

All tests use source.type="test_dataframe" — synthetic data is registered as
Spark temp views.  No JDBC driver or external database is required.
"""

from __future__ import annotations

from typing import Any, Callable

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# SparkSession fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession for unit testing.

    Returns:
        A local-mode SparkSession with Delta Lake support disabled
        (not needed for unit tests that operate on DataFrames only).
    """
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("crew_j_te_employee_phone_dw_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Pipeline config fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pipeline_config() -> dict[str, Any]:
    """Return a minimal pipeline config that mirrors the etl_job_config Delta table entry.

    The config uses source.type="test_dataframe" so SourceReader reads from temp views.

    Returns:
        Dict matching the structure stored in etl_job_config.
    """
    return {
        "pipeline": {
            "job_name": "CREW_J_TE_EMPLOYEE_PHONE_DW",
            "workflow_name": "CREW_J_TE_EMPLOYEE_PHONE_DW",
            "run_mode": "full_refresh",
            "catalog": "test_catalog",
            "schema": "test_schema",
        },
        "source": {
            "type": "test_dataframe",
            "table": "CREW_WSTELE_LND",
            "test_view_name": "crew_wstele_lnd_test",
        },
        "targets": {
            "main": {
                "table": "TE_EMPLOYEE_PHONE_NEW",
                "write_mode": "truncate_load",
                "business_keys": ["EMP_NBR", "PH_NBR"],
            },
            "error": {
                "table": "TE_EMPLOYEE_PHONE_ERR",
                "write_mode": "append",
            },
        },
        "target": {
            "table": "TE_EMPLOYEE_PHONE_NEW",
            "write_mode": "truncate_load",
            "business_keys": ["EMP_NBR", "PH_NBR"],
        },
        "delta": {
            "optimize_after_write": False,
            "vacuum_retention_hours": 168,
        },
        "logging": {
            "level": "INFO",
            "buffer_limit": 1000,
        },
        "dq_checks": [],
    }


# ---------------------------------------------------------------------------
# Source schema fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def phone_source_schema() -> StructType:
    """Return the full source schema for CREW_WSTELE_LND (post-source-SQL aliasing).

    All phone number columns are StringType because concatenation in source SQL
    produces a string.  EMP_NBR is also StringType so that invalid-EMP_NBR test
    rows (e.g. 'ABC') can be represented without schema coercion.

    Returns:
        StructType matching the SELECT output of WSTELE_LND_TDC source SQL.
    """
    string = StringType()
    ts = TimestampType()

    basic_fields: list[StructField] = []
    for i in range(1, 6):
        basic_fields += [
            StructField(f"BASIC_PH_NBR_{i}", string),
            StructField(f"BASIC_PH_ACCESS_{i}", string),
            StructField(f"BASIC_PH_COMMENTS_{i}", string),
            StructField(f"BASIC_PH_TYPE_{i}", string),
            StructField(f"BASIC_PH_UNLIST_CD_{i}", string),
            StructField(f"BASIC_PH_HOME_AWAY_CD_{i}", string),
        ]

    home_fields: list[StructField] = []
    for g in range(1, 4):
        home_fields += [
            StructField(f"TELE_HOME_PRI_FROM_{g}", string),
            StructField(f"TELE_HOME_PRI_TO_{g}", string),
        ]
        for s in range(1, 6):
            home_fields.append(StructField(f"TELE_HOME_PRI_SEQ_{s}_{g}", string))

    away_fields: list[StructField] = []
    for g in range(1, 4):
        away_fields += [
            StructField(f"TELE_AWAY_PRI_FROM_{g}", string),
            StructField(f"TELE_AWAY_PRI_TO_{g}", string),
        ]
        for s in range(1, 6):
            away_fields.append(StructField(f"TELE_AWAY_PRI_SEQ_{s}_{g}", string))

    return StructType(
        [
            StructField("EMP_NBR", string),           # string to allow 'ABC' invalid rows
            StructField("USER_ID", string),
            StructField("TELE_LAST_UPDATED_DATE", string),
            StructField("TELE_LAST_UPDATED_TIME", string),
            StructField("PH_COMMENTS", string),
            StructField("PH_NBR", string),
            StructField("TEMP_PH_NBR", string),
            StructField("TEMP_PH_ACCESS", string),
            StructField("TEMP_PH_COMMENTS", string),
            StructField("TELE_TEMP_PH_DATE", string),
            StructField("TELE_TEMP_PH_TIME", string),
        ]
        + basic_fields
        + home_fields
        + away_fields
    )


# ---------------------------------------------------------------------------
# make_source_row fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def make_source_row() -> Callable[..., dict[str, Any]]:
    """Return a factory function that builds valid source row dicts with sensible defaults.

    The factory accepts keyword overrides for any column.  All unspecified columns
    default to None or typical valid values so callers need only specify the fields
    relevant to their test case.

    Returns:
        A callable that returns a dict representing one source row.

    Example::

        row = make_source_row(EMP_NBR="12345", PH_NBR="4085551234")
    """

    def _build_row(**overrides: Any) -> dict[str, Any]:
        """Build a source row dict with defaults, applying overrides.

        Args:
            **overrides: Column name → value pairs to override defaults.

        Returns:
            Dict suitable for passing to spark.createDataFrame([row], schema).
        """
        defaults: dict[str, Any] = {
            "EMP_NBR": "12345",
            "USER_ID": "SYSADM",
            "TELE_LAST_UPDATED_DATE": "251231",
            "TELE_LAST_UPDATED_TIME": "1430",
            "PH_COMMENTS": None,
            "PH_NBR": "4085551234",
            "TEMP_PH_NBR": None,
            "TEMP_PH_ACCESS": None,
            "TEMP_PH_COMMENTS": None,
            "TELE_TEMP_PH_DATE": None,
            "TELE_TEMP_PH_TIME": None,
        }
        # BASIC phone slots
        for i in range(1, 6):
            defaults[f"BASIC_PH_NBR_{i}"] = None
            defaults[f"BASIC_PH_ACCESS_{i}"] = None
            defaults[f"BASIC_PH_COMMENTS_{i}"] = None
            defaults[f"BASIC_PH_TYPE_{i}"] = None
            defaults[f"BASIC_PH_UNLIST_CD_{i}"] = None
            defaults[f"BASIC_PH_HOME_AWAY_CD_{i}"] = None

        # HOME priority sequence columns
        for g in range(1, 4):
            defaults[f"TELE_HOME_PRI_FROM_{g}"] = None
            defaults[f"TELE_HOME_PRI_TO_{g}"] = None
            for s in range(1, 6):
                defaults[f"TELE_HOME_PRI_SEQ_{s}_{g}"] = None

        # AWAY priority sequence columns
        for g in range(1, 4):
            defaults[f"TELE_AWAY_PRI_FROM_{g}"] = None
            defaults[f"TELE_AWAY_PRI_TO_{g}"] = None
            for s in range(1, 6):
                defaults[f"TELE_AWAY_PRI_SEQ_{s}_{g}"] = None

        defaults.update(overrides)
        return defaults

    return _build_row
