"""DQ Validator tests focused on CREW_J_TE_EMPLOYEE_PHONE_DW validation patterns.

Extends the base DQ validator test suite (tests/framework/test_dq_validator.py)
with phone-pipeline-specific scenarios covering:
  - EMP_NBR null/blank validation
  - PH_NBR long-castability validation
  - Combined multi-column DQ rules

These tests use native PySpark expressions — no ExpressionConverter wrapper.
"""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from framework.common_utility import DQValidator


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_phone_schema() -> StructType:
    """Return a minimal schema for phone DQ tests.

    Returns:
        StructType with EMP_NBR and PH_NBR as StringType columns.
    """
    return StructType([
        StructField("EMP_NBR", StringType(), True),
        StructField("PH_NBR",  StringType(), True),
    ])


# ---------------------------------------------------------------------------
# TestPhonePipelineDQRules
# ---------------------------------------------------------------------------

class TestPhonePipelineDQRules:
    """DQ rules specific to the CREW_J_TE_EMPLOYEE_PHONE_DW pipeline.

    These tests exercise the DQValidator with rules that mirror the pre-flight
    checks that would gate the CREW_WSTELE_LND source data before routing.
    """

    def test_emp_nbr_not_null(self, spark: SparkSession) -> None:
        """EMP_NBR must not be null — null rows go to the failed DataFrame.

        DQ rule: not_null on EMP_NBR.
        The pipeline routes null-EMP_NBR rows to the error table via
        route_by_phone_type; this test verifies the DQ check that would
        surface those rows before they reach the pipeline transform.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [{"column": "EMP_NBR", "rules": ["not_null"]}],
        }
        dq = DQValidator(spark, config)
        schema = _make_phone_schema()

        df = spark.createDataFrame(
            [
                ("12345", "4085551001"),   # valid
                (None,    "4085551002"),   # null EMP_NBR → failed
                ("",      "4085551003"),   # blank EMP_NBR → failed
                ("00100", "4085551004"),   # valid
            ],
            schema=schema,
        )

        passed, failed = dq.validate(df, stage="emp_nbr_check")

        assert passed.count() == 2
        assert failed.count() == 2

    def test_ph_nbr_not_null(self, spark: SparkSession) -> None:
        """PH_NBR must not be null — null-PH_NBR rows fail the DQ check.

        In the pipeline, null PH_NBR means the emergency routing condition
        (ISVALID("int64", PH_NBR)) would be false and those rows would not
        enter the emergency branch.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [{"column": "PH_NBR", "rules": ["not_null"]}],
        }
        dq = DQValidator(spark, config)
        schema = _make_phone_schema()

        df = spark.createDataFrame(
            [
                ("00100", "4085551001"),   # valid
                ("00101", None),            # null PH_NBR → failed
                ("00102", ""),              # blank PH_NBR → failed
            ],
            schema=schema,
        )

        passed, failed = dq.validate(df, stage="ph_nbr_check")

        assert passed.count() == 1
        assert failed.count() == 2

    def test_ph_nbr_valid_long(self, spark: SparkSession) -> None:
        """PH_NBR must be a valid 10-digit number matching a phone number pattern.

        Uses matches_regex DQ rule to validate PH_NBR format.
        10-digit US phone numbers are typically 10 consecutive digits.
        Non-numeric strings like "INVALID_NUM" or short strings should fail.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [
                {"column": "PH_NBR", "rules": ['matches_regex("^\\d{10}$")']}
            ],
        }
        dq = DQValidator(spark, config)
        schema = _make_phone_schema()

        df = spark.createDataFrame(
            [
                ("00100", "4085551001"),    # valid 10-digit
                ("00101", "INVALID_NUM"),   # alphabetic → failed
                ("00102", "408555"),        # too short → failed
                ("00103", "40855510011"),   # too long → failed
                ("00104", "5551112000"),    # valid 10-digit
            ],
            schema=schema,
        )

        passed, failed = dq.validate(df, stage="ph_nbr_format_check")

        assert passed.count() == 2   # rows 1 and 5
        assert failed.count() == 3   # rows 2, 3, 4

    def test_emp_nbr_numeric_pattern(self, spark: SparkSession) -> None:
        """EMP_NBR must be numeric — non-numeric values fail the DQ check.

        This pattern mirrors the ISVALID("INT32", EMP_NBR) routing condition.
        Using matches_regex("^\\d+$") to identify non-numeric EMP_NBR values
        before they reach the pipeline router.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [
                {"column": "EMP_NBR", "rules": ['matches_regex("^\\d+$")']}
            ],
        }
        dq = DQValidator(spark, config)
        schema = _make_phone_schema()

        df = spark.createDataFrame(
            [
                ("12345",  "4085551001"),   # numeric → passed
                ("00100",  "4085551002"),   # numeric with leading zeros → passed
                ("ABC",    "4085551003"),   # alphabetic → failed
                ("12-345", "4085551004"),   # contains dash → failed
                ("00200",  "4085551005"),   # valid
            ],
            schema=schema,
        )

        passed, failed = dq.validate(df, stage="emp_nbr_numeric_check")

        assert passed.count() == 3
        assert failed.count() == 2

    def test_multiple_checks_emp_nbr_and_ph_nbr(self, spark: SparkSession) -> None:
        """Combined DQ check: EMP_NBR not_null AND PH_NBR not_null.

        When multiple DQ checks are configured, a row fails if it violates
        ANY of the configured rules. This test verifies that a row with a null
        PH_NBR but valid EMP_NBR still goes to the failed partition.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [
                {"column": "EMP_NBR", "rules": ["not_null"]},
                {"column": "PH_NBR",  "rules": ["not_null"]},
            ],
        }
        dq = DQValidator(spark, config)
        schema = _make_phone_schema()

        df = spark.createDataFrame(
            [
                ("00100", "4085551001"),   # both valid → passed
                (None,    "4085551002"),   # null EMP_NBR → failed
                ("00102", None),            # null PH_NBR → failed
                (None,    None),            # both null → failed
                ("00104", "4085551004"),   # both valid → passed
            ],
            schema=schema,
        )

        passed, failed = dq.validate(df, stage="multi_col_check")

        assert passed.count() == 2
        assert failed.count() == 3

    def test_no_dq_checks_passes_all_rows(self, spark: SparkSession) -> None:
        """With an empty dq_checks list, all rows are passed through unchanged.

        The CREW_J_TE_EMPLOYEE_PHONE_DW pipeline_config fixture has
        dq_checks=[] by default. This test confirms DQValidator is a no-op
        when no rules are defined.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {"dq_checks": []}
        dq = DQValidator(spark, config)
        schema = _make_phone_schema()

        df = spark.createDataFrame(
            [
                ("00100", "4085551001"),
                (None,    None),
                ("ABC",   "INVALID"),
            ],
            schema=schema,
        )

        passed, failed = dq.validate(df, stage="no_op")

        assert passed.count() == 3
        assert failed.count() == 0

    def test_dq_failed_rows_contain_dq_error_column(self, spark: SparkSession) -> None:
        """Failed rows must have a _dq_error column explaining the failure.

        DQValidator adds a _dq_error string column to the failed DataFrame so
        that downstream error logging (DatabricksJobLogger.log_rejects) can
        surface the reason.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [{"column": "EMP_NBR", "rules": ["not_null"]}],
        }
        dq = DQValidator(spark, config)
        schema = _make_phone_schema()

        df = spark.createDataFrame(
            [("00100", "4085551001"), (None, "4085551002")],
            schema=schema,
        )

        _, failed = dq.validate(df, stage="error_col_test")

        assert failed.count() == 1
        assert "_dq_error" in failed.columns


# ---------------------------------------------------------------------------
# TestDQValidatorEdgeCases
# ---------------------------------------------------------------------------

class TestDQValidatorEdgeCases:
    """Edge-case DQ tests relevant to the phone pipeline data patterns."""

    def test_whitespace_only_emp_nbr_fails_not_null(self, spark: SparkSession) -> None:
        """Whitespace-only EMP_NBR ("   ") fails the not_null check.

        The not_null rule treats blank/whitespace strings as null-equivalent,
        which matches the DataStage TRIMLEADINGTRAILING → SETNULL pattern.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [{"column": "EMP_NBR", "rules": ["not_null"]}],
        }
        dq = DQValidator(spark, config)

        df = spark.createDataFrame(
            [("   ",), ("12345",)],
            schema=StructType([StructField("EMP_NBR", StringType(), True)]),
        )

        passed, failed = dq.validate(df, stage="whitespace_test")

        assert passed.count() == 1
        assert failed.count() == 1

    def test_large_emp_nbr_passes_not_null(self, spark: SparkSession) -> None:
        """An oversized EMP_NBR string ("99999999999999") still passes not_null.

        The not_null DQ rule only checks for null/blank — it does not validate
        int32 castability. The routing logic in route_by_phone_type handles the
        int32 overflow check separately.

        Args:
            spark: Session fixture from conftest.py.
        """
        config: dict[str, Any] = {
            "dq_checks": [{"column": "EMP_NBR", "rules": ["not_null"]}],
        }
        dq = DQValidator(spark, config)

        df = spark.createDataFrame(
            [("99999999999999",), ("12345",)],
            schema=StructType([StructField("EMP_NBR", StringType(), True)]),
        )

        passed, failed = dq.validate(df, stage="large_emp_test")

        # Both are non-null, non-blank → both pass not_null
        assert passed.count() == 2
        assert failed.count() == 0
