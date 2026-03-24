"""End-to-end integration tests for CREW_J_TE_EMPLOYEE_PHONE_DW.

Builds a 10-row source DataFrame covering all routing paths, runs the full
transform() method, and asserts correctness of main and error outputs.

No JDBC or external databases are required — all data is registered as
Spark temp views and processed entirely in-memory.
"""

from __future__ import annotations

from typing import Any, Callable

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from jobs.crew_j_te_employee_phone_dw.run_pipeline import CrewEmployeePhoneDwPipeline


# ---------------------------------------------------------------------------
# Helper to build the 10-row integration source DataFrame
# ---------------------------------------------------------------------------

def _build_integration_source(
    spark: SparkSession,
    schema: StructType,
    make_source_row: Callable[..., dict[str, Any]],
) -> DataFrame:
    """Build a 10-row synthetic source DataFrame covering all phone routing paths.

    Row breakdown:
        Row 1  — EMERGENCY: valid EMP_NBR + valid PH_NBR
        Row 2  — TEMP: valid EMP_NBR + valid TEMP_PH_NBR (no valid PH_NBR)
        Row 3  — BASIC-B: valid EMP_NBR + BASIC_PH_NBR_1 with HOME_AWAY_IND='B'
        Row 4  — BASIC-H: valid EMP_NBR + BASIC_PH_NBR_2 with HOME_AWAY_IND='H'
        Row 5  — BASIC-A: valid EMP_NBR + BASIC_PH_NBR_3 with HOME_AWAY_IND='A'
        Row 6  — HOME: valid EMP_NBR + HOME priority sequences (no AWAY sequences)
        Row 7  — AWAY: valid EMP_NBR + AWAY priority sequences (no HOME sequences)
        Row 8  — ERROR: invalid EMP_NBR='XYZ'
        Row 9  — ERROR: null EMP_NBR
        Row 10 — BASIC only (no HOME/AWAY): valid EMP_NBR, valid BASIC_PH_NBR, HOME_AWAY_IND=None

    Args:
        spark: Active SparkSession.
        schema: Source schema (from phone_source_schema fixture).
        make_source_row: Row factory from conftest.

    Returns:
        DataFrame with 10 rows matching the source schema.
    """
    rows = [
        # Row 1: EMERGENCY — valid EMP_NBR and valid PH_NBR
        make_source_row(
            EMP_NBR="10001",
            PH_NBR="4081111001",
            TELE_LAST_UPDATED_DATE="251231",
            TELE_LAST_UPDATED_TIME="0900",
        ),
        # Row 2: TEMP — valid EMP_NBR + valid TEMP_PH_NBR; PH_NBR invalid → not emergency
        make_source_row(
            EMP_NBR="10002",
            PH_NBR="NOTANUMBER",
            TEMP_PH_NBR="4082222001",
            TELE_TEMP_PH_DATE="251231",
            TELE_TEMP_PH_TIME="1000",
            TELE_LAST_UPDATED_DATE="251231",
            TELE_LAST_UPDATED_TIME="1000",
        ),
        # Row 3: BASIC-B — valid EMP_NBR; BASIC slot 1 with HOME_AWAY_IND='B'
        make_source_row(
            EMP_NBR="10003",
            PH_NBR="NOTANUMBER",
            BASIC_PH_NBR_1="4083333001",
            BASIC_PH_HOME_AWAY_CD_1="B",
            TELE_LAST_UPDATED_DATE="251231",
            TELE_LAST_UPDATED_TIME="1100",
        ),
        # Row 4: BASIC-H — valid EMP_NBR; BASIC slot 2 with HOME_AWAY_IND='H'
        make_source_row(
            EMP_NBR="10004",
            PH_NBR="NOTANUMBER",
            BASIC_PH_NBR_2="4084444002",
            BASIC_PH_HOME_AWAY_CD_2="H",
            TELE_LAST_UPDATED_DATE="251231",
            TELE_LAST_UPDATED_TIME="1200",
        ),
        # Row 5: BASIC-A — valid EMP_NBR; BASIC slot 3 with HOME_AWAY_IND='A'
        make_source_row(
            EMP_NBR="10005",
            PH_NBR="NOTANUMBER",
            BASIC_PH_NBR_3="4085555003",
            BASIC_PH_HOME_AWAY_CD_3="A",
            TELE_LAST_UPDATED_DATE="251231",
            TELE_LAST_UPDATED_TIME="1300",
        ),
        # Row 6: HOME — valid EMP_NBR; HOME priority sequence data
        make_source_row(
            EMP_NBR="10003",  # same as BASIC-B for join to work
            TELE_HOME_PRI_FROM_1="0800",
            TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_1_1="1",
        ),
        # Row 7: AWAY — valid EMP_NBR; AWAY priority sequence data
        make_source_row(
            EMP_NBR="10005",  # same as BASIC-A for join to work
            TELE_AWAY_PRI_FROM_1="0900",
            TELE_AWAY_PRI_TO_1="1800",
            TELE_AWAY_PRI_SEQ_1_1="3",
        ),
        # Row 8: ERROR — EMP_NBR is non-numeric string
        make_source_row(EMP_NBR="XYZ", PH_NBR="4088888001"),
        # Row 9: ERROR — null EMP_NBR
        make_source_row(EMP_NBR=None, PH_NBR="4089999001"),
        # Row 10: BASIC only — valid EMP_NBR; BASIC phone, no HOME_AWAY_IND set
        make_source_row(
            EMP_NBR="10010",
            BASIC_PH_NBR_1="4080000010",
            BASIC_PH_HOME_AWAY_CD_1=None,
            TELE_LAST_UPDATED_DATE="251231",
            TELE_LAST_UPDATED_TIME="1400",
        ),
    ]
    return spark.createDataFrame([list(r.values()) for r in rows], schema=schema)


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestTransformIntegration:
    """End-to-end integration tests for CrewEmployeePhoneDwPipeline.transform()."""

    @pytest.fixture(scope="class")
    def transform_results(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> dict[str, DataFrame]:
        """Run the full transform() once and cache results for all test methods.

        Returns:
            Dict with "main" and "error" DataFrames.
        """
        source_df = _build_integration_source(spark, phone_source_schema, make_source_row)

        # Instantiate pipeline without running full PipelineRunner lifecycle (no Delta writes)
        pipeline = CrewEmployeePhoneDwPipeline.__new__(CrewEmployeePhoneDwPipeline)
        results = pipeline.transform(source_df, pipeline_config)

        # Cache for repeated access across test methods
        results["main"] = results["main"].cache()
        results["error"] = results["error"].cache()
        return results

    # --- Main output assertions ---

    def test_main_output_is_non_empty(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """Main output must contain at least one row from valid phone records."""
        assert transform_results["main"].count() > 0

    def test_main_output_has_correct_columns(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """Main output must have exactly the 14 TE_EMPLOYEE_PHONE target columns."""
        expected_cols = {
            "EMP_NBR", "EMP_NO", "PH_LIST", "PH_PRTY",
            "EFF_START_TM", "EFF_END_TM", "PH_NBR", "PH_ACCESS",
            "PH_COMMENTS", "PH_TYPE", "UNLISTD_IND", "HOME_AWAY_IND",
            "TEMP_PH_EXP_TS", "TD_LD_TS",
        }
        assert set(transform_results["main"].columns) == expected_cols

    def test_main_output_has_no_invalid_ph_nbr(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """All PH_NBR values in main output must be castable to long (NULL_VAL_TFM filter)."""
        from pyspark.sql import functions as F
        invalid_rows = transform_results["main"].filter(F.col("PH_NBR").cast("long").isNull())
        assert invalid_rows.count() == 0

    def test_main_output_emp_no_padded(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """All EMP_NO values in main output are exactly 9 characters long."""
        from pyspark.sql import functions as F
        bad_emp_no = transform_results["main"].filter(F.length("EMP_NO") != 9)
        assert bad_emp_no.count() == 0

    def test_main_output_eff_start_tm_formatted(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """EFF_START_TM values in main output match HH:MM:SS format."""
        import re
        time_pattern = re.compile(r"^\d{2}:\d{2}:\d{2}$")
        rows = transform_results["main"].select("EFF_START_TM").collect()
        for r in rows:
            if r["EFF_START_TM"] is not None:
                assert time_pattern.match(r["EFF_START_TM"]), (
                    f"EFF_START_TM '{r['EFF_START_TM']}' does not match HH:MM:SS"
                )

    def test_main_output_ph_list_values(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """PH_LIST values in main output must be one of the known phone type constants."""
        valid_ph_lists = {"EMERGENCY", "TEMP", "BASIC", "HOME", "AWAY"}
        ph_lists = {r["PH_LIST"] for r in transform_results["main"].select("PH_LIST").collect()}
        assert ph_lists.issubset(valid_ph_lists)

    def test_main_output_td_ld_ts_not_null(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """TD_LD_TS must be non-null for all rows in main output."""
        from pyspark.sql import functions as F
        null_td_rows = transform_results["main"].filter(F.col("TD_LD_TS").isNull())
        assert null_td_rows.count() == 0

    def test_emergency_record_in_main(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """EMP_NBR=10001 (emergency) should appear in main output with PH_LIST='EMERGENCY'."""
        from pyspark.sql import functions as F
        emg_rows = transform_results["main"].filter(
            (F.col("EMP_NBR") == 10001) & (F.col("PH_LIST") == "EMERGENCY")
        )
        assert emg_rows.count() >= 1

    def test_temp_record_in_main(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """EMP_NBR=10002 (temp) should appear in main output with PH_LIST='TEMP'."""
        from pyspark.sql import functions as F
        temp_rows = transform_results["main"].filter(
            (F.col("EMP_NBR") == 10002) & (F.col("PH_LIST") == "TEMP")
        )
        assert temp_rows.count() >= 1

    # --- Error output assertions ---

    def test_error_output_contains_invalid_emp_nbr_rows(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """Error output must contain at least 2 rows (EMP_NBR='XYZ' and null EMP_NBR)."""
        assert transform_results["error"].count() >= 2

    def test_error_output_has_invalid_emp_nbr(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """Error output must include the row with EMP_NBR='XYZ'."""
        emp_nbrs = {r["EMP_NBR"] for r in transform_results["error"].select("EMP_NBR").collect()}
        assert "XYZ" in emp_nbrs

    def test_error_output_has_null_emp_nbr_row(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """Error output must include the row where EMP_NBR is null."""
        from pyspark.sql import functions as F
        null_emp_rows = transform_results["error"].filter(F.col("EMP_NBR").isNull())
        assert null_emp_rows.count() >= 1

    def test_no_overlap_between_main_and_error(
        self, transform_results: dict[str, DataFrame]
    ) -> None:
        """Valid EMP_NBR records should not appear in error output."""
        from pyspark.sql import functions as F
        # All EMP_NBR values in error output should NOT be castable to int
        valid_in_error = transform_results["error"].filter(F.col("EMP_NBR").cast("int").isNotNull())
        assert valid_in_error.count() == 0
