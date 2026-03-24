"""Unit tests for CREW_J_TE_EMPLOYEE_PHONE_DW transformation functions.

All tests use synthetic DataFrames — no JDBC or external databases required.
Uses chispa for DataFrame equality assertions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.crew_j_te_employee_phone_dw.transformations import (
    add_away_seq_offset,
    add_home_seq_offset,
    adjust_priority,
    aggregate_max_priority,
    enrich_basic_records,
    funnel_sequences,
    increment_priority,
    join_home_records,
    join_max_priority,
    merge_all_records,
    pivot_away_sequence,
    pivot_basic_phones,
    pivot_home_sequence,
    route_by_phone_type,
    split_basic_by_home_away,
    validate_and_finalize,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source_df(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    schema: StructType,
) -> DataFrame:
    """Create a source DataFrame from a list of row dicts and schema.

    Args:
        spark: Active SparkSession.
        rows: List of row dicts.
        schema: StructType schema.

    Returns:
        Spark DataFrame.
    """
    return spark.createDataFrame([list(r.values()) for r in rows], schema=schema)


# ---------------------------------------------------------------------------
# TestRouteByPhoneType
# ---------------------------------------------------------------------------

class TestRouteByPhoneType:
    """Tests for route_by_phone_type (SEPARATE_PHTYPE_TFM)."""

    def _base_row(self, make_source_row: Callable[..., dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
        return make_source_row(**kwargs)

    def test_emergency_routing(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Valid EMP_NBR and valid PH_NBR → row appears in emergency_df with CALL_LIST='EMERGENCY'."""
        row = make_source_row(EMP_NBR="12345", PH_NBR="4085551234")
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        emergency_df, *_ = route_by_phone_type(source_df, pipeline_config)
        result = emergency_df.collect()

        assert len(result) == 1
        assert result[0]["CALL_LIST"] == "EMERGENCY"
        assert result[0]["CALL_PRTY"] == 1
        assert result[0]["EFF_START_TM"] == "0000"
        assert result[0]["EFF_END_TM"] == "2359"

    def test_error_routing(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """EMP_NBR='ABC' (not valid int32) → row appears in error_df only."""
        row = make_source_row(EMP_NBR="ABC", PH_NBR="4085551234")
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        emergency_df, temp_df, basic_df, home_df, away_df, error_df = route_by_phone_type(
            source_df, pipeline_config
        )

        assert error_df.count() == 1
        assert emergency_df.count() == 0
        assert temp_df.count() == 0
        assert basic_df.count() == 0

    def test_basic_routing(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Valid EMP_NBR with invalid PH_NBR → goes to basic_df (EMP_NBR validity only required)."""
        row = make_source_row(EMP_NBR="12345", PH_NBR="NOTANUMBER")
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        _, _, basic_df, _, _, error_df = route_by_phone_type(source_df, pipeline_config)

        assert basic_df.count() == 1
        result = basic_df.collect()[0]
        assert result["CALL_LIST"] == "BASIC"
        assert result["EFF_START_TM"] == "0001"
        assert error_df.count() == 0

    def test_temp_routing(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Valid EMP_NBR and valid TEMP_PH_NBR → row appears in temp_df with CALL_LIST='TEMP'."""
        row = make_source_row(EMP_NBR="12345", PH_NBR="NOTANUMBER", TEMP_PH_NBR="4085559999")
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        _, temp_df, _, _, _, _ = route_by_phone_type(source_df, pipeline_config)

        assert temp_df.count() == 1
        result = temp_df.collect()[0]
        assert result["CALL_LIST"] == "TEMP"
        assert result["CALL_PRTY"] == 1

    def test_null_emp_nbr_to_error(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Null EMP_NBR → error_df (IsValid("int32", null) = False)."""
        row = make_source_row(EMP_NBR=None, PH_NBR="4085551234")
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        _, _, _, _, _, error_df = route_by_phone_type(source_df, pipeline_config)

        assert error_df.count() == 1

    def test_emergency_sets_null_fields(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """EMERGENCY records should have PH_ACCESS, PH_TYPE, UNLISTD_IND, HOME_AWAY_IND, TEMP_PH_EXP_TS all null."""
        row = make_source_row(EMP_NBR="12345", PH_NBR="4085551234")
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        emergency_df, *_ = route_by_phone_type(source_df, pipeline_config)
        result = emergency_df.collect()[0]

        assert result["PH_ACCESS"] is None
        assert result["PH_TYPE"] is None
        assert result["UNLISTD_IND"] is None
        assert result["HOME_AWAY_IND"] is None
        assert result["TEMP_PH_EXP_TS"] is None

    def test_upd_ts_valid(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Valid TELE_LAST_UPDATED_DATE='251231' TIME='1430' → UPD_TS='2025-12-31 14:30:00'."""
        row = make_source_row(
            EMP_NBR="12345",
            PH_NBR="4085551234",
            TELE_LAST_UPDATED_DATE="251231",
            TELE_LAST_UPDATED_TIME="1430",
        )
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        emergency_df, *_ = route_by_phone_type(source_df, pipeline_config)
        result = emergency_df.collect()[0]

        assert result["UPD_TS"] is not None
        assert result["UPD_TS"] == datetime(2025, 12, 31, 14, 30, 0)

    def test_upd_ts_blank_date_gives_null(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Blank TELE_LAST_UPDATED_DATE → UPD_TS is null."""
        row = make_source_row(
            EMP_NBR="12345",
            PH_NBR="4085551234",
            TELE_LAST_UPDATED_DATE="",
            TELE_LAST_UPDATED_TIME="1430",
        )
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        emergency_df, *_ = route_by_phone_type(source_df, pipeline_config)
        result = emergency_df.collect()[0]

        assert result["UPD_TS"] is None

    def test_temp_ph_exp_ts_valid(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """TEMP record with TELE_TEMP_PH_DATE='251231' TIME='1430' → TEMP_PH_EXP_TS='2025-12-31 14:30:00'."""
        row = make_source_row(
            EMP_NBR="12345",
            TEMP_PH_NBR="4085559999",
            TELE_TEMP_PH_DATE="251231",
            TELE_TEMP_PH_TIME="1430",
        )
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        _, temp_df, *_ = route_by_phone_type(source_df, pipeline_config)
        result = temp_df.collect()[0]

        assert result["TEMP_PH_EXP_TS"] == datetime(2025, 12, 31, 14, 30, 0)

    def test_temp_ph_exp_ts_blank_date_gives_null(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """TEMP record with blank TELE_TEMP_PH_DATE → TEMP_PH_EXP_TS is null."""
        row = make_source_row(
            EMP_NBR="12345",
            TEMP_PH_NBR="4085559999",
            TELE_TEMP_PH_DATE="",
            TELE_TEMP_PH_TIME="1430",
        )
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        _, temp_df, *_ = route_by_phone_type(source_df, pipeline_config)
        result = temp_df.collect()[0]

        assert result["TEMP_PH_EXP_TS"] is None

    def test_away_columns_remapped_to_home_names(
        self,
        spark: SparkSession,
        phone_source_schema: StructType,
        make_source_row: Callable[..., dict[str, Any]],
        pipeline_config: dict[str, Any],
    ) -> None:
        """AWAY output remaps TELE_AWAY_PRI_FROM_1 → TELE_HOME_PRI_FROM_1 (AWAY_TYPE_CPY)."""
        row = make_source_row(EMP_NBR="12345", TELE_AWAY_PRI_FROM_1="0800")
        source_df = spark.createDataFrame([list(row.values())], schema=phone_source_schema)

        _, _, _, _, away_df, _ = route_by_phone_type(source_df, pipeline_config)

        assert "TELE_HOME_PRI_FROM_1" in away_df.columns
        assert "TELE_AWAY_PRI_FROM_1" not in away_df.columns
        result = away_df.collect()[0]
        assert result["TELE_HOME_PRI_FROM_1"] == "0800"


# ---------------------------------------------------------------------------
# TestPivotBasicPhones
# ---------------------------------------------------------------------------

class TestPivotBasicPhones:
    """Tests for pivot_basic_phones (BASIC_TYPE_PVT)."""

    def _make_basic_df(self, spark: SparkSession, emp_nbr: str = "12345") -> DataFrame:
        """Create a minimal basic DataFrame with 5 phone slots."""
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("CALL_LIST", StringType()),
            StructField("EFF_START_TM", StringType()),
            StructField("EFF_END_TM", StringType()),
            StructField("USER_ID", StringType()),
            StructField("UPD_TS", TimestampType()),
            StructField("BASIC_PH_NBR_1", StringType()),
            StructField("BASIC_PH_ACCESS_1", StringType()),
            StructField("BASIC_PH_COMMENT_1", StringType()),
            StructField("BASIC_PH_TYPE_1", StringType()),
            StructField("BASIC_PH_UNLIST_CD_1", StringType()),
            StructField("BASIC_PH_HOME_AWAY_CD_1", StringType()),
            StructField("TEMP_PH_EXP_TS_1", TimestampType()),
            StructField("BASIC_PH_NBR_2", StringType()),
            StructField("BASIC_PH_ACCESS_2", StringType()),
            StructField("BASIC_PH_COMMENT_2", StringType()),
            StructField("BASIC_PH_TYPE_2", StringType()),
            StructField("BASIC_PH_UNLIST_CD_2", StringType()),
            StructField("BASIC_PH_HOME_AWAY_CD_2", StringType()),
            StructField("TEMP_PH_EXP_TS_2", TimestampType()),
            StructField("BASIC_PH_NBR_3", StringType()),
            StructField("BASIC_PH_ACCESS_3", StringType()),
            StructField("BASIC_PH_COMMENT_3", StringType()),
            StructField("BASIC_PH_TYPE_3", StringType()),
            StructField("BASIC_PH_UNLIST_CD_3", StringType()),
            StructField("BASIC_PH_HOME_AWAY_CD_3", StringType()),
            StructField("TEMP_PH_EXP_TS_3", TimestampType()),
            StructField("BASIC_PH_NBR_4", StringType()),
            StructField("BASIC_PH_ACCESS_4", StringType()),
            StructField("BASIC_PH_COMMENT_4", StringType()),
            StructField("BASIC_PH_TYPE_4", StringType()),
            StructField("BASIC_PH_UNLIST_CD_4", StringType()),
            StructField("BASIC_PH_HOME_AWAY_CD_4", StringType()),
            StructField("TEMP_PH_EXP_TS_4", TimestampType()),
            StructField("BASIC_PH_NBR_5", StringType()),
            StructField("BASIC_PH_ACCESS_5", StringType()),
            StructField("BASIC_PH_COMMENT_5", StringType()),
            StructField("BASIC_PH_TYPE_5", StringType()),
            StructField("BASIC_PH_UNLIST_CD_5", StringType()),
            StructField("BASIC_PH_HOME_AWAY_CD_5", StringType()),
            StructField("TEMP_PH_EXP_TS_5", TimestampType()),
        ])
        row = (
            emp_nbr, "BASIC", "0001", "2359", "SYSADM", None,
            "4085551111", None, None, "WORK", None, "B", None,
            "4085552222", None, None, "HOME", None, "H", None,
            None, None, None, None, None, "A", None,
            "4085554444", None, None, "WORK", None, "B", None,
            "4085555555", None, None, "HOME", None, "H", None,
        )
        return spark.createDataFrame([row], schema=schema)

    def test_five_slots_become_five_rows(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """One employee with 5 BASIC phone columns → 5 rows after pivot."""
        basic_df = self._make_basic_df(spark)
        result = pivot_basic_phones(basic_df, pipeline_config)

        assert result.count() == 5

    def test_call_prty_idx_is_zero_based(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """CALL_PRTY_IDX should range from 0 to 4 after pivot."""
        basic_df = self._make_basic_df(spark)
        result = pivot_basic_phones(basic_df, pipeline_config)
        idxs = sorted([r["CALL_PRTY_IDX"] for r in result.collect()])

        assert idxs == [0, 1, 2, 3, 4]

    def test_null_phone_preserved(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Phone slot 3 (index 2) has null PH_NBR → row 3 has PH_NBR=null."""
        basic_df = self._make_basic_df(spark)
        result = pivot_basic_phones(basic_df, pipeline_config)
        rows = {r["CALL_PRTY_IDX"]: r for r in result.collect()}

        assert rows[2]["PH_NBR"] is None

    def test_pass_through_columns_preserved(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EMP_NBR, CALL_LIST, EFF_START_TM, EFF_END_TM identical on all 5 rows."""
        basic_df = self._make_basic_df(spark)
        result = pivot_basic_phones(basic_df, pipeline_config)
        rows = result.collect()

        for r in rows:
            assert r["EMP_NBR"] == "12345"
            assert r["CALL_LIST"] == "BASIC"
            assert r["EFF_START_TM"] == "0001"
            assert r["EFF_END_TM"] == "2359"


# ---------------------------------------------------------------------------
# TestEnrichBasicRecords
# ---------------------------------------------------------------------------

class TestEnrichBasicRecords:
    """Tests for enrich_basic_records (BASIC_REC_TFM column derivations)."""

    def _make_pivoted_df(self, spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("CALL_LIST", StringType()),
            StructField("EFF_START_TM", StringType()),
            StructField("EFF_END_TM", StringType()),
            StructField("USER_ID", StringType()),
            StructField("UPD_TS", TimestampType()),
            StructField("PH_NBR", StringType()),
            StructField("PH_ACCESS", StringType()),
            StructField("PH_COMMENTS", StringType()),
            StructField("PH_TYPE", StringType()),
            StructField("UNLISTD_IND", StringType()),
            StructField("HOME_AWAY_IND", StringType()),
            StructField("TEMP_PH_EXP_TS", TimestampType()),
            StructField("CALL_PRTY_IDX", IntegerType()),
        ])
        rows = [
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085551111", None, None, "WORK", None, "B", None, 0),
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085552222", None, None, "HOME", None, "H", None, 1),
            ("12345", "BASIC", "0001", "2359", "U1", None, None,         None, None, None,   None, "A", None, 2),
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085554444", None, None, "WORK", None, "B", None, 3),
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085555555", None, None, "HOME", None, "H", None, 4),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_call_prty_incremented(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """CALL_PRTY_IDX=0 → CALL_PRTY=1; IDX=4 → CALL_PRTY=5."""
        df = self._make_pivoted_df(spark)
        result = enrich_basic_records(df, pipeline_config)
        rows = sorted(result.collect(), key=lambda r: r["CALL_PRTY"])

        assert rows[0]["CALL_PRTY"] == 1
        assert rows[4]["CALL_PRTY"] == 5

    def test_lkp_call_prty_equals_call_prty(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """LKP_CALL_PRTY == CALL_PRTY for all rows."""
        df = self._make_pivoted_df(spark)
        result = enrich_basic_records(df, pipeline_config)
        for r in result.collect():
            assert r["LKP_CALL_PRTY"] == r["CALL_PRTY"]

    def test_call_prty_idx_dropped(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """CALL_PRTY_IDX column must be dropped from output."""
        df = self._make_pivoted_df(spark)
        result = enrich_basic_records(df, pipeline_config)
        assert "CALL_PRTY_IDX" not in result.columns


# ---------------------------------------------------------------------------
# TestSplitBasicByHomeAway
# ---------------------------------------------------------------------------

class TestSplitBasicByHomeAway:
    """Tests for split_basic_by_home_away (BASIC_REC_TFM 5-way routing)."""

    def _make_enriched_df(self, spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("CALL_LIST", StringType()),
            StructField("EFF_START_TM", StringType()),
            StructField("EFF_END_TM", StringType()),
            StructField("USER_ID", StringType()),
            StructField("UPD_TS", TimestampType()),
            StructField("PH_NBR", StringType()),
            StructField("PH_ACCESS", StringType()),
            StructField("PH_COMMENTS", StringType()),
            StructField("PH_TYPE", StringType()),
            StructField("UNLISTD_IND", StringType()),
            StructField("HOME_AWAY_IND", StringType()),
            StructField("TEMP_PH_EXP_TS", TimestampType()),
            StructField("CALL_PRTY", IntegerType()),
            StructField("LKP_CALL_PRTY", IntegerType()),
        ])
        rows = [
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085551111", None, None, "WORK", None, "B", None, 1, 1),
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085552222", None, None, "HOME", None, "H", None, 2, 2),
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085553333", None, None, "AWAY", None, "A", None, 3, 3),
            ("12345", "BASIC", "0001", "2359", "U1", None, "NOTANUMBER", None, None, "WORK", None, "B", None, 4, 4),
            ("12345", "BASIC", "0001", "2359", "U1", None, "4085555555", None, None, "HOME", None, None, None, 5, 5),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_all_basic_filters_by_valid_ph_nbr(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """all_basic_df: only rows where PH_NBR castable to long."""
        df = self._make_enriched_df(spark)
        all_basic_df, *_ = split_basic_by_home_away(df, pipeline_config)

        ph_nbrs = [r["PH_NBR"] for r in all_basic_df.collect()]
        assert "NOTANUMBER" not in ph_nbrs

    def test_home_jnr_b_and_h(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """basic_home_jnr_df includes HOME_AWAY_IND='B' and 'H', excludes 'A'."""
        df = self._make_enriched_df(spark)
        _, basic_home_jnr_df, _, _, _ = split_basic_by_home_away(df, pipeline_config)
        inds = {r["HOME_AWAY_IND"] for r in basic_home_jnr_df.collect()}

        assert "A" not in inds
        assert inds.issubset({"B", "H"})

    def test_home_away_b_in_both_branches(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """HOME_AWAY_IND='B' appears in both home_jnr and away_jnr branches."""
        df = self._make_enriched_df(spark)
        _, basic_home_jnr_df, _, basic_away_jnr_df, _ = split_basic_by_home_away(df, pipeline_config)

        home_ph = {r["PH_NBR"] for r in basic_home_jnr_df.collect()}
        away_ph = {r["PH_NBR"] for r in basic_away_jnr_df.collect()}
        # PH_NBR="4085551111" has HOME_AWAY_IND='B'
        assert "4085551111" in home_ph
        assert "4085551111" in away_ph

    def test_home_away_h_only_in_home_branch(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """HOME_AWAY_IND='H' → only in home branches, not away."""
        df = self._make_enriched_df(spark)
        _, basic_home_jnr_df, _, basic_away_jnr_df, _ = split_basic_by_home_away(df, pipeline_config)

        home_inds = {r["HOME_AWAY_IND"] for r in basic_home_jnr_df.collect()}
        away_inds = {r["HOME_AWAY_IND"] for r in basic_away_jnr_df.collect()}

        assert "H" in home_inds
        assert "H" not in away_inds

    def test_home_away_a_only_in_away_branch(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """HOME_AWAY_IND='A' → only in away branches, not home."""
        df = self._make_enriched_df(spark)
        _, basic_home_jnr_df, _, basic_away_jnr_df, _ = split_basic_by_home_away(df, pipeline_config)

        home_inds = {r["HOME_AWAY_IND"] for r in basic_home_jnr_df.collect()}
        away_inds = {r["HOME_AWAY_IND"] for r in basic_away_jnr_df.collect()}

        assert "A" not in home_inds
        assert "A" in away_inds


# ---------------------------------------------------------------------------
# TestPivotHomeSequence
# ---------------------------------------------------------------------------

class TestPivotHomeSequence:
    """Tests for pivot_home_sequence and add_home_seq_offset."""

    def _make_home_df(self, spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("TELE_HOME_PRI_FROM_1", StringType()),
            StructField("TELE_HOME_PRI_TO_1", StringType()),
            StructField("TELE_HOME_PRI_SEQ_1_1", StringType()),
            StructField("TELE_HOME_PRI_SEQ_2_1", StringType()),
            StructField("TELE_HOME_PRI_SEQ_3_1", StringType()),
            StructField("TELE_HOME_PRI_SEQ_4_1", StringType()),
            StructField("TELE_HOME_PRI_SEQ_5_1", StringType()),
            StructField("TELE_HOME_PRI_FROM_2", StringType()),
            StructField("TELE_HOME_PRI_TO_2", StringType()),
            StructField("TELE_HOME_PRI_SEQ_1_2", StringType()),
            StructField("TELE_HOME_PRI_SEQ_2_2", StringType()),
            StructField("TELE_HOME_PRI_SEQ_3_2", StringType()),
            StructField("TELE_HOME_PRI_SEQ_4_2", StringType()),
            StructField("TELE_HOME_PRI_SEQ_5_2", StringType()),
            StructField("TELE_HOME_PRI_FROM_3", StringType()),
            StructField("TELE_HOME_PRI_TO_3", StringType()),
            StructField("TELE_HOME_PRI_SEQ_1_3", StringType()),
            StructField("TELE_HOME_PRI_SEQ_2_3", StringType()),
            StructField("TELE_HOME_PRI_SEQ_3_3", StringType()),
            StructField("TELE_HOME_PRI_SEQ_4_3", StringType()),
            StructField("TELE_HOME_PRI_SEQ_5_3", StringType()),
        ])
        row = ("12345", "0800", "1700", "1", "2", "3", "4", "5",
               "0900", "1800", "6", "7", "8", "9", "10",
               "1000", "1900", "11", "12", "13", "14", "15")
        return spark.createDataFrame([row], schema=schema)

    def test_home_seq1_pivot_produces_five_rows(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """pivot_home_sequence with seq_num=1 → 5 rows."""
        home_df = self._make_home_df(spark)
        result = pivot_home_sequence(home_df, seq_num=1, config=pipeline_config)

        assert result.count() == 5

    def test_seq2_offset_adds_5(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """CALL_PRTY=0 (first row of SEQ2 pivot) → after SEQ2_TFM offset: CALL_PRTY=5."""
        home_df = self._make_home_df(spark)
        seq2_pivoted = pivot_home_sequence(home_df, seq_num=2, config=pipeline_config)
        seq2_offset = add_home_seq_offset(seq2_pivoted, offset=5, config=pipeline_config)

        prty_vals = sorted([r["CALL_PRTY"] for r in seq2_offset.collect()])
        assert prty_vals == [5, 6, 7, 8, 9]

    def test_seq3_offset_adds_10(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """CALL_PRTY=0 (first row of SEQ3 pivot) → after SEQ3_TFM offset: CALL_PRTY=10."""
        home_df = self._make_home_df(spark)
        seq3_pivoted = pivot_home_sequence(home_df, seq_num=3, config=pipeline_config)
        seq3_offset = add_home_seq_offset(seq3_pivoted, offset=10, config=pipeline_config)

        prty_vals = sorted([r["CALL_PRTY"] for r in seq3_offset.collect()])
        assert prty_vals == [10, 11, 12, 13, 14]

    def test_funnel_sequences_produces_15_rows(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """3 groups × 5 rows each = 15 rows after funnel."""
        home_df = self._make_home_df(spark)
        seq1 = pivot_home_sequence(home_df, seq_num=1, config=pipeline_config)
        seq2 = add_home_seq_offset(pivot_home_sequence(home_df, seq_num=2, config=pipeline_config), 5, pipeline_config)
        seq3 = add_home_seq_offset(pivot_home_sequence(home_df, seq_num=3, config=pipeline_config), 10, pipeline_config)
        result = funnel_sequences(seq1, seq2, seq3, pipeline_config)

        assert result.count() == 15


# ---------------------------------------------------------------------------
# TestAggregateMaxPriority
# ---------------------------------------------------------------------------

class TestAggregateMaxPriority:
    """Tests for aggregate_max_priority (MAX_PRTY_AGG / MAX_ABPRTY_AGG)."""

    def _make_cprty_df(self, spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("CALL_PRTY", IntegerType()),
        ])
        rows = [
            ("11111", 1), ("11111", 2), ("11111", 3),
            ("22222", 1), ("22222", 2),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_group_by_emp_nbr(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """2 distinct EMP_NBR values → 2 rows in aggregate output."""
        df = self._make_cprty_df(spark)
        result = aggregate_max_priority(df, pipeline_config)

        assert result.count() == 2

    def test_max_call_prty_correct(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EMP_NBR=11111 has CALL_PRTY 1,2,3 → MAX_CALL_PRTY=3."""
        df = self._make_cprty_df(spark)
        result = aggregate_max_priority(df, pipeline_config)
        rows = {r["EMP_NBR"]: r["MAX_CALL_PRTY"] for r in result.collect()}

        assert rows["11111"] == 3
        assert rows["22222"] == 2


# ---------------------------------------------------------------------------
# TestAdjustPriority
# ---------------------------------------------------------------------------

class TestAdjustPriority:
    """Tests for adjust_priority (ADJUSTING_PRTY_TFM / ADJUST_PRTY_TFM)."""

    def _make_joined_max_df(self, spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("CALL_PRTY", IntegerType()),
            StructField("MAX_CALL_PRTY", IntegerType()),
            StructField("PH_NBR", StringType()),
            StructField("CALL_LIST", StringType()),
            StructField("EFF_START_TM", StringType()),
            StructField("EFF_END_TM", StringType()),
            StructField("PH_ACCESS", StringType()),
            StructField("PH_COMMENTS", StringType()),
            StructField("PH_TYPE", StringType()),
            StructField("UNLISTD_IND", StringType()),
            StructField("HOME_AWAY_IND", StringType()),
            StructField("TEMP_PH_EXP_TS", TimestampType()),
            StructField("USER_ID", StringType()),
            StructField("UPD_TS", TimestampType()),
        ])
        rows = [
            ("12345", 1, 3, "4085551111", "BASIC", "0001", "2359", None, None, "WORK", None, "H", None, "U1", None),
            ("12345", 2, 3, "4085552222", "BASIC", "0001", "2359", None, None, "HOME", None, "H", None, "U1", None),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_adj_prty_starts_at_max_plus_1(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """First row for EMP_NBR: CALL_PRTY = MAX_CALL_PRTY + 1 = 3 + 1 = 4."""
        df = self._make_joined_max_df(spark)
        result = adjust_priority(df, pipeline_config)
        rows = sorted(result.collect(), key=lambda r: r["CALL_PRTY"])

        assert rows[0]["CALL_PRTY"] == 4

    def test_adj_prty_increments_within_group(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Second row for same EMP_NBR: CALL_PRTY = MAX_CALL_PRTY + 2 = 5."""
        df = self._make_joined_max_df(spark)
        result = adjust_priority(df, pipeline_config)
        rows = sorted(result.collect(), key=lambda r: r["CALL_PRTY"])

        assert rows[1]["CALL_PRTY"] == 5

    def test_max_call_prty_dropped(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """MAX_CALL_PRTY should be dropped from the output."""
        df = self._make_joined_max_df(spark)
        result = adjust_priority(df, pipeline_config)

        assert "MAX_CALL_PRTY" not in result.columns

    def test_eff_start_and_end_set_to_constants(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EFF_START_TM='0001' and EFF_END_TM='2359' (verbatim from XML derivations)."""
        df = self._make_joined_max_df(spark)
        result = adjust_priority(df, pipeline_config)
        for r in result.collect():
            assert r["EFF_START_TM"] == "0001"
            assert r["EFF_END_TM"] == "2359"


# ---------------------------------------------------------------------------
# TestValidateAndFinalize
# ---------------------------------------------------------------------------

class TestValidateAndFinalize:
    """Tests for validate_and_finalize (NULL_VAL_TFM)."""

    def _make_all_df(
        self,
        spark: SparkSession,
        emp_nbr: str = "12345",
        ph_nbr: str = "4085551234",
        call_list: str = "EMERGENCY",
        call_prty: int = 1,
        eff_start: str = "0000",
        eff_end: str = "2359",
        ph_access: str | None = None,
        ph_comments: str | None = None,
    ) -> DataFrame:
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("CALL_LIST", StringType()),
            StructField("CALL_PRTY", IntegerType()),
            StructField("EFF_START_TM", StringType()),
            StructField("EFF_END_TM", StringType()),
            StructField("PH_NBR", StringType()),
            StructField("PH_ACCESS", StringType()),
            StructField("PH_COMMENTS", StringType()),
            StructField("PH_TYPE", StringType()),
            StructField("UNLISTD_IND", StringType()),
            StructField("HOME_AWAY_IND", StringType()),
            StructField("TEMP_PH_EXP_TS", TimestampType()),
        ])
        row = (emp_nbr, call_list, call_prty, eff_start, eff_end, ph_nbr, ph_access, ph_comments, None, None, None, None)
        return spark.createDataFrame([row], schema=schema)

    def test_invalid_ph_nbr_filtered(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """PH_NBR='ABC' (not castable to long) → row excluded."""
        df = self._make_all_df(spark, ph_nbr="ABC")
        result = validate_and_finalize(df, pipeline_config)

        assert result.count() == 0

    def test_emp_no_zero_padded(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EMP_NBR='1234567' (7 chars) → EMP_NO='001234567' (padded to 9 chars)."""
        df = self._make_all_df(spark, emp_nbr="1234567")
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["EMP_NO"] == "001234567"

    def test_emp_no_no_pad_for_9_chars(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EMP_NBR='123456789' (9 chars) → EMP_NO='123456789' (no padding)."""
        df = self._make_all_df(spark, emp_nbr="123456789")
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["EMP_NO"] == "123456789"

    def test_eff_time_formatted(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EFF_START_TM='0830' → '08:30:00'; EFF_END_TM='2359' → '23:59:00'."""
        df = self._make_all_df(spark, eff_start="0830", eff_end="2359")
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["EFF_START_TM"] == "08:30:00"
        assert row["EFF_END_TM"] == "23:59:00"

    def test_eff_start_0000_formatted(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EFF_START_TM='0000' → '00:00:00'."""
        df = self._make_all_df(spark, eff_start="0000")
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["EFF_START_TM"] == "00:00:00"

    def test_ph_access_blank_nulled(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """PH_ACCESS='   ' (blank) → null in output."""
        df = self._make_all_df(spark, ph_access="   ")
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["PH_ACCESS"] is None

    def test_ph_access_trimmed_if_valid(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """PH_ACCESS='  5551  ' → '5551' (trimmed)."""
        df = self._make_all_df(spark, ph_access="  5551  ")
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["PH_ACCESS"] == "5551"

    def test_ph_comments_blank_nulled(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """PH_COMMENTS=' ' (single space) → null."""
        df = self._make_all_df(spark, ph_comments=" ")
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["PH_COMMENTS"] is None

    def test_td_ld_ts_is_timestamp(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """TD_LD_TS must be a non-null timestamp (CurrentTimestamp())."""
        df = self._make_all_df(spark)
        result = validate_and_finalize(df, pipeline_config)
        row = result.collect()[0]

        assert row["TD_LD_TS"] is not None

    def test_call_list_renamed_to_ph_list(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Output column PH_LIST present; CALL_LIST absent."""
        df = self._make_all_df(spark)
        result = validate_and_finalize(df, pipeline_config)

        assert "PH_LIST" in result.columns
        assert "CALL_LIST" not in result.columns

    def test_call_prty_renamed_to_ph_prty(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Output column PH_PRTY present; CALL_PRTY absent."""
        df = self._make_all_df(spark)
        result = validate_and_finalize(df, pipeline_config)

        assert "PH_PRTY" in result.columns
        assert "CALL_PRTY" not in result.columns

    def test_output_has_14_columns(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Final output must have exactly 14 target columns."""
        expected_cols = {
            "EMP_NBR", "EMP_NO", "PH_LIST", "PH_PRTY",
            "EFF_START_TM", "EFF_END_TM", "PH_NBR", "PH_ACCESS",
            "PH_COMMENTS", "PH_TYPE", "UNLISTD_IND", "HOME_AWAY_IND",
            "TEMP_PH_EXP_TS", "TD_LD_TS",
        }
        df = self._make_all_df(spark)
        result = validate_and_finalize(df, pipeline_config)

        assert set(result.columns) == expected_cols


# ---------------------------------------------------------------------------
# TestJoinHomeRecords
# ---------------------------------------------------------------------------

class TestJoinHomeRecords:
    """Tests for join_home_records (HOME_BASIC_JNR / AWAY_BASIC_JNR)."""

    def _make_seq_df(self, spark: SparkSession) -> DataFrame:
        """Build a minimal incremented sequence DataFrame (left side of join)."""
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("LKP_CALL_PRTY", IntegerType()),
            StructField("CALL_PRTY", IntegerType()),
            StructField("TELE_HOME_PRI_FROM", StringType()),
            StructField("TELE_HOME_PRI_TO", StringType()),
        ])
        rows = [
            ("12345", 1, 2, "0800", "1700"),  # will match basic row
            ("12345", 3, 4, "0900", "1800"),  # no match — LKP_CALL_PRTY=3 absent in basic
            ("99999", 1, 2, "0700", "1600"),  # EMP_NBR not in basic → excluded
        ]
        return spark.createDataFrame(rows, schema=schema)

    def _make_basic_df(self, spark: SparkSession) -> DataFrame:
        """Build a minimal basic priority DataFrame (right side of join)."""
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("LKP_CALL_PRTY", IntegerType()),
            StructField("PH_NBR", StringType()),
            StructField("PH_ACCESS", StringType()),
            StructField("PH_COMMENTS", StringType()),
            StructField("PH_TYPE", StringType()),
            StructField("UNLISTD_IND", StringType()),
            StructField("HOME_AWAY_IND", StringType()),
            StructField("TEMP_PH_EXP_TS", TimestampType()),
            StructField("CALL_LIST", StringType()),
            StructField("EFF_START_TM", StringType()),
            StructField("EFF_END_TM", StringType()),
            StructField("USER_ID", StringType()),
            StructField("UPD_TS", TimestampType()),
        ])
        rows = [
            ("12345", 1, "4085551111", None, None, "WORK", None, "B", None, "BASIC", "0001", "2359", "U1", None),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_inner_join_returns_only_matched_rows(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Only rows where both EMP_NBR and LKP_CALL_PRTY match → inner join behaviour."""
        seq_df = self._make_seq_df(spark)
        basic_df = self._make_basic_df(spark)
        result = join_home_records(seq_df, basic_df, pipeline_config)

        assert result.count() == 1

    def test_unmatched_emp_nbr_excluded(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EMP_NBR=99999 has no matching basic record → excluded from output."""
        seq_df = self._make_seq_df(spark)
        basic_df = self._make_basic_df(spark)
        result = join_home_records(seq_df, basic_df, pipeline_config)

        emp_nbrs = {r["EMP_NBR"] for r in result.collect()}
        assert "99999" not in emp_nbrs

    def test_matched_row_has_ph_nbr(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Matched row carries PH_NBR from the basic side."""
        seq_df = self._make_seq_df(spark)
        basic_df = self._make_basic_df(spark)
        result = join_home_records(seq_df, basic_df, pipeline_config)
        row = result.collect()[0]

        assert row["PH_NBR"] == "4085551111"


# ---------------------------------------------------------------------------
# TestCalculateNewPriority
# ---------------------------------------------------------------------------

class TestCalculateNewPriority:
    """Tests for calculate_new_priority (NEW_PRTY_TFM / ABREC_NEW_PRTY_TFM)."""

    def _make_joined_df(self, spark: SparkSession) -> DataFrame:
        """Build a minimal joined DataFrame with TELE_HOME_PRI_FROM/TO and CALL_PRTY."""
        schema = StructType([
            StructField("EMP_NBR", StringType()),
            StructField("CALL_PRTY", IntegerType()),
            StructField("LKP_CALL_PRTY", IntegerType()),
            StructField("TELE_HOME_PRI_FROM", StringType()),
            StructField("TELE_HOME_PRI_TO", StringType()),
            StructField("PH_NBR", StringType()),
            StructField("PH_ACCESS", StringType()),
            StructField("PH_COMMENTS", StringType()),
            StructField("PH_TYPE", StringType()),
            StructField("UNLISTD_IND", StringType()),
            StructField("HOME_AWAY_IND", StringType()),
            StructField("TEMP_PH_EXP_TS", TimestampType()),
            StructField("CALL_LIST", StringType()),
            StructField("EFF_START_TM", StringType()),
            StructField("EFF_END_TM", StringType()),
            StructField("USER_ID", StringType()),
            StructField("UPD_TS", TimestampType()),
        ])
        rows = [
            ("12345", 5, 1, "0800", "1700", "4085551111", None, None, "WORK", None, "B", None, "HOME", "0001", "2359", "U1", None),
            ("12345", 3, 2, "0900", "1800", "4085552222", None, None, "HOME", None, "H", None, "HOME", "0001", "2359", "U1", None),
            ("99999", 7, 1, "0700", "1600", "4085553333", None, None, "WORK", None, "B", None, "HOME", "0001", "2359", "U1", None),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_returns_tuple_of_two(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """calculate_new_priority returns a 2-tuple (to_all, to_agg)."""
        df = self._make_joined_df(spark)
        result = calculate_new_priority(df, pipeline_config)

        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_call_prty_is_dense_rank_within_emp_nbr(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """EMP_NBR=12345 has 2 rows → CALL_PRTY values are 1 and 2 (dense_rank)."""
        df = self._make_joined_df(spark)
        to_all, _ = calculate_new_priority(df, pipeline_config)
        rows_12345 = sorted(
            [r for r in to_all.collect() if r["EMP_NBR"] == "12345"],
            key=lambda r: r["CALL_PRTY"],
        )

        assert rows_12345[0]["CALL_PRTY"] == 1
        assert rows_12345[1]["CALL_PRTY"] == 2

    def test_eff_start_and_end_renamed_from_tele_columns(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """TELE_HOME_PRI_FROM → EFF_START_TM; TELE_HOME_PRI_TO → EFF_END_TM after rename."""
        df = self._make_joined_df(spark)
        to_all, _ = calculate_new_priority(df, pipeline_config)
        cols = to_all.columns

        assert "EFF_START_TM" in cols
        assert "EFF_END_TM" in cols
        assert "TELE_HOME_PRI_FROM" not in cols
        assert "TELE_HOME_PRI_TO" not in cols

    def test_both_outputs_have_same_row_count(
        self, spark: SparkSession, pipeline_config: dict[str, Any]
    ) -> None:
        """Both output DataFrames (to_all and to_agg) reference the same cached data."""
        df = self._make_joined_df(spark)
        to_all, to_agg = calculate_new_priority(df, pipeline_config)

        assert to_all.count() == to_agg.count()
