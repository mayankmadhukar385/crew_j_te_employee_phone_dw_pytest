"""Phone record transformation functions for CREW_J_TE_EMPLOYEE_PHONE_DW.

Migrated from IBM DataStage parallel job: CREW_J_TE_EMPLOYEE_PHONE_DW.
Source: CREW_WSTELE_LND (Teradata). Target: TE_EMPLOYEE_PHONE (via work table TE_EMPLOYEE_PHONE_NEW).

All functions are pure functions — each takes (df, config) and returns a DataFrame or tuple of DataFrames.
No framework code is duplicated here; orchestration lives in run_pipeline.py.
"""

from __future__ import annotations

from functools import reduce
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Helper: build UPD_TS from TELE_LAST_UPDATED_DATE + TELE_LAST_UPDATED_TIME
# DataStage: '20':LEFT(DATE,2):'-':RIGHT(LEFT(DATE,4),2):'-':RIGHT(DATE,2):' ':LEFT(TIME,2):':':RIGHT(TIME,2):':00'
# ---------------------------------------------------------------------------

def _build_upd_ts(  # noqa: F821
    date_col: str = "TELE_LAST_UPDATED_DATE",
    time_col: str = "TELE_LAST_UPDATED_TIME",
) -> "Column":  # type: ignore[name-defined]
    """Build a timestamp Column expression from YYMMDD date + HHMM time source columns.

    DataStage expression (verbatim):
        IF TRIMLEADINGTRAILING(date)='' OR TRIMLEADINGTRAILING(time)=''
        THEN SETNULL()
        ELSE IF NOT(ISVALID("TIMESTAMP", assembled)) THEN SETNULL()
        ELSE assembled

    Args:
        date_col: Name of the YYMMDD string column.
        time_col: Name of the HHMM string column.

    Returns:
        A PySpark Column expression of type timestamp.
    """
    assembled = F.concat(
        F.lit("20"), F.substring(date_col, 1, 2),
        F.lit("-"), F.substring(date_col, 3, 2),
        F.lit("-"), F.substring(date_col, 5, 2),
        F.lit(" "), F.substring(time_col, 1, 2),
        F.lit(":"), F.substring(time_col, 3, 2),
        F.lit(":00"),
    )
    blank_date = F.trim(F.coalesce(F.col(date_col), F.lit(""))) == ""
    blank_time = F.trim(F.coalesce(F.col(time_col), F.lit(""))) == ""
    return F.when(blank_date | blank_time, F.lit(None).cast("timestamp")).otherwise(
        F.to_timestamp(assembled, "yyyy-MM-dd HH:mm:ss")
    )


def _null_if_blank(col_name: str) -> "Column":  # type: ignore[name-defined]  # noqa: F821
    """Return NULL if the trimmed value is blank, otherwise the trimmed value.

    DataStage expression: IF TRIMLEADINGTRAILING(NullToValue(col,''))='' THEN SETNULL() ELSE Trim(col)

    Args:
        col_name: Column name to evaluate.

    Returns:
        A PySpark Column expression of type string.
    """
    return F.when(
        F.trim(F.coalesce(F.col(col_name), F.lit(""))) == "",
        F.lit(None).cast("string"),
    ).otherwise(F.trim(F.col(col_name)))


# ---------------------------------------------------------------------------
# Function 1 — SEPARATE_PHTYPE_TFM  (6-way router)
# ---------------------------------------------------------------------------

def route_by_phone_type(
    source_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """Implement SEPARATE_PHTYPE_TFM — 6-way routing by phone type.

    Verbatim DataStage constraints (from XML Constraint/ParsedConstraint):
    - emergency : ISVALID("INT32", EMP_NBR) and isvalid("int64", PH_NBR)
    - temp      : ISVALID("INT32", EMP_NBR) and isvalid("int64", TEMP_PH_NBR)
    - basic     : ISVALID("INT32", EMP_NBR)
    - home      : ISVALID("INT32", EMP_NBR)  (all valid EMP_NBR rows — HOME cols pass-through)
    - away      : ISVALID("INT32", EMP_NBR)  (all valid EMP_NBR rows — AWAY cols remapped to HOME names)
    - error     : Not(IsValid("int32", EMP_NBR))

    Column derivations follow SEPARATE_PHTYPE_TFM XML verbatim.

    Args:
        source_df: Wide source DataFrame from CREW_WSTELE_LND (post-DQ), containing all source columns.
        config: Full pipeline config dict (not used directly in this function — kept for signature consistency).

    Returns:
        Tuple of (emergency_df, temp_df, basic_df, home_df, away_df, error_df).
    """
    # --- routing conditions ---
    valid_emp = F.col("EMP_NBR").cast("int").isNotNull()
    valid_ph_nbr = F.col("PH_NBR").cast("long").isNotNull()
    valid_temp_ph_nbr = F.col("TEMP_PH_NBR").cast("long").isNotNull()

    emergency_cond = valid_emp & valid_ph_nbr
    temp_cond = valid_emp & valid_temp_ph_nbr
    basic_cond = valid_emp
    home_cond = valid_emp
    away_cond = valid_emp
    error_cond = ~valid_emp

    # --- UPD_TS expression (shared across all valid-EMP branches) ---
    upd_ts_expr = _build_upd_ts("TELE_LAST_UPDATED_DATE", "TELE_LAST_UPDATED_TIME")
    user_id_expr = _null_if_blank("USER_ID")

    # --- EMERGENCY_IN_TFM output ---
    emergency_df = source_df.filter(emergency_cond).select(
        F.col("EMP_NBR"),
        F.lit("EMERGENCY").alias("CALL_LIST"),
        F.lit(1).alias("CALL_PRTY"),
        F.lit("0000").alias("EFF_START_TM"),
        F.lit("2359").alias("EFF_END_TM"),
        F.lit(None).cast("string").alias("PH_ACCESS"),
        F.when(
            F.trim(F.coalesce(F.col("PH_COMMENTS"), F.lit(""))) == "",
            F.lit(None).cast("string"),
        ).otherwise(F.col("PH_COMMENTS")).alias("PH_COMMENTS"),
        F.lit(None).cast("string").alias("PH_TYPE"),
        F.lit(None).cast("string").alias("UNLISTD_IND"),
        F.lit(None).cast("string").alias("HOME_AWAY_IND"),
        F.lit(None).cast("timestamp").alias("TEMP_PH_EXP_TS"),
        F.col("PH_NBR"),
        user_id_expr.alias("USER_ID"),
        upd_ts_expr.alias("UPD_TS"),
    )

    # --- TEMP_TYPE_TFM output ---
    temp_ph_exp_ts_expr = _build_upd_ts("TELE_TEMP_PH_DATE", "TELE_TEMP_PH_TIME")
    temp_df = source_df.filter(temp_cond).select(
        F.col("EMP_NBR"),
        F.lit("TEMP").alias("CALL_LIST"),
        F.lit(1).alias("CALL_PRTY"),
        F.lit("0000").alias("EFF_START_TM"),
        F.lit("2359").alias("EFF_END_TM"),
        F.when(
            F.trim(F.coalesce(F.col("TEMP_PH_ACCESS"), F.lit(""))) == "",
            F.lit(None).cast("string"),
        ).otherwise(F.col("TEMP_PH_ACCESS")).alias("PH_ACCESS"),
        F.when(
            F.trim(F.coalesce(F.col("TEMP_PH_COMMENTS"), F.lit(""))) == "",
            F.lit(None).cast("string"),
        ).otherwise(F.col("TEMP_PH_COMMENTS")).alias("PH_COMMENTS"),
        F.lit(None).cast("string").alias("PH_TYPE"),
        F.lit(None).cast("string").alias("UNLISTD_IND"),
        F.lit(None).cast("string").alias("HOME_AWAY_IND"),
        temp_ph_exp_ts_expr.alias("TEMP_PH_EXP_TS"),
        F.col("TEMP_PH_NBR").alias("PH_NBR"),
        user_id_expr.alias("USER_ID"),
        upd_ts_expr.alias("UPD_TS"),
    )

    # --- BASIC_TYPE_TFM output (feeds BASIC_TYPE_PVT) ---
    def _basic_null_if_blank(col_name: str) -> "Column":  # type: ignore[name-defined]  # noqa: F821
        return F.when(
            F.trim(F.coalesce(F.col(col_name), F.lit(""))) == "",
            F.lit(None).cast("string"),
        ).otherwise(F.col(col_name))

    basic_selects = [
        F.col("EMP_NBR"),
        F.lit("BASIC").alias("CALL_LIST"),
        F.lit("0001").alias("EFF_START_TM"),
        F.lit("2359").alias("EFF_END_TM"),
    ]
    for i in range(1, 6):
        basic_selects += [
            F.col(f"BASIC_PH_NBR_{i}"),
            _basic_null_if_blank(f"BASIC_PH_ACCESS_{i}").alias(f"BASIC_PH_ACCESS_{i}"),
            _basic_null_if_blank(f"BASIC_PH_COMMENTS_{i}").alias(f"BASIC_PH_COMMENT_{i}"),
            _basic_null_if_blank(f"BASIC_PH_TYPE_{i}").alias(f"BASIC_PH_TYPE_{i}"),
            _basic_null_if_blank(f"BASIC_PH_UNLIST_CD_{i}").alias(f"BASIC_PH_UNLIST_CD_{i}"),
            _basic_null_if_blank(f"BASIC_PH_HOME_AWAY_CD_{i}").alias(f"BASIC_PH_HOME_AWAY_CD_{i}"),
            F.lit(None).cast("timestamp").alias(f"TEMP_PH_EXP_TS_{i}"),
        ]
    basic_selects += [
        user_id_expr.alias("USER_ID"),
        upd_ts_expr.alias("UPD_TS"),
    ]
    basic_df = source_df.filter(basic_cond).select(*basic_selects)

    # --- HOME_TYPE_CPY output (feeds HOME_CPY — pass-through HOME columns) ---
    home_selects = [F.col("EMP_NBR")]
    for g in range(1, 4):
        home_selects += [
            F.col(f"TELE_HOME_PRI_FROM_{g}"),
            F.col(f"TELE_HOME_PRI_TO_{g}"),
        ]
        for s in range(1, 6):
            home_selects.append(F.col(f"TELE_HOME_PRI_SEQ_{s}_{g}"))
    home_df = source_df.filter(home_cond).select(*home_selects)

    # --- AWAY_TYPE_CPY output (feeds AWAY_CPY — AWAY columns remapped to HOME column names) ---
    away_selects = [F.col("EMP_NBR")]
    for g in range(1, 4):
        away_selects += [
            F.col(f"TELE_AWAY_PRI_FROM_{g}").alias(f"TELE_HOME_PRI_FROM_{g}"),
            F.col(f"TELE_AWAY_PRI_TO_{g}").alias(f"TELE_HOME_PRI_TO_{g}"),
        ]
        for s in range(1, 6):
            away_selects.append(F.col(f"TELE_AWAY_PRI_SEQ_{s}_{g}").alias(f"TELE_HOME_PRI_SEQ_{s}_{g}"))
    away_df = source_df.filter(away_cond).select(*away_selects)

    # --- LNK_OUT_ERR_SEQ output (error records — all source columns pass-through) ---
    error_df = source_df.filter(error_cond)

    return emergency_df, temp_df, basic_df, home_df, away_df, error_df


# ---------------------------------------------------------------------------
# Function 2 — BASIC_TYPE_PVT  (PxPivot — stack 5 slots → rows)
# ---------------------------------------------------------------------------

def pivot_basic_phones(
    basic_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement BASIC_TYPE_PVT — unpivot 5 BASIC phone slots into individual rows.

    DataStage stage type: PxPivot.  The pivot produces one output row per phone slot
    (0-based CALL_PRTY_IDX).  Pass-through columns: EMP_NBR, CALL_LIST, EFF_START_TM,
    EFF_END_TM, USER_ID, UPD_TS.

    Args:
        basic_df: Output of BASIC_TYPE_TFM branch with BASIC_PH_NBR_1..5 wide columns.
        config: Full pipeline config dict.

    Returns:
        Tall DataFrame with one row per phone slot; CALL_PRTY_IDX is 0-based.
    """
    stack_expr = """
        stack(5,
          BASIC_PH_NBR_1, BASIC_PH_ACCESS_1, BASIC_PH_COMMENT_1, BASIC_PH_TYPE_1,
            BASIC_PH_UNLIST_CD_1, BASIC_PH_HOME_AWAY_CD_1, TEMP_PH_EXP_TS_1, 0,
          BASIC_PH_NBR_2, BASIC_PH_ACCESS_2, BASIC_PH_COMMENT_2, BASIC_PH_TYPE_2,
            BASIC_PH_UNLIST_CD_2, BASIC_PH_HOME_AWAY_CD_2, TEMP_PH_EXP_TS_2, 1,
          BASIC_PH_NBR_3, BASIC_PH_ACCESS_3, BASIC_PH_COMMENT_3, BASIC_PH_TYPE_3,
            BASIC_PH_UNLIST_CD_3, BASIC_PH_HOME_AWAY_CD_3, TEMP_PH_EXP_TS_3, 2,
          BASIC_PH_NBR_4, BASIC_PH_ACCESS_4, BASIC_PH_COMMENT_4, BASIC_PH_TYPE_4,
            BASIC_PH_UNLIST_CD_4, BASIC_PH_HOME_AWAY_CD_4, TEMP_PH_EXP_TS_4, 3,
          BASIC_PH_NBR_5, BASIC_PH_ACCESS_5, BASIC_PH_COMMENT_5, BASIC_PH_TYPE_5,
            BASIC_PH_UNLIST_CD_5, BASIC_PH_HOME_AWAY_CD_5, TEMP_PH_EXP_TS_5, 4
        ) AS (PH_NBR, PH_ACCESS, PH_COMMENTS, PH_TYPE, UNLISTD_IND, HOME_AWAY_IND, TEMP_PH_EXP_TS, CALL_PRTY_IDX)
    """
    return basic_df.select(
        "EMP_NBR",
        "CALL_LIST",
        "EFF_START_TM",
        "EFF_END_TM",
        "USER_ID",
        "UPD_TS",
        F.expr(stack_expr),
    )


# ---------------------------------------------------------------------------
# Function 3 — BASIC_REC_TFM  (converts 0-based index to 1-based CALL_PRTY)
# ---------------------------------------------------------------------------

def enrich_basic_records(
    pivoted_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement BASIC_REC_TFM column derivations — convert 0-based pivot index to 1-based CALL_PRTY.

    DataStage derivation (verbatim): CALL_PRTY = BREC_OUT_PVT.CALL_PRTY + 1
    Also adds LKP_CALL_PRTY = CALL_PRTY (used as join key in HOME_BASIC_JNR / AWAY_BASIC_JNR).

    Args:
        pivoted_df: Output of pivot_basic_phones() with CALL_PRTY_IDX (0-based).
        config: Full pipeline config dict.

    Returns:
        DataFrame with CALL_PRTY (1-based) and LKP_CALL_PRTY columns added; CALL_PRTY_IDX dropped.
    """
    return (
        pivoted_df
        .withColumn("CALL_PRTY", F.col("CALL_PRTY_IDX") + 1)
        .withColumn("LKP_CALL_PRTY", F.col("CALL_PRTY_IDX") + 1)
        .drop("CALL_PRTY_IDX")
    )


# ---------------------------------------------------------------------------
# Function 4 — BASIC_REC_TFM 5-way split
# ---------------------------------------------------------------------------

def split_basic_by_home_away(
    basic_enriched_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """Implement BASIC_REC_TFM 5-way output routing.

    Verbatim DataStage output constraints (from XML):
    - all_basic          (ALL_BREC_OUT_TFM → ALL_REC_FNL):
        isvalid("int64", PH_NBR)
    - basic_home_jnr     (CALL_PRIORITY_RIGHT_JNR → HOME_BASIC_JNR):
        isvalid("int64", PH_NBR) and not(isnull(HOME_AWAY_IND)) and (HOME_AWAY_IND='B' or HOME_AWAY_IND='H')
    - basic_home_max     (BREC_OUT_TFM → BASIC_MAX_JNR):
        same as basic_home_jnr
    - basic_away_jnr     (CALL_PRI_RIGHT_JNR → AWAY_BASIC_JNR):
        isvalid("int64", PH_NBR) and not(isnull(HOME_AWAY_IND)) and (HOME_AWAY_IND='B' or HOME_AWAY_IND='A')
    - basic_away_max     (BASIC_REC_OUT_TFM → AWAY_MAX_JNR):
        same as basic_away_jnr

    The DataFrame is cached because it feeds 5 downstream consumers.

    Args:
        basic_enriched_df: Output of enrich_basic_records() with CALL_PRTY (1-based).
        config: Full pipeline config dict.

    Returns:
        Tuple of (all_basic_df, basic_home_jnr_df, basic_home_max_df, basic_away_jnr_df, basic_away_max_df).
    """
    cached = basic_enriched_df.cache()

    valid_ph = F.col("PH_NBR").cast("long").isNotNull()
    not_null_home_away = F.col("HOME_AWAY_IND").isNotNull()
    is_b_or_h = F.col("HOME_AWAY_IND").isin("B", "H")
    is_b_or_a = F.col("HOME_AWAY_IND").isin("B", "A")

    home_cond = valid_ph & not_null_home_away & is_b_or_h
    away_cond = valid_ph & not_null_home_away & is_b_or_a

    all_basic_df = cached.filter(valid_ph)
    basic_home_jnr_df = cached.filter(home_cond)
    basic_home_max_df = cached.filter(home_cond)
    basic_away_jnr_df = cached.filter(away_cond)
    basic_away_max_df = cached.filter(away_cond)

    return all_basic_df, basic_home_jnr_df, basic_home_max_df, basic_away_jnr_df, basic_away_max_df


# ---------------------------------------------------------------------------
# Function 5 — HOME_SEQ{n}_PVT  (PxPivot — 5 priority sequence slots → rows)
# ---------------------------------------------------------------------------

def pivot_home_sequence(
    home_df: DataFrame,
    seq_num: int,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement HOME_SEQ1_PVT / HOME_SEQ2_PVT / HOME_SEQ3_PVT.

    DataStage stage type: PxPivot.  Pivots 5 priority sequence columns for the given
    sequence group (seq_num 1, 2, or 3) into individual rows.  Also used for the AWAY
    branch because AWAY columns are already remapped to TELE_HOME_PRI_* names.

    Pivoted columns (verbatim from XML PivotCustomOSH):
        LKP_CALL_PRTY from TELE_HOME_PRI_SEQ_1_N .. TELE_HOME_PRI_SEQ_5_N
    Index column: CALL_PRTY (0-based)

    Args:
        home_df: DataFrame with TELE_HOME_PRI_FROM_{N}, TELE_HOME_PRI_TO_{N}, and
                 TELE_HOME_PRI_SEQ_1_{N} .. TELE_HOME_PRI_SEQ_5_{N} columns.
        seq_num: Sequence group number (1, 2, or 3).
        config: Full pipeline config dict.

    Returns:
        DataFrame with EMP_NBR, TELE_HOME_PRI_FROM, TELE_HOME_PRI_TO, LKP_CALL_PRTY, CALL_PRTY.
    """
    from_col = f"TELE_HOME_PRI_FROM_{seq_num}"
    to_col = f"TELE_HOME_PRI_TO_{seq_num}"
    seq_cols = [f"TELE_HOME_PRI_SEQ_{i}_{seq_num}" for i in range(1, 6)]

    stack_expr = (
        f"stack(5, "
        f"{seq_cols[0]},0, "
        f"{seq_cols[1]},1, "
        f"{seq_cols[2]},2, "
        f"{seq_cols[3]},3, "
        f"{seq_cols[4]},4"
        f") AS (LKP_CALL_PRTY, CALL_PRTY)"
    )
    return home_df.select(
        "EMP_NBR",
        F.col(from_col).alias("TELE_HOME_PRI_FROM"),
        F.col(to_col).alias("TELE_HOME_PRI_TO"),
        F.expr(stack_expr),
    )


# ---------------------------------------------------------------------------
# Function 6 — HOME_SEQ2_TFM / HOME_SEQ3_TFM  (add offset to CALL_PRTY)
# ---------------------------------------------------------------------------

def add_home_seq_offset(
    seq_df: DataFrame,
    offset: int,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement HOME_SEQ2_TFM (offset=5) and HOME_SEQ3_TFM (offset=10).

    DataStage derivation (verbatim): CALL_PRTY = CALL_PRTY + <offset>

    Args:
        seq_df: Output of pivot_home_sequence() with 0-based CALL_PRTY.
        offset: Value to add to CALL_PRTY (5 for SEQ2, 10 for SEQ3).
        config: Full pipeline config dict.

    Returns:
        DataFrame with CALL_PRTY incremented by offset.
    """
    return seq_df.withColumn("CALL_PRTY", F.col("CALL_PRTY") + offset)


# ---------------------------------------------------------------------------
# Function 7 — AWAY_SEQ{n}_PVT  (same logic as pivot_home_sequence)
# ---------------------------------------------------------------------------

def pivot_away_sequence(
    away_df: DataFrame,
    seq_num: int,
    config: dict[str, Any],
) -> DataFrame:
    """Implement AWAY_SEQ1_PVT / AWAY_SEQ2_PVT / AWAY_SEQ3_PVT.

    Identical pivot logic to pivot_home_sequence because AWAY columns were already
    remapped to TELE_HOME_PRI_* names in route_by_phone_type (AWAY_TYPE_CPY output).

    Args:
        away_df: AWAY DataFrame with TELE_HOME_PRI_FROM_{N}, TELE_HOME_PRI_TO_{N}, and
                 TELE_HOME_PRI_SEQ_1_{N} .. TELE_HOME_PRI_SEQ_5_{N} columns (remapped).
        seq_num: Sequence group number (1, 2, or 3).
        config: Full pipeline config dict.

    Returns:
        DataFrame with EMP_NBR, TELE_HOME_PRI_FROM, TELE_HOME_PRI_TO, LKP_CALL_PRTY, CALL_PRTY.
    """
    return pivot_home_sequence(away_df, seq_num, config)


# ---------------------------------------------------------------------------
# Function 8 — AWAY_SEQ2_TFM / AWAY_SEQ3_TFM  (add offset — same as home)
# ---------------------------------------------------------------------------

def add_away_seq_offset(
    seq_df: DataFrame,
    offset: int,
    config: dict[str, Any],
) -> DataFrame:
    """Implement AWAY_SEQ2_TFM (offset=5) and AWAY_SEQ3_TFM (offset=10).

    DataStage derivation (verbatim): CALL_PRTY = CALL_PRTY + <offset>

    Args:
        seq_df: Output of pivot_away_sequence() with 0-based CALL_PRTY.
        offset: Value to add to CALL_PRTY (5 for SEQ2, 10 for SEQ3).
        config: Full pipeline config dict.

    Returns:
        DataFrame with CALL_PRTY incremented by offset.
    """
    return add_home_seq_offset(seq_df, offset, config)


# ---------------------------------------------------------------------------
# Function 9 — HOME_SEQ_FNL / AWAY_SEQ_FNL  (PxFunnel — union 3 pivot DFs)
# ---------------------------------------------------------------------------

def funnel_sequences(
    seq1_df: DataFrame,
    seq2_df: DataFrame,
    seq3_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement HOME_SEQ_FNL and AWAY_SEQ_FNL — union three pivot DataFrames.

    DataStage stage type: PxFunnel.  Combines SEQ1, SEQ2 (offset+5), SEQ3 (offset+10)
    outputs into a single DataFrame using unionByName with allowMissingColumns=True.

    Args:
        seq1_df: Output of pivot_home/away_sequence(..., seq_num=1).
        seq2_df: Output of add_home/away_seq_offset(..., offset=5).
        seq3_df: Output of add_home/away_seq_offset(..., offset=10).
        config: Full pipeline config dict.

    Returns:
        Unioned DataFrame from all three sequence groups.
    """
    return reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [seq1_df, seq2_df, seq3_df],
    )


# ---------------------------------------------------------------------------
# Function 10 — INC_PRTY_TFM / INC_AWAY_PRTY_TFM  (increment + filter + CALL_LIST)
# ---------------------------------------------------------------------------

def increment_priority(
    seq_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement INC_PRTY_TFM (home) and INC_AWAY_PRTY_TFM (away).

    DataStage derivations (verbatim from XML):
        CALL_PRTY = CALL_PRTY + 1
        TELE_HOME_PRI_FROM = if not(isvalid("time", ...)) then setnull() else trimleadingtrailing(...)
        TELE_HOME_PRI_TO   = same pattern
        CALL_LIST = 'HOME' or 'AWAY' (set by caller context; applied in run_pipeline.py)

    Output constraint (verbatim from XML):
        isvalid("int32", EMP_NBR) and isvalid("int32", LKP_CALL_PRTY)

    The CALL_LIST assignment ('HOME' vs 'AWAY') is handled in run_pipeline.py after this function,
    consistent with DataStage derivations that set it on the output link.

    Args:
        seq_df: Unioned sequence DataFrame from funnel_sequences().
        config: Full pipeline config dict.

    Returns:
        Filtered and enriched DataFrame with incremented CALL_PRTY and validated time columns.
    """
    valid_emp = F.col("EMP_NBR").cast("int").isNotNull()
    valid_lkp = F.col("LKP_CALL_PRTY").cast("int").isNotNull()

    def _validate_time(col_name: str) -> "Column":  # type: ignore[name-defined]  # noqa: F821
        """Return null if not a valid HH:MM:SS time, otherwise trimmed value."""
        time_expr = F.concat(
            F.substring(col_name, 1, 2), F.lit(":"),
            F.substring(col_name, 3, 2), F.lit(":00"),
        )
        valid_time = F.to_timestamp(time_expr, "HH:mm:ss").isNotNull()
        return F.when(~valid_time, F.lit(None).cast("string")).otherwise(F.trim(F.col(col_name)))

    return (
        seq_df
        .filter(valid_emp & valid_lkp)
        .withColumn("CALL_PRTY", F.col("CALL_PRTY") + 1)
        .withColumn("TELE_HOME_PRI_FROM", _validate_time("TELE_HOME_PRI_FROM"))
        .withColumn("TELE_HOME_PRI_TO", _validate_time("TELE_HOME_PRI_TO"))
    )


# ---------------------------------------------------------------------------
# Function 11 — HOME_BASIC_JNR / AWAY_BASIC_JNR  (inner join on EMP_NBR, LKP_CALL_PRTY)
# ---------------------------------------------------------------------------

def join_home_records(
    home_seq_df: DataFrame,
    basic_priority_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement HOME_BASIC_JNR and AWAY_BASIC_JNR — inner join on EMP_NBR and LKP_CALL_PRTY.

    DataStage stage type: PxJoin (innerjoin).
    Join keys (verbatim from XML): EMP_NBR, LKP_CALL_PRTY.

    Args:
        home_seq_df: Left DataFrame — output of increment_priority() (home or away branch).
        basic_priority_df: Right DataFrame — basic_home_jnr_df or basic_away_jnr_df
                           from split_basic_by_home_away().
        config: Full pipeline config dict.

    Returns:
        Joined DataFrame containing all home/away sequence fields plus phone detail columns.
    """
    right_cols = [
        "EMP_NBR", "LKP_CALL_PRTY", "PH_NBR", "PH_ACCESS", "PH_COMMENTS", "PH_TYPE",
        "UNLISTD_IND", "HOME_AWAY_IND", "TEMP_PH_EXP_TS", "CALL_LIST",
        "EFF_START_TM", "EFF_END_TM", "USER_ID", "UPD_TS",
    ]
    return home_seq_df.join(
        basic_priority_df.select(*right_cols),
        on=["EMP_NBR", "LKP_CALL_PRTY"],
        how="inner",
    )


# ---------------------------------------------------------------------------
# Function 12 — NEW_PRTY_TFM / ABREC_NEW_PRTY_TFM  (svCalcNew sequential numbering)
# ---------------------------------------------------------------------------

def calculate_new_priority(
    joined_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> tuple[DataFrame, DataFrame]:
    """Implement NEW_PRTY_TFM (home) and ABREC_NEW_PRTY_TFM (away) — per-EMP_NBR sequential numbering.

    DataStage stage variables (verbatim from XML):
        svCalcNew = if svEmpNbrNew <> svEmpNbrOld then 1 else svCalcOld + 1
    PySpark equivalent: F.dense_rank().over(Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY"))

    The column EFF_START_TM / EFF_END_TM are populated from TELE_HOME_PRI_FROM / TELE_HOME_PRI_TO
    (verbatim from XML NPRTY_OUT_TFM derivations).

    Both output links (NPRTY_OUT_TFM → ALL_REC_FNL and CPRTY_OUT_TFM → MAX_PRTY_AGG) receive
    the same rows.  The DataFrame is cached before splitting.

    Args:
        joined_df: Output of join_home_records().
        config: Full pipeline config dict.

    Returns:
        Tuple of (to_all_records_df, to_max_agg_df) — same data, two references.
    """
    w = Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY")
    enriched = (
        joined_df
        .withColumn("CALL_PRTY", F.dense_rank().over(w))
        .withColumnRenamed("TELE_HOME_PRI_FROM", "EFF_START_TM")
        .withColumnRenamed("TELE_HOME_PRI_TO", "EFF_END_TM")
        .cache()
    )
    to_all = enriched
    to_agg = enriched
    return to_all, to_agg


# ---------------------------------------------------------------------------
# Function 13 — MAX_PRTY_AGG / MAX_ABPRTY_AGG  (groupBy EMP_NBR, max CALL_PRTY)
# ---------------------------------------------------------------------------

def aggregate_max_priority(
    cprty_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement MAX_PRTY_AGG (home) and MAX_ABPRTY_AGG (away).

    DataStage stage type: PxAggregator.
    Verbatim from XML: GROUP BY EMP_NBR, MAX(CALL_PRTY) AS MAX_CALL_PRTY.

    Args:
        cprty_df: DataFrame from calculate_new_priority() cprty output (same as to_all).
        config: Full pipeline config dict.

    Returns:
        Aggregated DataFrame with one row per EMP_NBR and MAX_CALL_PRTY.
    """
    return cprty_df.groupBy("EMP_NBR").agg(F.max("CALL_PRTY").alias("MAX_CALL_PRTY"))


# ---------------------------------------------------------------------------
# Function 14 — BASIC_MAX_JNR / AWAY_MAX_JNR  (inner join on EMP_NBR)
# ---------------------------------------------------------------------------

def join_max_priority(
    brec_df: DataFrame,
    max_agg_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement BASIC_MAX_JNR (home) and AWAY_MAX_JNR (away) — inner join on EMP_NBR.

    DataStage stage type: PxJoin (innerjoin).
    Join key (verbatim from XML): EMP_NBR.

    Args:
        brec_df: Right side — basic_home_max_df or basic_away_max_df from split_basic_by_home_away().
        max_agg_df: Left side — output of aggregate_max_priority().
        config: Full pipeline config dict.

    Returns:
        Joined DataFrame with MAX_CALL_PRTY column added.
    """
    return brec_df.join(max_agg_df, on="EMP_NBR", how="inner")


# ---------------------------------------------------------------------------
# Function 15 — ADJUSTING_PRTY_TFM / ADJUST_PRTY_TFM  (MAX_CALL_PRTY + row_number offset)
# ---------------------------------------------------------------------------

def adjust_priority(
    joined_max_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement ADJUSTING_PRTY_TFM (home) and ADJUST_PRTY_TFM (away).

    DataStage stage variable pattern (verbatim from XML):
        svCalcNew = if svEmpNbrNew <> svEmpNbrOld
                    then MAX_CALL_PRTY + 1
                    else svCalcOld + 1
    PySpark: CALL_PRTY = MAX_CALL_PRTY + row_number() per EMP_NBR group.

    EFF_START_TM is set to '0001' and EFF_END_TM to '2359' (verbatim from XML derivations).
    MAX_CALL_PRTY is dropped after use.

    Args:
        joined_max_df: Output of join_max_priority() with MAX_CALL_PRTY column.
        config: Full pipeline config dict.

    Returns:
        DataFrame with adjusted CALL_PRTY and MAX_CALL_PRTY dropped.
    """
    w = Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY")
    return (
        joined_max_df
        .withColumn("CALL_PRTY", F.col("MAX_CALL_PRTY") + F.row_number().over(w))
        .withColumn("EFF_START_TM", F.lit("0001"))
        .withColumn("EFF_END_TM", F.lit("2359"))
        .drop("MAX_CALL_PRTY")
    )


# ---------------------------------------------------------------------------
# Function 16 — ALL_REC_FNL  (PxFunnel — 7-stream union)
# ---------------------------------------------------------------------------

def merge_all_records(
    emergency_df: DataFrame,
    temp_df: DataFrame,
    all_basic_df: DataFrame,
    home_priority_df: DataFrame,
    away_priority_df: DataFrame,
    adj_home_df: DataFrame,
    adj_away_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement ALL_REC_FNL — 7-stream union of all phone record streams.

    DataStage stage type: PxFunnel.
    Input streams (verbatim from XML):
        1. EMERGENCY_IN_TFM  (emergency records)
        2. TEMP_TYPE_TFM     (temp records)
        3. ALL_BREC_OUT_TFM  (all valid-PH_NBR basic records)
        4. NPRTY_OUT_TFM     (home priority records from NEW_PRTY_TFM)
        5. ABNEW_PRTY_OUT_TFM (away priority records from ABREC_NEW_PRTY_TFM)
        6. ADJ_PRTY_OUT_TFM  (home basic records with adjusted priority)
        7. ADJUST_PRTY_OUT_TFM (away basic records with adjusted priority)

    Args:
        emergency_df: Emergency phone records.
        temp_df: Temporary phone records.
        all_basic_df: All valid basic phone records.
        home_priority_df: Home priority records (NEW_PRTY_TFM output).
        away_priority_df: Away priority records (ABREC_NEW_PRTY_TFM output).
        adj_home_df: Home basic records with adjusted call priority.
        adj_away_df: Away basic records with adjusted call priority.
        config: Full pipeline config dict.

    Returns:
        Single unioned DataFrame with all phone records.
    """
    return reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [emergency_df, temp_df, all_basic_df, home_priority_df, away_priority_df, adj_home_df, adj_away_df],
    )


# ---------------------------------------------------------------------------
# Function 17 — NULL_VAL_TFM  (final filter, null handling, column renames)
# ---------------------------------------------------------------------------

def validate_and_finalize(
    all_df: DataFrame,
    config: dict[str, Any],  # noqa: ARG001
) -> DataFrame:
    """Implement NULL_VAL_TFM — final filter, null handling, and column derivations.

    Verbatim DataStage constraint: isvalid("int64", PH_NBR)

    Column derivations (verbatim from XML):
        EMP_NBR    = ALL_REC_OUT_FNL.EMP_NBR (cast to int)
        EMP_NO     = If Len(TrimLeadingTrailing(EMP_NBR))=9 Then TrimLeadingTrailing(EMP_NBR)
                     Else Str('0', 9-Len(TrimLeadingTrailing(EMP_NBR))):TrimLeadingTrailing(EMP_NBR)
        PH_LIST    = CALL_LIST   (rename)
        PH_PRTY    = CALL_PRTY   (rename)
        EFF_START_TM = left(EFF_START_TM,2):':':right(EFF_START_TM,2):':00'  → HH:MM:SS
        EFF_END_TM   = left(EFF_END_TM,2):':':right(EFF_END_TM,2):':00'      → HH:MM:SS
        PH_ACCESS  = if TrimLeadingTrailing(nulltovalue(PH_ACCESS,''))='' or < ' '
                     then setnull() else TrimLeadingTrailing(...)
        PH_COMMENTS = same pattern as PH_ACCESS
        TD_LD_TS   = CurrentTimestamp()

    Args:
        all_df: Output of merge_all_records() with all 7 merged streams.
        config: Full pipeline config dict.

    Returns:
        Final DataFrame matching TE_EMPLOYEE_PHONE target schema (14 columns).
    """
    trimmed_emp = F.trim(F.col("EMP_NBR").cast("string"))
    emp_no = F.lpad(trimmed_emp, 9, "0")

    eff_start = F.concat(
        F.substring("EFF_START_TM", 1, 2),
        F.lit(":"),
        F.substring("EFF_START_TM", 3, 2),
        F.lit(":00"),
    )
    eff_end = F.concat(
        F.substring("EFF_END_TM", 1, 2),
        F.lit(":"),
        F.substring("EFF_END_TM", 3, 2),
        F.lit(":00"),
    )

    def _clean_string(col_name: str) -> "Column":  # type: ignore[name-defined]  # noqa: F821
        """Null if blank or < space; else trimmed value."""
        trimmed = F.trim(F.coalesce(F.col(col_name), F.lit("")))
        return F.when(
            (trimmed == "") | (trimmed < F.lit(" ")),
            F.lit(None).cast("string"),
        ).otherwise(trimmed)

    ph_access = _clean_string("PH_ACCESS")
    ph_comments = _clean_string("PH_COMMENTS")

    filtered = all_df.filter(F.col("PH_NBR").cast("long").isNotNull())

    return filtered.select(
        F.col("EMP_NBR").cast("int").alias("EMP_NBR"),
        emp_no.alias("EMP_NO"),
        F.col("CALL_LIST").alias("PH_LIST"),
        F.col("CALL_PRTY").alias("PH_PRTY"),
        eff_start.alias("EFF_START_TM"),
        eff_end.alias("EFF_END_TM"),
        F.col("PH_NBR").cast("long").alias("PH_NBR"),
        ph_access.alias("PH_ACCESS"),
        ph_comments.alias("PH_COMMENTS"),
        F.col("PH_TYPE").alias("PH_TYPE"),
        F.col("UNLISTD_IND").alias("UNLISTD_IND"),
        F.col("HOME_AWAY_IND").alias("HOME_AWAY_IND"),
        F.col("TEMP_PH_EXP_TS").cast("timestamp").alias("TEMP_PH_EXP_TS"),
        F.current_timestamp().alias("TD_LD_TS"),
    )
