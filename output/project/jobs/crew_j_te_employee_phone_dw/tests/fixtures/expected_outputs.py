"""Expected output DataFrames for CREW_J_TE_EMPLOYEE_PHONE_DW integration tests.

These represent the correct transformation output for the synthetic input data
defined in synthetic_data.py. Used with chispa.dataframe_comparer.assert_df_equality.

Important: TD_LD_TS is set by F.current_timestamp() at runtime so it must be
excluded from equality comparisons. Use ignore_columns=["TD_LD_TS"] in
assert_df_equality calls, or compare individual columns separately.

Target schema reference:
    EMP_NBR (IntegerType)  — source EMP_NBR cast to int
    EMP_NO  (StringType)   — 9-char zero-padded EMP_NBR
    PH_LIST (StringType)   — CALL_LIST rename
    PH_PRTY (IntegerType)  — CALL_PRTY rename
    EFF_START_TM (StringType) — formatted HH:MM:SS
    EFF_END_TM   (StringType) — formatted HH:MM:SS
    PH_NBR       (LongType)   — phone number as long integer
    PH_ACCESS    (StringType) — access code, null if blank
    PH_COMMENTS  (StringType) — comments, null if blank
    PH_TYPE      (StringType) — phone type
    UNLISTD_IND  (StringType) — unlisted indicator
    HOME_AWAY_IND (StringType)— home/away indicator
    TEMP_PH_EXP_TS (TimestampType) — temp phone expiry
    TD_LD_TS       (TimestampType) — load timestamp (runtime, exclude from comparisons)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

def get_target_schema() -> StructType:
    """Return the StructType for the TE_EMPLOYEE_PHONE target table.

    This schema mirrors the SELECT list in validate_and_finalize() in
    transformations.py. TD_LD_TS is included because the table contains it,
    but integration tests should exclude it from equality checks.

    Returns:
        StructType representing TE_EMPLOYEE_PHONE columns.
    """
    return StructType([
        StructField("EMP_NBR",        IntegerType(), True),
        StructField("EMP_NO",         StringType(),  True),
        StructField("PH_LIST",        StringType(),  True),
        StructField("PH_PRTY",        IntegerType(), True),
        StructField("EFF_START_TM",   StringType(),  True),
        StructField("EFF_END_TM",     StringType(),  True),
        StructField("PH_NBR",         LongType(),    True),
        StructField("PH_ACCESS",      StringType(),  True),
        StructField("PH_COMMENTS",    StringType(),  True),
        StructField("PH_TYPE",        StringType(),  True),
        StructField("UNLISTD_IND",    StringType(),  True),
        StructField("HOME_AWAY_IND",  StringType(),  True),
        StructField("TEMP_PH_EXP_TS", TimestampType(), True),
        StructField("TD_LD_TS",       TimestampType(), True),
    ])


def get_error_schema() -> StructType:
    """Return the StructType for the TE_EMPLOYEE_PHONE_ERR error table.

    Error records are the raw source rows that failed the EMP_NBR validity
    check.  All columns are StringType (matching phone_source_schema).

    Returns:
        StructType representing TE_EMPLOYEE_PHONE_ERR columns.
    """
    string = StringType()

    basic_fields: list[StructField] = []
    for i in range(1, 6):
        basic_fields += [
            StructField(f"BASIC_PH_NBR_{i}",          string),
            StructField(f"BASIC_PH_ACCESS_{i}",        string),
            StructField(f"BASIC_PH_COMMENTS_{i}",      string),
            StructField(f"BASIC_PH_TYPE_{i}",          string),
            StructField(f"BASIC_PH_UNLIST_CD_{i}",     string),
            StructField(f"BASIC_PH_HOME_AWAY_CD_{i}",  string),
        ]

    home_fields: list[StructField] = []
    for g in range(1, 4):
        home_fields += [
            StructField(f"TELE_HOME_PRI_FROM_{g}", string),
            StructField(f"TELE_HOME_PRI_TO_{g}",   string),
        ]
        for s in range(1, 6):
            home_fields.append(StructField(f"TELE_HOME_PRI_SEQ_{s}_{g}", string))

    away_fields: list[StructField] = []
    for g in range(1, 4):
        away_fields += [
            StructField(f"TELE_AWAY_PRI_FROM_{g}", string),
            StructField(f"TELE_AWAY_PRI_TO_{g}",   string),
        ]
        for s in range(1, 6):
            away_fields.append(StructField(f"TELE_AWAY_PRI_SEQ_{s}_{g}", string))

    return StructType(
        [
            StructField("EMP_NBR",                  string),
            StructField("USER_ID",                  string),
            StructField("TELE_LAST_UPDATED_DATE",   string),
            StructField("TELE_LAST_UPDATED_TIME",   string),
            StructField("PH_COMMENTS",              string),
            StructField("PH_NBR",                   string),
            StructField("TEMP_PH_NBR",              string),
            StructField("TEMP_PH_ACCESS",           string),
            StructField("TEMP_PH_COMMENTS",         string),
            StructField("TELE_TEMP_PH_DATE",        string),
            StructField("TELE_TEMP_PH_TIME",        string),
        ]
        + basic_fields
        + home_fields
        + away_fields
    )


# ---------------------------------------------------------------------------
# Internal helper — builds a target row dict with all columns None
# ---------------------------------------------------------------------------

def _empty_target_row() -> dict:
    """Return a target row dict with all TE_EMPLOYEE_PHONE columns set to None.

    Returns:
        Dict with keys matching get_target_schema() column names, all None.
    """
    return {
        "EMP_NBR":         None,
        "EMP_NO":          None,
        "PH_LIST":         None,
        "PH_PRTY":         None,
        "EFF_START_TM":    None,
        "EFF_END_TM":      None,
        "PH_NBR":          None,
        "PH_ACCESS":       None,
        "PH_COMMENTS":     None,
        "PH_TYPE":         None,
        "UNLISTD_IND":     None,
        "HOME_AWAY_IND":   None,
        "TEMP_PH_EXP_TS":  None,
        "TD_LD_TS":        None,
    }


# ---------------------------------------------------------------------------
# EMERGENCY expected outputs
# ---------------------------------------------------------------------------

def build_expected_emergency_outputs(spark: SparkSession) -> DataFrame:
    """Build the 3 expected output rows for the EMERGENCY source rows.

    Source rows from build_emergency_rows() → route_by_phone_type EMERGENCY branch
    → validate_and_finalize → TE_EMPLOYEE_PHONE.

    EFF_START_TM: "00:00:00" (from "0000"), EFF_END_TM: "23:59:00" (from "2359").
    PH_COMMENTS for Row 3 (EMP_NBR=102) is None because source had blank string.
    TD_LD_TS is None here — exclude from equality assertions.

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame with 3 rows matching TE_EMPLOYEE_PHONE schema.
    """
    # Row 1: EMP_NBR=100, no comments
    row1 = _empty_target_row()
    row1.update({
        "EMP_NBR":       100,
        "EMP_NO":        "000000100",
        "PH_LIST":       "EMERGENCY",
        "PH_PRTY":       1,
        "EFF_START_TM":  "00:00:00",
        "EFF_END_TM":    "23:59:00",
        "PH_NBR":        4085551001,
        "PH_ACCESS":     None,
        "PH_COMMENTS":   None,
        "PH_TYPE":       None,
        "UNLISTD_IND":   None,
        "HOME_AWAY_IND": None,
        "TEMP_PH_EXP_TS": None,
    })

    # Row 2: EMP_NBR=101, non-blank PH_COMMENTS preserved
    row2 = _empty_target_row()
    row2.update({
        "EMP_NBR":       101,
        "EMP_NO":        "000000101",
        "PH_LIST":       "EMERGENCY",
        "PH_PRTY":       1,
        "EFF_START_TM":  "00:00:00",
        "EFF_END_TM":    "23:59:00",
        "PH_NBR":        4085551002,
        "PH_ACCESS":     None,
        "PH_COMMENTS":   "EMERGENCY CONTACT",
        "PH_TYPE":       None,
        "UNLISTD_IND":   None,
        "HOME_AWAY_IND": None,
        "TEMP_PH_EXP_TS": None,
    })

    # Row 3: EMP_NBR=102, blank PH_COMMENTS → NULL
    row3 = _empty_target_row()
    row3.update({
        "EMP_NBR":       102,
        "EMP_NO":        "000000102",
        "PH_LIST":       "EMERGENCY",
        "PH_PRTY":       1,
        "EFF_START_TM":  "00:00:00",
        "EFF_END_TM":    "23:59:00",
        "PH_NBR":        4085551003,
        "PH_ACCESS":     None,
        "PH_COMMENTS":   None,     # blank → null
        "PH_TYPE":       None,
        "UNLISTD_IND":   None,
        "HOME_AWAY_IND": None,
        "TEMP_PH_EXP_TS": None,
    })

    schema = get_target_schema()
    return spark.createDataFrame([row1, row2, row3], schema=schema)


# ---------------------------------------------------------------------------
# TEMP expected outputs
# ---------------------------------------------------------------------------

def build_expected_temp_outputs(spark: SparkSession) -> DataFrame:
    """Build the 3 expected output rows for the TEMP source rows.

    Source rows from build_temp_rows() → route_by_phone_type TEMP branch
    → validate_and_finalize → TE_EMPLOYEE_PHONE.

    Row 2 (EMP_NBR=201) has None TEMP_PH_EXP_TS because source date/time were blank.

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame with 3 rows matching TE_EMPLOYEE_PHONE schema.
    """
    # Row 1: EMP_NBR=200 with access code and valid TEMP_PH_EXP_TS
    row1 = _empty_target_row()
    row1.update({
        "EMP_NBR":        200,
        "EMP_NO":         "000000200",
        "PH_LIST":        "TEMP",
        "PH_PRTY":        1,
        "EFF_START_TM":   "00:00:00",
        "EFF_END_TM":     "23:59:00",
        "PH_NBR":         5551112000,
        "PH_ACCESS":      "8",
        "PH_COMMENTS":    "Temp assignment",
        "PH_TYPE":        None,
        "UNLISTD_IND":    None,
        "HOME_AWAY_IND":  None,
        # TELE_TEMP_PH_DATE=251231, TELE_TEMP_PH_TIME=1800 → 2025-12-31 18:00:00
        "TEMP_PH_EXP_TS": datetime(2025, 12, 31, 18, 0, 0),
    })

    # Row 2: EMP_NBR=201 — blank date/time → NULL TEMP_PH_EXP_TS; blank TEMP_PH_COMMENTS → NULL
    row2 = _empty_target_row()
    row2.update({
        "EMP_NBR":        201,
        "EMP_NO":         "000000201",
        "PH_LIST":        "TEMP",
        "PH_PRTY":        1,
        "EFF_START_TM":   "00:00:00",
        "EFF_END_TM":     "23:59:00",
        "PH_NBR":         5551112001,
        "PH_ACCESS":      None,
        "PH_COMMENTS":    None,   # blank → null
        "PH_TYPE":        None,
        "UNLISTD_IND":    None,
        "HOME_AWAY_IND":  None,
        "TEMP_PH_EXP_TS": None,  # blank date/time → null
    })

    # Row 3: EMP_NBR=202 with TEMP_PH_EXP_TS from date 250101 time 0900
    row3 = _empty_target_row()
    row3.update({
        "EMP_NBR":        202,
        "EMP_NO":         "000000202",
        "PH_LIST":        "TEMP",
        "PH_PRTY":        1,
        "EFF_START_TM":   "00:00:00",
        "EFF_END_TM":     "23:59:00",
        "PH_NBR":         5551112002,
        "PH_ACCESS":      None,
        "PH_COMMENTS":    None,
        "PH_TYPE":        None,
        "UNLISTD_IND":    None,
        "HOME_AWAY_IND":  None,
        # TELE_TEMP_PH_DATE=250101, TELE_TEMP_PH_TIME=0900 → 2025-01-01 09:00:00
        "TEMP_PH_EXP_TS": datetime(2025, 1, 1, 9, 0, 0),
    })

    schema = get_target_schema()
    return spark.createDataFrame([row1, row2, row3], schema=schema)


# ---------------------------------------------------------------------------
# ERROR expected outputs
# ---------------------------------------------------------------------------

def build_expected_error_outputs(spark: SparkSession) -> DataFrame:
    """Build the 3 expected error rows for route_by_phone_type error branch.

    Source rows from build_error_rows() are passed through unchanged to the
    error DataFrame.  Callers should compare against the raw source schema
    (get_error_schema()), not the target schema.

    These 3 rows have EMP_NBR values that fail ISVALID("INT32", EMP_NBR):
        Row 1: "ABC"             — alphabetic
        Row 2: None              — null
        Row 3: "99999999999999"  — exceeds int32 range

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame with 3 error rows matching get_error_schema().
    """
    def _empty_err() -> dict:
        row: dict = {
            "EMP_NBR": None,
            "USER_ID": None,
            "TELE_LAST_UPDATED_DATE": None,
            "TELE_LAST_UPDATED_TIME": None,
            "PH_COMMENTS": None,
            "PH_NBR": None,
            "TEMP_PH_NBR": None,
            "TEMP_PH_ACCESS": None,
            "TEMP_PH_COMMENTS": None,
            "TELE_TEMP_PH_DATE": None,
            "TELE_TEMP_PH_TIME": None,
        }
        for i in range(1, 6):
            row[f"BASIC_PH_NBR_{i}"] = None
            row[f"BASIC_PH_ACCESS_{i}"] = None
            row[f"BASIC_PH_COMMENTS_{i}"] = None
            row[f"BASIC_PH_TYPE_{i}"] = None
            row[f"BASIC_PH_UNLIST_CD_{i}"] = None
            row[f"BASIC_PH_HOME_AWAY_CD_{i}"] = None
        for g in range(1, 4):
            row[f"TELE_HOME_PRI_FROM_{g}"] = None
            row[f"TELE_HOME_PRI_TO_{g}"] = None
            for s in range(1, 6):
                row[f"TELE_HOME_PRI_SEQ_{s}_{g}"] = None
        for g in range(1, 4):
            row[f"TELE_AWAY_PRI_FROM_{g}"] = None
            row[f"TELE_AWAY_PRI_TO_{g}"] = None
            for s in range(1, 6):
                row[f"TELE_AWAY_PRI_SEQ_{s}_{g}"] = None
        return row

    # Row 1: alphabetic EMP_NBR
    row1 = _empty_err()
    row1.update({
        "EMP_NBR": "ABC",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085559001",
    })

    # Row 2: null EMP_NBR
    row2 = _empty_err()
    row2.update({
        "EMP_NBR": None,
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085559002",
    })

    # Row 3: oversized EMP_NBR
    row3 = _empty_err()
    row3.update({
        "EMP_NBR": "99999999999999",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085559003",
    })

    schema = get_error_schema()
    return spark.createDataFrame([row1, row2, row3], schema=schema)


# ---------------------------------------------------------------------------
# EMP_NO boundary expected outputs
# ---------------------------------------------------------------------------

def build_expected_boundary_outputs(spark: SparkSession) -> DataFrame:
    """Build the 3 expected output rows for EMP_NO boundary test cases.

    Source rows from build_boundary_rows() exercise F.lpad(trim(EMP_NBR), 9, '0').

    Row 1: EMP_NBR="000000001" → EMP_NO="000000001" (9 chars, no change).
    Row 2: EMP_NBR="1"         → EMP_NO="000000001" (1 char, pad 8 zeros).
    Row 3: EMP_NBR="12345678"  → EMP_NO="012345678" (8 chars, pad 1 zero).

    These rows route to EMERGENCY because they have valid PH_NBR values.

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame with 3 rows matching TE_EMPLOYEE_PHONE schema (no TD_LD_TS).
    """
    row1 = _empty_target_row()
    row1.update({
        "EMP_NBR":       1,
        "EMP_NO":        "000000001",
        "PH_LIST":       "EMERGENCY",
        "PH_PRTY":       1,
        "EFF_START_TM":  "00:00:00",
        "EFF_END_TM":    "23:59:00",
        "PH_NBR":        4085556001,
        "PH_ACCESS":     None,
        "PH_COMMENTS":   None,
        "PH_TYPE":       None,
        "UNLISTD_IND":   None,
        "HOME_AWAY_IND": None,
        "TEMP_PH_EXP_TS": None,
    })

    row2 = _empty_target_row()
    row2.update({
        "EMP_NBR":       1,
        "EMP_NO":        "000000001",
        "PH_LIST":       "EMERGENCY",
        "PH_PRTY":       1,
        "EFF_START_TM":  "00:00:00",
        "EFF_END_TM":    "23:59:00",
        "PH_NBR":        4085556002,
        "PH_ACCESS":     None,
        "PH_COMMENTS":   None,
        "PH_TYPE":       None,
        "UNLISTD_IND":   None,
        "HOME_AWAY_IND": None,
        "TEMP_PH_EXP_TS": None,
    })

    row3 = _empty_target_row()
    row3.update({
        "EMP_NBR":       12345678,
        "EMP_NO":        "012345678",
        "PH_LIST":       "EMERGENCY",
        "PH_PRTY":       1,
        "EFF_START_TM":  "00:00:00",
        "EFF_END_TM":    "23:59:00",
        "PH_NBR":        4085556003,
        "PH_ACCESS":     None,
        "PH_COMMENTS":   None,
        "PH_TYPE":       None,
        "UNLISTD_IND":   None,
        "HOME_AWAY_IND": None,
        "TEMP_PH_EXP_TS": None,
    })

    schema = get_target_schema()
    return spark.createDataFrame([row1, row2, row3], schema=schema)
