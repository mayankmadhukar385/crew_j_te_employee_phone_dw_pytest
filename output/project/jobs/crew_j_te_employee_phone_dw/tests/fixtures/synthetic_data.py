"""Synthetic test data factories for CREW_J_TE_EMPLOYEE_PHONE_DW.

Each factory function returns a list of dicts representing source rows from
CREW_WSTELE_LND (post-source-SQL aliasing). Every row is commented with
its purpose and expected routing path.

All data is deterministic — no random values.

Column naming follows the phone_source_schema fixture in conftest.py exactly:
- EMP_NBR, USER_ID, TELE_LAST_UPDATED_DATE, TELE_LAST_UPDATED_TIME
- PH_COMMENTS, PH_NBR, TEMP_PH_NBR, TEMP_PH_ACCESS, TEMP_PH_COMMENTS
- TELE_TEMP_PH_DATE, TELE_TEMP_PH_TIME
- BASIC_PH_NBR_1..5, BASIC_PH_ACCESS_1..5, BASIC_PH_COMMENTS_1..5
- BASIC_PH_TYPE_1..5, BASIC_PH_UNLIST_CD_1..5, BASIC_PH_HOME_AWAY_CD_1..5
- TELE_HOME_PRI_FROM_1..3, TELE_HOME_PRI_TO_1..3
- TELE_HOME_PRI_SEQ_1_1..TELE_HOME_PRI_SEQ_5_1 (group 1)
- TELE_HOME_PRI_SEQ_1_2..TELE_HOME_PRI_SEQ_5_2 (group 2)
- TELE_HOME_PRI_SEQ_1_3..TELE_HOME_PRI_SEQ_5_3 (group 3)
- TELE_AWAY_PRI_FROM_1..3, TELE_AWAY_PRI_TO_1..3
- TELE_AWAY_PRI_SEQ_1_1..TELE_AWAY_PRI_SEQ_5_1 (group 1)
- TELE_AWAY_PRI_SEQ_1_2..TELE_AWAY_PRI_SEQ_5_2 (group 2)
- TELE_AWAY_PRI_SEQ_1_3..TELE_AWAY_PRI_SEQ_5_3 (group 3)
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession


# ---------------------------------------------------------------------------
# Internal helper — builds a fully-populated row with all columns set to None
# ---------------------------------------------------------------------------

def _empty_row() -> dict[str, Any]:
    """Return a row dict with every schema column initialised to None.

    Callers override only the fields relevant to their test scenario. This
    prevents KeyErrors when Spark tries to match row keys to the StructType
    schema and avoids accidental schema drift when new columns are added.

    Returns:
        Dict with all CREW_WSTELE_LND source columns set to None.
    """
    row: dict[str, Any] = {
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

    # BASIC phone slots 1-5
    for i in range(1, 6):
        row[f"BASIC_PH_NBR_{i}"] = None
        row[f"BASIC_PH_ACCESS_{i}"] = None
        row[f"BASIC_PH_COMMENTS_{i}"] = None
        row[f"BASIC_PH_TYPE_{i}"] = None
        row[f"BASIC_PH_UNLIST_CD_{i}"] = None
        row[f"BASIC_PH_HOME_AWAY_CD_{i}"] = None

    # HOME priority sequence columns — 3 groups × (FROM + TO + 5 sequences)
    for g in range(1, 4):
        row[f"TELE_HOME_PRI_FROM_{g}"] = None
        row[f"TELE_HOME_PRI_TO_{g}"] = None
        for s in range(1, 6):
            row[f"TELE_HOME_PRI_SEQ_{s}_{g}"] = None

    # AWAY priority sequence columns — 3 groups × (FROM + TO + 5 sequences)
    for g in range(1, 4):
        row[f"TELE_AWAY_PRI_FROM_{g}"] = None
        row[f"TELE_AWAY_PRI_TO_{g}"] = None
        for s in range(1, 6):
            row[f"TELE_AWAY_PRI_SEQ_{s}_{g}"] = None

    return row


# ---------------------------------------------------------------------------
# EMERGENCY rows
# ---------------------------------------------------------------------------

def build_emergency_rows() -> list[dict[str, Any]]:
    """Build 3 source rows that should route to the EMERGENCY branch.

    Routing condition (route_by_phone_type): ISVALID("INT32", EMP_NBR) AND
    ISVALID("int64", PH_NBR).

    Expected output:
        CALL_LIST='EMERGENCY', CALL_PRTY=1, EFF_START_TM='0000', EFF_END_TM='2359'

    Row 1: Valid EMP_NBR + valid PH_NBR, no comments.
    Row 2: Valid EMP_NBR + valid PH_NBR + non-blank PH_COMMENTS.
    Row 3: Valid EMP_NBR + valid PH_NBR + blank PH_COMMENTS (→ NULL in output).

    Returns:
        List of 3 source row dicts for the EMERGENCY path.
    """
    # Row 1 — clean emergency record, no comments
    row1 = _empty_row()
    row1.update({
        "EMP_NBR": "00100",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0900",
        "PH_NBR": "4085551001",
        "PH_COMMENTS": None,
    })

    # Row 2 — emergency record with a non-blank comment
    row2 = _empty_row()
    row2.update({
        "EMP_NBR": "00101",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0930",
        "PH_NBR": "4085551002",
        "PH_COMMENTS": "EMERGENCY CONTACT",
    })

    # Row 3 — emergency record with blank PH_COMMENTS → should become NULL in output
    row3 = _empty_row()
    row3.update({
        "EMP_NBR": "00102",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "1000",
        "PH_NBR": "4085551003",
        "PH_COMMENTS": "",      # blank → _null_if_blank → NULL
    })

    return [row1, row2, row3]


# ---------------------------------------------------------------------------
# TEMP rows
# ---------------------------------------------------------------------------

def build_temp_rows() -> list[dict[str, Any]]:
    """Build 3 source rows that should route to the TEMP branch.

    Routing condition: ISVALID("INT32", EMP_NBR) AND ISVALID("int64", TEMP_PH_NBR).

    Expected output:
        CALL_LIST='TEMP', CALL_PRTY=1, EFF_START_TM='0000', EFF_END_TM='2359'

    Row 1: Valid record with TEMP_PH_DATE + TEMP_PH_TIME → populated TEMP_PH_EXP_TS.
    Row 2: Blank TEMP_PH_DATE and TEMP_PH_TIME → NULL TEMP_PH_EXP_TS.
    Row 3: Valid record with alternate date/time values.

    Returns:
        List of 3 source row dicts for the TEMP path.
    """
    # Row 1 — full temp record: date + time provided → TEMP_PH_EXP_TS assembled
    row1 = _empty_row()
    row1.update({
        "EMP_NBR": "00200",
        "USER_ID": "CREW01",
        "TELE_LAST_UPDATED_DATE": "251015",
        "TELE_LAST_UPDATED_TIME": "1200",
        "TEMP_PH_NBR": "5551112000",
        "TEMP_PH_ACCESS": "8",
        "TEMP_PH_COMMENTS": "Temp assignment",
        "TELE_TEMP_PH_DATE": "251231",
        "TELE_TEMP_PH_TIME": "1800",
    })

    # Row 2 — blank date/time → TEMP_PH_EXP_TS should be NULL
    row2 = _empty_row()
    row2.update({
        "EMP_NBR": "00201",
        "USER_ID": "CREW02",
        "TELE_LAST_UPDATED_DATE": "251015",
        "TELE_LAST_UPDATED_TIME": "1200",
        "TEMP_PH_NBR": "5551112001",
        "TEMP_PH_ACCESS": None,
        "TEMP_PH_COMMENTS": "",      # blank → NULL
        "TELE_TEMP_PH_DATE": "",     # blank → NULL TEMP_PH_EXP_TS
        "TELE_TEMP_PH_TIME": "",     # blank → NULL TEMP_PH_EXP_TS
    })

    # Row 3 — alternate date/time
    row3 = _empty_row()
    row3.update({
        "EMP_NBR": "00202",
        "USER_ID": "CREW03",
        "TELE_LAST_UPDATED_DATE": "251015",
        "TELE_LAST_UPDATED_TIME": "1200",
        "TEMP_PH_NBR": "5551112002",
        "TEMP_PH_ACCESS": None,
        "TEMP_PH_COMMENTS": None,
        "TELE_TEMP_PH_DATE": "250101",
        "TELE_TEMP_PH_TIME": "0900",
    })

    return [row1, row2, row3]


# ---------------------------------------------------------------------------
# BASIC rows
# ---------------------------------------------------------------------------

def build_basic_rows() -> list[dict[str, Any]]:
    """Build 4 source rows exercising the BASIC phone branch.

    Routing condition: ISVALID("INT32", EMP_NBR).
    After pivot_basic_phones + enrich_basic_records, split_basic_by_home_away
    routes by HOME_AWAY_IND:
        'H' or 'B' → HOME join path
        'A' or 'B' → AWAY join path
        None or invalid PH_NBR → filtered out of join paths

    Row 1 (EMP_NBR=00300): Mixed HOME_AWAY_IND — H, A, B, None across 5 slots.
    Row 2 (EMP_NBR=00301): Only slots 1+2 used (B + A).
    Row 3 (EMP_NBR=00302): Slot 1 has non-castable PH_NBR → filtered after pivot.
    Row 4 (EMP_NBR=00303): All PH_NBR slots are None → no rows survive split.

    Returns:
        List of 4 source row dicts for the BASIC path.
    """
    # Row 1 — mixed HOME_AWAY_IND across 5 slots
    row1 = _empty_row()
    row1.update({
        "EMP_NBR": "00300",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        # Slot 1: H → only HOME path
        "BASIC_PH_NBR_1": "4085553001",
        "BASIC_PH_ACCESS_1": None,
        "BASIC_PH_COMMENTS_1": "Home line",
        "BASIC_PH_TYPE_1": "VOICE",
        "BASIC_PH_UNLIST_CD_1": "N",
        "BASIC_PH_HOME_AWAY_CD_1": "H",
        # Slot 2: A → only AWAY path
        "BASIC_PH_NBR_2": "4085553002",
        "BASIC_PH_ACCESS_2": None,
        "BASIC_PH_COMMENTS_2": None,
        "BASIC_PH_TYPE_2": "VOICE",
        "BASIC_PH_UNLIST_CD_2": "N",
        "BASIC_PH_HOME_AWAY_CD_2": "A",
        # Slot 3: B → both HOME and AWAY paths
        "BASIC_PH_NBR_3": "4085553003",
        "BASIC_PH_ACCESS_3": "9",
        "BASIC_PH_COMMENTS_3": None,
        "BASIC_PH_TYPE_3": "VOICE",
        "BASIC_PH_UNLIST_CD_3": "N",
        "BASIC_PH_HOME_AWAY_CD_3": "B",
        # Slot 4: None HOME_AWAY_IND → not_null check fails → filtered from join paths
        "BASIC_PH_NBR_4": "4085553004",
        "BASIC_PH_ACCESS_4": None,
        "BASIC_PH_COMMENTS_4": None,
        "BASIC_PH_TYPE_4": None,
        "BASIC_PH_UNLIST_CD_4": None,
        "BASIC_PH_HOME_AWAY_CD_4": None,
        # Slot 5: not populated
        "BASIC_PH_NBR_5": None,
        "BASIC_PH_HOME_AWAY_CD_5": None,
    })

    # Row 2 — 2 slots: B + A
    row2 = _empty_row()
    row2.update({
        "EMP_NBR": "00301",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        # Slot 1: B → both HOME and AWAY
        "BASIC_PH_NBR_1": "4085553011",
        "BASIC_PH_ACCESS_1": None,
        "BASIC_PH_COMMENTS_1": None,
        "BASIC_PH_TYPE_1": "VOICE",
        "BASIC_PH_UNLIST_CD_1": "N",
        "BASIC_PH_HOME_AWAY_CD_1": "B",
        # Slot 2: A → only AWAY
        "BASIC_PH_NBR_2": "4085553012",
        "BASIC_PH_ACCESS_2": None,
        "BASIC_PH_COMMENTS_2": None,
        "BASIC_PH_TYPE_2": "VOICE",
        "BASIC_PH_UNLIST_CD_2": "N",
        "BASIC_PH_HOME_AWAY_CD_2": "A",
    })

    # Row 3 — slot 1 has alpha characters in PH_NBR (cannot cast to long → filtered after pivot)
    row3 = _empty_row()
    row3.update({
        "EMP_NBR": "00302",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "BASIC_PH_NBR_1": "INVALID_NUM",   # non-castable → filtered by valid_ph in split
        "BASIC_PH_ACCESS_1": None,
        "BASIC_PH_COMMENTS_1": None,
        "BASIC_PH_TYPE_1": None,
        "BASIC_PH_UNLIST_CD_1": None,
        "BASIC_PH_HOME_AWAY_CD_1": "H",
    })

    # Row 4 — all phone slots are None → no output rows after pivot/filter
    row4 = _empty_row()
    row4.update({
        "EMP_NBR": "00303",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        # All BASIC_PH_NBR_* left as None (from _empty_row)
    })

    return [row1, row2, row3, row4]


# ---------------------------------------------------------------------------
# HOME rows
# ---------------------------------------------------------------------------

def build_home_rows() -> list[dict[str, Any]]:
    """Build 3 source rows exercising the HOME sequence branch.

    Routing condition: ISVALID("INT32", EMP_NBR) — same as basic/away.
    The HOME branch reads TELE_HOME_PRI_* columns for 3 groups, each with 5
    priority sequence slots. pivot_home_sequence + add_home_seq_offset +
    funnel_sequences + increment_priority + join_home_records combine these
    with BASIC records matched on EMP_NBR + LKP_CALL_PRTY.

    Row 1 (EMP_NBR=00300): Matches basic row 1.  Group 1 fully populated.
    Row 2 (EMP_NBR=00301): Matches basic row 2.  Group 1 slots 1+2 populated.
    Row 3 (EMP_NBR=00400): Home-only employee (no matching basic row → no join output).

    Returns:
        List of 3 source row dicts for the HOME path.
    """
    # Row 1 — EMP_NBR=00300 with group 1 fully populated (5 seqs)
    row1 = _empty_row()
    row1.update({
        "EMP_NBR": "00300",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        # Group 1
        "TELE_HOME_PRI_FROM_1": "0800",
        "TELE_HOME_PRI_TO_1": "1700",
        "TELE_HOME_PRI_SEQ_1_1": "1",    # LKP_CALL_PRTY for slot 1
        "TELE_HOME_PRI_SEQ_2_1": "2",
        "TELE_HOME_PRI_SEQ_3_1": "3",
        "TELE_HOME_PRI_SEQ_4_1": "4",
        "TELE_HOME_PRI_SEQ_5_1": "5",
        # Group 2 — partially populated
        "TELE_HOME_PRI_FROM_2": "1700",
        "TELE_HOME_PRI_TO_2": "2200",
        "TELE_HOME_PRI_SEQ_1_2": "1",
        "TELE_HOME_PRI_SEQ_2_2": "2",
        "TELE_HOME_PRI_SEQ_3_2": None,
        "TELE_HOME_PRI_SEQ_4_2": None,
        "TELE_HOME_PRI_SEQ_5_2": None,
        # Group 3 — not populated
        "TELE_HOME_PRI_FROM_3": None,
        "TELE_HOME_PRI_TO_3": None,
    })

    # Row 2 — EMP_NBR=00301 with group 1 slots 1+2 only
    row2 = _empty_row()
    row2.update({
        "EMP_NBR": "00301",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "TELE_HOME_PRI_FROM_1": "0700",
        "TELE_HOME_PRI_TO_1": "1900",
        "TELE_HOME_PRI_SEQ_1_1": "1",
        "TELE_HOME_PRI_SEQ_2_1": "2",
        "TELE_HOME_PRI_SEQ_3_1": None,
        "TELE_HOME_PRI_SEQ_4_1": None,
        "TELE_HOME_PRI_SEQ_5_1": None,
    })

    # Row 3 — EMP_NBR=00400, home-only (no matching basic row → inner join produces no output)
    row3 = _empty_row()
    row3.update({
        "EMP_NBR": "00400",
        "USER_ID": "CREW04",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        # Group 1 fully populated
        "TELE_HOME_PRI_FROM_1": "0600",
        "TELE_HOME_PRI_TO_1": "2300",
        "TELE_HOME_PRI_SEQ_1_1": "1",
        "TELE_HOME_PRI_SEQ_2_1": "2",
        "TELE_HOME_PRI_SEQ_3_1": "3",
        "TELE_HOME_PRI_SEQ_4_1": "4",
        "TELE_HOME_PRI_SEQ_5_1": "5",
        # Group 2 fully populated
        "TELE_HOME_PRI_FROM_2": "2300",
        "TELE_HOME_PRI_TO_2": "0600",
        "TELE_HOME_PRI_SEQ_1_2": "1",
        "TELE_HOME_PRI_SEQ_2_2": "2",
        "TELE_HOME_PRI_SEQ_3_2": "3",
        "TELE_HOME_PRI_SEQ_4_2": "4",
        "TELE_HOME_PRI_SEQ_5_2": "5",
        # Group 3 — one slot
        "TELE_HOME_PRI_FROM_3": "0000",
        "TELE_HOME_PRI_TO_3": "2359",
        "TELE_HOME_PRI_SEQ_1_3": "1",
        "TELE_HOME_PRI_SEQ_2_3": None,
        "TELE_HOME_PRI_SEQ_3_3": None,
        "TELE_HOME_PRI_SEQ_4_3": None,
        "TELE_HOME_PRI_SEQ_5_3": None,
    })

    return [row1, row2, row3]


# ---------------------------------------------------------------------------
# AWAY rows
# ---------------------------------------------------------------------------

def build_away_rows() -> list[dict[str, Any]]:
    """Build 3 source rows exercising the AWAY sequence branch.

    Routing condition: ISVALID("INT32", EMP_NBR).
    AWAY columns (TELE_AWAY_PRI_*) are remapped to TELE_HOME_PRI_* names by
    route_by_phone_type (AWAY_TYPE_CPY output), so pivot_away_sequence delegates
    to pivot_home_sequence with the same logic.

    Row 1 (EMP_NBR=00300): Matches basic row 1 for AWAY join.
    Row 2 (EMP_NBR=00301): Matches basic row 2 for AWAY join.
    Row 3 (EMP_NBR=00500): Away-only employee (no matching basic row).

    Returns:
        List of 3 source row dicts for the AWAY path.
    """
    # Row 1 — EMP_NBR=00300 with group 1 fully populated
    row1 = _empty_row()
    row1.update({
        "EMP_NBR": "00300",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "TELE_AWAY_PRI_FROM_1": "0800",
        "TELE_AWAY_PRI_TO_1": "2000",
        "TELE_AWAY_PRI_SEQ_1_1": "1",
        "TELE_AWAY_PRI_SEQ_2_1": "2",
        "TELE_AWAY_PRI_SEQ_3_1": "3",
        "TELE_AWAY_PRI_SEQ_4_1": None,
        "TELE_AWAY_PRI_SEQ_5_1": None,
        "TELE_AWAY_PRI_FROM_2": None,
        "TELE_AWAY_PRI_TO_2": None,
        "TELE_AWAY_PRI_FROM_3": None,
        "TELE_AWAY_PRI_TO_3": None,
    })

    # Row 2 — EMP_NBR=00301 with group 1 slot 1 only
    row2 = _empty_row()
    row2.update({
        "EMP_NBR": "00301",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "TELE_AWAY_PRI_FROM_1": "0700",
        "TELE_AWAY_PRI_TO_1": "1800",
        "TELE_AWAY_PRI_SEQ_1_1": "1",
        "TELE_AWAY_PRI_SEQ_2_1": None,
        "TELE_AWAY_PRI_SEQ_3_1": None,
        "TELE_AWAY_PRI_SEQ_4_1": None,
        "TELE_AWAY_PRI_SEQ_5_1": None,
    })

    # Row 3 — EMP_NBR=00500, away-only (no matching basic row → inner join empty)
    row3 = _empty_row()
    row3.update({
        "EMP_NBR": "00500",
        "USER_ID": "CREW05",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "TELE_AWAY_PRI_FROM_1": "0500",
        "TELE_AWAY_PRI_TO_1": "2100",
        "TELE_AWAY_PRI_SEQ_1_1": "1",
        "TELE_AWAY_PRI_SEQ_2_1": "2",
        "TELE_AWAY_PRI_SEQ_3_1": "3",
        "TELE_AWAY_PRI_SEQ_4_1": "4",
        "TELE_AWAY_PRI_SEQ_5_1": "5",
        "TELE_AWAY_PRI_FROM_2": "2100",
        "TELE_AWAY_PRI_TO_2": "0500",
        "TELE_AWAY_PRI_SEQ_1_2": "1",
        "TELE_AWAY_PRI_SEQ_2_2": "2",
        "TELE_AWAY_PRI_SEQ_3_2": None,
        "TELE_AWAY_PRI_SEQ_4_2": None,
        "TELE_AWAY_PRI_SEQ_5_2": None,
    })

    return [row1, row2, row3]


# ---------------------------------------------------------------------------
# ERROR rows
# ---------------------------------------------------------------------------

def build_error_rows() -> list[dict[str, Any]]:
    """Build 3 source rows that should route to the ERROR branch.

    Routing condition: NOT(ISVALID("INT32", EMP_NBR)).
    These rows are passed through to TE_EMPLOYEE_PHONE_ERR unchanged.

    Row 1: EMP_NBR is alphabetic — cannot cast to int.
    Row 2: EMP_NBR is None — null cannot cast to int.
    Row 3: EMP_NBR is too large for int32 (max int32 = 2,147,483,647).

    Returns:
        List of 3 source row dicts for the ERROR path.
    """
    # Row 1 — alphabetic EMP_NBR
    row1 = _empty_row()
    row1.update({
        "EMP_NBR": "ABC",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085559001",
    })

    # Row 2 — null EMP_NBR
    row2 = _empty_row()
    row2.update({
        "EMP_NBR": None,
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085559002",
    })

    # Row 3 — EMP_NBR exceeds int32 range (2^31 - 1 = 2,147,483,647)
    row3 = _empty_row()
    row3.update({
        "EMP_NBR": "99999999999999",   # too large for int32 → cast returns null
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085559003",
    })

    return [row1, row2, row3]


# ---------------------------------------------------------------------------
# BOUNDARY rows
# ---------------------------------------------------------------------------

def build_boundary_rows() -> list[dict[str, Any]]:
    """Build 3 source rows for boundary/edge-case testing of EMP_NO padding.

    validate_and_finalize pads EMP_NBR to 9 chars with leading zeros.
    DataStage derivation:
        If Len(Trim(EMP_NBR)) = 9 Then Trim(EMP_NBR)
        Else Str('0', 9 - Len(Trim(EMP_NBR))) : Trim(EMP_NBR)
    PySpark equivalent: F.lpad(F.trim(EMP_NBR), 9, '0')

    Row 1: EMP_NBR already 9 chars — EMP_NO unchanged ("000000001").
    Row 2: EMP_NBR 1 char — EMP_NO = "000000001".
    Row 3: EMP_NBR 8 chars — EMP_NO = "012345678".

    All 3 rows have a valid PH_NBR so they survive the final filter.

    Returns:
        List of 3 source row dicts for EMP_NO boundary testing.
    """
    # Row 1 — 9-char EMP_NBR → no padding needed
    row1 = _empty_row()
    row1.update({
        "EMP_NBR": "000000001",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085556001",
    })

    # Row 2 — 1-char EMP_NBR → lpad to "000000001"
    row2 = _empty_row()
    row2.update({
        "EMP_NBR": "1",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085556002",
    })

    # Row 3 — 8-char EMP_NBR → lpad to "012345678"
    row3 = _empty_row()
    row3.update({
        "EMP_NBR": "12345678",
        "USER_ID": "SYSADM",
        "TELE_LAST_UPDATED_DATE": "251001",
        "TELE_LAST_UPDATED_TIME": "0800",
        "PH_NBR": "4085556003",
    })

    return [row1, row2, row3]


# ---------------------------------------------------------------------------
# Master dataset builder
# ---------------------------------------------------------------------------

def build_full_source_dataset(spark: SparkSession, schema: "StructType") -> DataFrame:  # type: ignore[name-defined]  # noqa: F821
    """Combine all factory rows into a single source DataFrame and register as a temp view.

    This is the primary entry point for integration tests.  The returned DataFrame
    contains all test rows from every branch so a single pipeline run exercises
    all code paths simultaneously.

    The temp view name ("crew_wstele_lnd_test") matches pipeline_config fixture
    ``source.test_view_name``.

    Args:
        spark: Active SparkSession.
        schema: StructType matching phone_source_schema fixture from conftest.py.

    Returns:
        DataFrame registered as temp view "crew_wstele_lnd_test".
    """
    rows = (
        build_emergency_rows()
        + build_temp_rows()
        + build_basic_rows()
        + build_home_rows()
        + build_away_rows()
        + build_error_rows()
        + build_boundary_rows()
    )
    df = spark.createDataFrame(rows, schema=schema)
    df.createOrReplaceTempView("crew_wstele_lnd_test")
    return df
