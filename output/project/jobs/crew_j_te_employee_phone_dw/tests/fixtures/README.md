# Synthetic Test Data — CREW_J_TE_EMPLOYEE_PHONE_DW

## Source Table: CREW_WSTELE_LND

Wide table with one row per employee containing ALL phone types denormalized.
Each test row targets a specific transformation branch or edge case.

---

## Row Inventory

| Row # | Function | EMP_NBR | Purpose | Expected Route | Expected Target |
|---|---|---|---|---|---|
| 1 | `build_emergency_rows` | 00100 | Valid EMP_NBR + valid PH_NBR, no comments | EMERGENCY branch | TE_EMPLOYEE_PHONE (PH_LIST=EMERGENCY) |
| 2 | `build_emergency_rows` | 00101 | Valid EMP_NBR + valid PH_NBR + non-blank PH_COMMENTS | EMERGENCY branch | TE_EMPLOYEE_PHONE (PH_COMMENTS preserved) |
| 3 | `build_emergency_rows` | 00102 | Valid EMP_NBR + valid PH_NBR + blank PH_COMMENTS | EMERGENCY branch | TE_EMPLOYEE_PHONE (PH_COMMENTS=NULL) |
| 4 | `build_temp_rows` | 00200 | Valid TEMP_PH_NBR + date + time | TEMP branch | TE_EMPLOYEE_PHONE (TEMP_PH_EXP_TS populated) |
| 5 | `build_temp_rows` | 00201 | Valid TEMP_PH_NBR, blank date/time | TEMP branch | TE_EMPLOYEE_PHONE (TEMP_PH_EXP_TS=NULL) |
| 6 | `build_temp_rows` | 00202 | Valid TEMP_PH_NBR + alternate date/time | TEMP branch | TE_EMPLOYEE_PHONE (TEMP_PH_EXP_TS=2025-01-01) |
| 7 | `build_basic_rows` | 00300 | 5 BASIC slots: H, A, B, None-IND, None-NBR | BASIC branch | All 4 valid slots pivot; H→HOME, A→AWAY, B→both, None-IND→all_basic only |
| 8 | `build_basic_rows` | 00301 | 2 BASIC slots: B + A | BASIC branch | Slot 1 (B)→HOME+AWAY, Slot 2 (A)→AWAY only |
| 9 | `build_basic_rows` | 00302 | Slot 1 has non-numeric PH_NBR | BASIC branch | Pivot succeeds but invalid PH_NBR filtered by split_basic_by_home_away |
| 10 | `build_basic_rows` | 00303 | All BASIC slots None | BASIC branch | Pivot produces rows but all filter out (PH_NBR=NULL) |
| 11 | `build_home_rows` | 00300 | 3 HOME groups, groups 1+2 populated | HOME branch | Joins with basic rows for EMP_NBR=00300 |
| 12 | `build_home_rows` | 00301 | GROUP 1 slots 1+2 only | HOME branch | Joins with basic rows for EMP_NBR=00301 |
| 13 | `build_home_rows` | 00400 | All 3 HOME groups populated, no matching BASIC | HOME branch | Sequence pivot succeeds; inner join with basic produces no output |
| 14 | `build_away_rows` | 00300 | GROUP 1 partially populated | AWAY branch | Joins with basic rows for EMP_NBR=00300 (AWAY IND) |
| 15 | `build_away_rows` | 00301 | GROUP 1 slot 1 only | AWAY branch | Joins with basic rows for EMP_NBR=00301 (AWAY IND) |
| 16 | `build_away_rows` | 00500 | All AWAY groups, no matching BASIC | AWAY branch | No join output |
| 17 | `build_error_rows` | ABC | Non-numeric EMP_NBR | ERROR branch | TE_EMPLOYEE_PHONE_ERR (row unchanged) |
| 18 | `build_error_rows` | None | Null EMP_NBR | ERROR branch | TE_EMPLOYEE_PHONE_ERR (row unchanged) |
| 19 | `build_error_rows` | 99999999999999 | EMP_NBR > int32 max | ERROR branch | TE_EMPLOYEE_PHONE_ERR (row unchanged) |
| 20 | `build_boundary_rows` | 000000001 | 9-char EMP_NBR — no padding needed | EMERGENCY branch | EMP_NO=000000001 |
| 21 | `build_boundary_rows` | 1 | 1-char EMP_NBR — pad 8 zeros | EMERGENCY branch | EMP_NO=000000001 |
| 22 | `build_boundary_rows` | 12345678 | 8-char EMP_NBR — pad 1 zero | EMERGENCY branch | EMP_NO=012345678 |

---

## Routing Coverage

| Branch | Rows Covering It | Key Condition |
|---|---|---|
| EMERGENCY | Rows 1-3, 20-22 | ISVALID("INT32", EMP_NBR) AND ISVALID("int64", PH_NBR) |
| TEMP | Rows 4-6 | ISVALID("INT32", EMP_NBR) AND ISVALID("int64", TEMP_PH_NBR) |
| BASIC | Rows 7-10 | ISVALID("INT32", EMP_NBR) — all valid-EMP rows feed this |
| HOME | Rows 11-13 | ISVALID("INT32", EMP_NBR) — TELE_HOME_PRI_* columns |
| AWAY | Rows 14-16 | ISVALID("INT32", EMP_NBR) — TELE_AWAY_PRI_* → remapped to HOME names |
| ERROR | Rows 17-19 | NOT(ISVALID("INT32", EMP_NBR)) |

---

## Join Coverage

| Join | Left (seq) Rows | Right (basic) Rows | Expected Match Condition | Expected Matches |
|---|---|---|---|---|
| HOME_BASIC_JNR | EMP_NBR=00300 HOME seqs | EMP_NBR=00300 H/B basic slots | EMP_NBR + LKP_CALL_PRTY | Slots 1,3 (H, B) join where seq value = CALL_PRTY |
| HOME_BASIC_JNR | EMP_NBR=00301 HOME seqs | EMP_NBR=00301 B basic slot | EMP_NBR + LKP_CALL_PRTY | Slot 1 (B) |
| HOME_BASIC_JNR | EMP_NBR=00400 HOME seqs | No matching BASIC | — | 0 rows (inner join) |
| AWAY_BASIC_JNR | EMP_NBR=00300 AWAY seqs | EMP_NBR=00300 A/B basic slots | EMP_NBR + LKP_CALL_PRTY | Slots 2,3 (A, B) join |
| AWAY_BASIC_JNR | EMP_NBR=00301 AWAY seqs | EMP_NBR=00301 A/B basic slots | EMP_NBR + LKP_CALL_PRTY | Slots 1,2 (B, A) |
| AWAY_BASIC_JNR | EMP_NBR=00500 AWAY seqs | No matching BASIC | — | 0 rows (inner join) |
| BASIC_MAX_JNR | EMP_NBR=00300 MAX agg | EMP_NBR=00300 HOME basic | EMP_NBR | MAX_CALL_PRTY attached |
| AWAY_MAX_JNR | EMP_NBR=00300 MAX agg | EMP_NBR=00300 AWAY basic | EMP_NBR | MAX_CALL_PRTY attached |

---

## Column Coverage

| Column | Test Scenarios |
|---|---|
| EMP_NBR | Valid int (various), alphabetic (error), null (error), > int32 max (error) |
| PH_NBR | Valid long (emergency rows), non-castable ("INVALID_NUM" in basic), null (boundary after route) |
| TEMP_PH_NBR | Valid long (temp rows), null (all non-temp rows) |
| PH_COMMENTS | Non-blank preserved (row 2), blank → NULL (row 3), null stays null (row 1) |
| TEMP_PH_ACCESS | Non-blank "8" preserved (row 4), blank → NULL (row 5), null (row 6) |
| TEMP_PH_COMMENTS | Non-blank preserved (row 4), blank → NULL (row 5) |
| TELE_TEMP_PH_DATE + TIME | Both valid → TEMP_PH_EXP_TS assembled (rows 4, 6), both blank → NULL (row 5) |
| TELE_LAST_UPDATED_DATE + TIME | Valid on all non-error rows → UPD_TS assembled |
| BASIC_PH_HOME_AWAY_CD_* | H (row 7 slot 1), A (row 7 slot 2), B (row 7 slot 3), None (row 7 slot 4) |
| TELE_HOME_PRI_SEQ_*_* | Groups 1-3 with all 5 seq slots (row 13), partial (rows 11-12) |
| TELE_AWAY_PRI_SEQ_*_* | Group 1 partial (rows 14-15), all groups (row 16) |
| EFF_START_TM / EFF_END_TM | "0000"→"00:00:00", "2359"→"23:59:00" (emergency/temp), "0001"→"00:01:00" (adjust_priority) |
| EMP_NO | 9-char unchanged (row 20), 1-char pad (row 21), 8-char pad (row 22) |

---

## Notes for Integration Tests

1. **TD_LD_TS exclusion**: `TD_LD_TS = F.current_timestamp()` — always exclude from
   `assert_df_equality` using `ignore_columns=["TD_LD_TS"]`.

2. **Row ordering**: The final union (merge_all_records) uses `unionByName` which does
   not guarantee row order. Use `assert_df_equality(..., ignore_row_order=True)`.

3. **BASIC join matching**: LKP_CALL_PRTY in the basic branch equals the 1-based slot
   index (after enrich_basic_records adds +1 to the 0-based pivot index). The HOME/AWAY
   sequence's `LKP_CALL_PRTY` comes from the TELE_*_PRI_SEQ_N_G values. A join match
   only happens when these values align — use matching integer strings in the seq columns.

4. **AWAY column remapping**: AWAY rows in the source use `TELE_AWAY_PRI_*` columns.
   `route_by_phone_type` remaps these to `TELE_HOME_PRI_*` names before handing off to
   `pivot_away_sequence`, so AWAY test rows must populate `TELE_AWAY_PRI_*` in the
   source but expect `TELE_HOME_PRI_*` column names in the away_df output.

5. **Caching**: `split_basic_by_home_away` calls `.cache()` internally. Tests that
   exercise this function should be aware that subsequent filter results share the
   cached plan — this is transparent to test logic but affects memory in larger suites.

6. **Error rows**: Error rows are returned as-is from `route_by_phone_type`. The error
   DataFrame retains all source schema columns (matching `get_error_schema()`), not the
   target schema.

7. **Home-only / away-only employees** (rows 13, 16): These have no matching BASIC rows.
   The inner joins produce zero output rows for these employees. They do not appear in
   the final TE_EMPLOYEE_PHONE output from the HOME/AWAY pipeline branches.
