# Code Review Report: CREW_J_TE_EMPLOYEE_PHONE_DW

**Reviewer:** Claude Code (Step 3B Independent Audit)
**Date:** 2026-03-24
**Job:** CREW_J_TE_EMPLOYEE_PHONE_DW
**Source XML:** input/CREW_J_TE_EMPLOYEE_PHONE_DW.xml
**Files Reviewed:**
- `output/project/jobs/crew_j_te_employee_phone_dw/transformations.py`
- `output/project/jobs/crew_j_te_employee_phone_dw/run_pipeline.py`
- `output/project/jobs/crew_j_te_employee_phone_dw/tests/conftest.py`
- `output/project/jobs/crew_j_te_employee_phone_dw/tests/test_transformations.py`
- `output/project/jobs/crew_j_te_employee_phone_dw/tests/test_integration.py`
- `output/project/jobs/crew_j_te_employee_phone_dw/deploy_config.sql`

---

## Summary

| Metric | Value |
|--------|-------|
| Checks passed (before fixes) | 11 / 13 |
| Checks passed (after fixes) | 13 / 13 |
| Critical issues found | 0 |
| Minor issues found | 2 |
| Fixes applied | 2 |
| Overall score (before) | 85 / 100 |
| Overall score (after) | 96 / 100 |

---

## Check Results

### CHECK 1: Stage Coverage — PASS

All 34 DataStage stages are accounted for in the generated code:

| Stage | Implementation |
|-------|---------------|
| WSTELE_LND_TDC | SourceReader in PipelineRunner (no job code needed) |
| SEPARATE_PHTYPE_TFM | `route_by_phone_type()` (lines 77–216) |
| BASIC_TYPE_PVT | `pivot_basic_phones()` (lines 223–262) |
| BASIC_REC_TFM | `enrich_basic_records()` (lines 269–290) |
| BASIC_REC_TFM 5-way split | `split_basic_by_home_away()` (lines 297–340) |
| HOME_CPY, AWAY_CPY | Python variable reference — no function needed |
| HOME_SEQ1/2/3_PVT | `pivot_home_sequence(seq_num=1/2/3)` (lines 347–389) |
| AWAY_SEQ1/2/3_PVT | `pivot_away_sequence(seq_num=1/2/3)` (lines 420–439) |
| HOME_SEQ2_TFM, HOME_SEQ3_TFM | `add_home_seq_offset(offset=5/10)` (lines 396–413) |
| AWAY_SEQ2_TFM, AWAY_SEQ3_TFM | `add_away_seq_offset(offset=5/10)` (lines 446–463) |
| HOME_SEQ_FNL, AWAY_SEQ_FNL | `funnel_sequences()` (lines 470–493) |
| INC_PRTY_TFM, INC_AWAY_PRTY_TFM | `increment_priority()` (lines 500–543) |
| HOME_BASIC_JNR, AWAY_BASIC_JNR | `join_home_records()` (lines 550–578) |
| NEW_PRTY_TFM, ABREC_NEW_PRTY_TFM | `calculate_new_priority()` (lines 585–618) |
| MAX_PRTY_AGG, MAX_ABPRTY_AGG | `aggregate_max_priority()` (lines 625–641) |
| BASIC_MAX_JNR, AWAY_MAX_JNR | `join_max_priority()` (lines 648–666) |
| ADJUSTING_PRTY_TFM, ADJUST_PRTY_TFM | `adjust_priority()` (lines 673–702) |
| ALL_REC_FNL | `merge_all_records()` (lines 709–747) |
| NULL_VAL_TFM | `validate_and_finalize()` (lines 754–826) |
| TE_EMPLOYEE_PHONE_DW | DeltaManager in PipelineRunner |
| WSSEN_ERR_SEQ | Error target — handled by DeltaManager in PipelineRunner |

Note: discovery.json reports stage_count=30; the review specification lists 34 logical stages (some stages appear twice for home/away symmetry). All logical stages are covered.

**Fix applied:** None.

---

### CHECK 2: Column Mapping Completeness — PASS

`validate_and_finalize()` outputs all 14 target columns of TE_EMPLOYEE_PHONE_DW.
Verified column-by-column:

| Target Column | Derivation | Status |
|--------------|-----------|--------|
| EMP_NBR | `F.col("EMP_NBR").cast("int")` | CORRECT |
| EMP_NO | `F.lpad(F.trim(F.col("EMP_NBR").cast("string")), 9, "0")` | CORRECT |
| PH_LIST | `F.col("CALL_LIST").alias("PH_LIST")` | CORRECT |
| PH_PRTY | `F.col("CALL_PRTY").alias("PH_PRTY")` | CORRECT |
| EFF_START_TM | `F.concat(F.substring("EFF_START_TM",1,2), F.lit(":"), F.substring("EFF_START_TM",3,2), F.lit(":00"))` | CORRECT |
| EFF_END_TM | same pattern as EFF_START_TM | CORRECT |
| PH_NBR | `F.col("PH_NBR").cast("long")` | CORRECT |
| PH_ACCESS | `_clean_string("PH_ACCESS")` — null if blank/less-than-space | CORRECT |
| PH_COMMENTS | `_clean_string("PH_COMMENTS")` — null if blank/less-than-space | CORRECT |
| PH_TYPE | `F.col("PH_TYPE")` | CORRECT |
| UNLISTD_IND | `F.col("UNLISTD_IND")` | CORRECT |
| HOME_AWAY_IND | `F.col("HOME_AWAY_IND")` | CORRECT |
| TEMP_PH_EXP_TS | `F.col("TEMP_PH_EXP_TS").cast("timestamp")` | CORRECT |
| TD_LD_TS | `F.current_timestamp()` | CORRECT |

**Fix applied:** None.

---

### CHECK 3: Expression Conversion Accuracy — PASS

All DataStage expressions are correctly converted to native PySpark (no ExpressionConverter wrappers):

| DataStage Expression | PySpark Equivalent | Code Location | Status |
|---------------------|-------------------|---------------|--------|
| `ISVALID("INT32", EMP_NBR)` | `F.col("EMP_NBR").cast("int").isNotNull()` | route_by_phone_type line 101 | CORRECT |
| `isvalid("int64", PH_NBR)` | `F.col("PH_NBR").cast("long").isNotNull()` | route_by_phone_type line 102 | CORRECT |
| `Not(IsValid(...))` | `~valid_emp` | route_by_phone_type line 110 | CORRECT |
| `SETNULL()` | `F.lit(None).cast("type")` | multiple locations | CORRECT |
| `TRIMLEADINGTRAILING(X)` | `F.trim(F.col("X"))` | multiple locations | CORRECT |
| `IF A THEN B ELSE C` | `F.when(A, B).otherwise(C)` | multiple locations | CORRECT |
| `LEFT(X,2):':':RIGHT(X,2)` | `F.concat(F.substring(X,1,2), F.lit(":"), F.substring(X,3,2))` | validate_and_finalize lines 785-795 | CORRECT |
| `ISVALID("TIMESTAMP", X)` | `F.to_timestamp(X, fmt).isNotNull()` | _build_upd_ts | CORRECT |
| `IsNull(X)` | `F.col("HOME_AWAY_IND").isNotNull()` (negated) | split_basic_by_home_away line 327 | CORRECT |

No `E.ds_*()` wrapper methods present. Fully native PySpark.

**Fix applied:** None.

---

### CHECK 4: Filter Implementation — PASS

All verbatim filter constraints from the XML are correctly implemented:

| Filter | Code | Status |
|--------|------|--------|
| emergency | `valid_emp & valid_ph_nbr` = `cast("int").isNotNull() & cast("long").isNotNull()` | CORRECT |
| temp | `valid_emp & valid_temp_ph_nbr` = `cast("int").isNotNull() & cast("long").isNotNull()` | CORRECT |
| basic | `valid_emp` = `cast("int").isNotNull()` | CORRECT |
| home | `valid_emp` = `cast("int").isNotNull()` | CORRECT |
| away | `valid_emp` = `cast("int").isNotNull()` | CORRECT |
| error | `~valid_emp` = `cast("int").isNull()` | CORRECT |
| ALL_BREC_OUT_TFM | `F.col("PH_NBR").cast("long").isNotNull()` | CORRECT (split_basic line 334) |
| CALL_PRIORITY_RIGHT_JNR / basic_home_jnr | `valid_ph & not_null_home_away & is_b_or_h` | CORRECT (split_basic line 331) |
| CALL_PRI_RIGHT_JNR / basic_away_jnr | `valid_ph & not_null_home_away & is_b_or_a` | CORRECT (split_basic line 332) |
| NULL_VAL_TFM | `F.col("PH_NBR").cast("long").isNotNull()` | CORRECT (validate_and_finalize line 809) |

**Fix applied:** None.

---

### CHECK 5: Join Implementation — PASS

Both join functions implement INNER JOIN correctly:

- `join_home_records()`: `on=["EMP_NBR", "LKP_CALL_PRTY"], how="inner"` — verified at line 574-578.
- `join_max_priority()`: `on="EMP_NBR", how="inner"` — verified at line 666.

**Fix applied:** None.

---

### CHECK 6: Aggregation Implementation — PASS

Both aggregators use the correct pattern:

`cprty_df.groupBy("EMP_NBR").agg(F.max("CALL_PRTY").alias("MAX_CALL_PRTY"))` — verified at line 641.

`aggregate_max_priority` is called for both `home_cprty_df` and `away_cprty_df` in `run_pipeline.py` (lines 138 and 146).

**Fix applied:** None.

---

### CHECK 7: Framework Usage — No Duplication — PASS

Verified no framework duplication in job files:

- No SourceReader / JDBC logic in transformations.py or run_pipeline.py.
- No DeltaManager / Delta write logic in job files.
- No AuditLogger / WatermarkManager / DQValidator code in job files.
- `run_pipeline.py` imports only from `framework.pipeline_runner` and `jobs.crew_j_te_employee_phone_dw.transformations`.
- `CrewEmployeePhoneDwPipeline` extends `PipelineRunner` and only overrides `transform()`.
- `transform()` returns `dict[str, DataFrame]` with keys `"main"` and `"error"`.
- `__main__` block is present and correct.

**Fix applied:** None.

---

### CHECK 8: Config/Deployment Completeness — PASS

`deploy_config.sql` validated:

| Required Field | Value | Status |
|---------------|-------|--------|
| job_name | `CREW_J_TE_EMPLOYEE_PHONE_DW` | PRESENT |
| source table | `CREW_WSTELE_LND` (in jdbc_options.dbtable) | PRESENT |
| main target | `TE_EMPLOYEE_PHONE_NEW` | PRESENT (`targets.main.table`) |
| error target | `TE_EMPLOYEE_PHONE_ERR` | PRESENT (`targets.error.table`) |
| write_mode main | `truncate_load` | PRESENT |
| write_mode error | `append` | PRESENT |
| business_keys | `["EMP_NBR", "PH_NBR"]` | PRESENT |
| Config JSON | Valid JSON structure | VALID |

DDL present for TE_EMPLOYEE_PHONE_NEW, TE_EMPLOYEE_PHONE (live), and TE_EMPLOYEE_PHONE_ERR with all 14 target columns. No YAML files generated (correct per CLAUDE.md).

**Fix applied:** None.

---

### CHECK 9: No Hardcoded Values — PASS

Scanned transformations.py and run_pipeline.py:

- No literal table names in Python code (table names flow from config).
- No hardcoded connection strings.
- Routing conditions use column names and business constants (`"EMERGENCY"`, `"TEMP"`, `"BASIC"`, `"HOME"`, `"AWAY"`) — these are transformation logic, not config values, which is correct and expected.
- `F.lit("0000")`, `F.lit("2359")`, `F.lit("0001")` are domain constants representing time boundaries ("midnight-to-close-of-day") from the DataStage XML — not hardcoded config values.

**Fix applied:** None.

---

### CHECK 10: Code Quality — Linting — PASS (after fix)

**Issue found:** Line 25 of transformations.py exceeded 120 characters.

```python
# BEFORE (154 chars):
def _build_upd_ts(date_col: str = "TELE_LAST_UPDATED_DATE", time_col: str = "TELE_LAST_UPDATED_TIME") -> "Column":  # type: ignore[name-defined]  # noqa: F821

# AFTER (split across 4 lines — all under 120 chars):
def _build_upd_ts(  # noqa: F821
    date_col: str = "TELE_LAST_UPDATED_DATE",
    time_col: str = "TELE_LAST_UPDATED_TIME",
) -> "Column":  # type: ignore[name-defined]
```

All other quality checks:
- `from __future__ import annotations` present in all Python files.
- Type annotations on all public function parameters and return types — PASS.
- Google-style docstrings on all public functions and modules — PASS.
- No unused imports — PASS (`F` import in run_pipeline.py has `# noqa: F401` note — intentional, available for subclass use).
- Consistent f-string usage — PASS.

**Fix applied:** `transformations.py` line 25 — split long function signature.

---

### CHECK 11: Code Quality — Architecture — PASS

- All transformation functions are pure: `(DataFrame, config) -> DataFrame` or `tuple[DataFrame, ...]`. No side effects.
- `run_pipeline.py` extends `PipelineRunner` — does not duplicate its logic.
- `transform()` returns `dict[str, DataFrame]` for multi-target (`"main"` and `"error"`).
- `__main__` block: `CrewEmployeePhoneDwPipeline(spark, job_name="CREW_J_TE_EMPLOYEE_PHONE_DW").run()` — correct.
- `split_basic_by_home_away()` correctly caches the DataFrame before splitting to 5 consumers (avoiding re-computation).
- `calculate_new_priority()` correctly caches before returning two references to the same DataFrame.

**Fix applied:** None.

---

### CHECK 12: Test Coverage — PASS (after fix)

**Issue found:** `TestJoinHomeRecords` and `TestCalculateNewPriority` test classes were absent from test_transformations.py. Both are required by the CHECK 12 specification.

**Fix applied:** Added both classes to `test_transformations.py`:

**`TestJoinHomeRecords`** (3 test methods):
- `test_inner_join_returns_only_matched_rows` — verifies only matching EMP_NBR+LKP_CALL_PRTY rows survive
- `test_unmatched_emp_nbr_excluded` — verifies EMP_NBR with no basic match is excluded
- `test_matched_row_has_ph_nbr` — verifies PH_NBR from right side is present in output

**`TestCalculateNewPriority`** (4 test methods):
- `test_returns_tuple_of_two` — verifies function returns a 2-tuple
- `test_call_prty_is_dense_rank_within_emp_nbr` — verifies row numbering restarts at 1 per EMP_NBR group
- `test_eff_start_and_end_renamed_from_tele_columns` — verifies column rename from TELE_HOME_PRI_FROM/TO
- `test_both_outputs_have_same_row_count` — verifies both tuple elements reference the same cached data

Pre-existing test classes verified:
- `TestRouteByPhoneType` — PASS (8 test methods: emergency, error, basic, temp, null EMP_NBR, null field assertions, UPD_TS, AWAY remap)
- `TestPivotBasicPhones` — PASS (4 test methods: 5-slot → 5-row, 0-based idx, null preserved, pass-through cols)
- `TestEnrichBasicRecords` — PASS (3 test methods)
- `TestSplitBasicByHomeAway` — PASS (5 test methods)
- `TestPivotHomeSequence` — PASS (4 test methods including funnel)
- `TestAggregateMaxPriority` — PASS (2 test methods)
- `TestAdjustPriority` — PASS (4 test methods)
- `TestValidateAndFinalize` — PASS (11 test methods covering all required scenarios)
- `TestTransformIntegration` — PASS (full end-to-end pipeline, 12 test methods)

---

### CHECK 13: Databricks Workflow — N/A / PASS

`output/discovery.json` confirms:
```json
"orchestration": null,
"scenario": "single_parallel_job",
"generates_orchestration": false
```

No sequence orchestration needed. Single parallel job processed independently. No `databricks.yml` workflow entries required for this job.

**Status:** PASS — No sequence orchestration; single parallel job.

---

## Fixes Applied Summary

| Fix # | File | Location | Issue | Change |
|-------|------|----------|-------|--------|
| 1 | `transformations.py` | Line 25 | Function signature exceeded 120 char limit (154 chars) | Split `_build_upd_ts` signature across 4 lines |
| 2 | `tests/test_transformations.py` | End of file | `TestJoinHomeRecords` and `TestCalculateNewPriority` classes missing | Added both classes with 3 and 4 test methods respectively |

---

## Scoring Table

| Dimension | Weight | Before Fixes | After Fixes | Notes |
|-----------|--------|-------------|-------------|-------|
| Functional correctness (CHECKs 1–6) | 35% | 35/35 | 35/35 | All stage mappings, column derivations, expressions, filters, joins, aggregations correct |
| Framework usage (CHECK 7, 11) | 20% | 20/20 | 20/20 | Clean separation; no duplication; PipelineRunner correctly extended |
| Config-driven (CHECK 8, 9) | 15% | 15/15 | 15/15 | deploy_config.sql complete; no hardcoded values |
| Code quality (CHECK 10) | 15% | 12/15 | 15/15 | One long line fixed |
| Test coverage (CHECK 12) | 15% | 3/15 | 15/15 | Missing TestJoinHomeRecords + TestCalculateNewPriority added |
| **Overall** | **100%** | **85/100** | **100/100** | |

> Note: The integration test (CHECK 12 integration item) was already present and comprehensive (12 test methods). The deduction before fixes was solely for the 2 missing unit test classes.

---

## Manual Validation Items

The following items require manual verification that cannot be confirmed by static code review alone:

1. **Teradata JDBC URL and credentials**: `${TERADATA_JDBC_URL}`, `${TD_WORK_DB}` placeholders in deploy_config.sql must be substituted with actual values before deployment.
2. **Unity Catalog names**: `${UNITY_CATALOG}` and `${UNITY_SCHEMA}` must be substituted.
3. **After-SQL promotion**: The DataStage job has an "After-SQL" equivalent — the pattern of truncate-loading TE_EMPLOYEE_PHONE_NEW and then promoting to TE_EMPLOYEE_PHONE must be confirmed with the deployment team. This is documented in deploy_config.sql comments but is not automated in the pipeline code.
4. **Error file path**: Original DataStage writes errors to `$LOG_DIR/EMPLOYEE_PHONE.err` (sequential file). The Databricks migration writes to TE_EMPLOYEE_PHONE_ERR Delta table — this change must be approved by the business.
5. **WSSEN_ERR_SEQ (PxSequentialFile)**: The error output is re-routed from a flat file to a Delta table. Verify the error schema (TE_EMPLOYEE_PHONE_ERR DDL) meets downstream consumer requirements.
