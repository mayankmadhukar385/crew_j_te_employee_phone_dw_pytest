# Step 3B: LLM-as-a-Judge — Validate and Correct Generated Job Code

## Your Role

You are an independent code reviewer. You did NOT generate the Databricks job code.
Your job is to audit the generated code against the original XML, the corrected
lineage Excel (all 3 tabs), the shared framework, and engineering standards.
Find every gap, fix them, and produce a corrected project with a detailed report.

## Multi-Job Awareness

Read `output/discovery.json`. Validate EACH parallel job's code independently.
Also validate the Databricks Workflow if one was generated.

---

## Your Inputs

1. Original DataStage XML from `input/` — ground truth
2. Corrected lineage Excel per job from `output/{job}_lineage.xlsx`
3. Generated job code from `output/project/jobs/{job}/`
4. Shared framework from `output/project/framework/` — verify it's being used correctly
5. `output/discovery.json` — job inventory and orchestration spec

## Your Outputs Per Job

1. `output/project/jobs/{job}/` — CORRECTED job files (overwrite what needs fixing)
2. `output/{job}_code_review_report.md` — detailed findings and scores

Overall:
3. `output/code_review_summary.md` — aggregated scores across all jobs

---

## Validation Checks

### CHECK 1: Stage Coverage

Read `Stage_Sequence` sheet from the corrected Excel. For each stage:
- [ ] There is corresponding code that implements this stage's logic in `transformations.py`
- [ ] The stage is called in `run_pipeline.py` in the correct execution order

**If a stage has no implementing code → flag as CRITICAL, write the missing function.**

### CHECK 2: Column Mapping Completeness

Read `Source_to_Target_Mapping` sheet. For each row:
- [ ] The target column appears in the transformation output
- [ ] The derivation logic is implemented in `transformations.py`
- [ ] Pass-through columns are indeed passed through unchanged
- [ ] Derived columns have the correct PySpark expression
- [ ] Constant columns are generated with correct values

**If a column is missing or its derivation is wrong → FIX the transformation code.**

### CHECK 3: Expression Conversion Accuracy

For each derived column, compare:
- The DataStage expression (from Excel `Derivation_Logic` column)
- The PySpark expression in the generated code

Verify each DataStage function uses the shared `native PySpark`:
- [ ] `If/Then/Else` → `E.ds_if_then_else()`
- [ ] `IsValid()` → `E.ds_is_valid()`
- [ ] `TrimLeadingTrailing()` → `E.ds_trim()`
- [ ] `NullToValue()` → `E.ds_null_to_value()`
- [ ] `Left()/Right()` → `E.ds_left()` / `E.ds_right()`
- [ ] `CurrentTimestamp()` → `E.ds_current_timestamp()`
- [ ] Concatenation (`:`) → `E.ds_concat()`
- [ ] All other functions used in this pipeline

**If any expression is incorrectly converted or doesn't use native PySpark → FIX.**

### CHECK 4: Filter Implementation

Read `Filter_Logic` column from Excel. For each non-null filter:
- [ ] The filter is implemented as `df.filter(...)` in the code
- [ ] The filter condition is correctly translated to PySpark

### CHECK 5: Join Implementation

Read `Join_Logic` column from Excel. For each non-null join:
- [ ] A `.join()` call exists with the correct keys
- [ ] Join type (inner/left/right) matches the XML

### CHECK 6: Aggregation Implementation

Read `Aggregation_Logic` column. For each non-null aggregation:
- [ ] A `.groupBy().agg()` call exists with correct keys and functions

### CHECK 7: Framework Usage — No Duplication

Scan ALL Python files in `jobs/{job}/`:
- [ ] No reader code duplicated from `framework/readers/`
- [ ] No writer/SCD code duplicated from `framework/common_utility.py`
- [ ] No audit/error/watermark logic duplicated from framework
- [ ] No expression converter logic duplicated — uses `E.ds_*()` methods
- [ ] `run_pipeline.py` imports `AuditLogger`, `DatabricksJobLogger.log_rejects`, `DeltaManager`,
      `WatermarkManager`, `DQValidator` from `framework.common_utility`

**If framework code is duplicated → FIX: replace with imports from framework.**

### CHECK 8: Config Completeness

Check `configs/pipeline_config.yaml`:
- [ ] Has actual table names from the pipeline (not just placeholders)
- [ ] Source type matches the actual source connector
- [ ] Target table name matches the actual target
- [ ] SCD type is specified (0, 1, 2, or 4)
- [ ] Business keys are specified for the target
- [ ] Merge keys are specified (if incremental mode)

Check `configs/column_mappings.yaml`:
- [ ] Every target column from Excel is listed
- [ ] Derivation expressions are present for derived columns

Check `configs/business_rules.yaml`:
- [ ] Routing conditions are present (if routing exists)
- [ ] Filter expressions are present
- [ ] DQ check rules are defined

### CHECK 9: No Hardcoded Values

Scan ALL Python files in `jobs/{job}/`:
- [ ] No hardcoded table names (should come from config)
- [ ] No hardcoded column names in transformation logic (should come from config/mappings)
- [ ] No hardcoded connection strings
- [ ] Table references use config-driven paths

**If hardcoded values found → FIX by replacing with config references.**

### CHECK 10: Code Quality — Linting

Check every Python file:
- [ ] All functions have type annotations (parameters + return type)
- [ ] All modules have Google-style docstrings
- [ ] All public functions have docstrings
- [ ] Line length ≤ 120 characters
- [ ] Imports are at module top (no inline imports except in conditional blocks)
- [ ] No unused imports
- [ ] Uses `ruff` rule set (E, F, I, N, W, UP, S, B, A, C4, SIM, D)

**If linting violations found → FIX them.**

### CHECK 11: Code Quality — Architecture

- [ ] Every transformation function is a pure function: `(DataFrame, config) → DataFrame`
- [ ] No side effects in transformation functions
- [ ] `run_pipeline.py` uses `AuditLogger.start_run()` and `AuditLogger.end_run()`
- [ ] `run_pipeline.py` uses `DatabricksJobLogger.log_rejects.log_rejects()` and `DatabricksJobLogger.log_rejects.flush()`
- [ ] `run_pipeline.py` uses `DQValidator.validate()` after source read
- [ ] `run_pipeline.py` uses `DeltaManager.write()` with SCD type from config
- [ ] `run_pipeline.py` uses `WatermarkManager` for incremental loads
- [ ] Error handling (try/except) wraps the pipeline with proper audit logging

### CHECK 12: Test Coverage

For each transformation function in `jobs/{job}/transformations.py`:
- [ ] A corresponding test exists in `tests/test_transformations.py`
- [ ] At least 3 test functions (happy path, null handling, edge case)
- [ ] Tests use `chispa.assert_df_equality` for DataFrame assertions
- [ ] Test data uses actual column names from this pipeline
- [ ] Integration test exists that wires source → transform → validate output

**If test files are missing or incomplete → WRITE them.**

### CHECK 13: Databricks Workflow (if sequence job exists)

If `discovery.json` has a sequence orchestration:
- [ ] Workflow JSON/YAML exists
- [ ] Every parallel job has a corresponding task
- [ ] Conditional branches match the sequence job's conditions
- [ ] Task dependencies match the execution graph
- [ ] Exception handling / failure notifications are configured
- [ ] Job parameters are parameterized (not hardcoded)

---

## Report Format

Write `output/{job_name}_code_review_report.md`:

```markdown
# Code Review Report: {job_name}

## Summary
- **Checks passed**: X / 13
- **Critical issues**: N
- **Fixes applied**: N
- **Code quality score**: X / 10

## Check Results
### CHECK 1: Stage Coverage
- Status: PASS / FAIL
- Missing stages: (list or "None")
- Fix applied: (description or "N/A")

(repeat for all 13 checks)

## Scoring

| Category | Score | Notes |
|---|---|---|
| Functional correctness (stages, columns, expressions) | X/10 | |
| Framework usage (no duplication) | X/10 | |
| Config-driven design | X/10 | |
| Code quality (types, docs, lint) | X/10 | |
| Test coverage | X/10 | |
| **Overall** | **X/10** | |
```

---

## Critical Rules

- Parse the XML and Excel INDEPENDENTLY — do not trust the generated code
- Every fix must be traceable in the report
- Do not break working code when fixing issues
- When writing missing tests, use actual column names and realistic data
- When fixing expressions, verify against the original DataStage expression in the XML
- All expression conversions MUST use `native PySpark` from framework
- No framework code should exist in job directories — only imports
- Be conservative: flag as WARNING if uncertain, don't make speculative fixes
