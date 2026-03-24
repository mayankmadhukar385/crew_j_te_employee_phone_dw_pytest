# Step 1B: LLM-as-a-Judge — Validate and Correct Lineage Outputs

## Multi-Job Awareness

Read `output/discovery.json`. For EACH parallel job in `execution_plan.jobs_to_process`,
validate and correct its lineage Excel and Mermaid independently. Generate a validation
report per job plus an overall summary.

Output files per job:
- `output/{job_name}_lineage.xlsx` — CORRECTED (overwrite)
- `output/{job_name}_lineage_diagram.md` — CORRECTED (overwrite)
- `output/{job_name}_validation_report.md` — Validation findings and scores

Overall summary:
- `output/validation_summary.md` — Aggregated scores across all jobs

## Your Role

You are an independent reviewer and corrector. You did NOT generate the lineage files.
Your job is to audit them against the original XML, find every gap, fix them, and
produce corrected output files.

You are skeptical by default. Assume the lineage generator may have missed things.

---

## Your Inputs

1. The original DataStage XML from `input/` folder — this is your ground truth
2. The generated Excel workbook from `output/` folder (`*_lineage.xlsx`)
3. The generated Mermaid diagram from `output/` folder (`*_lineage_diagram.md`)

---

## Your Outputs

1. `output/{job_name}_lineage.xlsx` — **CORRECTED** Excel (overwrite the original)
2. `output/{job_name}_lineage_diagram.md` — **CORRECTED** Mermaid (overwrite the original)
3. `output/{job_name}_validation_report.md` — Validation report with findings and scores

---

## Validation Process

Execute these checks in order. For each check, record: PASS, FAIL, or WARNING with details.

### CHECK 1: Stage Completeness

Parse the XML independently. Build your own stage list from scratch:
- Read every Record with Type in (CustomStage, TransformerStage)
- Extract stage name and stage type
- Classify as Source/Transformation/Target by port analysis

Compare against the Excel `Stage_Sequence` sheet:
- [ ] Every stage in XML exists in Excel — no missing stages
- [ ] No extra stages in Excel that don't exist in XML
- [ ] Stage types match exactly
- [ ] Stage categories (Source/Transformation/Target) are correct

**If any stage is missing or miscategorized → FIX the Excel.**

### CHECK 2: Edge Completeness

Parse the XML independently. Build your own edge list:
- Map output ports (CustomOutput/TrxOutput) and input ports (CustomInput/TrxInput)
- Match by link name to build edges

Compare against:
- Excel `Stage_Sequence` Link_Names_In / Link_Names_Out columns
- Mermaid diagram edges

- [ ] Every edge in XML exists in the Mermaid diagram
- [ ] Every edge in XML is reflected in the Excel link columns
- [ ] No phantom edges that don't exist in XML
- [ ] Edge labels (link names) match exactly

**If any edge is missing from the Mermaid → FIX the Mermaid.**
**If link names are wrong in Excel → FIX the Excel.**

### CHECK 3: Source SQL Transformation Layer

For every source connector stage (TeradataConnectorPX, DB2ConnectorPX,
OracleConnectorPX, ODBCConnectorPX with Stage_Category = Source):

Extract the SQL query from the XML. Look for:
- `CAST(...)` expressions
- `TRIM(...)` / `LTRIM(...)` / `RTRIM(...)` expressions
- `COALESCE(...)` expressions
- `CASE WHEN ... END` expressions
- `SUBSTRING(...)` expressions
- Aliasing (`AS new_name`)
- Any function applied to a column in the SELECT clause

If the source SQL applies transformations to columns:
- [ ] These columns should NOT be marked as "Pass-through" in Source_to_Target_Mapping
- [ ] Derivation_Logic should include both the source SQL transformation AND any
      downstream transformer derivation, combined
- [ ] Mapping_Type should be "Derived" if the source SQL modifies the column
- [ ] Remarks should note: "Source SQL applies CAST/TRIM before transformer stage"

**If columns with source SQL transformations are marked Pass-through → FIX:**
- Change Mapping_Type from "Pass-through" to "Derived"
- Add the source SQL expression to Derivation_Logic (prepend it, e.g.,
  "Source SQL: CAST(TRIM(col) AS VARCHAR(100)) → Transformer: pass-through")
- Add Remarks noting the source-level transformation

### CHECK 4: Transformer Derivation Completeness

For every CTransformerStage / TransformerStage in the XML:
- Find all TrxOutput Records for this stage
- Extract the Columns Collection
- For each column, read the Derivation property

Compare against the Excel `Source_to_Target_Mapping`:
- [ ] Every column with a non-trivial derivation expression is captured
- [ ] Derivation expressions match the XML exactly (verbatim)
- [ ] Columns with no derivation (pass-through) are correctly marked

**If a derivation is missing, simplified, or paraphrased → FIX with the verbatim XML expression.**

### CHECK 5: Target Stage Validation in Source_to_Target_Mapping

- [ ] The Target_Stage column contains ONLY actual target stages (stages with no output links)
- [ ] No intermediate transformation stages appear as Target_Stage
- [ ] Every column on every target stage's input link is represented

**If intermediate stages appear as Target_Stage → FIX: restructure rows so only
actual target stages appear, with end-to-end derivation aggregated.**

### CHECK 6: Constant and System Variable Columns

For columns derived from constants or system variables:
- [ ] Source_Column should say "(constant)" or "(system variable)", not NaN/blank
- [ ] Source_Table_or_File should say "(none)" or "(generated)", not NaN/blank
- [ ] Default_or_Constant_Logic should contain the actual constant value or variable name
- [ ] Mapping_Type should be "Constant"

**If constant columns have blank Source_Column → FIX with descriptive placeholder.**

### CHECK 7: Target Load Strategy

For every target connector stage, extract from XML:
- Write mode (insert, overwrite, upsert, merge)
- Table action (truncate, create, replace)
- Target table name

Compare against Excel `Stage_Sequence` Transformation_Summary:
- [ ] Load strategy is described (e.g., "Truncate-and-load", "Merge/Upsert", "Append")
- [ ] Target table name is mentioned

**If load strategy is missing from Transformation_Summary → FIX.**

### CHECK 8: Filter and Constraint Expressions

For every transformer output link, check for:
- WhereClause properties
- Constraint expressions
- Reject link conditions

Compare against Excel Source_to_Target_Mapping Filter_Logic column:
- [ ] Every filter/constraint from the XML is captured in Filter_Logic
- [ ] Filter expressions are verbatim from XML

**If filters are missing → FIX by adding them.**

### CHECK 9: Join Key Validation

For every PxJoin stage, extract from XML:
- Key columns on each input link
- Join type (inner, left, right, full)

Compare against Excel Source_to_Target_Mapping Join_Logic column:
- [ ] Join keys are documented for columns affected by joins
- [ ] Join type is specified

**If join information is missing → FIX.**

### CHECK 10: Aggregation Validation

For every PxAggregator stage, extract from XML:
- Group-by columns
- Aggregate functions and their target columns

Compare against Excel Source_to_Target_Mapping Aggregation_Logic column:
- [ ] Aggregation details are documented for columns affected by aggregations

**If aggregation info is missing → FIX.**

### CHECK 11: Mermaid Format Compliance

Verify the Mermaid diagram follows the required format:
- [ ] Uses `flowchart LR`
- [ ] Source stages use stadium shape: `(["STAGE\n(Type)"])`
- [ ] Target stages use cylinder shape: `[("STAGE\n(Type)")]`
- [ ] Transformation stages use rectangle: `["STAGE\n(Type)"]`
- [ ] NO diamonds `{"..."}`, hexagons `{{"..."}}`, or other shapes
- [ ] Colors match: source=#d4edda, target=#f8d7da, xform=#cce5ff
- [ ] Every node has two-line label: stage name + (type)
- [ ] Edge labels use link names in double quotes
- [ ] The .md file contains ONLY the heading and Mermaid block — no summary, no footer

**If any format violation → FIX the Mermaid.**

### CHECK 12: Sequence Number Consistency

Verify the Stage_Sequence sheet:
- [ ] Sequence numbers follow topological order
- [ ] Parallel branches use letter suffixes (3A, 3B, 3C)
- [ ] No gaps in sequence numbering
- [ ] Source stages come first, target stages come last

**If sequencing is wrong → FIX.**

---

## How to Apply Fixes

Write Python code that:
1. Reads the existing Excel using `pandas` + `openpyxl`
2. Reads the existing Mermaid `.md` file
3. Parses the XML independently
4. Runs all 12 checks
5. Applies fixes to the DataFrames and Mermaid text
6. Writes corrected Excel (with same formatting: bold headers, autofilter, freeze panes)
7. Writes corrected Mermaid `.md` file
8. Writes the validation report

---

## Validation Report Format

Write `output/{job_name}_validation_report.md` with this structure:

```markdown
# Validation Report: {job_name}

## Summary
- **Checks passed**: X / 12
- **Fixes applied**: Y
- **Original confidence**: X.X / 10
- **Corrected confidence**: X.X / 10

## Check Results

### CHECK 1: Stage Completeness
- Status: PASS / FAIL / WARNING
- Details: ...
- Fix applied: (if any)

### CHECK 2: Edge Completeness
...

(repeat for all 12 checks)

## Fixes Applied

1. **Source SQL transformations**: Changed N columns from Pass-through to Derived,
   added source SQL expressions to Derivation_Logic
2. **Constant columns**: Filled blank Source_Column with "(constant)" for N rows
3. ...

## Confidence Scoring

| Category | Before | After | Notes |
|---|---|---|---|
| Pipeline structure | X/10 | X/10 | ... |
| Source extraction | X/10 | X/10 | ... |
| Transformation logic | X/10 | X/10 | ... |
| Target load | X/10 | X/10 | ... |
| Column mapping | X/10 | X/10 | ... |
| Edge completeness | X/10 | X/10 | ... |
| **Overall** | **X/10** | **X/10** | |
```

---

## Critical Rules

- Parse the XML INDEPENDENTLY — do not trust the Excel or Mermaid as correct
- Every fix must be traceable: say what was wrong and what you changed
- Do not remove or alter content that is already correct
- Copy derivation expressions VERBATIM from XML — never summarize or simplify
- The corrected Excel must retain the same formatting (bold headers, autofilter, etc.)
- The corrected Mermaid must follow the exact format spec (shapes, colors, no extras)
- Be conservative: if uncertain whether something is wrong, flag as WARNING, don't fix
