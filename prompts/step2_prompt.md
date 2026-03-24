# Step 2: XML + Lineage Excel + Mermaid → Databricks Code Generation Prompt

## Multi-Job Awareness

Read `output/discovery.json`. Generate a code prompt for EACH parallel job in
`execution_plan.jobs_to_process`.

Output per job:
- `output/{job_name}_databricks_code_prompt.md`

If a sequence orchestration exists, also generate:
- `output/{seq_name}_workflow_prompt.md` — a prompt for generating the Databricks
  Workflow JSON/YAML that orchestrates all the parallel job tasks with the same
  conditional branching logic as the DataStage sequence job.

## Your Task

Read three inputs:
1. The DataStage XML from `input/` folder
2. The lineage Excel workbook from `output/` folder (`*_lineage.xlsx`)
3. The Mermaid diagram from `output/` folder (`*_lineage_diagram.md`)

The Excel has three sheets:
- **`Stage_Sequence`** — stage inventory with execution order, types, links, transformation summaries
- **`Mermaid_Lineage`** — Mermaid diagram text and plain-English flow summary
- **`Source_to_Target_Mapping`** — column-level lineage with derivation logic

Read all three sheets using Python (pandas). Read the XML for exact expression extraction.
Read the Mermaid `.md` file to verify edge completeness.

Produce one output file:
- `output/databricks_code_prompt.md`

This specification must be so detailed that the code generator in Step 3 needs
ZERO additional context. Everything it needs must be in this document.

---

## How to Read the Excel

```python
import pandas as pd

# Read all three sheets
stages_df = pd.read_excel("output/<job>_lineage.xlsx", sheet_name="Stage_Sequence")
mappings_df = pd.read_excel("output/<job>_lineage.xlsx", sheet_name="Source_to_Target_Mapping")
mermaid_df = pd.read_excel("output/<job>_lineage.xlsx", sheet_name="Mermaid_Lineage")
```

From `Stage_Sequence` you get:
- `Sequence_No`, `Stage_Name`, `Stage_Type`, `Stage_Category`
- `Input_Stages`, `Output_Stages`
- `Link_Names_In`, `Link_Names_Out`
- `Transformation_Summary`

From `Source_to_Target_Mapping` you get:
- `Target_Stage`, `Target_Table_or_File`, `Target_Column`
- `Source_Stage`, `Source_Table_or_File`, `Source_Column`
- `Derivation_Logic`, `Transformation_Types`
- `Join_Logic`, `Filter_Logic`, `Lookup_Logic`, `Aggregation_Logic`
- `Default_or_Constant_Logic`, `Mapping_Type`, `Remarks`

From the XML you get the exact verbatim derivation expressions, SQL queries,
filter conditions, join keys — things the Excel may have summarized.

---

## Output Document Structure

Generate `output/databricks_code_prompt.md` with these sections:

### SECTION 1: Pipeline Identity

```markdown
## 1. Pipeline identity

- **Job name**: <from Stage_Sequence or XML>
- **Source system**: <database type from source stage's Stage_Type>
- **Source table(s)**: <from Source_to_Target_Mapping Source_Table_or_File, or SQL in XML>
- **Target system**: Delta Lake (Databricks)
- **Target table(s)**: <from Source_to_Target_Mapping Target_Table_or_File>
- **Error target**: <if any stage with Stage_Category=Target writes errors, list it>
- **Total stages**: <count from Stage_Sequence>
- **Total edges**: <count unique links from Link_Names_In/Out>
- **Total target columns**: <count rows in Source_to_Target_Mapping per target>
```

### SECTION 2: Stage Types Found and PySpark Mappings

Read the `Stage_Type` column from `Stage_Sequence`. List ONLY types present:

```markdown
## 2. Stage types and PySpark mappings

| DataStage Stage Type | Count | PySpark Implementation |
|---|---|---|
| <type from Excel> | <count> | <PySpark equivalent — see reference below> |
```

Reference for mapping (include only rows for types actually found in Stage_Sequence):

- `TeradataConnectorPX` (Source) → `spark.read.format("jdbc")` or Auto Loader
- `TeradataConnectorPX` (Target) → Delta MERGE or overwrite
- `DB2ConnectorPX` (Source/Target) → JDBC read / Delta MERGE
- `OracleConnectorPX` (Source/Target) → JDBC read / Delta MERGE
- `ODBCConnectorPX` (Source/Target) → JDBC read / Delta MERGE
- `CTransformerStage` → `df.select()` / `df.withColumn()` with `F.when`, `F.coalesce`, etc.
- `PxPivot` → `F.expr("stack(N, ...)")` for unpivot
- `PxCopy` → DataFrame variable reuse (no-op in Spark)
- `PxFunnel` → `unionByName(allowMissingColumns=True)`
- `PxJoin` → `df.join(other, on=keys, how=type)`
- `PxAggregator` → `df.groupBy(*keys).agg(F.max(...), ...)`
- `PxSort` → `df.orderBy(...)`
- `PxLookup` → `df.join(F.broadcast(lookup_df), on=keys, how="left")`
- `PxRemDup` → `df.dropDuplicates(keys)`
- `PxFilter` → `df.filter(condition)`
- `PxSequentialFile` → `df.write.format("delta")` or file write
- `PxSurrogateKeyGenerator` → `F.monotonically_increasing_id()`
- Any other type → `UNMAPPED: <type> — implement closest PySpark equivalent, mark TODO`

Use `Stage_Category` from the Excel to determine Source vs Target for connector stages.

### SECTION 3: Complete Transformation Chain

For EVERY row in `Stage_Sequence`, in order of `Sequence_No`, write:

```markdown
### Stage: <Stage_Name> (Sequence: <Sequence_No>)
- **Type**: <Stage_Type>
- **Category**: <Stage_Category>
- **Inputs**: <Link_Names_In> from <Input_Stages>
- **Outputs**: <Link_Names_Out> to <Output_Stages>
- **Summary**: <Transformation_Summary from Excel>

**Derivation expressions** (verbatim from XML):
<For CTransformerStage stages, go to the XML, find the TrxOutput Records
for this stage, extract every column's Derivation property, and list them:>
- <COLUMN_NAME>: `<exact derivation expression from XML>`

**Filter condition** (from XML):
<WhereClause or constraint expression from the stage's output link, or "None">

**SQL query** (for source connectors, from XML):
<The SELECT statement if found in the XML>

**Join keys** (from XML, for PxJoin stages):
<Key columns and join type>

**Aggregation** (from XML, for PxAggregator stages):
<Group-by keys and aggregate functions>
```

**CRITICAL**: For derivation expressions, filters, SQL, join keys — go back to the XML
and copy them verbatim. The Excel `Derivation_Logic` column may summarize or simplify them.
The code generator needs the exact DataStage syntax.

### SECTION 4: DataStage Expression Dictionary

Scan every derivation expression you extracted from the XML in Section 3.
For each unique DataStage function found, list its PySpark equivalent.

**Include ONLY functions actually used in this pipeline:**

```markdown
## 4. Expression dictionary

| DataStage Function | PySpark Equivalent |
|---|---|
| If COND Then A Else B | `F.when(COND, A).otherwise(B)` |
| IsValid("int32", X) | `F.col("X").cast("int").isNotNull()` |
| IsValid("int64", X) | `F.col("X").cast("long").isNotNull()` |
| IsNull(X) | `F.isnull(F.col("X"))` |
| SetNull() | `F.lit(None).cast("<target_type>")` |
| NullToValue(X, val) | `F.coalesce(F.col("X"), F.lit(val))` |
| TrimLeadingTrailing(X) | `F.trim(F.col("X"))` |
| Left(X, N) | `F.substring(F.col("X"), 1, N)` |
| Right(X, N) | `F.expr(f"right(X, {N})")` |
| Len(X) | `F.length(F.col("X"))` |
| Str(char, N) | `F.expr(f"repeat('{char}', {N})")` |
| X : Y (concatenation) | `F.concat(F.col("X"), F.col("Y"))` |
| CurrentTimestamp() | `F.current_timestamp()` |
| ... |
```

Delete rows for functions NOT used in this pipeline's expressions.

### SECTION 5: Column Mappings

Read from `Source_to_Target_Mapping` sheet. For each unique `Target_Stage`, build a table:

```markdown
## 5. Column mappings

### Target: <Target_Table_or_File> via <Target_Stage>

| # | Target Column | Source Column | Mapping Type | Derivation Logic |
|---|---|---|---|---|
| 1 | <Target_Column> | <Source_Column> | <Mapping_Type> | <Derivation_Logic or "—"> |
```

Include ALL rows from the Excel — do not skip any column.

### SECTION 6: Routing/Branching Logic

Look at `Stage_Sequence` for stages where `Output_Stages` has multiple entries.
For each such stage, extract the routing conditions from the XML:

```markdown
## 6. Routing logic

### Router: <Stage_Name>

| Output Link | Downstream Stage | Condition |
|---|---|---|
| <Link_Names_Out[0]> | <Output_Stages[0]> | <filter/constraint from XML> |
| <Link_Names_Out[1]> | <Output_Stages[1]> | <filter/constraint from XML> |
```

Extract conditions from the XML transformer's output port properties
(WhereClause, constraint expressions). Do NOT guess from stage names.

If no routing exists (all stages have single outputs), write:
"No routing logic — linear pipeline."

### SECTION 7: Pipeline Config Template

```yaml
## 7. Pipeline config

pipeline:
  job_name: <job name from Excel/XML>
  run_mode: full_refresh
  catalog: "${UNITY_CATALOG}"
  schema: "${UNITY_SCHEMA}"

source:
  type: <jdbc | file | delta — based on source Stage_Type>
  table: <actual table from Source_Table_or_File column>
  jdbc_options:
    url: "${JDBC_URL}"
    dbtable: "<table name or SQL from XML>"
    driver: "<driver class matching source DB type>"
    fetchsize: 10000

target:
  table: <actual table from Target_Table_or_File column>
  merge_keys: <inferred from join keys or primary keys visible in XML>
  zorder_columns: <first key column>

error:
  enabled: <true if error target exists, false otherwise>
  table: <error target name if exists>

delta:
  vacuum_retention_hours: 168
  optimize_after_write: true
```

### SECTION 8: Module Structure

Based on stages in `Stage_Sequence`, specify Python modules to generate:

```markdown
## 8. Module structure

### src/readers/source_reader.py
- Implements: <source stage name(s)>
- Method: <JDBC / Auto Loader / Delta>

### src/transformations/<name>.py
- Implements: <stage_1>, <stage_2>, ...
- Pattern: <router | pivot_pipeline | join | aggregation | union | final_transform>
```

**Grouping rules** — apply by reading the `Stage_Type` and graph structure:
- Router (CTransformerStage with multiple Output_Stages) → standalone module
- PxCopy + downstream PxPivots + post-pivot TFMs + PxFunnel → one module
- Matching parallel-branch stages → one module
- PxJoin + its input preparation → one module
- PxAggregator + pre/post transforms → one module
- Final PxFunnel (many inputs) → standalone module
- Final CTransformerStage before target → standalone module
- Simple 1-in-1-out CTransformerStage → fold into upstream module

### SECTION 9: Test Specifications

For each transformation module in Section 8, specify tests using actual column
names from `Source_to_Target_Mapping`:

```markdown
## 9. Test specifications

### test_<module>.py

**test_happy_path**
- Input columns: <actual columns from this pipeline>
- Sample row: <realistic values>
- Expected: <describe expected output>

**test_null_handling**
- Input: nulls/empty strings in key columns
- Expected: <correct behavior per derivation logic>

**test_edge_case**
- Input: <pipeline-specific boundary — e.g., string shorter than LPAD width>
- Expected: <correct behavior>
```

### SECTION 10: Execution Order

Read `Stage_Sequence` in `Sequence_No` order. Write the exact DAG execution:

```markdown
## 10. Execution order

Step 1:   source_df = read_source(spark, config)
Step 2:   <stage> → path_a_df, path_b_df, error_df = route(source_df, config)
Step 3A:  <stage> → transformed_a_df = transform_a(path_a_df, config)
Step 3B:  <stage> → transformed_b_df = transform_b(path_b_df, config)
...
Step N:   write_target(spark, final_df, config)
Step N+1: write_errors(error_df, config)
```

### SECTION 11: Project Requirements

Always end with:

```markdown
## 11. Project requirements

- PySpark only — no pandas UDFs unless absolutely necessary
- Delta Lake format exclusively — no Parquet/CSV for intermediate or targets
- Unity Catalog compatible — all tables use catalog.schema.table from config
- Config-driven — no hardcoded table names, column names, or connection strings
- Type annotations on all function signatures
- Google-style docstrings on all modules and public functions
- Ruff linting: line-length=120, select=["E","F","I","N","W","UP","S","B","A","C4","SIM"]
- Black formatting: line-length=120
- Bazel build: WORKSPACE with rules_python, py_library per package, py_test per test
- Pytest with chispa: ≥80% code coverage
- Pure functions: every transform takes (DataFrame, config) → DataFrame
- Logging: logger.info() at each stage boundary with row counts
- Error handling: try/except per phase, write partial results to error table
```

---

## Verification Checklist

Before saving `output/databricks_code_prompt.md`:

- [ ] Every stage from `Stage_Sequence` sheet appears in Section 3
- [ ] Every edge (Link_Names_In/Out) appears in Section 10 execution order
- [ ] Every row from `Source_to_Target_Mapping` is in Section 5 column mappings
- [ ] Derivation expressions in Section 3 are from the XML (not the Excel summary)
- [ ] Expression dictionary (Section 4) includes only functions found in this pipeline
- [ ] Module structure (Section 8) accounts for all stages — none orphaned
- [ ] Test specs (Section 9) exist for every transformation module
- [ ] Config (Section 7) has actual table names from the pipeline
- [ ] Routing conditions (Section 6) extracted from XML, not guessed from names
- [ ] Join keys extracted from XML, not guessed
