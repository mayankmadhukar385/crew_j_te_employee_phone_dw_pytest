# Step 4: Generate Synthetic Test Data for All Source Tables

## Your Role

You generate realistic synthetic test data for every source table in the pipeline,
so the migrated Databricks code can be tested end-to-end without access to the
original source systems.

## Multi-Job Awareness

Read `output/discovery.json`. Generate synthetic data for EACH parallel job.
If multiple jobs share the same source table, generate data once and reference it
from each job's test fixtures.

---

## Your Inputs

1. Original DataStage XML from `input/` — for source schemas, SQL, data types
2. Corrected lineage Excel per job from `output/{job}_lineage.xlsx`
   - `Stage_Sequence` — to identify source stages
   - `Source_to_Target_Mapping` — to understand column types and derivation logic
3. Generated job code from `output/project/jobs/{job}/`
4. Shared framework from `output/project/framework/`
5. `output/discovery.json`

## Your Outputs Per Job

1. `output/project/jobs/{job}/tests/fixtures/synthetic_data.py` — PySpark DataFrame factories
2. `output/project/jobs/{job}/tests/fixtures/expected_outputs.py` — Expected result DataFrames
3. `output/project/jobs/{job}/tests/fixtures/README.md` — Data dictionary

Also generate shared framework tests:
4. `output/project/tests/conftest.py` — Shared SparkSession fixture
5. `output/project/tests/framework/test_common_utility.py` — Framework unit tests
6. `output/project/tests/framework/test_expression_converter.py` — Expression tests
7. `output/project/tests/framework/test_dq_validator.py` — DQ validation tests

---

## How to Generate Synthetic Data

### Step A: Extract Source Schemas from XML

For each source connector stage, extract:
- Column names from the output link's column definitions
- Data types (from Property Name="SqlType" or "Type")
- Column lengths/precision
- Nullability

Also read the source SQL if present — the CAST/TRIM expressions tell you the
expected data types and lengths at the transformer input.

### Step B: Analyze Transformation Logic for Data Requirements

Read the corrected Excel `Source_to_Target_Mapping` to understand:
- Which columns feed into filters (e.g., `IsValid("int32", EMP_NBR)`) →
  you need both valid AND invalid values for these
- Which columns are used in routing conditions →
  you need values that exercise EVERY branch
- Which columns are used in join keys →
  you need matching values across joined datasets
- Which columns are used in aggregations →
  you need multiple rows per group-by key
- Which columns are pivoted →
  you need values across all pivot slots

### Step C: Generate Data Covering All Paths

For each source table, create rows that cover:

**1. Happy path rows** (at least 5):
- Valid values in all columns
- Values that pass all filter conditions
- Values that exercise the primary routing path

**2. Null/empty handling rows** (at least 3):
- Null in key columns
- Empty string in string columns
- Zero in numeric columns

**3. Branch coverage rows** (at least 2 per branch):
- If there's a router that splits on conditions (e.g., phone_type = 'HOME' vs 'AWAY'),
  generate rows for EACH branch
- If there's a condition like `$pFull_Load = 'Y'`, generate rows for both Y and N

**4. Error path rows** (at least 2):
- Values that should be routed to error targets
- Invalid data types (e.g., non-numeric string where int is expected)

**5. Join key coverage** (if joins exist):
- Rows in left table that have matching rows in right table
- Rows in left table that have NO match (to test left join behavior)
- Multiple rows with the same join key (to test one-to-many joins)

**6. Aggregation coverage** (if aggregations exist):
- Multiple rows with the same group-by key
- Edge cases: single-row groups, groups with null values

**7. Boundary values**:
- Strings at exact length limits (e.g., if LPAD pads to 9, test strings of length 8, 9, 10)
- Maximum and minimum numeric values
- Date boundary values (e.g., epoch, far future)

### Step D: Generate the Fixture Code

Create `synthetic_data.py` with PySpark DataFrame factories using actual column
names and types from the XML. Each row must be commented with its purpose.

### Step E: Generate Expected Output DataFrames

Create `expected_outputs.py` with the correct output after all transformations
are applied to the synthetic input data. Used for integration test assertions
with `chispa.assert_df_equality`.

### Step F: Generate Framework Tests

Create tests for the shared framework components:

**test_expression_converter.py** — verify every DS→PySpark conversion used by this pipeline
**test_dq_validator.py** — verify DQ rules catch invalid data correctly
**test_common_utility.py** — verify SCD logic, audit logging, error logging

### Step G: Generate Data Dictionary

Create `README.md` documenting every synthetic row's purpose, routing path, and
expected target.

---

## Critical Rules

- EVERY source table in the pipeline must have synthetic data
- If multiple parallel jobs share a source table, generate data ONCE
- Use actual column names and types from the XML — never use generic names
- Include enough rows to exercise EVERY routing branch at least twice
- Include rows that SHOULD go to error targets
- Generate expected output DataFrames for integration test assertions
- Data must be deterministic — same code always produces same data
- Do NOT use random generators — all values should be hand-crafted for testability
- Comment each row with its purpose (which path it exercises)
- Also generate tests for the shared framework (DQValidator)
- All test files must use `chispa.assert_df_equality` for DataFrame comparisons
