# Step 3: Pipeline Specification → Databricks PySpark Job Using Shared Framework

## Multi-Job Awareness

Read `output/discovery.json`. For EACH parallel job, read its code prompt
(`output/{job_name}_databricks_code_prompt.md`) and generate the job-specific
code under `output/project/{job_name}/`.

**CRITICAL**: Each job generates ONLY job-specific code — a thin wrapper that imports
from the shared `framework/` package. Do NOT regenerate readers, writers, SCD logic,
audit logging, error handling, watermark management, DQ validation, or expression
converters per job. All of that lives in the shared framework.

If a sequence orchestration exists (`orchestration.sequence_job_name` is not null in
`discovery.json`), also generate:
- `output/{seq_name}_databricks_workflow.yml` — Databricks Workflow definition

This file is a COMPLETE `resources.jobs` block for `databricks.yml`. It defines
every task, condition, dependency, failure notification, and parameter — ready
to paste into the project's `databricks.yml` or deploy standalone.

### Databricks Workflow Generation Rules

The DataStage sequence job maps to a Databricks multi-task job workflow.
Read the orchestration section of `discovery.json` for:
- `activities` — each JSJobActivity becomes a notebook_task
- `execution_graph` — from/to/condition becomes depends_on + condition_task
- `exception_handling` — becomes on_failure email notifications
- `parameters` — become job-level parameters with defaults

**Task type mapping:**

| DataStage Activity | Databricks Task Type |
|---|---|
| JSJobActivity (calls parallel job) | `notebook_task` pointing to that job's notebook |
| JSCondition (branch on parameter) | `condition_task` with op/left/right |
| JSExceptionHandler | `on_failure` email notification on the workflow |
| JSTerminatorActivity | Implicit — workflow fails when a task fails |
| JSExecCmdActivity | `spark_python_task` or `notebook_task` |
| JSUserVarsActivity | Absorbed into the condition_task or parameters |

**Condition branching pattern:**

DataStage sequence:
```
nc_extracttype: if $pFull_Load = 'Y' → Full_Load job, else → Incr_Load job
```

Becomes:
```yaml
tasks:
  - task_key: check_load_type
    condition_task:
      op: EQUAL_TO
      left: "{{job.parameters.pFull_Load}}"
      right: "Y"
    depends_on:
      - task_key: init_timestamp

  - task_key: full_load
    depends_on:
      - task_key: check_load_type
        outcome: "true"
    notebook_task:
      notebook_path: /Repos/.../notebooks/jb_lmis_full_ld/run_pipeline

  - task_key: incr_load
    depends_on:
      - task_key: check_load_type
        outcome: "false"
    notebook_task:
      notebook_path: /Repos/.../notebooks/jb_lmis_incr_ld/run_pipeline
```

**Dependency chain:** Follow the execution_graph from discovery.json exactly.
If activity A links to activity B with condition C:
- If C is a status condition (OK/WARN) → simple `depends_on` (no condition_task)
- If C is a parameter condition ($pFull_Load = 'Y') → insert a `condition_task`
  between A and B

**Convergence after branches:** If both branches lead to different downstream tasks
(e.g., full_load → reset_ts, incr_load → last_ts), each downstream task depends
on its respective branch. If branches converge to a single task, use multiple
`depends_on` entries.

**Concrete example — full workflow YAML for the NBME0019_SEQ sequence:**

```yaml
# Databricks Workflow: NBME0019_SEQ
# Generated from DataStage sequence job.
# Paste this into the resources.jobs section of databricks.yml

nbme0019_seq:
  name: "NBME0019_SEQ"

  parameters:
    - name: pFull_Load_EVOLUTIONPARMS
      default: "N"

  job_clusters:
    - job_cluster_key: etl_cluster
      new_cluster:
        spark_version: "14.3.x-scala2.12"
        node_type_id: "Standard_DS3_v2"
        num_workers: 2
        spark_conf:
          spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
          spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"

  tasks:
    # Task 1: Initialize timestamp (first activity in sequence)
    - task_key: init_timestamp
      job_cluster_key: etl_cluster
      notebook_task:
        notebook_path: /Repos/${var.repo}/notebooks/jb_load_last_process_ts_dtst/run_pipeline
        base_parameters:
          job_name: Jb_load_last_process_ts_dtst
      timeout_seconds: 1800

    # Task 2: Condition check — branch on pFull_Load parameter
    - task_key: check_load_type
      condition_task:
        op: EQUAL_TO
        left: "{{job.parameters.pFull_Load_EVOLUTIONPARMS}}"
        right: "Y"
      depends_on:
        - task_key: init_timestamp

    # Task 3A: Full load branch (runs when pFull_Load = 'Y')
    - task_key: full_load
      job_cluster_key: etl_cluster
      depends_on:
        - task_key: check_load_type
          outcome: "true"
      notebook_task:
        notebook_path: /Repos/${var.repo}/notebooks/jb_lmis_full_ld/run_pipeline
        base_parameters:
          job_name: Jb_LMIS_Evolutionparms_Full_Ld
      timeout_seconds: 3600

    # Task 3B: Incremental load branch (runs when pFull_Load != 'Y')
    - task_key: incr_load
      job_cluster_key: etl_cluster
      depends_on:
        - task_key: check_load_type
          outcome: "false"
      notebook_task:
        notebook_path: /Repos/${var.repo}/notebooks/jb_lmis_incr_ld/run_pipeline
        base_parameters:
          job_name: Jb_LMIS_Evolutionparms_Incr_Ld
      timeout_seconds: 3600

    # Task 4A: Reset timestamp after full load
    - task_key: reset_timestamp
      job_cluster_key: etl_cluster
      depends_on:
        - task_key: full_load
      notebook_task:
        notebook_path: /Repos/${var.repo}/notebooks/jb_load_reset_process_ts/run_pipeline
        base_parameters:
          job_name: Jb_load_reset_process_ts
      timeout_seconds: 1800

    # Task 4B: Persist timestamp after incremental load
    - task_key: persist_timestamp
      job_cluster_key: etl_cluster
      depends_on:
        - task_key: incr_load
      notebook_task:
        notebook_path: /Repos/${var.repo}/notebooks/jb_load_last_process_ts/run_pipeline
        base_parameters:
          job_name: Jb_load_last_process_ts
      timeout_seconds: 1800

  # Exception handling → email notification on failure
  email_notifications:
    on_failure:
      - data-ops@company.com

  # Schedule (extract from XML if available, otherwise leave for manual config)
  schedule:
    quartz_cron_expression: "0 0 6 * * ?"
    timezone_id: "Asia/Kolkata"

  max_concurrent_runs: 1
```

**CRITICAL rules for workflow generation:**
- Extract condition expressions VERBATIM from the XML — do NOT guess parameter names
- Every JSJobActivity must become a notebook_task pointing to that job's notebook
- Every JSCondition must become a condition_task with the exact parameter and operator
- `depends_on` must match the execution_graph edges exactly
- `outcome: "true"` / `outcome: "false"` must match the condition branches
- Job parameters must include every sequence-level parameter with its default value
- The exception handler chain (handler → vars → log → terminate) maps to
  `email_notifications.on_failure` — Databricks handles task failure notification
  natively, so the DS exception chain does not need separate tasks
- Generate the INSERT SQL for each parallel job's config alongside the workflow

## Your Task

Read `output/databricks_code_prompt.md` (generated in Step 2). It contains a complete
pipeline specification with 10 sections covering every stage, expression, column mapping,
and test case.

Generate the job-specific code under `output/project/`.
Create every file. Fully implement every function. No stubs. No TODOs.

---

## Project Structure

The project uses a SHARED FRAMEWORK architecture. The framework already exists at
`framework/` in the project root. Each job generates only its specific files:

```
output/project/
├── pyproject.toml                  ← ONE file for entire project (already exists)
├── ruff.toml                       ← Linting config (already exists)
├── databricks.yml                  ← Databricks Asset Bundles config (already exists)
├── framework/                      ← SHARED — do NOT regenerate
│   ├── __init__.py
│   ├── common_utility.py           ← DeltaManager, AuditLogger, 
│   │                                  WatermarkManager, DQValidator, native PySpark
│   ├── readers/
│   │   ├── __init__.py
│   │   └── source_reader.py        ← Generic JDBC/file/delta reader
│   ├── writers/
│   │   ├── __init__.py
│   │   └── delta_writer.py         ← Wraps DeltaManager for pipeline use
│   └── utils/
│       ├── __init__.py
│       ├── config_loader.py        ← YAML config loading with env var substitution
│       └── logger.py               ← Structured logging setup
│
├── jobs/                            ← ONE FOLDER PER MIGRATED JOB
│   └── {job_name_snake_case}/
│       ├── __init__.py
│       ├── configs/
│       │   ├── pipeline_config.yaml     ← From Section 7 of spec
│       │   ├── column_mappings.yaml     ← From Section 5 of spec
│       │   └── business_rules.yaml      ← From Section 6 of spec
│       ├── transformations.py           ← Job-specific transformation logic
│       ├── run_pipeline.py              ← Pipeline orchestrator using framework
│       └── tests/
│           ├── __init__.py
│           ├── conftest.py
│           ├── test_transformations.py  ← Unit tests for job transforms
│           └── test_integration.py      ← Integration test wiring all stages
│
└── notebooks/
    └── {job_name}/
        └── run_pipeline.py              ← Databricks notebook entry point
```

**Key principle**: If the code is the same across jobs, it belongs in `framework/`.
If it's unique to this job (specific column mappings, derivation logic, config values),
it belongs in `jobs/{job_name}/`.

---

## Code Generation Rules

### 1. Import From Framework — Never Duplicate

Every job file starts with imports from the shared framework:

```python
from framework.common_utility import (
    AuditLogger,
    DeltaManager,
    DQValidator,
    
    # native PySpark removed — use native PySpark directly
    WatermarkManager,
)
from framework.readers.source_reader import SourceReader
from framework.utils.config_loader import load_config
from framework.utils.logger import get_logger
```

**NEVER copy framework code into the job directory.** If you find yourself writing
a generic reader, writer, SCD merge, audit call, or expression converter in the
job's code — stop. It belongs in the framework.

### 2. Config-Driven Everything

Every transformation function must accept config as a parameter:
```python
def transform_name(df: DataFrame, config: dict) -> DataFrame:
```

**Never hardcode**:
- Table names → read from `config["source"]["table"]`, `config["target"]["table"]`
- Column names → read from `config["columns"]` or `config["mappings"]`
- Connection strings → read from `config["source"]["jdbc_options"]`
- Filter values → read from `config["business_rules"]`
- Join keys → read from `config["join_keys"]`
- SCD type → read from `config["target"]["scd_type"]`

### 3. Expression Conversion Using native PySpark

Use the shared `native PySpark` class methods for all DataStage-to-PySpark
conversions. Reference the expression dictionary from Section 4 of the spec:

```python
from framework.common_utility import native PySpark as E

# DataStage: If Len(TrimLeadingTrailing(X))=9 Then TrimLeadingTrailing(X)
#            Else Str('0',9-Len(TrimLeadingTrailing(X))):TrimLeadingTrailing(X)
# PySpark via framework:
result = F.lpad(F.col("X"), 9, "0")

# DataStage: If TrimLeadingTrailing(NullToValue(X,''))='' Then SetNull()
#            Else TrimLeadingTrailing(NullToValue(X,''))
# PySpark via framework:
trimmed = F.trim_null(F.col("X"), "")
result = F.if_then_else(trimmed == "", F.set_null("string"), trimmed)

# DataStage: IsValid("int32", X)
result = F.is_valid(F.col("X"), "int32")

# DataStage: CurrentTimestamp()
result = F.current_timestamp()
```

### 4. Pipeline Runner Pattern — Jobs Extend PipelineRunner

Every job extends `PipelineRunner` from the framework. The base class handles
audit logging, DQ validation, error logging, watermark updates, and target writes
automatically. The job ONLY implements `transform()`.

**Audit logging, error logging, and DQ checks CANNOT be skipped.**
They are baked into `PipelineRunner.run()` which the job does not override.

```python
"""Pipeline for {job_name}. Extends PipelineRunner."""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework import PipelineRunner


class {JobNamePascalCase}Pipeline(PipelineRunner):
    """Migrated from DataStage job: {job_name}."""

    def transform(self, source_df: DataFrame, config: dict) -> DataFrame:
        """Apply job-specific transformations.

        This is the ONLY method each job implements. Everything else —
        config loading, audit start/end, DQ checks, reject logging,
        watermark, target writes — is handled by PipelineRunner.run().

        Args:
            source_df: Source data AFTER DQ validation (clean rows only).
            config: Full config from the Delta config table.

        Returns:
            Transformed DataFrame ready for target write.
        """
        # Derivation logic from DataStage XML goes here.
        # Use native PySpark: F.trim(), F.when(), F.coalesce(), etc.
        result = source_df
        # ... transformation stages ...
        return result


# Entry point — used by Databricks notebook
if __name__ == "__main__":
    {JobNamePascalCase}Pipeline(spark, job_name="{job_name}").run()
```

**What PipelineRunner.run() does automatically:**
1. Loads config from `etl_job_config` Delta table via ConfigManager
2. Calls `AuditLogger.start_run()` — records run start in `etl_audit_log`
3. Creates `DatabricksJobLogger` — all logs go to `etl_job_log`
4. Reads source via SourceReader (with watermark for incremental)
5. Runs `DQValidator.validate()` — splits passed/failed records
6. Calls `logger.log_rejects()` for failed records
7. Calls `self.transform()` — **the job-specific part**
8. Calls `DeltaManager.write()` — returns actual insert/update counts
9. Updates watermark (if incremental mode)
10. Calls `AuditLogger.end_run(SUCCESS)` with counts
11. On ANY exception: logs error, calls `AuditLogger.end_run(FAILED)`, flushes logger

A job cannot forget audit logging. It's structural.

### 5. Job-Specific Transformations

The `transform()` method is the ONLY code unique to each job:

```python
def transform(self, source_df: DataFrame, config: dict) -> DataFrame:
    """Implement CTransformerStage derivation logic.

    Each column derivation is copied VERBATIM from the DataStage XML
    and converted to native PySpark. No wrapper functions.
    """
    result = source_df

    # Example: If Len(TrimLeadingTrailing(X))=9 Then X Else LPAD(X,9,'0')
    result = result.withColumn("EMP_NO", F.lpad(F.trim(F.col("EMP_NBR")), 9, "0"))

    # Example: If TrimLeadingTrailing(NullToValue(X,''))='' Then SetNull()
    trimmed = F.trim(F.coalesce(F.col("RAW_NAME"), F.lit("")))
    result = result.withColumn(
        "FULL_NAME",
        F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed),
    )

    # Example: CurrentTimestamp()
    result = result.withColumn("TD_LD_TS", F.current_timestamp())

    return result
```

For multi-target jobs, return a dict:

```python
def transform(self, source_df: DataFrame, config: dict) -> dict[str, DataFrame]:
    main_df = source_df.filter(F.col("status") == "ACTIVE")
    error_df = source_df.filter(F.col("status") != "ACTIVE")
    return {"main_target": main_df, "error_target": error_df}
```

### 6. Pivot Implementation

DataStage PxPivot typically unpivots repeating column groups:
```python
def unpivot_phone_slots(df: DataFrame, config: dict) -> DataFrame:
    key_cols = config["pivot"]["key_columns"]
    groups = config["pivot"]["groups"]

    frames = []
    for group in groups:
        select_exprs = [F.col(c) for c in key_cols]
        for canonical_name, original_col in group["columns"].items():
            select_exprs.append(F.col(original_col).alias(canonical_name))
        select_exprs.append(F.lit(group["slot"]).alias("SLOT_SEQ"))
        frames.append(df.select(*select_exprs))

    from functools import reduce
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
```

---

## Linting Configuration

**`ruff.toml`** (single file, replaces pylint + black + isort + flake8):
```toml
line-length = 120
target-version = "py310"

[lint]
select = ["E", "F", "I", "N", "W", "UP", "S", "B", "A", "C4", "SIM", "TCH", "D"]
ignore = ["D100", "D104", "S101"]

[lint.pydocstyle]
convention = "google"

[lint.isort]
known-first-party = ["framework", "jobs"]
```

---

## Testing

**`conftest.py`** per job:
```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a test SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def config():
    """Load test configuration."""
    from framework.utils.config_loader import load_config
    return load_config("configs")
```

**Test pattern** (use sample data from Section 9 of spec):
```python
from chispa import assert_df_equality

def test_transform_happy_path(spark, config):
    input_df = spark.createDataFrame([...], schema=...)
    result = transform_function(input_df, config)
    expected = spark.createDataFrame([...], schema=...)
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)
```

Minimum 3 tests per transformation module:
- Happy path with valid data
- Null/empty handling
- Edge cases (boundary values, routing branches)

Integration test with 10+ rows covering all routing paths.

---

## Notebook Entry Point

`notebooks/{job_name}/run_pipeline.py`:
```python
# Databricks notebook source
# MAGIC %pip install pyyaml chispa

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Databricks widgets for parameterization
dbutils.widgets.text("config_path", "configs", "Config Path")
dbutils.widgets.text("job_name", "", "Job Name")
dbutils.widgets.dropdown("run_mode", "full_refresh", ["full_refresh", "incremental"], "Run Mode")

config_path = dbutils.widgets.get("config_path")

from jobs.{job_name}.run_pipeline import run_pipeline
run_pipeline(spark, config_dir=config_path)
```

---

## Final Verification

After generating all job-specific files, verify:
- [ ] Job's `transformations.py` imports from `framework.common_utility` — not local copies
- [ ] Job's `run_pipeline.py` uses `AuditLogger`, `DatabricksJobLogger (via log_rejects)`, `DeltaManager`, `WatermarkManager`, `DQValidator` from framework
- [ ] Every stage from the spec has corresponding implementation code in `transformations.py`
- [ ] Every column mapping from Section 5 is implemented
- [ ] Every derivation expression uses `native PySpark` methods
- [ ] Every filter condition is implemented
- [ ] Every join uses the correct keys and type
- [ ] Every aggregation has correct group-by and functions
- [ ] Config YAMLs have actual values from the spec (not placeholders for table names)
- [ ] All Python files have type annotations on function signatures
- [ ] All modules have Google-style docstrings
- [ ] Test files exist with ≥3 tests per transformation function
- [ ] Integration test covers all routing paths
- [ ] `run_pipeline.py` follows the exact execution order from Section 10
- [ ] No framework code is duplicated in the job directory
- [ ] No hardcoded table names, column names, or connection strings in Python code
- [ ] SCD type is read from config, not hardcoded in writer calls
