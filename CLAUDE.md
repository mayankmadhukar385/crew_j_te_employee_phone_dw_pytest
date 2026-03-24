# DataStage to Databricks ETL Conversion Workspace

## What This Project Does

Converts IBM DataStage XML exports into production-grade Databricks PySpark projects.
Uses a **shared framework** architecture: common ETL patterns (SCD, audit, logging,
watermark, DQ, config management) live in `framework/`, and each migrated job
generates only its specific transformation logic under `jobs/`.

## Folder Structure

```
datastage-to-databricks/
├── .claude/settings.json           ← Claude Code permissions
├── pyproject.toml                  ← Build, test, coverage config
├── ruff.toml                       ← Lint + format rules (replaces pylint + black)
├── databricks.yml                  ← Databricks Asset Bundles deployment config
│
├── framework/                      ← SHARED LIBRARY (central, reused by all jobs)
│   ├── common_utility.py           ← DeltaManager (SCD + plain writes),
│   │                                  AuditLogger, WatermarkManager, DQValidator
│   ├── readers/
│   │   └── source_reader.py        ← JDBC / file / Delta / test_dataframe reader
│   ├── writers/
│   │   └── delta_writer.py         ← Pipeline-level wrapper around DeltaManager
│   └── utils/
│       ├── config_manager.py       ← Delta table config store (single source of truth)
│       ├── databricks_context.py   ← Auto-detect job_id, cluster_id, notebook_path
│       └── job_logger.py           ← Central Delta logger + log_rejects() + factory
│
├── jobs/                            ← ONE FOLDER PER MIGRATED JOB (Step 3 generates)
│   └── {job_name}/
│       ├── transformations.py       ← Job-specific derivation logic only
│       ├── run_pipeline.py          ← Orchestrator using framework
│       └── tests/
│           ├── test_transformations.py
│           └── fixtures/
│
├── tests/                           ← Framework tests
│   ├── conftest.py                  ← SparkSession + config seeded into Delta table
│   └── framework/
│       └── test_dq_validator.py
│
├── input/                           ← DataStage XML exports
├── output/                          ← Generated artifacts per step
├── notebooks/                       ← Databricks notebook entry points
└── prompts/                         ← Step prompts for Claude Code (steps 0-5)
```

## Architecture Decisions

| Decision | Rationale |
|---|---|
| Config in Delta table only | Single source of truth. No YAML. Step 3 generates INSERT SQL. |
| One logger class | DatabricksJobLogger handles info/warn/error + record-level rejects |
| No stdout logger | All logs go to etl_job_log Delta table. Exceptions propagate. |
| No ExpressionConverter | Step 3 generates native PySpark directly. No wrapper functions. |
| No CI yml in project | CI/CD is centralized in GitLab. Local testing via ruff + pytest. |
| test_dataframe source type | SourceReader reads from temp views in VS Code (no JDBC needed) |

## Framework Components

| Class | File | Delta Table | Purpose |
|---|---|---|---|
| PipelineRunner | pipeline_runner.py | *(orchestrates all below)* | Base class — audit/logging/DQ/write baked in |
| DeltaManager | common_utility.py | *(target tables)* | SCD 0/1/2/4 + append, truncate_load, update, upsert |
| AuditLogger | common_utility.py | etl_audit_log | Start/end row per pipeline run |
| WatermarkManager | common_utility.py | etl_watermark | Incremental state (with job_id) |
| DQValidator | common_utility.py | *(none)* | Config-driven DQ checks |
| ConfigManager | config_manager.py | etl_job_config | Versioned config as JSON |
| DatabricksJobLogger | job_logger.py | etl_job_log | All logs + record rejects |
| SourceReader | source_reader.py | *(none)* | JDBC / file / Delta / test_dataframe |

**Jobs extend PipelineRunner and override `transform()` only.**
Audit logging, error logging, DQ checks, and target writes are structural —
they cannot be skipped or forgotten.

## Local Testing in VS Code

```bash
pip install -e ".[dev]"
ruff check .                   # Lint
ruff format --check .          # Format check
pytest                         # Test + coverage (≥80% gate)
open reports/htmlcov/index.html  # Browse coverage report
```

Tests use `source.type: "test_dataframe"` — synthetic data is registered as
Spark temp views. No JDBC driver or external database needed.

## Seven-Step Process

| Step | What | Judge? |
|---|---|---|
| 0 | Discovery — detect jobs in XML, classify, plan execution | — |
| 1 | Generate lineage Excel + Mermaid per parallel job | — |
| 1B | Validate & correct lineage against XML | Judge |
| 2 | Generate code gen prompts per job | — |
| 3 | Generate job code (thin wrappers) + config INSERT SQL | — |
| 3B | Validate & correct code against XML + Excel | Judge |
| 4 | Generate synthetic test data + framework tests | — |
| 5 | Generate deployment guide with DDL and INSERT scripts | — |

## Critical Rules

- Config lives in Delta table ONLY — Step 3 generates INSERT SQL for deployment
- Every exception catch must log to etl_job_log AND mark audit as FAILED
- All framework tables carry job_id, workflow_name for cross-table joins
- Step 3 generates THIN WRAPPERS that import from framework — no duplication
- Step 3 generates native PySpark expressions — no wrapper classes
- SCD type and write_mode are config parameters — never hardcoded
- Tests use source.type = "test_dataframe" with temp views (no JDBC)
