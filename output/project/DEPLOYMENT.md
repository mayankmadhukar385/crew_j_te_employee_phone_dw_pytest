# Deployment Guide — CREW_J_TE_EMPLOYEE_PHONE_DW

**Migrated from:** IBM DataStage job `CREW_J_TE_EMPLOYEE_PHONE_DW`
**Source:** `CREW_WSTELE_LND` (Teradata — TeradataConnectorPX)
**Main target:** `TE_EMPLOYEE_PHONE_NEW` (truncate+load) → promoted to `TE_EMPLOYEE_PHONE`
**Error target:** `TE_EMPLOYEE_PHONE_ERR` (append — invalid `EMP_NBR` records)
**Run mode:** `full_refresh` (all rows replaced on every run)

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Create Framework Tables](#2-create-framework-tables)
3. [Create Target Tables](#3-create-target-tables)
4. [Upload Project to Databricks](#4-upload-project-to-databricks)
5. [Configure the Pipeline](#5-configure-the-pipeline)
6. [Local Testing](#6-local-testing)
7. [Deploy as Databricks Job](#7-deploy-as-databricks-job)
8. [Delta Table Maintenance](#8-delta-table-maintenance)
9. [Monitoring and Alerting](#9-monitoring-and-alerting)
10. [Rollback Procedures](#10-rollback-procedures)
11. [Common Issues](#11-common-issues)

---

## 1. Prerequisites

### Databricks Workspace

- Databricks workspace with **Unity Catalog enabled**
- A catalog and schema created for each target environment:

```sql
-- Development
CREATE CATALOG IF NOT EXISTS dev_catalog;
CREATE SCHEMA IF NOT EXISTS dev_catalog.etl_dev;

-- Production
CREATE CATALOG IF NOT EXISTS prod_catalog;
CREATE SCHEMA IF NOT EXISTS prod_catalog.etl_prod;
```

### Required Permissions

| Permission | On |
|---|---|
| `USE CATALOG` | `dev_catalog` / `prod_catalog` |
| `USE SCHEMA` | `etl_dev` / `etl_prod` |
| `CREATE TABLE` | `etl_dev` / `etl_prod` |
| `MODIFY` | All Delta tables in the target schema |
| `SELECT` | Teradata source (granted at Teradata level, not Unity Catalog) |

### JDBC Driver — Teradata

This pipeline reads from **Teradata** via JDBC.

| Item | Value |
|---|---|
| Driver class | `com.teradata.jdbc.TeraDriver` |
| JAR name | `terajdbc4.jar` (and `tdgssconfig.jar`) |
| Download | [Teradata Downloads](https://downloads.teradata.com/download/connectivity/jdbc-driver) (requires Teradata account) |
| Installation | Upload to `dbfs:/FileStore/jars/` or install via cluster init script |
| Cluster library | Add both JARs as cluster-scoped libraries in Databricks |

Init script to install JARs automatically:

```bash
# cluster init script: /dbfs/init_scripts/install_teradata_jdbc.sh
cp /dbfs/FileStore/jars/terajdbc4.jar /databricks/jars/
cp /dbfs/FileStore/jars/tdgssconfig.jar /databricks/jars/
```

### Cluster Configuration

| Setting | Recommended Value |
|---|---|
| Databricks Runtime | **14.3 LTS** or later (Delta Lake included) |
| Node type | `Standard_DS4_v2` (28 GB RAM, 8 cores) or equivalent |
| Workers | 2–4 (full-refresh pipeline; scale by CREW_WSTELE_LND volume) |
| Auto-termination | 30 minutes |

Cluster Spark config (add in Advanced Options → Spark):

```
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
```

### Local Development Setup

```bash
# From the project root
pip install -e ".[dev]"   # Installs framework + test dependencies (pyspark, chispa, pytest, ruff)
ruff check .               # Lint
ruff format --check .      # Format check
pytest                     # Run all tests (coverage gate ≥ 80%)
```

---

## 2. Create Framework Tables

The shared framework requires **four metadata tables**. Run these DDL statements **once per
catalog/schema** — they are shared across all migrated jobs in that environment.

> **Note:** The framework auto-creates these tables on first run if they don't exist.
> Running DDL manually is optional but recommended for visibility in Catalog Explorer.

### Config Table

```sql
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_config (
    job_name      STRING    NOT NULL COMMENT 'Unique job identifier (matches PipelineRunner job_name)',
    config_json   STRING    NOT NULL COMMENT 'Pipeline configuration as JSON string',
    version       INT       NOT NULL COMMENT 'Config version number — increment on updates',
    is_active     STRING    NOT NULL COMMENT 'Active flag: Y = current, N = superseded',
    created_by    STRING             COMMENT 'User or system that created this version',
    created_ts    TIMESTAMP NOT NULL COMMENT 'Timestamp when this version was created',
    description   STRING             COMMENT 'Human-readable description of this config version'
)
USING DELTA
COMMENT 'Pipeline configuration store — single source of truth for all ETL job configs';
```

### Audit Log Table

```sql
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_audit_log (
    audit_id         STRING    NOT NULL COMMENT 'UUID for this audit record',
    job_name         STRING    NOT NULL COMMENT 'Pipeline job name',
    workflow_name    STRING             COMMENT 'Databricks workflow name (if orchestrated)',
    run_id           STRING    NOT NULL COMMENT 'Unique run identifier',
    start_ts         TIMESTAMP NOT NULL COMMENT 'Pipeline start timestamp',
    end_ts           TIMESTAMP          COMMENT 'Pipeline end timestamp (null if still running)',
    status           STRING    NOT NULL COMMENT 'RUNNING / SUCCESS / FAILED / PARTIAL',
    source_count     LONG               COMMENT 'Row count read from source',
    target_count     LONG               COMMENT 'Row count written to main target',
    error_count      LONG               COMMENT 'Row count written to error target',
    reject_count     LONG               COMMENT 'Row count rejected by DQ checks',
    parameters       STRING             COMMENT 'Runtime parameters as JSON',
    error_message    STRING             COMMENT 'Exception message if status = FAILED',
    cluster_id       STRING             COMMENT 'Databricks cluster ID',
    duration_seconds DOUBLE             COMMENT 'Total pipeline duration in seconds'
)
USING DELTA
COMMENT 'Audit trail for all ETL pipeline runs';
```

### Job Log Table

```sql
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_log (
    log_id        STRING    NOT NULL COMMENT 'UUID for this log entry',
    audit_run_id  STRING    NOT NULL COMMENT 'Foreign key to etl_audit_log.run_id',
    job_name      STRING    NOT NULL COMMENT 'Pipeline job name',
    workflow_name STRING             COMMENT 'Databricks workflow name',
    log_level     STRING    NOT NULL COMMENT 'INFO / WARN / ERROR',
    stage_name    STRING             COMMENT 'Pipeline stage that generated this log',
    message       STRING    NOT NULL COMMENT 'Log message text',
    source_key    STRING             COMMENT 'Record key for reject-level logs',
    source_record STRING             COMMENT 'Serialised source record for reject-level logs',
    cluster_id    STRING             COMMENT 'Databricks cluster ID',
    created_ts    TIMESTAMP NOT NULL COMMENT 'Log entry timestamp'
)
USING DELTA
COMMENT 'Structured log table for all ETL pipeline messages and record rejects';
```

### Watermark Table

```sql
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_watermark (
    job_name         STRING    NOT NULL COMMENT 'Pipeline job name',
    source_table     STRING    NOT NULL COMMENT 'Source table name',
    watermark_column STRING    NOT NULL COMMENT 'Column used for incremental filtering',
    last_value       STRING             COMMENT 'Last processed watermark value',
    last_run_ts      TIMESTAMP          COMMENT 'Timestamp of last successful run',
    run_mode         STRING             COMMENT 'full_refresh or incremental'
)
USING DELTA
COMMENT 'Incremental load watermarks — tracks high-water marks per job and source table';
```

---

## 3. Create Target Tables

Run these DDL statements **before the first pipeline execution**. They are also included in
`output/project/jobs/crew_j_te_employee_phone_dw/deploy_config.sql`.

### Work Table — `TE_EMPLOYEE_PHONE_NEW`

Receives a full truncate+load on every pipeline run. Data is promoted to the live table
by the pipeline's post-write step (equivalent to the DataStage After-SQL DELETE+INSERT).

```sql
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE_NEW (
  EMP_NBR        INT           NOT NULL COMMENT 'Employee number (int32)',
  EMP_NO         VARCHAR(9)    NOT NULL COMMENT 'Employee number zero-padded to 9 chars',
  PH_LIST        VARCHAR(20)   NOT NULL COMMENT 'Phone list type: EMERGENCY/TEMP/BASIC/HOME/AWAY',
  PH_PRTY        INT           NOT NULL COMMENT 'Phone call priority (sequential per EMP_NBR)',
  EFF_START_TM   VARCHAR(8)    NOT NULL COMMENT 'Effective start time HH:MM:SS',
  EFF_END_TM     VARCHAR(8)    NOT NULL COMMENT 'Effective end time HH:MM:SS',
  PH_NBR         BIGINT        NOT NULL COMMENT 'Phone number (int64)',
  PH_ACCESS      VARCHAR(100)           COMMENT 'Phone access code',
  PH_COMMENTS    VARCHAR(500)           COMMENT 'Phone comments / name',
  PH_TYPE        VARCHAR(10)            COMMENT 'Phone type code',
  UNLISTD_IND    VARCHAR(1)             COMMENT 'Unlisted indicator',
  HOME_AWAY_IND  VARCHAR(1)             COMMENT 'Home/Away indicator (H/A/B)',
  TEMP_PH_EXP_TS TIMESTAMP              COMMENT 'Temporary phone expiry timestamp',
  TD_LD_TS       TIMESTAMP     NOT NULL COMMENT 'Teradata load timestamp (current_timestamp at load)'
)
USING DELTA
COMMENT 'Work table for TE_EMPLOYEE_PHONE — truncate+loaded on every run of CREW_J_TE_EMPLOYEE_PHONE_DW';
```

### Live Table — `TE_EMPLOYEE_PHONE`

The production employee phone table. The pipeline writes to `TE_EMPLOYEE_PHONE_NEW` and
then promotes to this table, replacing all rows atomically.

```sql
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE (
  EMP_NBR        INT           NOT NULL COMMENT 'Employee number (int32)',
  EMP_NO         VARCHAR(9)    NOT NULL COMMENT 'Employee number zero-padded to 9 chars',
  PH_LIST        VARCHAR(20)   NOT NULL COMMENT 'Phone list type: EMERGENCY/TEMP/BASIC/HOME/AWAY',
  PH_PRTY        INT           NOT NULL COMMENT 'Phone call priority (sequential per EMP_NBR)',
  EFF_START_TM   VARCHAR(8)    NOT NULL COMMENT 'Effective start time HH:MM:SS',
  EFF_END_TM     VARCHAR(8)    NOT NULL COMMENT 'Effective end time HH:MM:SS',
  PH_NBR         BIGINT        NOT NULL COMMENT 'Phone number (int64)',
  PH_ACCESS      VARCHAR(100)           COMMENT 'Phone access code',
  PH_COMMENTS    VARCHAR(500)           COMMENT 'Phone comments / name',
  PH_TYPE        VARCHAR(10)            COMMENT 'Phone type code',
  UNLISTD_IND    VARCHAR(1)             COMMENT 'Unlisted indicator',
  HOME_AWAY_IND  VARCHAR(1)             COMMENT 'Home/Away indicator (H/A/B)',
  TEMP_PH_EXP_TS TIMESTAMP              COMMENT 'Temporary phone expiry timestamp',
  TD_LD_TS       TIMESTAMP     NOT NULL COMMENT 'Teradata load timestamp (current_timestamp at load)'
)
USING DELTA
COMMENT 'Live employee phone table — migrated from DataStage CREW_J_TE_EMPLOYEE_PHONE_DW';
```

### Error Table — `TE_EMPLOYEE_PHONE_ERR`

Receives records where `EMP_NBR` cannot be cast to `INT` (invalid employee numbers).
Equivalent to the original DataStage sequential file `$LOG_DIR/EMPLOYEE_PHONE.err`.

```sql
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE_ERR (
  EMP_NBR        VARCHAR(100)           COMMENT 'Raw EMP_NBR value (invalid — not castable to int32)',
  PH_NBR         VARCHAR(100)           COMMENT 'Raw PH_NBR value',
  TEMP_PH_NBR    VARCHAR(100)           COMMENT 'Raw TEMP_PH_NBR value',
  BASIC_PH_NBR_1 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_1 value',
  BASIC_PH_NBR_2 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_2 value',
  BASIC_PH_NBR_3 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_3 value',
  BASIC_PH_NBR_4 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_4 value',
  BASIC_PH_NBR_5 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_5 value',
  _error_ts      TIMESTAMP     NOT NULL COMMENT 'Timestamp when error record was written'
)
USING DELTA
COMMENT 'Error records from CREW_J_TE_EMPLOYEE_PHONE_DW — rows with invalid EMP_NBR';
```

---

## 4. Upload Project to Databricks

### Option A: Databricks Repos (recommended for development)

1. Push the project to your Git repository (GitHub / Azure DevOps / GitLab)
2. In Databricks: **Workspace → Repos → Add Repo**
3. Clone the repository — `framework/` and `jobs/` packages are immediately importable
4. The sync config in `databricks.yml` includes:

```yaml
sync:
  include:
    - "framework/**"
    - "jobs/**"
    - "notebooks/**"
```

### Option B: Databricks Asset Bundles (recommended for CI/CD)

```bash
# Install Databricks CLI
pip install databricks-cli

# Authenticate (set DATABRICKS_HOST and DATABRICKS_TOKEN in your shell or CI/CD secrets)
databricks configure --token

# Validate the bundle against dev environment
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t production
```

The bundle name is `datastage-etl-framework` (see `databricks.yml`).

### Option C: Manual DBFS Upload (not recommended for production)

```bash
databricks fs cp -r ./framework dbfs:/FileStore/etl-framework/
databricks fs cp -r ./jobs dbfs:/FileStore/etl-framework/
databricks fs cp -r ./notebooks dbfs:/FileStore/etl-framework/
```

---

## 5. Configure the Pipeline

### Environment Variables

Set these in your Databricks cluster configuration, job parameters, or CI/CD secrets.
They are referenced in `deploy_config.sql` and the JDBC connection string.

| Variable | Description | Dev Example | Prod Example |
|---|---|---|---|
| `UNITY_CATALOG` | Unity Catalog name | `dev_catalog` | `prod_catalog` |
| `UNITY_SCHEMA` | Schema name | `etl_dev` | `etl_prod` |
| `TERADATA_JDBC_URL` | Full Teradata JDBC URL | `jdbc:teradata://dev-tera-host/DBS_PORT=1025,DATABASE=CREW` | `jdbc:teradata://prod-tera-host/DBS_PORT=1025,DATABASE=CREW` |
| `TD_WORK_DB` | Teradata database containing `CREW_WSTELE_LND` | `CREW_DEV` | `CREW_PROD` |
| `DATABRICKS_HOST` | Databricks workspace URL | `https://adb-xxxx.azuredatabricks.net` | `https://adb-yyyy.azuredatabricks.net` |

Set cluster-level Spark config for Teradata credentials (do not hardcode in SQL):

```
spark.hadoop.javax.jdo.option.ConnectionUserName ${secrets/teradata/user}
spark.hadoop.javax.jdo.option.ConnectionPassword ${secrets/teradata/password}
```

> Use **Databricks Secrets** to store `TERADATA_JDBC_URL` and credentials securely.
> See: [Databricks Secret Scopes](https://docs.databricks.com/security/secrets/secret-scopes.html)

### Deploy the Pipeline Config (run once per environment)

Config lives in the `etl_job_config` Delta table — not YAML files.
Run the generated INSERT statement once per environment:

```bash
# File: output/project/jobs/crew_j_te_employee_phone_dw/deploy_config.sql
# Substitute variables before running:
UNITY_CATALOG=prod_catalog
UNITY_SCHEMA=etl_prod
TERADATA_JDBC_URL=jdbc:teradata://prod-host/DBS_PORT=1025
TD_WORK_DB=CREW_PROD

databricks sql query --query-file output/project/jobs/crew_j_te_employee_phone_dw/deploy_config.sql
```

Verify config was stored:

```sql
SELECT job_name, version, is_active, created_ts, description
FROM ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_config
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW';
```

### Updating Config After First Deployment

Config changes are SQL `UPDATE` statements — no code changes needed:

```sql
-- Example: change JDBC fetch size
UPDATE ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_config
SET config_json = json_set(config_json, '$.source.jdbc_options.fetchsize', '20000'),
    version = version + 1,
    created_ts = current_timestamp()
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
  AND is_active = 'Y';

-- Example: disable Delta OPTIMIZE after write
UPDATE ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_config
SET config_json = json_set(config_json, '$.delta.optimize_after_write', false),
    version = version + 1,
    created_ts = current_timestamp()
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
  AND is_active = 'Y';
```

---

## 6. Local Testing

Tests run in VS Code without JDBC drivers or external databases.
Source data uses `source.type: "test_dataframe"` — registered as Spark temp views.

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run linter
ruff check .

# Run formatter check
ruff format --check .

# Run all tests with coverage
pytest

# View HTML coverage report
open reports/htmlcov/index.html       # macOS/Linux
start reports/htmlcov/index.html      # Windows
```

Test files for this job:

| File | Coverage |
|---|---|
| `jobs/crew_j_te_employee_phone_dw/tests/test_transformations.py` | 10 test classes, all 17 transformation functions |
| `jobs/crew_j_te_employee_phone_dw/tests/test_integration.py` | End-to-end pipeline with 10 source rows, all 6 routing branches |
| `jobs/crew_j_te_employee_phone_dw/tests/fixtures/synthetic_data.py` | 22 test rows — emergency, temp, basic, home, away, error, boundary |
| `jobs/crew_j_te_employee_phone_dw/tests/fixtures/expected_outputs.py` | Target schema + expected output DataFrames |

Synthetic data is registered as temp view `crew_wstele_lnd_test` (matches `source.test_view_name` in test config).

### CI/CD (GitLab)

CI/CD is centralized in GitLab. The GitLab pipeline runs the same local commands:

```yaml
# .gitlab-ci.yml (centralized — not in this repo)
test:
  stage: test
  script:
    - pip install -e ".[dev]"
    - ruff check .
    - ruff format --check .
    - pytest --cov=. --cov-fail-under=80
  artifacts:
    reports:
      coverage_report:
        coverage_format: cobertura
        path: reports/coverage.xml
```

### Azure DevOps Pipeline (if applicable)

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include: [main, develop]

pool:
  vmImage: ubuntu-latest

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.10'

  - script: pip install -e ".[dev]"
    displayName: 'Install dependencies'

  - script: ruff check . && ruff format --check .
    displayName: 'Lint and format check'

  - script: pytest --junitxml=reports/test-results.xml --cov=. --cov-fail-under=80
    displayName: 'Run tests'

  - task: PublishTestResults@2
    inputs:
      testResultsFiles: reports/test-results.xml
```

---

## 7. Deploy as Databricks Job

This is a **single parallel job** — no Databricks Workflow or conditional branching is needed.

### Step 1: Add the Job Resource to `databricks.yml`

Add the following block under `resources.jobs` in `databricks.yml`:

```yaml
resources:
  jobs:
    crew_j_te_employee_phone_dw:
      name: "CREW_J_TE_EMPLOYEE_PHONE_DW"

      tasks:
        - task_key: run_pipeline
          notebook_task:
            notebook_path: notebooks/crew_j_te_employee_phone_dw/run_pipeline
            base_parameters:
              job_name: "CREW_J_TE_EMPLOYEE_PHONE_DW"
              run_mode: "full_refresh"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS4_v2"
            num_workers: 2
            spark_conf:
              spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
              spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            libraries:
              - jar: "dbfs:/FileStore/jars/terajdbc4.jar"
              - jar: "dbfs:/FileStore/jars/tdgssconfig.jar"
          timeout_seconds: 7200

      schedule:
        quartz_cron_expression: "0 0 5 * * ?"   # 05:00 UTC daily — adjust as needed
        timezone_id: "America/Los_Angeles"
        pause_status: UNPAUSED

      email_notifications:
        on_failure:
          - data-ops@company.com

      parameters:
        - name: job_name
          default: "CREW_J_TE_EMPLOYEE_PHONE_DW"
        - name: run_mode
          default: "full_refresh"
```

### Step 2: Deploy

```bash
# Validate
databricks bundle validate -t production

# Deploy
databricks bundle deploy -t production

# Trigger a manual run to verify
databricks bundle run crew_j_te_employee_phone_dw -t production
```

### Step 3: Verify First Run

After the first successful run, verify:

```sql
-- 1. Audit log shows SUCCESS
SELECT job_name, status, source_count, target_count, error_count, duration_seconds
FROM prod_catalog.etl_prod.etl_audit_log
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
ORDER BY start_ts DESC
LIMIT 1;

-- 2. Live table has rows
SELECT COUNT(*) FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE;

-- 3. Work table matches live table (after promotion)
SELECT COUNT(*) FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_NEW;

-- 4. Check for error records (expected for any non-numeric EMP_NBR in source)
SELECT COUNT(*) FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR;
```

---

## 8. Delta Table Maintenance

### OPTIMIZE — Run After Each Pipeline Execution

The pipeline config sets `optimize_after_write: true`, so OPTIMIZE runs automatically
after every successful write. To run manually:

```sql
-- Main live table — Z-order by EMP_NBR for query performance
OPTIMIZE prod_catalog.etl_prod.TE_EMPLOYEE_PHONE ZORDER BY (EMP_NBR);

-- Work table
OPTIMIZE prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_NEW ZORDER BY (EMP_NBR);

-- Audit log — Z-order by job_name + start_ts for monitoring queries
OPTIMIZE prod_catalog.etl_prod.etl_audit_log ZORDER BY (job_name, start_ts);
```

### VACUUM — Run Weekly

The config sets `vacuum_retention_hours: 168` (7 days). Schedule weekly:

```sql
VACUUM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE RETAIN 168 HOURS;
VACUUM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_NEW RETAIN 168 HOURS;
VACUUM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR RETAIN 168 HOURS;
```

### Audit Log Cleanup — Retain 90 Days

```sql
DELETE FROM prod_catalog.etl_prod.etl_audit_log
WHERE start_ts < current_timestamp() - INTERVAL 90 DAYS;
```

### Job Log Cleanup — Retain 30 Days

```sql
DELETE FROM prod_catalog.etl_prod.etl_job_log
WHERE created_ts < current_timestamp() - INTERVAL 30 DAYS;
```

### Error Table Archival

The `TE_EMPLOYEE_PHONE_ERR` table grows on every run if invalid `EMP_NBR` records exist
in the source. Archive or clear periodically:

```sql
-- View current error counts by date
SELECT DATE(created_ts) AS run_date, COUNT(*) AS error_rows
FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR
GROUP BY 1
ORDER BY 1 DESC;

-- Archive rows older than 30 days (adjust retention as needed)
DELETE FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR
WHERE _error_ts < current_timestamp() - INTERVAL 30 DAYS;
```

---

## 9. Monitoring and Alerting

### Audit Log Queries

```sql
-- Last 10 runs for this job
SELECT job_name, status, source_count, target_count, error_count,
       duration_seconds, start_ts, end_ts
FROM prod_catalog.etl_prod.etl_audit_log
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
ORDER BY start_ts DESC
LIMIT 10;

-- Failed runs in last 24 hours
SELECT job_name, status, error_message, start_ts
FROM prod_catalog.etl_prod.etl_audit_log
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
  AND status = 'FAILED'
  AND start_ts > current_timestamp() - INTERVAL 24 HOURS;

-- Row count trend (detect unexpected drops)
SELECT DATE(start_ts) AS run_date,
       source_count,
       target_count,
       error_count,
       ROUND(error_count * 100.0 / NULLIF(source_count, 0), 2) AS error_pct,
       duration_seconds
FROM prod_catalog.etl_prod.etl_audit_log
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
  AND status = 'SUCCESS'
ORDER BY run_date DESC
LIMIT 30;

-- Average duration trend (detect performance regressions)
SELECT job_name,
       DATE(start_ts) AS run_date,
       AVG(duration_seconds) AS avg_seconds,
       MAX(duration_seconds) AS max_seconds
FROM prod_catalog.etl_prod.etl_audit_log
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
  AND status = 'SUCCESS'
GROUP BY job_name, DATE(start_ts)
ORDER BY run_date DESC;
```

### Error Analysis

```sql
-- Error records in TE_EMPLOYEE_PHONE_ERR — most recent batch
SELECT EMP_NBR, PH_NBR, _error_ts
FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR
ORDER BY _error_ts DESC
LIMIT 100;

-- Error volume over time
SELECT DATE(_error_ts) AS error_date, COUNT(*) AS error_count
FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR
GROUP BY 1
ORDER BY 1 DESC;

-- Job log: warnings and errors for a specific run
SELECT log_level, stage_name, message, created_ts
FROM prod_catalog.etl_prod.etl_job_log
WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW'
  AND log_level IN ('WARN', 'ERROR')
  AND created_ts > current_timestamp() - INTERVAL 24 HOURS
ORDER BY created_ts DESC;
```

### Target Table Health

```sql
-- Row counts by phone list type (sanity check after each run)
SELECT PH_LIST, COUNT(*) AS row_count
FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE
GROUP BY PH_LIST
ORDER BY row_count DESC;

-- Distinct employees with phone records
SELECT COUNT(DISTINCT EMP_NBR) AS employees_with_phones
FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE;

-- Priority distribution (should be sequential 1..N per employee)
SELECT PH_PRTY, COUNT(*) AS count
FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE
GROUP BY PH_PRTY
ORDER BY PH_PRTY;
```

### Databricks Job Alerting

Configure on the Databricks Job:
- **On failure:** notify `data-ops@company.com` immediately
- **On duration exceeded:** alert if runtime > 2× baseline (detected via audit log query above)
- **Row count threshold:** alert if `target_count` drops more than 10% from prior run

### Unity Catalog Lineage

Once running on Databricks, Unity Catalog automatically tracks data lineage.
View in: **Catalog Explorer → `prod_catalog.etl_prod.TE_EMPLOYEE_PHONE` → Lineage tab**

This will show: `CREW_WSTELE_LND` → `TE_EMPLOYEE_PHONE_NEW` → `TE_EMPLOYEE_PHONE`

---

## 10. Rollback Procedures

### Delta Time Travel — Restore Target Table

If a pipeline run produces incorrect data in `TE_EMPLOYEE_PHONE`:

```sql
-- View the version history
DESCRIBE HISTORY prod_catalog.etl_prod.TE_EMPLOYEE_PHONE;

-- Preview data at a previous version
SELECT COUNT(*) FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE
VERSION AS OF <version_number>;

-- Restore to a previous version
RESTORE TABLE prod_catalog.etl_prod.TE_EMPLOYEE_PHONE
TO VERSION AS OF <version_number>;

-- Or restore by timestamp
RESTORE TABLE prod_catalog.etl_prod.TE_EMPLOYEE_PHONE
TO TIMESTAMP AS OF '2026-03-23T05:00:00';
```

### Full Rerun

This pipeline is `full_refresh` — the next run will overwrite all data in
`TE_EMPLOYEE_PHONE_NEW` and then promote to `TE_EMPLOYEE_PHONE`. No manual
rollback is needed; simply re-trigger the Databricks job.

```bash
# Manual re-run via CLI
databricks bundle run crew_j_te_employee_phone_dw -t production
```

### Clear Error Table Before Re-run (if needed)

If re-running after fixing source data quality:

```sql
-- Preview errors that will be re-processed
SELECT COUNT(*) FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR;

-- Clear errors from the last run only (append mode — older errors remain)
DELETE FROM prod_catalog.etl_prod.TE_EMPLOYEE_PHONE_ERR
WHERE DATE(_error_ts) = CURRENT_DATE();
```

### Watermark Rollback

This job runs in `full_refresh` mode and does not use watermarks. No watermark
rollback is required.

---

## 11. Common Issues

| Issue | Cause | Resolution |
|---|---|---|
| `JDBC connection refused` | Wrong `TERADATA_JDBC_URL` or firewall | Verify hostname, port 1025, and network access from Databricks cluster to Teradata host |
| `ClassNotFoundException: com.teradata.jdbc.TeraDriver` | Teradata JAR not installed on cluster | Upload `terajdbc4.jar` + `tdgssconfig.jar` to `dbfs:/FileStore/jars/` and add as cluster libraries |
| `Table not found: CREW_WSTELE_LND` | Wrong `TD_WORK_DB` variable | Check the `TD_WORK_DB` environment variable matches the Teradata database name |
| `Table or view not found: etl_job_config` | Framework tables not created | Run framework DDL from Section 2 first |
| `Config not found for job: CREW_J_TE_EMPLOYEE_PHONE_DW` | `deploy_config.sql` not run | Execute `deploy_config.sql` with correct `UNITY_CATALOG`/`UNITY_SCHEMA` substitution |
| `AnalysisException: Table not found: TE_EMPLOYEE_PHONE_NEW` | Target tables not created | Run target table DDL from Section 3 |
| `All records going to error table` | All `EMP_NBR` values fail `ISVALID(INT32)` | Check Teradata source — `TELE_EMP_NBR` column may have changed type or format |
| `TE_EMPLOYEE_PHONE row count = 0` | Source table empty or all rows errored | Check `etl_audit_log.source_count` and `TE_EMPLOYEE_PHONE_ERR` |
| `Duration exceeds SLA` | Large data volume or cluster undersized | Increase `num_workers` from 2 to 4, or increase `fetchsize` in JDBC options |
| `Coverage below 80%` | New code without tests | Run `pytest --cov` locally and check `reports/htmlcov` to identify uncovered lines |
| `ruff lint failure in CI` | Code style violations | Run `ruff check --fix .` locally to auto-fix, then `ruff format .` |
| `MERGE conflict on business keys` | Duplicate `(EMP_NBR, PH_NBR)` pairs in source | Add DQ dedup rule to `dq_checks` in `etl_job_config` |

---

## Appendix — File Reference

| File | Purpose |
|---|---|
| `databricks.yml` | DABs bundle config — targets: dev, production |
| `jobs/crew_j_te_employee_phone_dw/run_pipeline.py` | PipelineRunner subclass — orchestrates transform steps |
| `jobs/crew_j_te_employee_phone_dw/transformations.py` | All 17 pure transformation functions |
| `jobs/crew_j_te_employee_phone_dw/deploy_config.sql` | DDL + config INSERT SQL for first deployment |
| `notebooks/crew_j_te_employee_phone_dw/run_pipeline.py` | Databricks notebook entry point with `dbutils.widgets` |
| `jobs/crew_j_te_employee_phone_dw/tests/test_transformations.py` | Unit tests for all transformation functions |
| `jobs/crew_j_te_employee_phone_dw/tests/test_integration.py` | End-to-end pipeline integration test |
| `jobs/crew_j_te_employee_phone_dw/tests/fixtures/synthetic_data.py` | Synthetic test data factory functions |
| `jobs/crew_j_te_employee_phone_dw/tests/fixtures/expected_outputs.py` | Target schema + expected output DataFrames |
| `framework/pipeline_runner.py` | Base class — audit/DQ/logging/write (do not modify) |
| `framework/common_utility.py` | DeltaManager, AuditLogger, WatermarkManager, DQValidator |
| `framework/readers/source_reader.py` | JDBC / file / Delta / test_dataframe reader |
| `framework/utils/config_manager.py` | ConfigManager — reads from `etl_job_config` Delta table |
| `framework/utils/job_logger.py` | DatabricksJobLogger — all logs + record rejects |

---

*Generated by Claude Code — DataStage to Databricks migration accelerator.*
*Source: CREW_J_TE_EMPLOYEE_PHONE_DW.xml | Date: 2026-03-24*
