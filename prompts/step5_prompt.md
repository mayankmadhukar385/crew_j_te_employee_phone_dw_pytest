# Step 5: Generate Databricks Deployment Guide

## Your Role

Generate a complete deployment guide that takes the generated project from a local
development environment to a running pipeline on Databricks with Unity Catalog.

## Multi-Job Awareness

Read `output/discovery.json`. If the XML contained a sequence job with multiple
parallel jobs, the deployment guide must cover deploying ALL jobs plus the
Databricks Workflow that orchestrates them.

---

## Your Inputs

1. `output/discovery.json` — job inventory
2. Generated project under `output/project/`
3. Corrected lineage Excel(s) — for table names and connection details
4. Shared framework from `framework/`
5. DABs config from `databricks.yml`
6. Generated workflow YAML from `output/{seq_name}_databricks_workflow.yml` (if sequence)
7. Config INSERT SQL files from `output/project/jobs/{job}/deploy_config.sql`

## Your Output

`output/project/DEPLOYMENT.md` — a single deployment guide covering all jobs.

---

## Document Structure

Generate a Markdown file with these sections:

### 1. Prerequisites

```markdown
## 1. Prerequisites

### Databricks Workspace
- Databricks workspace with Unity Catalog enabled
- A catalog and schema created for this pipeline:
  ```sql
  CREATE CATALOG IF NOT EXISTS <catalog_name>;
  CREATE SCHEMA IF NOT EXISTS <catalog_name>.<schema_name>;
  ```

### Required Permissions
- `USE CATALOG` on the target catalog
- `USE SCHEMA`, `CREATE TABLE`, `MODIFY` on the target schema
- `READ FILES` on source file locations (if file-based sources)

### JDBC Drivers (if applicable)
<List the specific JDBC drivers needed based on the source connector types
found in the pipeline. For each:>
- Driver: <driver class name>
- Download: <where to get it>
- Installation: Upload to `dbfs:/FileStore/jars/` or install via cluster init script

### Cluster Configuration
- Databricks Runtime: 14.x LTS or later (with Delta Lake included)
- Node type: <recommend based on pipeline complexity>
- Workers: <recommend based on data volume expectations>
- Spark config:
  ```
  spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
  spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
  ```

### Local Development
- Python 3.10+
- Install dev dependencies: `pip install -e ".[dev]"`
- Run tests: `pytest`
- Run linter: `ruff check .`
- Run formatter: `ruff format .`
```

### 2. Framework Tables — Create Once

```markdown
## 2. Create Framework Tables

The shared framework requires three metadata tables. Run these DDL statements
ONCE per catalog/schema — they are shared across all migrated jobs.

### Audit Log Table
```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.etl_audit_log (
    audit_id STRING NOT NULL,
    job_name STRING NOT NULL,
    run_id STRING NOT NULL,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP,
    status STRING NOT NULL,
    source_count LONG,
    target_count LONG,
    error_count LONG,
    reject_count LONG,
    parameters STRING,
    error_message STRING,
    cluster_id STRING,
    duration_seconds DOUBLE
) USING DELTA;
```

### Error Log Table
```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.etl_error_log (
    error_id STRING NOT NULL,
    audit_run_id STRING NOT NULL,
    job_name STRING NOT NULL,
    stage_name STRING NOT NULL,
    error_type STRING NOT NULL,
    error_message STRING,
    source_key STRING,
    source_record STRING,
    created_ts TIMESTAMP NOT NULL
) USING DELTA;
```

### Watermark Table
```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.etl_watermark (
    job_name STRING NOT NULL,
    source_table STRING NOT NULL,
    watermark_column STRING NOT NULL,
    last_value STRING,
    last_run_ts TIMESTAMP,
    run_mode STRING
) USING DELTA;
```

> Note: The framework auto-creates these tables on first run if they don't exist.
> Running the DDL manually is optional but recommended for visibility.
```

### 3. Target Tables

```markdown
## 3. Create Target Tables

Create the target Delta tables before first run:
```sql
<Generate CREATE TABLE statements for each target table,
using column names and types from the lineage Excel>
```
```

### 4. Project Upload

```markdown
## 4. Upload Project to Databricks

### Option A: Databricks Repos (recommended)
1. Push the project to your Git repository
2. In Databricks: Workspace → Repos → Add Repo
3. Clone the repository
4. The `framework/` and `jobs/` packages are immediately available

### Option B: Databricks Asset Bundles (recommended for CI/CD)
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev environment
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t production
```
```

### 5. Configuration

```markdown
## 5. Configure the Pipeline

### Environment Variables
Set these in your Databricks cluster, job configuration, or CI/CD secrets:

| Variable | Description | Example |
|---|---|---|
| `UNITY_CATALOG` | Target Unity Catalog name | `my_catalog` |
| `UNITY_SCHEMA` | Target schema name | `my_schema` |
| `JDBC_URL` | Source database connection URL | `jdbc:teradata://host/DBS_PORT=1025` |
<... list all ${VAR} references found in the config files>

### Config Deployment Per Job
Config lives in the `etl_job_config` Delta table — not YAML files.
Step 3 generates a `deploy_config.sql` file per job. Run it once:
```sql
-- Run the generated INSERT statement for each job
-- File: output/project/jobs/{job_name}/deploy_config.sql
```

After first deployment, config changes are SQL UPDATE statements:
```sql
-- Example: switch SCD type
UPDATE {catalog}.{schema}.etl_job_config
SET config_json = json_set(config_json, '$.target.scd_type', 2)
WHERE job_name = '{job_name}' AND is_active = 'Y';
```
```

### 6. Local Testing (VS Code)

```markdown
## 6. Local Testing

Tests run in VS Code without JDBC drivers or external databases.
Source data uses `source.type: "test_dataframe"` with Spark temp views.

```bash
pip install -e ".[dev]"
ruff check .                   # Lint
ruff format --check .          # Format check
pytest                         # Test + coverage (≥80% gate)
open reports/htmlcov/index.html  # Browse coverage report
```

CI/CD is centralized in GitLab. The above commands are what the GitLab
pipeline runs — same commands locally and in CI.
```

### 7. Deploy as Databricks Job

**If single parallel job:**
```markdown
### Single Job Deployment

1. Merge the generated job's resource block into `databricks.yml`
2. Deploy:
```bash
databricks bundle deploy -t production
```
```

**If sequence with multiple jobs:**
```markdown
### Multi-Job Workflow Deployment

The DataStage sequence job `{seq_name}` translates to a Databricks Workflow.
Step 3 generates `output/{seq_name}_databricks_workflow.yml` containing the
complete task graph with conditional branching.

1. Copy the generated workflow YAML into the `resources.jobs` section of `databricks.yml`
2. Verify:
   - Every parallel job has a `notebook_task` pointing to its notebook
   - `condition_task` nodes match the DataStage sequence conditions
   - `depends_on` chains match the execution graph
   - Job parameters have correct defaults
   - `email_notifications.on_failure` has the right recipients
3. Deploy:
```bash
databricks bundle deploy -t production
```

The workflow replicates the DataStage sequence logic:
- `condition_task` with `op: EQUAL_TO` for parameter-based branching
- `depends_on` with `outcome: "true"/"false"` for conditional paths
- `email_notifications` replacing the DataStage exception handler chain
- Job-level `parameters` replacing DataStage sequence parameters
```

### 8. Delta Table Maintenance

```markdown
## 8. Delta Table Maintenance

### OPTIMIZE Schedule
Run after each pipeline execution:
```sql
OPTIMIZE {catalog}.{schema}.{target_table} ZORDER BY ({key_columns});
```

### VACUUM Schedule
Run weekly to clean up old files:
```sql
VACUUM {catalog}.{schema}.{target_table} RETAIN 168 HOURS;
```

### Audit Log Cleanup
Retain 90 days of audit logs:
```sql
DELETE FROM {catalog}.{schema}.etl_audit_log
WHERE start_ts < current_timestamp() - INTERVAL 90 DAYS;
```

### Error Log Cleanup
Retain 30 days of error logs:
```sql
DELETE FROM {catalog}.{schema}.etl_error_log
WHERE created_ts < current_timestamp() - INTERVAL 30 DAYS;
```
```

### 9. Monitoring

```markdown
## 9. Monitoring and Alerting

### Audit Log Queries
```sql
-- Last 10 runs for a job
SELECT * FROM {catalog}.{schema}.etl_audit_log
WHERE job_name = '{job_name}'
ORDER BY start_ts DESC
LIMIT 10;

-- Failed runs in last 24 hours
SELECT * FROM {catalog}.{schema}.etl_audit_log
WHERE status = 'FAILED'
  AND start_ts > current_timestamp() - INTERVAL 24 HOURS;

-- Average duration trend
SELECT job_name, DATE(start_ts) as run_date,
       AVG(duration_seconds) as avg_seconds,
       SUM(source_count) as total_rows
FROM {catalog}.{schema}.etl_audit_log
WHERE status = 'SUCCESS'
GROUP BY job_name, DATE(start_ts)
ORDER BY run_date DESC;
```

### Error Analysis
```sql
-- Top error types in last 7 days
SELECT job_name, stage_name, error_type, COUNT(*) as error_count
FROM {catalog}.{schema}.etl_error_log
WHERE created_ts > current_timestamp() - INTERVAL 7 DAYS
GROUP BY job_name, stage_name, error_type
ORDER BY error_count DESC;
```

### Alerting
Configure email/Slack notifications on the Databricks Job:
- On failure: notify immediately
- On success with warnings: notify if row count drops >10% from previous run

### Unity Catalog Lineage
Once running on Databricks, Unity Catalog automatically tracks lineage
between Delta tables. View in:
Catalog Explorer → {table} → Lineage tab
```

### 10. Rollback

```markdown
## 10. Rollback Procedures

### Delta Time Travel
If a pipeline run produces incorrect data:
```sql
DESCRIBE HISTORY {catalog}.{schema}.{target_table};
RESTORE TABLE {catalog}.{schema}.{target_table} TO VERSION AS OF <version>;
```

### Watermark Rollback
If you need to re-process data from an earlier point:
```sql
UPDATE {catalog}.{schema}.etl_watermark
SET last_value = '<earlier_timestamp>'
WHERE job_name = '{job_name}' AND source_table = '{table}';
```
Then re-run the pipeline in incremental mode.

### Full Rerun
For full-refresh pipelines: the next run will overwrite — no manual rollback needed.
```

### 11. Troubleshooting

```markdown
## 11. Common Issues

| Issue | Cause | Resolution |
|---|---|---|
| JDBC connection refused | Wrong URL or firewall | Verify JDBC_URL, check network |
| Table not found | Schema/catalog mismatch | Check UNITY_CATALOG and UNITY_SCHEMA vars |
| Column type mismatch | Source schema changed | Update column_mappings.yaml |
| MERGE conflict | Duplicate keys in source | Check DQ rules, add dedup |
| Out of memory | Data volume too large | Increase cluster size or add partitioning |
| Coverage below 80% | Missing tests | Add tests, check reports/htmlcov |
| ruff lint failure | Code style issues | Run `ruff check --fix .` locally |
```

---

## Critical Rules

- Use ACTUAL table names, column names, and config values from the pipeline
- Do not write generic deployment instructions — everything should be specific to THIS pipeline
- If the pipeline uses JDBC, include the specific driver class and download link
- Include CREATE TABLE DDL for all target tables AND framework tables
- Include the Databricks Workflow definition for sequence jobs
- Include CI/CD instructions for both GitHub Actions and Azure DevOps
- Reference `databricks.yml` DABs config for deployment commands
- Include audit log and error log monitoring queries
- Include watermark rollback procedures for incremental pipelines
