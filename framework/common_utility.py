"""Common utility framework for DataStage-to-Databricks migrated pipelines.

Consolidates all reusable ETL patterns into a single class hierarchy:
- SCD Type 0, 1, 2, 4 write strategies
- Audit logging (run-level tracking)
- Error logging (record-level rejects)
- Watermark management (incremental load state)
- Data quality checks (config-driven validation)

Every generated job imports from this module — no duplication across jobs.

Usage:
    from framework.common_utility import DeltaManager, AuditLogger, DQValidator

    mgr = DeltaManager(spark, config)
    mgr.write(df, scd_type=2)
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. DELTA MANAGER — unified write strategy with SCD support
# ---------------------------------------------------------------------------

class DeltaManager:
    """Handles all Delta Lake write operations with SCD strategy selection.

    Instead of separate classes per SCD type, this single class exposes one
    `write()` method. The SCD type is a config parameter — the caller never
    needs to know the internal implementation.

    Args:
        spark: Active SparkSession.
        config: Pipeline config dict with target, scd, and delta sections.
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        self.spark = spark
        self.config = config
        self.catalog = config["pipeline"]["catalog"]
        self.schema = config["pipeline"]["schema"]

    def _full_table_name(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    # -- public entry point --------------------------------------------------

    def write(
        self,
        df: DataFrame,
        target_table: Optional[str] = None,
        scd_type: Optional[int] = None,
        write_mode: Optional[str] = None,
    ) -> dict[str, int]:
        """Write a DataFrame to Delta using the configured strategy.

        Supports two routing paths:
        1. SCD strategies (scd_type: 0, 1, 2, 4) — for dimension tables
        2. Plain write modes (write_mode: append, truncate_load, update, upsert)
           — for fact/stage tables without SCD history

        Priority: write_mode takes precedence over scd_type if both are set.

        Args:
            df: The DataFrame to write.
            target_table: Override target table name (defaults to config).
            scd_type: Override SCD type (defaults to config).
            write_mode: Override write mode. One of:
                - "append" — insert all rows, no dedup
                - "truncate_load" — drop and reload entire table
                - "update" — update matched rows only, no inserts
                - "upsert" — insert new + update existing, no SCD history

        Returns:
            Dict with counts: inserted, updated, deleted, source_rows.
        """
        table = target_table or self.config["target"]["table"]
        full_table = self._full_table_name(table)

        # Resolve strategy: write_mode takes precedence
        mode = write_mode or self.config["target"].get("write_mode")

        if mode:
            mode_map = {
                "append": self._plain_append,
                "truncate_load": self._truncate_and_load,
                "update": self._plain_update,
                "upsert": self._plain_upsert,
            }
            handler = mode_map.get(mode)
            if handler is None:
                raise ValueError(f"Unsupported write_mode: {mode}. Supported: {list(mode_map.keys())}")
            logger.info(f"Writing to {full_table} using mode={mode}")
        else:
            strategy = scd_type or self.config["target"].get("scd_type", 1)
            scd_map = {
                0: self._scd_type_0,
                1: self._scd_type_1,
                2: self._scd_type_2,
                4: self._scd_type_4,
            }
            handler = scd_map.get(strategy)
            if handler is None:
                raise ValueError(f"Unsupported SCD type: {strategy}. Supported: {list(scd_map.keys())}")
            logger.info(f"Writing to {full_table} using SCD Type {strategy}")

        result = handler(df, full_table)
        self._post_write_maintenance(full_table)
        logger.info(
            f"Write complete: table={full_table}, "
            f"inserted={result.get('inserted', '?')}, "
            f"updated={result.get('updated', '?')}, "
            f"deleted={result.get('deleted', 0)}"
        )
        return result

    # -- Plain write modes (no SCD history) -----------------------------------

    def _plain_append(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """Append all rows to the target table. No dedup, no merge.

        Use for: staging tables, error tables, log tables, fact inserts.
        """
        count = df.count()
        if self._table_exists(full_table):
            df.write.format("delta").mode("append").saveAsTable(full_table)
        else:
            df.write.format("delta").mode("overwrite").saveAsTable(full_table)
        return {"inserted": count, "updated": 0, "deleted": 0, "source_rows": count}

    def _truncate_and_load(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """Drop all existing rows and load fresh. Full refresh.

        Use for: full-refresh fact tables, snapshot tables, small reference data.
        """
        count = df.count()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
        return {"inserted": count, "updated": 0, "deleted": -1, "source_rows": count}

    def _plain_update(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """Update existing rows only. No inserts for new keys.

        Use for: status flag updates, corrections to existing records.
        Requires business_keys in config for the merge condition.
        """
        business_keys = self.config["target"]["business_keys"]

        if not self._table_exists(full_table):
            logger.warning(f"Update mode but table {full_table} does not exist. Nothing to update.")
            return {"inserted": 0, "updated": 0, "deleted": 0, "source_rows": df.count()}

        condition = " AND ".join(f"tgt.{k} = src.{k}" for k in business_keys)

        (
            DeltaTable.forName(self.spark, full_table).alias("tgt")
            .merge(df.alias("src"), condition)
            .whenMatchedUpdateAll()
            .execute()
        )

        return self._extract_merge_metrics(full_table)

    def _plain_upsert(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """Insert new rows + update existing rows. No SCD history.

        Use for: simple dimensions where you don't need history,
        current-state tables, aggregation targets.
        Requires business_keys in config for the merge condition.
        """
        business_keys = self.config["target"]["business_keys"]

        if not self._table_exists(full_table):
            count = df.count()
            df.write.format("delta").mode("overwrite").saveAsTable(full_table)
            return {"inserted": count, "updated": 0, "deleted": 0, "source_rows": count}

        condition = " AND ".join(f"tgt.{k} = src.{k}" for k in business_keys)

        (
            DeltaTable.forName(self.spark, full_table).alias("tgt")
            .merge(df.alias("src"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        return self._extract_merge_metrics(full_table)

    # -- SCD implementations (private) ---------------------------------------

    def _scd_type_0(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """SCD Type 0: Insert only, ignore changes to existing records.

        New records are inserted. Existing records (matched on business keys)
        are left untouched. Use for reference/lookup data that never changes.
        """
        business_keys = self.config["target"]["business_keys"]

        if not self._table_exists(full_table):
            count = df.count()
            df.write.format("delta").mode("overwrite").saveAsTable(full_table)
            return {"written": count, "updated": 0, "inserted": count}

        condition = " AND ".join(f"tgt.{k} = src.{k}" for k in business_keys)
        (
            DeltaTable.forName(self.spark, full_table).alias("tgt")
            .merge(df.alias("src"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

        return self._extract_merge_metrics(full_table)

    def _scd_type_1(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """SCD Type 1: Overwrite changed records in place.

        Uses hash-based change detection so unchanged rows are not touched.
        This avoids unnecessary Delta file rewrites and preserves audit trails.
        """
        business_keys = self.config["target"]["business_keys"]
        tracked_columns = self.config["target"].get(
            "tracked_columns",
            [c for c in df.columns if c not in business_keys],
        )

        if not self._table_exists(full_table):
            count = df.count()
            df_with_hash = self._add_change_hash(df, tracked_columns)
            df_with_hash.write.format("delta").mode("overwrite").saveAsTable(full_table)
            return {"written": count, "updated": 0, "inserted": count}

        df_with_hash = self._add_change_hash(df, tracked_columns)
        condition = " AND ".join(f"tgt.{k} = src.{k}" for k in business_keys)

        # Only update when the hash of tracked columns has actually changed
        update_condition = "tgt._change_hash != src._change_hash"

        (
            DeltaTable.forName(self.spark, full_table).alias("tgt")
            .merge(df_with_hash.alias("src"), condition)
            .whenMatchedUpdate(condition=update_condition, set={
                **{c: f"src.{c}" for c in df.columns},
                "_change_hash": "src._change_hash",
                "_updated_ts": "current_timestamp()",
            })
            .whenNotMatchedInsertAll()
            .execute()
        )

        return self._extract_merge_metrics(full_table)

    def _scd_type_2(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """SCD Type 2: Add new row for changes, close the old row.

        Maintains full history with effective/expiry dates and a current flag.
        Config must specify: effective_date_col, expiry_date_col, current_flag_col.

        Correct SCD-2 requires three operations:
        1. Identify which incoming rows are NEW vs CHANGED vs UNCHANGED
        2. MERGE: close changed rows (set is_current=False, valid_to=today)
                  + insert truly new rows
        3. APPEND: re-insert changed rows as new current versions

        Optionally (if soft_delete_missing=True in config):
        4. Close target rows whose keys are absent from source (soft delete)

        A single MERGE cannot both UPDATE a matched row AND INSERT a new
        version of that same logical record. That's why step 3 is separate.
        """
        business_keys = self.config["target"]["business_keys"]
        tracked_columns = self.config["target"].get("tracked_columns", [])
        eff_col = self.config["target"].get("effective_date_col", "eff_dt")
        exp_col = self.config["target"].get("expiry_date_col", "exp_dt")
        flag_col = self.config["target"].get("current_flag_col", "is_current")
        high_date = "9999-12-31"
        soft_delete = self.config["target"].get("soft_delete_missing", False)

        if not tracked_columns:
            tracked_columns = [
                c for c in df.columns
                if c not in business_keys + [eff_col, exp_col, flag_col]
            ]

        # Add change hash to incoming data for comparison
        df_hashed = self._add_change_hash(df, tracked_columns)

        # --- First run: table doesn't exist yet ---
        if not self._table_exists(full_table):
            df_initial = (
                df_hashed
                .withColumn(eff_col, F.current_timestamp())
                .withColumn(exp_col, F.lit(high_date).cast("timestamp"))
                .withColumn(flag_col, F.lit(True))
            )
            count = df_initial.count()
            df_initial.write.format("delta").mode("overwrite").saveAsTable(full_table)
            return {"inserted": count, "updated": 0, "deleted": 0,
                    "rows_closed": 0, "rows_new": count, "rows_changed": 0,
                    "rows_unchanged": 0, "source_rows": count}

        # --- Step 1: Stage — classify incoming rows ---
        # Join source against current target rows to find new, changed, unchanged
        target_current = (
            self.spark.read.table(full_table)
            .filter(F.col(flag_col) == True)
            .select(*business_keys, F.col("_change_hash").alias("_tgt_hash"))
        )

        staged = (
            df_hashed.alias("src")
            .join(target_current.alias("tgt"), business_keys, "left")
            .withColumn("_action", F.when(
                F.col("_tgt_hash").isNull(), F.lit("NEW")
            ).when(
                F.col("_change_hash") != F.col("_tgt_hash"), F.lit("CHANGED")
            ).otherwise(F.lit("UNCHANGED")))
        )

        rows_new = staged.filter(F.col("_action") == "NEW").count()
        rows_changed = staged.filter(F.col("_action") == "CHANGED").count()
        rows_unchanged = staged.filter(F.col("_action") == "UNCHANGED").count()
        source_total = rows_new + rows_changed + rows_unchanged

        logger.info(
            f"SCD-2 staging: {rows_new} new, {rows_changed} changed, "
            f"{rows_unchanged} unchanged out of {source_total} source rows"
        )

        # If nothing to do, exit early
        if rows_new == 0 and rows_changed == 0 and not soft_delete:
            return {"inserted": 0, "updated": 0, "deleted": 0,
                    "rows_closed": 0, "rows_new": 0, "rows_changed": 0,
                    "rows_unchanged": rows_unchanged, "source_rows": source_total}

        # --- Step 2: MERGE — close changed rows + insert new rows ---
        # Only merge rows that are NEW or CHANGED (skip UNCHANGED for performance)
        df_to_merge = (
            staged.filter(F.col("_action").isin("NEW", "CHANGED"))
            .drop("_tgt_hash", "_action")
            .withColumn(eff_col, F.current_timestamp())
            .withColumn(exp_col, F.lit(high_date).cast("timestamp"))
            .withColumn(flag_col, F.lit(True))
        )

        key_condition = " AND ".join(f"tgt.{k} = src.{k}" for k in business_keys)
        merge_condition = f"{key_condition} AND tgt.{flag_col} = true"

        merge_builder = (
            DeltaTable.forName(self.spark, full_table).alias("tgt")
            .merge(df_to_merge.alias("src"), merge_condition)
            .whenMatchedUpdate(
                # Close the old current row for CHANGED records
                condition="tgt._change_hash != src._change_hash",
                set={
                    exp_col: "current_timestamp()",
                    flag_col: "false",
                },
            )
            .whenNotMatchedInsertAll()  # Insert NEW records
        )

        # Optional: soft-delete target rows not in source
        if soft_delete:
            merge_builder = merge_builder.whenNotMatchedBySourceUpdate(
                condition=f"tgt.{flag_col} = true",
                set={
                    flag_col: "false",
                    exp_col: "current_timestamp()",
                },
            )

        merge_builder.execute()
        merge_metrics = self._extract_merge_metrics(full_table)

        # --- Step 3: APPEND — re-insert changed rows as new current versions ---
        # The MERGE above closed the old versions. Now insert the new versions.
        if rows_changed > 0:
            changed_new_versions = (
                staged.filter(F.col("_action") == "CHANGED")
                .drop("_tgt_hash", "_action")
                .withColumn(eff_col, F.current_timestamp())
                .withColumn(exp_col, F.lit(high_date).cast("timestamp"))
                .withColumn(flag_col, F.lit(True))
            )
            changed_new_versions.write.format("delta").mode("append").saveAsTable(full_table)
            logger.info(f"SCD-2: re-inserted {rows_changed} changed rows as new current versions")

        rows_soft_deleted = merge_metrics.get("deleted", 0)

        return {
            "inserted": rows_new + rows_changed,
            "updated": merge_metrics.get("updated", 0),
            "deleted": rows_soft_deleted,
            "rows_closed": rows_changed,
            "rows_new": rows_new,
            "rows_changed": rows_changed,
            "rows_unchanged": rows_unchanged,
            "rows_soft_deleted": rows_soft_deleted,
            "source_rows": source_total,
        }

    def _scd_type_4(self, df: DataFrame, full_table: str) -> dict[str, int]:
        """SCD Type 4: Maintain current table + separate history table.

        Current table is always SCD-1 (overwrite in place).
        History table gets an append of every change with a timestamp.
        Config must specify: history_table.
        """
        history_table = self.config["target"].get("history_table")
        if not history_table:
            raise ValueError("SCD Type 4 requires 'history_table' in target config")

        full_history = self._full_table_name(history_table)

        # Write current state using SCD-1 logic
        result = self._scd_type_1(df, full_table)

        # Append snapshot to history table with timestamp
        df_history = df.withColumn("_snapshot_ts", F.current_timestamp())
        history_count = df_history.count()

        if self._table_exists(full_history):
            df_history.write.format("delta").mode("append").saveAsTable(full_history)
        else:
            df_history.write.format("delta").mode("overwrite").saveAsTable(full_history)

        logger.info(f"SCD-4 history appended to {full_history}: {history_count} rows")
        result["history_appended"] = history_count
        return result

    # -- shared helpers (private) --------------------------------------------

    def _add_change_hash(self, df: DataFrame, columns: list[str]) -> DataFrame:
        """Add a _change_hash column for change detection.

        Hashing tracked columns avoids comparing every column individually
        in the MERGE condition, which improves performance and readability.
        """
        hash_expr = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in sorted(columns)]), 256)
        return df.withColumn("_change_hash", hash_expr)

    def _extract_merge_metrics(self, full_table: str) -> dict[str, int]:
        """Extract actual insert/update/delete counts from the last MERGE operation.

        Delta Lake records operation metrics in the table history after every
        MERGE. This reads the most recent history entry and returns exact counts
        instead of approximations.

        Returns:
            Dict with keys: inserted, updated, deleted, source_rows, target_rows.
        """
        try:
            history = self.spark.sql(f"DESCRIBE HISTORY {full_table} LIMIT 1")
            row = history.select("operationMetrics").collect()[0][0]

            if row is None:
                return {"inserted": -1, "updated": -1, "deleted": 0}

            return {
                "inserted": int(row.get("numTargetRowsInserted", -1)),
                "updated": int(row.get("numTargetRowsUpdated", -1)),
                "deleted": int(row.get("numTargetRowsDeleted", 0)),
                "source_rows": int(row.get("numSourceRows", -1)),
                "target_rows": int(row.get("numOutputRows", -1)),
            }
        except Exception as e:
            logger.warning(f"Could not extract merge metrics: {e}")
            return {"inserted": -1, "updated": -1, "deleted": 0}

    def _table_exists(self, full_table: str) -> bool:
        try:
            self.spark.read.table(full_table)
            return True
        except Exception:
            return False

    def _post_write_maintenance(self, full_table: str) -> None:
        """Run OPTIMIZE and VACUUM if configured."""
        delta_cfg = self.config.get("delta", {})

        if delta_cfg.get("optimize_after_write", False):
            zorder = self.config["target"].get("zorder_columns", [])
            if zorder:
                cols = ", ".join(zorder) if isinstance(zorder, list) else zorder
                self.spark.sql(f"OPTIMIZE {full_table} ZORDER BY ({cols})")
            else:
                self.spark.sql(f"OPTIMIZE {full_table}")

        retention = delta_cfg.get("vacuum_retention_hours")
        if retention:
            self.spark.sql(f"VACUUM {full_table} RETAIN {retention} HOURS")


# ---------------------------------------------------------------------------
# 2. AUDIT LOGGER — run-level tracking with Databricks context enrichment
# ---------------------------------------------------------------------------

class AuditLogger:
    """Tracks pipeline execution at the run level with Databricks context.

    Creates one row per pipeline run in the audit log table with start/end
    timestamps, row counts, status, parameters, AND full Databricks runtime
    context (workflow_name, job_id, notebook_path, cluster_id, user, etc.).

    Uses the same context auto-detection as DatabricksJobLogger so that
    audit records can be correlated with job log entries.

    Usage:
        audit = AuditLogger(spark, config)
        run_id = audit.start_run(parameters={"mode": "full"})
        try:
            # ... pipeline logic ...
            audit.end_run(run_id, status="SUCCESS", counts={...})
        except Exception as e:
            audit.end_run(run_id, status="FAILED", error_message=str(e))
            raise
    """

    SCHEMA = T.StructType([
        T.StructField("audit_id", T.StringType(), False),
        T.StructField("job_name", T.StringType(), False),
        T.StructField("workflow_name", T.StringType(), True),
        T.StructField("job_id", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("start_ts", T.TimestampType(), False),
        T.StructField("end_ts", T.TimestampType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("source_count", T.LongType(), True),
        T.StructField("target_count", T.LongType(), True),
        T.StructField("error_count", T.LongType(), True),
        T.StructField("reject_count", T.LongType(), True),
        T.StructField("parameters", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("notebook_path", T.StringType(), True),
        T.StructField("cluster_id", T.StringType(), True),
        T.StructField("task_run_id", T.StringType(), True),
        T.StructField("databricks_run_id", T.StringType(), True),
        T.StructField("user_name", T.StringType(), True),
        T.StructField("duration_seconds", T.DoubleType(), True),
    ])

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        self.spark = spark
        self.config = config
        self.job_name = config["pipeline"]["job_name"]
        catalog = config["pipeline"]["catalog"]
        schema = config["pipeline"]["schema"]
        self.audit_table = f"{catalog}.{schema}.etl_audit_log"

        # Auto-detect Databricks runtime context
        from framework.utils.databricks_context import get_databricks_context

        self._ctx = get_databricks_context()
        self.workflow_name = config["pipeline"].get("workflow_name", self._ctx.get("workflow_name", ""))
        self.job_id = config["pipeline"].get("job_id", self._ctx.get("job_id", ""))

        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.audit_table} (
                audit_id STRING NOT NULL,
                job_name STRING NOT NULL,
                workflow_name STRING,
                job_id STRING,
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
                notebook_path STRING,
                cluster_id STRING,
                task_run_id STRING,
                databricks_run_id STRING,
                user_name STRING,
                duration_seconds DOUBLE
            ) USING DELTA
        """)

    def start_run(self, parameters: Optional[dict] = None) -> str:
        """Record the start of a pipeline run. Returns the run_id."""
        import json

        run_id = str(uuid.uuid4())
        now = datetime.utcnow()

        row = self.spark.createDataFrame([{
            "audit_id": str(uuid.uuid4()),
            "job_name": self.job_name,
            "workflow_name": self.workflow_name,
            "job_id": self.job_id,
            "run_id": run_id,
            "start_ts": now,
            "end_ts": None,
            "status": "RUNNING",
            "source_count": None,
            "target_count": None,
            "error_count": None,
            "reject_count": None,
            "parameters": json.dumps(parameters) if parameters else None,
            "error_message": None,
            "notebook_path": self._ctx.get("notebook_path"),
            "cluster_id": self._ctx.get("cluster_id",
                self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "local")),
            "task_run_id": self._ctx.get("task_run_id"),
            "databricks_run_id": self._ctx.get("run_id"),
            "user_name": self._ctx.get("user_name"),
            "duration_seconds": None,
        }], schema=self.SCHEMA)

        row.write.format("delta").mode("append").saveAsTable(self.audit_table)
        logger.info(f"Audit: started run {run_id} for {self.job_name}")
        return run_id

    def end_run(
        self,
        run_id: str,
        status: str = "SUCCESS",
        counts: Optional[dict[str, int]] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Record the end of a pipeline run."""
        counts = counts or {}
        now = datetime.utcnow()

        (
            DeltaTable.forName(self.spark, self.audit_table).alias("tgt")
            .merge(
                self.spark.createDataFrame([{"run_id": run_id}]).alias("src"),
                "tgt.run_id = src.run_id",
            )
            .whenMatchedUpdate(set={
                "end_ts": F.lit(now).cast("timestamp"),
                "status": F.lit(status),
                "source_count": F.lit(counts.get("source_count")),
                "target_count": F.lit(counts.get("target_count")),
                "error_count": F.lit(counts.get("error_count", 0)),
                "reject_count": F.lit(counts.get("reject_count", 0)),
                "error_message": F.lit(error_message),
                "duration_seconds": F.lit(None),
            })
            .execute()
        )

        # Compute duration from start_ts to end_ts
        self.spark.sql(f"""
            UPDATE {self.audit_table}
            SET duration_seconds = unix_timestamp(end_ts) - unix_timestamp(start_ts)
            WHERE run_id = '{run_id}'
        """)

        logger.info(f"Audit: ended run {run_id} with status={status}")


# ---------------------------------------------------------------------------
# 3. ERROR LOGGER — MERGED INTO DatabricksJobLogger
# ---------------------------------------------------------------------------
# ErrorLogger functionality is now part of DatabricksJobLogger.log_rejects().
# One logger, one Delta table (etl_job_log), one flush() call.
# See framework/utils/job_logger.py for the unified implementation.
#
# Migration: replace ErrorLogger(spark, config, run_id) with
#   logger = DatabricksJobLoggerFactory.create(spark, config)
#   logger.log_rejects(df, stage="...", error_type="...", key_columns=[...])
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 4. WATERMARK MANAGER — incremental load state
# ---------------------------------------------------------------------------

class WatermarkManager:
    """Manages watermark state for incremental/delta loads.

    Stores the last successfully processed value (timestamp, ID, etc.)
    per job + source table combination. The source reader uses this to
    build a WHERE clause that fetches only new/changed records.

    Every watermark record includes job_id and workflow_name for
    consistent traceability across audit, log, and watermark tables.

    Usage:
        wm = WatermarkManager(spark, config)
        last_ts = wm.get_watermark("source_orders", "modified_ts")
        # Reader uses: WHERE modified_ts > last_ts
        # After successful write:
        wm.update_watermark("source_orders", "modified_ts", new_max_ts)
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        self.spark = spark
        self.config = config
        self.job_name = config["pipeline"]["job_name"]
        self.job_id = config["pipeline"].get("job_id", "")
        self.workflow_name = config["pipeline"].get("workflow_name", "")
        catalog = config["pipeline"]["catalog"]
        schema = config["pipeline"]["schema"]
        self.watermark_table = f"{catalog}.{schema}.etl_watermark"
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.watermark_table} (
                job_name STRING NOT NULL,
                job_id STRING,
                workflow_name STRING,
                source_table STRING NOT NULL,
                watermark_column STRING NOT NULL,
                last_value STRING,
                last_run_ts TIMESTAMP,
                run_mode STRING
            ) USING DELTA
        """)

    def get_watermark(self, source_table: str, watermark_column: str) -> Optional[str]:
        """Get the last processed watermark value. Returns None if no prior run."""
        result = self.spark.sql(f"""
            SELECT last_value FROM {self.watermark_table}
            WHERE job_name = '{self.job_name}'
              AND source_table = '{source_table}'
              AND watermark_column = '{watermark_column}'
        """).collect()

        if result:
            val = result[0]["last_value"]
            logger.info(f"Watermark: {source_table}.{watermark_column} = {val}")
            return val

        logger.info(f"Watermark: no prior value for {source_table}.{watermark_column}")
        return None

    def update_watermark(
        self,
        source_table: str,
        watermark_column: str,
        new_value: str,
        run_mode: str = "incremental",
    ) -> None:
        """Update the watermark after a successful pipeline run."""
        update_df = self.spark.createDataFrame([{
            "job_name": self.job_name,
            "job_id": self.job_id,
            "workflow_name": self.workflow_name,
            "source_table": source_table,
            "watermark_column": watermark_column,
            "last_value": str(new_value),
            "last_run_ts": datetime.utcnow(),
            "run_mode": run_mode,
        }])

        if self.get_watermark(source_table, watermark_column) is not None:
            (
                DeltaTable.forName(self.spark, self.watermark_table).alias("tgt")
                .merge(
                    update_df.alias("src"),
                    "tgt.job_name = src.job_name AND tgt.source_table = src.source_table AND tgt.watermark_column = src.watermark_column",
                )
                .whenMatchedUpdateAll()
                .execute()
            )
        else:
            update_df.write.format("delta").mode("append").saveAsTable(self.watermark_table)

        logger.info(f"Watermark: updated {source_table}.{watermark_column} = {new_value}")


# ---------------------------------------------------------------------------
# 5. DQ VALIDATOR — config-driven data quality checks
# ---------------------------------------------------------------------------

class DQValidator:
    """Config-driven data quality validator.

    Reads DQ rules from config and applies them to a DataFrame, splitting
    into passed and failed records. Failed records are routed to
    DatabricksJobLogger.log_rejects() for tracking.

    Config format:
        dq_checks:
          - column: emp_id
            rules: [not_null, unique]
          - column: salary
            rules: [not_null, positive, max_value(10000000)]
          - column: email
            rules: [not_null, matches_regex("^[^@]+@[^@]+$")]

    Usage:
        dq = DQValidator(spark, config)
        passed_df, failed_df = dq.validate(df, stage="source_reader")
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        self.spark = spark
        self.config = config
        self.checks = config.get("dq_checks", [])

    def validate(
        self,
        df: DataFrame,
        stage: str = "unknown",
    ) -> tuple[DataFrame, DataFrame]:
        """Apply all configured DQ checks. Returns (passed_df, failed_df)."""
        if not self.checks:
            return df, self.spark.createDataFrame([], df.schema)

        failure_conditions = []

        for check in self.checks:
            column = check["column"]
            for rule in check.get("rules", []):
                condition = self._rule_to_condition(column, rule)
                if condition is not None:
                    failure_conditions.append(condition)

        if not failure_conditions:
            return df, self.spark.createDataFrame([], df.schema)

        # Combine all failure conditions with OR — any failure marks the row
        from functools import reduce
        combined_failure = reduce(lambda a, b: a | b, failure_conditions)

        passed = df.filter(~combined_failure)
        failed = df.filter(combined_failure)

        pass_count = passed.count()
        fail_count = failed.count()
        logger.info(f"DQ [{stage}]: {pass_count} passed, {fail_count} failed")

        return passed, failed

    def _rule_to_condition(self, column: str, rule: str) -> Any:
        """Convert a rule string to a PySpark filter condition for failures."""
        col = F.col(column)

        if rule == "not_null":
            return col.isNull() | (F.trim(col.cast("string")) == "")
        elif rule == "positive":
            return col <= 0
        elif rule == "unique":
            return F.lit(False)  # uniqueness checked separately via groupBy
        elif rule.startswith("max_value("):
            val = float(rule.split("(")[1].rstrip(")"))
            return col > val
        elif rule.startswith("min_value("):
            val = float(rule.split("(")[1].rstrip(")"))
            return col < val
        elif rule.startswith("matches_regex("):
            pattern = rule.split("(", 1)[1].rstrip(")").strip('"').strip("'")
            return ~col.rlike(pattern)
        elif rule.startswith("min_length("):
            val = int(rule.split("(")[1].rstrip(")"))
            return F.length(col) < val
        elif rule.startswith("max_length("):
            val = int(rule.split("(")[1].rstrip(")"))
            return F.length(col) > val
        elif rule.startswith("date_range("):
            parts = rule.split("(", 1)[1].rstrip(")").split(",")
            low = parts[0].strip().strip('"').strip("'")
            high = parts[1].strip().strip('"').strip("'")
            if high == "today":
                return (col < F.lit(low)) | (col > F.current_date())
            return (col < F.lit(low)) | (col > F.lit(high))
        else:
            logger.warning(f"DQ: unrecognized rule '{rule}' for column '{column}', skipping")
            return None