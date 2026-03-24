"""Base pipeline runner with audit, logging, DQ, and error handling baked in.

Every generated job extends PipelineRunner and implements transform().
The base class guarantees that:
1. Config is loaded from the Delta config table
2. AuditLogger.start_run() is called BEFORE any processing
3. DatabricksJobLogger captures all operational logs
4. DQValidator runs on source data, rejects go to log_rejects()
5. DeltaManager.write() returns actual insert/update counts
6. WatermarkManager updates on success
7. AuditLogger.end_run() is called with SUCCESS or FAILED — ALWAYS
8. Logger.flush() is called in finally — ALWAYS
9. Every exception is caught, logged, and marked as FAILED in audit

A job CANNOT skip audit logging. It's structural, not optional.

Usage — what Step 3 generates per job:

    from framework.pipeline_runner import PipelineRunner
    from pyspark.sql import DataFrame

    class MyJobPipeline(PipelineRunner):

        def transform(self, source_df: DataFrame, config: dict) -> DataFrame:
            # Job-specific transformation logic — the ONLY thing each job writes
            return source_df.withColumn("new_col", F.trim(F.col("raw_col")))

    # Entry point
    if __name__ == "__main__":
        MyJobPipeline(spark, job_name="Jb_LMIS_Full_Ld").run()
"""

from __future__ import annotations

import os
from typing import Any, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from framework.common_utility import (
    AuditLogger,
    DeltaManager,
    DQValidator,
    WatermarkManager,
)
from framework.readers.source_reader import SourceReader
from framework.utils.config_manager import ConfigManager
from framework.utils.job_logger import DatabricksJobLoggerFactory


class PipelineRunner:
    """Base pipeline runner with mandatory audit and error handling.

    Subclasses implement ONE method: transform(source_df, config) → DataFrame.
    Everything else — audit, logging, DQ, watermark, error capture — is handled
    by the base class and cannot be skipped.

    Args:
        spark: Active SparkSession.
        job_name: Job identifier matching etl_job_config.job_name.
        catalog: Override catalog (defaults to UNITY_CATALOG env var).
        schema: Override schema (defaults to UNITY_SCHEMA env var).
    """

    def __init__(
        self,
        spark: SparkSession,
        job_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.job_name = job_name
        self.catalog = catalog or os.environ.get("UNITY_CATALOG", "spark_catalog")
        self.schema = schema or os.environ.get("UNITY_SCHEMA", "default")

    def transform(self, source_df: DataFrame, config: dict[str, Any]) -> Union[DataFrame, dict[str, DataFrame]]:
        """Apply job-specific transformations. MUST be overridden by each job.

        Args:
            source_df: Source DataFrame after DQ validation (clean rows only).
            config: Full pipeline config from the Delta config table.

        Returns:
            Single DataFrame for single-target jobs, or
            dict of {"target_name": DataFrame} for multi-target jobs.

        Raises:
            NotImplementedError: If the subclass doesn't override this.
        """
        raise NotImplementedError(
            f"Job '{self.job_name}' must implement transform(source_df, config)"
        )

    def run(self) -> dict[str, Any]:
        """Execute the full pipeline lifecycle.

        This method is FINAL — jobs do NOT override it. The lifecycle is:
        1. Load config from Delta table
        2. Initialize audit logger → start_run()
        3. Initialize job logger
        4. Read source data
        5. Run DQ checks → split passed/failed
        6. Log rejects for failed records
        7. Call self.transform() — the job-specific part
        8. Write to target(s) via DeltaManager
        9. Update watermark (if incremental)
        10. end_run(SUCCESS) or end_run(FAILED) — guaranteed

        Returns:
            Dict with execution summary: run_id, status, counts.
        """
        # --- 1. Load config ---
        config_mgr = ConfigManager(self.spark, self.catalog, self.schema)
        config = config_mgr.get_config(self.job_name)

        # --- 2. Initialize audit logger and start run ---
        audit = AuditLogger(self.spark, config)
        run_id = audit.start_run(parameters={
            "run_mode": config["pipeline"].get("run_mode", "full_refresh"),
            "job_id": config["pipeline"].get("job_id", ""),
        })

        # --- 3. Initialize job logger ---
        logger = DatabricksJobLoggerFactory.create(self.spark, config)

        counts: dict[str, Any] = {}

        try:
            logger.info("Pipeline started", {
                "job_name": self.job_name,
                "run_id": run_id,
                "run_mode": config["pipeline"].get("run_mode"),
            })

            # --- 4. Read source ---
            reader = SourceReader(self.spark, config)
            watermark = WatermarkManager(self.spark, config)
            last_wm = None

            if config["pipeline"].get("run_mode") == "incremental":
                last_wm = watermark.get_watermark(
                    config["source"]["table"],
                    config["source"].get("watermark_column", "modified_ts"),
                )

            source_df = reader.read(watermark_value=last_wm)
            source_count = source_df.count()
            counts["source_count"] = source_count
            logger.info("Source read complete", {"record_count": source_count})

            # --- 5. DQ checks ---
            dq = DQValidator(self.spark, config)
            passed_df, failed_df = dq.validate(source_df, stage="source_reader")

            # --- 6. Log rejects ---
            reject_count = 0
            if not failed_df.isEmpty():
                reject_count = logger.log_rejects(
                    failed_df,
                    stage="source_reader",
                    error_type="DQ_VALIDATION",
                    key_columns=config["target"].get("business_keys", []),
                    message="Failed DQ checks on source data",
                )
            counts["reject_count"] = reject_count

            logger.info("DQ complete", {
                "passed": passed_df.count(),
                "rejected": reject_count,
            })

            # --- 7. Transform (job-specific) ---
            logger.info("Transform starting")
            result = self.transform(passed_df, config)
            logger.info("Transform complete")

            # --- 8. Write to target(s) ---
            writer = DeltaManager(self.spark, config)

            if isinstance(result, dict):
                # Multi-target job
                for target_name, target_df in result.items():
                    target_config_section = config.get("targets", {}).get(target_name, config["target"])
                    write_result = writer.write(
                        target_df,
                        target_table=target_config_section.get("table"),
                        scd_type=target_config_section.get("scd_type"),
                        write_mode=target_config_section.get("write_mode"),
                    )
                    counts[f"{target_name}_inserted"] = write_result.get("inserted", 0)
                    counts[f"{target_name}_updated"] = write_result.get("updated", 0)
                    logger.info(f"Write complete: {target_name}", write_result)
            else:
                # Single-target job
                write_result = writer.write(result)
                counts["target_inserted"] = write_result.get("inserted", 0)
                counts["target_updated"] = write_result.get("updated", 0)
                logger.info("Write complete", write_result)

            counts["target_count"] = counts.get("target_inserted", 0) + counts.get("target_updated", 0)
            counts["error_count"] = reject_count

            # --- 9. Update watermark ---
            if config["pipeline"].get("run_mode") == "incremental":
                wm_col = config["source"].get("watermark_column", "modified_ts")
                new_wm = source_df.agg(F.max(wm_col)).collect()[0][0]
                if new_wm is not None:
                    watermark.update_watermark(
                        config["source"]["table"], wm_col, str(new_wm),
                    )
                    logger.info("Watermark updated", {"column": wm_col, "value": str(new_wm)})

            # --- 10. Success ---
            audit.end_run(run_id, status="SUCCESS", counts=counts)
            logger.info("Pipeline completed successfully", counts)

            return {"run_id": run_id, "status": "SUCCESS", "counts": counts}

        except Exception as e:
            # --- 10. Failure — guaranteed audit + log ---
            counts["error_count"] = counts.get("error_count", 0) + 1
            logger.error(
                "Pipeline failed",
                context={"stage": "pipeline", "counts": counts},
                exception=e,
            )
            audit.end_run(
                run_id,
                status="FAILED",
                error_message=str(e),
                counts=counts,
            )

            return {"run_id": run_id, "status": "FAILED", "error": str(e), "counts": counts}

        finally:
            # --- Always flush logs ---
            try:
                logger.flush()
            except Exception:
                pass  # Logger flush failure should not mask the original error
