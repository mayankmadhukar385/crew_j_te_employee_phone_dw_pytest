"""Centralized Delta table-based logger for Databricks ETL jobs.

Replaces stdout-only logging with persistent, queryable logs in a central
Delta table. Every log entry is enriched with Databricks runtime context
(job_id, workflow_name, cluster_id, etc.) and supports config-driven
log levels per job.

Architecture:
    - One central `etl_job_log` Delta table (partitioned by log_date)
    - Per-job log level from config or control table
    - Separate methods: debug, info, warning, error, critical
    - Stack traces captured automatically for error/critical with exceptions
    - extra_context stored as JSON for flexible per-stage metadata

Usage:
    from framework.utils.job_logger import DatabricksJobLogger

    logger = DatabricksJobLogger(
        spark=spark, config=config,
        workflow_name="daily_pipeline", job_name="silver_load",
    )
    logger.info("Source read complete", {"record_count": 50000})

    try:
        risky_operation()
    except Exception as e:
        logger.error("Transform failed", {"stage": "pivot"}, exception=e)
        raise

Factory usage (recommended):
    from framework.utils.job_logger import DatabricksJobLoggerFactory

    logger = DatabricksJobLoggerFactory.create(spark, config)
"""

from __future__ import annotations

import json
import traceback
import uuid
from datetime import datetime
from typing import Any, Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T

from framework.utils.databricks_context import get_databricks_context



class DatabricksJobLogger:
    """Config-driven logger that writes to a central Delta log table.

    Each log entry includes:
    - Standard fields: timestamp, level, message
    - Job identity: job_id, workflow_name, job_name
    - Runtime context: notebook_path, cluster_id, run_id, user_name
    - Error details: exception_type, stack_trace (for ERROR/CRITICAL)
    - Flexible metadata: extra_context as JSON

    Log level is controlled per job via config. If the configured level
    is WARNING, then debug() and info() calls are silently skipped.

    Args:
        spark: Active SparkSession.
        config: Pipeline config dict.
        workflow_name: Name of the parent workflow/sequence.
        job_name: Name of this specific job.
        configured_level: Minimum log level to write (DEBUG/INFO/WARNING/ERROR/CRITICAL).
        log_table: Override the default log table name.
    """

    LEVELS = {
        "DEBUG": 10,
        "INFO": 20,
        "WARNING": 30,
        "ERROR": 40,
        "CRITICAL": 50,
    }

    SCHEMA = T.StructType([
        T.StructField("log_id", T.StringType(), False),
        T.StructField("log_timestamp", T.TimestampType(), False),
        T.StructField("log_date", T.DateType(), False),
        T.StructField("log_level", T.StringType(), False),
        T.StructField("message", T.StringType(), False),
        T.StructField("job_id", T.StringType(), True),
        T.StructField("workflow_name", T.StringType(), True),
        T.StructField("job_name", T.StringType(), True),
        T.StructField("notebook_path", T.StringType(), True),
        T.StructField("task_run_id", T.StringType(), True),
        T.StructField("run_id", T.StringType(), True),
        T.StructField("cluster_id", T.StringType(), True),
        T.StructField("user_name", T.StringType(), True),
        T.StructField("exception_type", T.StringType(), True),
        T.StructField("stack_trace", T.StringType(), True),
        T.StructField("extra_context", T.StringType(), True),
    ])

    def __init__(
        self,
        spark: SparkSession,
        config: dict[str, Any],
        workflow_name: str = "",
        job_name: str = "",
        configured_level: str = "INFO",
        log_table: Optional[str] = None,
        # Runtime context — auto-populated by factory, or pass manually
        job_id: Optional[str] = None,
        notebook_path: Optional[str] = None,
        task_run_id: Optional[str] = None,
        run_id: Optional[str] = None,
        cluster_id: Optional[str] = None,
        user_name: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.config = config

        # Job identity
        self.job_id = job_id or config.get("pipeline", {}).get("job_id", "")
        self.workflow_name = workflow_name or config.get("pipeline", {}).get("workflow_name", "")
        self.job_name = job_name or config.get("pipeline", {}).get("job_name", "")

        # Log level
        self.configured_level = (configured_level or "INFO").upper()
        if self.configured_level not in self.LEVELS:
            raise ValueError(
                f"Invalid log level '{configured_level}'. "
                f"Expected one of: {list(self.LEVELS.keys())}"
            )

        # Runtime context
        self.notebook_path = notebook_path
        self.task_run_id = task_run_id
        self.run_id = run_id
        self.cluster_id = cluster_id
        self.user_name = user_name

        # Log table
        catalog = config.get("pipeline", {}).get("catalog", "")
        schema = config.get("pipeline", {}).get("schema", "")
        self.log_table = log_table or f"{catalog}.{schema}.etl_job_log"

        # Buffer for batch writing (reduces Delta overhead for chatty jobs)
        self._buffer: list[dict] = []
        self._buffer_limit = config.get("logging", {}).get("buffer_limit", 1)

        # Track whether we've ensured the table exists
        self._table_ensured = False

    # -- public logging methods -----------------------------------------------

    def debug(self, message: str, context: Optional[dict[str, Any]] = None) -> None:
        """Log a DEBUG message. Typically suppressed in production."""
        self._write_log("DEBUG", message, context=context)

    def info(self, message: str, context: Optional[dict[str, Any]] = None) -> None:
        """Log an INFO message. Standard operational logging."""
        self._write_log("INFO", message, context=context)

    def warning(self, message: str, context: Optional[dict[str, Any]] = None) -> None:
        """Log a WARNING message. Something unexpected but non-fatal."""
        self._write_log("WARNING", message, context=context)

    def error(
        self,
        message: str,
        context: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log an ERROR message. Operation failed but pipeline may continue."""
        self._write_log("ERROR", message, context=context, exception=exception)

    def critical(
        self,
        message: str,
        context: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log a CRITICAL message. Pipeline cannot continue."""
        self._write_log("CRITICAL", message, context=context, exception=exception)

    def log_rejects(
        self,
        df,
        stage: str,
        error_type: str,
        key_columns: list[str],
        message: str = "Record rejected",
    ) -> int:
        """Log rejected/failed records from a DataFrame.

        Each rejected row becomes an ERROR-level log entry with the full
        source record serialized in extra_context. One logger, one table,
        one flush().

        Args:
            df: PySpark DataFrame containing the rejected rows.
            stage: Name of the stage that produced the rejects.
            error_type: Category (VALIDATION, TYPE_CAST, LOOKUP_MISS, DQ_FAIL, etc.).
            key_columns: Columns to use as the source key for traceability.
            message: Human-readable error description.

        Returns:
            Number of records logged.
        """
        if df.isEmpty():
            return 0

        rows = df.toJSON().collect()

        for row_json in rows:
            row_data = json.loads(row_json)
            source_key = "|".join(str(row_data.get(k, "")) for k in key_columns)

            self._write_log("ERROR", message, context={
                "error_type": error_type,
                "stage": stage,
                "source_key": source_key,
                "source_record": row_json,
                "reject": True,
            })

        count = len(rows)
        return count

    def flush(self) -> None:
        """Write all buffered log entries to the Delta table.

        Call this at the end of a pipeline run or in a finally block
        to ensure all buffered entries are persisted.

        Raises:
            Exception: If Delta write fails. Buffer is preserved for retry.
        """
        if not self._buffer:
            return

        self._ensure_table_exists()
        df = self.spark.createDataFrame(self._buffer, schema=self.SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.log_table)
        self._buffer.clear()

    # -- internal methods -----------------------------------------------------

    def _should_log(self, level: str) -> bool:
        """Check if the given level meets the configured minimum."""
        return self.LEVELS[level] >= self.LEVELS[self.configured_level]

    def _write_log(
        self,
        level: str,
        message: str,
        context: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Create a log entry and buffer or write it."""
        level = level.upper()
        if not self._should_log(level):
            return

        # Also echo to Python logger for real-time visibility in notebook output

        now = datetime.utcnow()

        # Extract exception details only for ERROR/CRITICAL
        exception_type = None
        stack_trace = None
        if exception is not None:
            exception_type = type(exception).__name__
            stack_trace = "".join(
                traceback.format_exception(type(exception), exception, exception.__traceback__)
            )

        entry = {
            "log_id": str(uuid.uuid4()),
            "log_timestamp": now,
            "log_date": now.date(),
            "log_level": level,
            "message": message,
            "job_id": self.job_id,
            "workflow_name": self.workflow_name,
            "job_name": self.job_name,
            "notebook_path": self.notebook_path,
            "task_run_id": self.task_run_id,
            "run_id": self.run_id,
            "cluster_id": self.cluster_id,
            "user_name": self.user_name,
            "exception_type": exception_type,
            "stack_trace": stack_trace,
            "extra_context": self._serialize_context(context),
        }

        self._buffer.append(entry)

        # Flush immediately for ERROR/CRITICAL or when buffer is full
        if level in ("ERROR", "CRITICAL") or len(self._buffer) >= self._buffer_limit:
            self.flush()

    def _serialize_context(self, context: Optional[dict[str, Any]]) -> Optional[str]:
        """Serialize extra_context dict to JSON string."""
        if context is None:
            return None
        try:
            return json.dumps(context, default=str)
        except Exception:
            return json.dumps({"_serialization_error": "Unable to serialize context"})

    def _ensure_table_exists(self) -> None:
        """Create the log table if it doesn't exist. Called once lazily."""
        if self._table_ensured:
            return
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.log_table} (
                    log_id STRING NOT NULL,
                    log_timestamp TIMESTAMP NOT NULL,
                    log_date DATE NOT NULL,
                    log_level STRING NOT NULL,
                    message STRING NOT NULL,
                    job_id STRING,
                    workflow_name STRING,
                    job_name STRING,
                    notebook_path STRING,
                    task_run_id STRING,
                    run_id STRING,
                    cluster_id STRING,
                    user_name STRING,
                    exception_type STRING,
                    stack_trace STRING,
                    extra_context STRING
                ) USING DELTA
                PARTITIONED BY (log_date)
            """)
            self._table_ensured = True
        except Exception:
            pass  # Table creation failed — write will fail and raise at flush()

# ---------------------------------------------------------------------------
# Config table reader — fetch log level from a control table
# ---------------------------------------------------------------------------

def get_job_log_level(
    spark: SparkSession,
    config_table: str,
    job_id: str,
    workflow_name: str,
    job_name: str,
    default_level: str = "INFO",
) -> str:
    """Read the configured log level for a job from the control table.

    Falls back to default_level if the job is not configured or the
    table doesn't exist.

    Args:
        spark: Active SparkSession.
        config_table: Fully qualified name of the logging config table.
        job_id: Job identifier.
        workflow_name: Workflow/sequence name.
        job_name: Job name.
        default_level: Fallback if no config found.

    Returns:
        Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    try:
        result = spark.sql(f"""
            SELECT log_level
            FROM {config_table}
            WHERE job_id = '{job_id}'
              AND workflow_name = '{workflow_name}'
              AND job_name = '{job_name}'
              AND is_active = 'Y'
            LIMIT 1
        """).collect()
        if result:
            return result[0]["log_level"].upper()
    except Exception:
        pass

    return default_level


# ---------------------------------------------------------------------------
# Factory — standard initialization with auto context and config lookup
# ---------------------------------------------------------------------------

class DatabricksJobLoggerFactory:
    """Factory for creating DatabricksJobLogger with auto-detected context.

    Reads log level from a control table (if configured), auto-detects
    Databricks runtime context, and creates a fully configured logger
    in one call.

    Usage:
        logger = DatabricksJobLoggerFactory.create(spark, config)
        logger.info("Pipeline started")
    """

    @staticmethod
    def create(
        spark: SparkSession,
        config: dict[str, Any],
        workflow_name: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> DatabricksJobLogger:
        """Create a fully configured DatabricksJobLogger.

        Auto-detection priority:
        1. Explicit parameters (workflow_name, job_name)
        2. Pipeline config values
        3. Databricks runtime context

        Log level priority:
        1. Control table (if logging.config_table is set in config)
        2. Config file (logging.level)
        3. Default: INFO
        """
        ctx = get_databricks_context()

        # Resolve job identity
        pipeline_cfg = config.get("pipeline", {})
        resolved_job_name = job_name or pipeline_cfg.get("job_name", ctx.get("job_name", ""))
        resolved_workflow = workflow_name or pipeline_cfg.get("workflow_name", ctx.get("workflow_name", ""))
        resolved_job_id = pipeline_cfg.get("job_id", ctx.get("job_id", ""))

        # Resolve log level
        logging_cfg = config.get("logging", {})
        config_table = logging_cfg.get("config_table")
        if config_table:
            configured_level = get_job_log_level(
                spark, config_table,
                job_id=resolved_job_id,
                workflow_name=resolved_workflow,
                job_name=resolved_job_name,
            )
        else:
            configured_level = logging_cfg.get("level", "INFO")

        return DatabricksJobLogger(
            spark=spark,
            config=config,
            workflow_name=resolved_workflow,
            job_name=resolved_job_name,
            configured_level=configured_level,
            job_id=resolved_job_id,
            notebook_path=ctx.get("notebook_path"),
            task_run_id=ctx.get("task_run_id"),
            run_id=ctx.get("run_id"),
            cluster_id=ctx.get("cluster_id"),
            user_name=ctx.get("user_name"),
        )
