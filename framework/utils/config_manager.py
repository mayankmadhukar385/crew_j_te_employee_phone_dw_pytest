"""Delta table-based configuration manager for ETL job configs.

The config table is the SINGLE SOURCE OF TRUTH for all job configurations.
No YAML files. No dual maintenance. Config changes are SQL UPDATE statements.

Step 3 generates a SQL INSERT statement per job. This INSERT is executed
once during first deployment. After that, config lives exclusively in Delta.

Usage:
    from framework import ConfigManager

    config_mgr = ConfigManager(spark, catalog="prod", schema="etl")
    config = config_mgr.get_config("Jb_LMIS_Full_Ld")

    # Generate deployment SQL for a new job
    sql = ConfigManager.generate_insert_sql(config_dict, "Jb_LMIS_Full_Ld",
                                             catalog="prod", schema="etl")
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages job configurations in a central Delta table.

    Single source of truth. No YAML fallback.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.
        config_table_name: Override default table name.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        config_table_name: str = "etl_job_config",
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.config_table = f"{catalog}.{schema}.{config_table_name}"
        self._table_ensured = False

    def get_config(self, job_name: str) -> dict[str, Any]:
        """Get the active config for a job.

        Raises:
            ValueError: If no active config found.
        """
        config = self._read_from_table(job_name)
        if config is not None:
            return config

        raise ValueError(
            f"No active config for job '{job_name}' in {self.config_table}. "
            f"Run the deployment INSERT statement first."
        )

    def save_config(
        self,
        job_name: str,
        config: dict[str, Any],
        description: str = "",
        created_by: str = "system",
    ) -> int:
        """Save a new config version. Deactivates the previous version."""
        self._ensure_table_exists()
        next_version = self._get_next_version(job_name)
        self._deactivate_current(job_name)

        config_json = json.dumps(config, default=str)
        row = self.spark.createDataFrame([{
            "job_name": job_name,
            "config_json": config_json,
            "version": next_version,
            "is_active": "Y",
            "created_by": created_by,
            "created_ts": datetime.utcnow(),
            "description": description,
        }])
        row.write.format("delta").mode("append").saveAsTable(self.config_table)
        logger.info(f"Config saved: job={job_name}, version={next_version}")
        return next_version

    def update_config_key(
        self,
        job_name: str,
        key_path: str,
        value: Any,
        description: str = "",
        created_by: str = "system",
    ) -> int:
        """Update a single key using dot notation (e.g., 'target.scd_type')."""
        current = self._read_from_table(job_name)
        if current is None:
            raise ValueError(f"No active config for job '{job_name}' to update.")

        keys = key_path.split(".")
        target = current
        for key in keys[:-1]:
            if key not in target:
                target[key] = {}
            target = target[key]
        target[keys[-1]] = value

        if not description:
            description = f"Updated {key_path} = {value}"

        return self.save_config(job_name, current, description=description, created_by=created_by)

    def get_config_history(self, job_name: str) -> list[dict[str, Any]]:
        """Get all config versions for a job, newest first."""
        try:
            rows = self.spark.sql(f"""
                SELECT version, is_active, created_by, created_ts, description
                FROM {self.config_table}
                WHERE job_name = '{job_name}'
                ORDER BY version DESC
            """).collect()
            return [row.asDict() for row in rows]
        except Exception:
            return []

    def rollback_config(self, job_name: str, to_version: int, created_by: str = "system") -> int:
        """Rollback to a previous config version."""
        try:
            row = self.spark.sql(f"""
                SELECT config_json FROM {self.config_table}
                WHERE job_name = '{job_name}' AND version = {to_version}
                LIMIT 1
            """).collect()
            if not row:
                raise ValueError(f"Version {to_version} not found for job '{job_name}'")
            config = json.loads(row[0]["config_json"])
            return self.save_config(job_name, config,
                                    description=f"Rollback to version {to_version}",
                                    created_by=created_by)
        except json.JSONDecodeError as e:
            raise ValueError(f"Corrupt config at version {to_version}: {e}") from e

    # -- static: deployment SQL generators ------------------------------------

    @staticmethod
    def generate_insert_sql(
        config: dict[str, Any],
        job_name: str,
        catalog: str,
        schema: str,
        config_table_name: str = "etl_job_config",
        created_by: str = "deployment",
        description: str = "Initial deployment",
    ) -> str:
        """Generate SQL INSERT for first-time job deployment.

        Step 3 calls this to produce a SQL file. Run ONCE when deploying a new job.
        """
        full_table = f"{catalog}.{schema}.{config_table_name}"
        config_json = json.dumps(config, default=str).replace("'", "\\'")

        return f"""-- Config deployment for job: {job_name}
-- Run this ONCE during first deployment.
-- After that, use SQL UPDATE or ConfigManager.update_config_key().

INSERT INTO {full_table}
(job_name, config_json, version, is_active, created_by, created_ts, description)
VALUES (
    '{job_name}',
    '{config_json}',
    1,
    'Y',
    '{created_by}',
    current_timestamp(),
    '{description}'
);"""

    @staticmethod
    def generate_table_ddl(
        catalog: str,
        schema: str,
        config_table_name: str = "etl_job_config",
    ) -> str:
        """Generate DDL to create the config table. Run once per environment."""
        full_table = f"{catalog}.{schema}.{config_table_name}"
        return f"""CREATE TABLE IF NOT EXISTS {full_table} (
    job_name STRING NOT NULL,
    config_json STRING NOT NULL,
    version INT NOT NULL,
    is_active STRING NOT NULL,
    created_by STRING,
    created_ts TIMESTAMP,
    description STRING
) USING DELTA;"""

    # -- internal -------------------------------------------------------------

    def _read_from_table(self, job_name: str) -> Optional[dict[str, Any]]:
        try:
            rows = self.spark.sql(f"""
                SELECT config_json FROM {self.config_table}
                WHERE job_name = '{job_name}' AND is_active = 'Y'
                ORDER BY version DESC LIMIT 1
            """).collect()
            if rows:
                return json.loads(rows[0]["config_json"])
        except Exception as e:
            logger.debug(f"Config table read failed: {e}")
        return None

    def _get_next_version(self, job_name: str) -> int:
        try:
            rows = self.spark.sql(f"""
                SELECT COALESCE(MAX(version), 0) AS max_ver
                FROM {self.config_table} WHERE job_name = '{job_name}'
            """).collect()
            return rows[0]["max_ver"] + 1
        except Exception:
            return 1

    def _deactivate_current(self, job_name: str) -> None:
        try:
            self.spark.sql(f"""
                UPDATE {self.config_table}
                SET is_active = 'N'
                WHERE job_name = '{job_name}' AND is_active = 'Y'
            """)
        except Exception:
            pass

    def _ensure_table_exists(self) -> None:
        if self._table_ensured:
            return
        try:
            self.spark.sql(self.generate_table_ddl(self.catalog, self.schema))
            self._table_ensured = True
        except Exception as e:
            logger.warning(f"Could not ensure config table exists: {e}")
