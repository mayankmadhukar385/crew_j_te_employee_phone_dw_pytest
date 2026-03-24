"""Delta Lake writer wrapping DeltaManager for pipeline use.

Provides a simplified interface for writing DataFrames to Delta tables
with SCD support, logging, and error handling.

Usage:
    writer = DeltaWriter(spark, config)
    writer.write_target(df)
    writer.write_error(error_df)
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from framework.common_utility import DeltaManager

logger = logging.getLogger(__name__)


class DeltaWriter:
    """Pipeline-level Delta writer with target and error table support.

    Args:
        spark: Active SparkSession.
        config: Pipeline config dict with target and error sections.
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        self.spark = spark
        self.config = config
        self.manager = DeltaManager(spark, config)

    def write_target(
        self,
        df: DataFrame,
        target_table: Optional[str] = None,
        scd_type: Optional[int] = None,
    ) -> dict[str, int]:
        """Write to the main target table using configured SCD strategy.

        Args:
            df: DataFrame to write.
            target_table: Override target table name.
            scd_type: Override SCD type.

        Returns:
            Dict with row count information.
        """
        return self.manager.write(df, target_table=target_table, scd_type=scd_type)

    def write_error(self, df: DataFrame) -> None:
        """Write rejected records to the error target table.

        Uses simple append mode — error tables are always insert-only.
        """
        error_config = self.config.get("error", {})
        if not error_config.get("enabled", False):
            logger.warning("Error writing disabled in config, skipping")
            return

        error_table = error_config.get("table")
        if not error_table:
            logger.warning("No error table configured, skipping error write")
            return

        catalog = self.config["pipeline"]["catalog"]
        schema = self.config["pipeline"]["schema"]
        full_table = f"{catalog}.{schema}.{error_table}"

        from pyspark.sql import functions as F

        df_with_ts = df.withColumn("_error_ts", F.current_timestamp())
        df_with_ts.write.format("delta").mode("append").saveAsTable(full_table)
        logger.info(f"Error records written to {full_table}: {df.count()} rows")
