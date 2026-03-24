"""Generic source reader for all connector types.

Supports JDBC (Teradata, DB2, Oracle, ODBC), file-based (Auto Loader),
and Delta table sources. The source type is determined by config.

Usage:
    reader = SourceReader(spark, config)
    df = reader.read()                              # full load
    df = reader.read(watermark_value="2024-01-01")  # incremental
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class SourceReader:
    """Config-driven source reader supporting JDBC, file, and Delta sources.

    Args:
        spark: Active SparkSession.
        config: Pipeline config dict with source section.
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        self.spark = spark
        self.config = config
        self.source_config = config["source"]

    def read(self, watermark_value: Optional[str] = None) -> DataFrame:
        """Read from the configured source.

        Args:
            watermark_value: If provided, filters source for incremental load.

        Returns:
            Source DataFrame.
        """
        source_type = self.source_config["type"]

        reader_map = {
            "jdbc": self._read_jdbc,
            "file": self._read_file,
            "delta": self._read_delta,
            "test_dataframe": self._read_test_dataframe,
        }

        reader_fn = reader_map.get(source_type)
        if reader_fn is None:
            raise ValueError(
                f"Unsupported source type: {source_type}. "
                f"Supported: jdbc, file, delta, test_dataframe"
            )

        df = reader_fn(watermark_value)
        logger.info(f"Source read complete: type={source_type}")
        return df

    def _read_jdbc(self, watermark_value: Optional[str] = None) -> DataFrame:
        """Read from a JDBC source (Teradata, DB2, Oracle, ODBC)."""
        jdbc_options = dict(self.source_config["jdbc_options"])

        if watermark_value and "watermark_column" in self.source_config:
            wm_col = self.source_config["watermark_column"]
            dbtable = jdbc_options.get("dbtable", self.source_config["table"])

            if dbtable.strip().upper().startswith("SELECT"):
                jdbc_options["dbtable"] = (
                    f"({dbtable} WHERE {wm_col} > '{watermark_value}') AS incremental_src"
                )
            else:
                jdbc_options["dbtable"] = (
                    f"(SELECT * FROM {dbtable} WHERE {wm_col} > '{watermark_value}') AS incremental_src"
                )
            logger.info(f"Incremental read: {wm_col} > {watermark_value}")

        return self.spark.read.format("jdbc").options(**jdbc_options).load()

    def _read_file(self, watermark_value: Optional[str] = None) -> DataFrame:
        """Read from file source using Auto Loader."""
        file_config = self.source_config
        file_format = file_config.get("file_format", "csv")
        path = file_config["path"]

        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", file_config.get("schema_location", f"{path}/_schema"))
        )

        for key, value in file_config.get("file_options", {}).items():
            reader = reader.option(key, value)

        return reader.load(path)

    def _read_delta(self, watermark_value: Optional[str] = None) -> DataFrame:
        """Read from a Delta table."""
        catalog = self.config["pipeline"]["catalog"]
        schema = self.config["pipeline"]["schema"]
        table = self.source_config["table"]
        full_table = f"{catalog}.{schema}.{table}"

        df = self.spark.read.table(full_table)

        if watermark_value and "watermark_column" in self.source_config:
            from pyspark.sql import functions as F

            wm_col = self.source_config["watermark_column"]
            df = df.filter(F.col(wm_col) > watermark_value)
            logger.info(f"Incremental read: {wm_col} > {watermark_value}")

        return df

    def _read_test_dataframe(self, watermark_value: Optional[str] = None) -> DataFrame:
        """Read from a pre-registered temp view for local testing.

        Used in VS Code where no JDBC driver or external database is available.
        Tests register synthetic data as a temp view and set
        source.type = "test_dataframe" in the config.

        Config expects:
            source.test_view_name: name of the temp view (default: "test_source_data")
        """
        view_name = self.source_config.get("test_view_name", "test_source_data")
        df = self.spark.table(view_name)

        if watermark_value and "watermark_column" in self.source_config:
            from pyspark.sql import functions as F

            wm_col = self.source_config["watermark_column"]
            df = df.filter(F.col(wm_col) > watermark_value)

        return df
