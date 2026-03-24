"""Shared ETL framework for DataStage-to-Databricks migrated pipelines.

All components importable from one package:
    from framework import PipelineRunner, DeltaManager, ConfigManager, ...

Components:
- PipelineRunner:     Base class with audit/logging/DQ baked in — jobs override transform()
- DeltaManager:       SCD 0/1/2/4 + append/truncate_load/update/upsert
- AuditLogger:        Run-level pipeline tracking (with Databricks context)
- WatermarkManager:   Incremental load state management (with job_id)
- DQValidator:        Config-driven data quality checks
- ConfigManager:      Delta table config store (SINGLE source of truth, no YAML)
- DatabricksJobLogger: Central Delta logging + record reject logging
- DatabricksJobLoggerFactory: Auto-configured logger creation
- SourceReader:       JDBC / file / Delta / test_dataframe reader
- get_databricks_context: Databricks runtime context auto-detection
"""

from framework.common_utility import (
    AuditLogger,
    DeltaManager,
    DQValidator,
    WatermarkManager,
)
from framework.pipeline_runner import PipelineRunner
from framework.readers.source_reader import SourceReader
from framework.utils.config_manager import ConfigManager
from framework.utils.databricks_context import get_databricks_context
from framework.utils.job_logger import DatabricksJobLogger, DatabricksJobLoggerFactory

__all__ = [
    "PipelineRunner",
    "DeltaManager",
    "AuditLogger",
    "WatermarkManager",
    "DQValidator",
    "ConfigManager",
    "DatabricksJobLogger",
    "DatabricksJobLoggerFactory",
    "SourceReader",
    "get_databricks_context",
]
