"""Tests for DeltaManager, AuditLogger, and WatermarkManager from framework/common_utility.py.

These are integration-style tests that write to Delta tables in the local
warehouse directory configured by the session-scoped SparkSession fixture in
tests/conftest.py.

Each test class uses a uniquely-named table to avoid cross-test contamination.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from framework.common_utility import AuditLogger, DeltaManager, WatermarkManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_table(base: str) -> str:
    """Return a table name with a short UUID suffix for test isolation.

    Args:
        base: Base table name (e.g. "test_truncate").

    Returns:
        Table name like "test_truncate_a1b2c3d4".
    """
    suffix = uuid.uuid4().hex[:8]
    return f"{base}_{suffix}"


def _override_target(config: dict[str, Any], table: str) -> dict[str, Any]:
    """Return a copy of config with target.table overridden for test isolation.

    Args:
        config: Base framework_test_config dict.
        table:  Unique table name for this test.

    Returns:
        New dict (shallow copy with target section replaced).
    """
    import copy
    cfg = copy.deepcopy(config)
    cfg["target"]["table"] = table
    return cfg


# ---------------------------------------------------------------------------
# TestDeltaManagerTruncateLoad
# ---------------------------------------------------------------------------

class TestDeltaManagerTruncateLoad:
    """Tests for DeltaManager with write_mode='truncate_load'.

    truncate_load performs a full overwrite (DROP + reload) which is the
    write strategy used by the CREW_J_TE_EMPLOYEE_PHONE_DW pipeline for
    the main target table.
    """

    _SCHEMA = StructType([
        StructField("id",   IntegerType(), False),
        StructField("name", StringType(),  True),
    ])

    def test_truncate_load_creates_table_when_not_exists(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """DeltaManager.write() with truncate_load creates a new Delta table.

        When the target table does not yet exist, _truncate_and_load should
        write the DataFrame as a new Delta table in overwrite mode.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        table = _unique_table("test_trunc_create")
        cfg = _override_target(framework_test_config, table)

        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema=self._SCHEMA)
        mgr = DeltaManager(spark, cfg)
        result = mgr.write(df, write_mode="truncate_load")

        assert result["inserted"] == 2
        loaded = spark.table(f"spark_catalog.default.{table}")
        assert loaded.count() == 2

    def test_truncate_load_replaces_existing_rows(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """truncate_load overwrites all existing rows with new data.

        After an initial write, a second truncate_load should replace the
        existing 3-row table with a new 2-row table — old rows are gone.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        table = _unique_table("test_trunc_replace")
        cfg = _override_target(framework_test_config, table)
        full_table = f"spark_catalog.default.{table}"

        initial_df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Carol")],
            schema=self._SCHEMA,
        )
        replacement_df = spark.createDataFrame(
            [(10, "Dave"), (20, "Eve")],
            schema=self._SCHEMA,
        )

        mgr = DeltaManager(spark, cfg)
        mgr.write(initial_df, write_mode="truncate_load")
        assert spark.table(full_table).count() == 3

        result = mgr.write(replacement_df, write_mode="truncate_load")
        reloaded = spark.table(full_table).collect()

        assert result["inserted"] == 2
        ids = {row["id"] for row in reloaded}
        assert ids == {10, 20}          # old rows (1,2,3) should be gone
        assert 1 not in ids

    def test_truncate_load_returns_correct_count(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """DeltaManager.write() returns source_rows equal to DataFrame.count().

        The returned dict must include 'inserted' equal to the number of rows
        in the input DataFrame.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        table = _unique_table("test_trunc_count")
        cfg = _override_target(framework_test_config, table)

        rows = [(i, f"user_{i}") for i in range(1, 11)]   # 10 rows
        df = spark.createDataFrame(rows, schema=self._SCHEMA)

        mgr = DeltaManager(spark, cfg)
        result = mgr.write(df, write_mode="truncate_load")

        assert result["inserted"] == 10
        assert result["source_rows"] == 10

    def test_append_mode_accumulates_rows(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """DeltaManager append mode adds rows to existing table without overwriting.

        Two sequential append writes of 2 rows each should produce a table with
        4 total rows.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        table = _unique_table("test_append_accum")
        cfg = _override_target(framework_test_config, table)
        full_table = f"spark_catalog.default.{table}"

        batch1 = spark.createDataFrame([(1, "A"), (2, "B")], schema=self._SCHEMA)
        batch2 = spark.createDataFrame([(3, "C"), (4, "D")], schema=self._SCHEMA)

        mgr = DeltaManager(spark, cfg)
        mgr.write(batch1, write_mode="append")
        mgr.write(batch2, write_mode="append")

        assert spark.table(full_table).count() == 4


# ---------------------------------------------------------------------------
# TestAuditLogger
# ---------------------------------------------------------------------------

class TestAuditLogger:
    """Tests for AuditLogger start_run / end_run lifecycle.

    AuditLogger creates one row per pipeline run in etl_audit_log.
    start_run inserts a RUNNING record; end_run updates it to SUCCESS or FAILED.
    """

    def _make_audit_config(
        self,
        framework_test_config: dict[str, Any],
        audit_table_suffix: str,
    ) -> dict[str, Any]:
        """Return a config with a unique audit table name.

        AuditLogger derives the table from catalog.schema.etl_audit_log.
        We patch the schema to isolate each test's table.

        Args:
            framework_test_config: Base config fixture.
            audit_table_suffix: Unique suffix appended to schema name.

        Returns:
            Config dict with a unique schema for test isolation.
        """
        import copy
        cfg = copy.deepcopy(framework_test_config)
        # Use default catalog; change schema suffix for table isolation
        cfg["pipeline"]["schema"] = f"default"
        cfg["pipeline"]["job_name"] = f"TEST_AUDIT_{audit_table_suffix.upper()}"
        return cfg

    def test_start_run_inserts_running_record(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """start_run inserts a row with status='RUNNING' into etl_audit_log.

        After start_run the audit table should contain exactly one row for
        the returned run_id with status RUNNING.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        suffix = uuid.uuid4().hex[:6]
        cfg = self._make_audit_config(framework_test_config, suffix)

        audit = AuditLogger(spark, cfg)
        run_id = audit.start_run(parameters={"mode": "full_refresh"})

        rows = spark.sql(
            f"SELECT status FROM spark_catalog.default.etl_audit_log "
            f"WHERE run_id = '{run_id}'"
        ).collect()

        assert len(rows) == 1
        assert rows[0]["status"] == "RUNNING"

    def test_end_run_updates_status_to_success(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """end_run updates the audit record status to SUCCESS.

        After end_run(status='SUCCESS'), the row for that run_id should have
        status=SUCCESS and a non-null end_ts.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        suffix = uuid.uuid4().hex[:6]
        cfg = self._make_audit_config(framework_test_config, suffix)

        audit = AuditLogger(spark, cfg)
        run_id = audit.start_run()
        audit.end_run(
            run_id,
            status="SUCCESS",
            counts={"source_count": 100, "target_count": 95, "error_count": 5},
        )

        rows = spark.sql(
            f"SELECT status, end_ts, source_count, error_count "
            f"FROM spark_catalog.default.etl_audit_log "
            f"WHERE run_id = '{run_id}'"
        ).collect()

        assert len(rows) == 1
        assert rows[0]["status"] == "SUCCESS"
        assert rows[0]["end_ts"] is not None
        assert rows[0]["source_count"] == 100
        assert rows[0]["error_count"] == 5

    def test_end_run_records_failed_status(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """end_run records FAILED status with error message when pipeline fails.

        Simulates the audit pattern used in run_pipeline.py exception handlers:
            audit.end_run(run_id, status='FAILED', error_message=str(e))

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        suffix = uuid.uuid4().hex[:6]
        cfg = self._make_audit_config(framework_test_config, suffix)

        audit = AuditLogger(spark, cfg)
        run_id = audit.start_run()
        audit.end_run(
            run_id,
            status="FAILED",
            error_message="Simulated pipeline failure in test",
        )

        rows = spark.sql(
            f"SELECT status, error_message "
            f"FROM spark_catalog.default.etl_audit_log "
            f"WHERE run_id = '{run_id}'"
        ).collect()

        assert rows[0]["status"] == "FAILED"
        assert "Simulated" in rows[0]["error_message"]

    def test_multiple_runs_create_separate_records(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """Two pipeline runs create two distinct rows in etl_audit_log.

        Each run_id must be unique, so the audit table should accumulate
        one row per start_run call regardless of job_name.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        suffix = uuid.uuid4().hex[:6]
        cfg = self._make_audit_config(framework_test_config, suffix)

        audit = AuditLogger(spark, cfg)
        run_id_1 = audit.start_run()
        run_id_2 = audit.start_run()

        assert run_id_1 != run_id_2

        count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM spark_catalog.default.etl_audit_log "
            f"WHERE run_id IN ('{run_id_1}', '{run_id_2}')"
        ).collect()[0]["cnt"]

        assert count == 2


# ---------------------------------------------------------------------------
# TestWatermarkManager
# ---------------------------------------------------------------------------

class TestWatermarkManager:
    """Tests for WatermarkManager get/update watermark lifecycle.

    WatermarkManager persists the last successfully processed value to
    etl_watermark, enabling incremental loads to resume from where they left off.
    """

    def _make_wm_config(
        self,
        framework_test_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Return a config pointing to the shared watermark table.

        Args:
            framework_test_config: Base config fixture.

        Returns:
            Config dict suitable for WatermarkManager instantiation.
        """
        import copy
        cfg = copy.deepcopy(framework_test_config)
        cfg["pipeline"]["job_name"] = f"TEST_WM_{uuid.uuid4().hex[:6].upper()}"
        return cfg

    def test_get_watermark_returns_none_for_new_job(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """get_watermark returns None when no prior watermark exists for the job.

        On first run, there is no stored watermark so the pipeline reads from
        the beginning. WatermarkManager must return None, not raise an exception.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        cfg = self._make_wm_config(framework_test_config)
        wm = WatermarkManager(spark, cfg)

        result = wm.get_watermark("CREW_WSTELE_LND", "LAST_UPDATED_TS")
        assert result is None

    def test_update_then_get_returns_stored_value(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """update_watermark persists a value that get_watermark retrieves correctly.

        After storing "2025-12-31 18:00:00" the same value must be returned
        by a subsequent get_watermark call.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        cfg = self._make_wm_config(framework_test_config)
        wm = WatermarkManager(spark, cfg)

        source = "CREW_WSTELE_LND"
        col = "TELE_LAST_UPDATED_DATE"
        new_val = "2025-12-31 18:00:00"

        wm.update_watermark(source, col, new_val)
        retrieved = wm.get_watermark(source, col)

        assert retrieved == new_val

    def test_update_watermark_overwrites_previous_value(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """A second update_watermark call replaces the first stored value.

        Watermark should always reflect the most recent successful run, not
        accumulate multiple rows per source/column combination.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        cfg = self._make_wm_config(framework_test_config)
        wm = WatermarkManager(spark, cfg)

        source = "CREW_WSTELE_LND"
        col = "TELE_LAST_UPDATED_DATE"

        wm.update_watermark(source, col, "2025-01-01 00:00:00")
        wm.update_watermark(source, col, "2025-06-15 12:30:00")  # override

        retrieved = wm.get_watermark(source, col)
        assert retrieved == "2025-06-15 12:30:00"

        # Confirm only one row per job+source+column combination
        job_name = cfg["pipeline"]["job_name"]
        count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM spark_catalog.default.etl_watermark "
            f"WHERE job_name = '{job_name}' "
            f"AND source_table = '{source}' "
            f"AND watermark_column = '{col}'"
        ).collect()[0]["cnt"]

        assert count == 1

    def test_watermarks_are_isolated_by_job_name(
        self,
        spark: SparkSession,
        framework_test_config: dict[str, Any],
    ) -> None:
        """Two different job names store independent watermarks.

        Each job's watermark must not bleed into another job's watermark,
        even when using the same source table and column name.

        Args:
            spark: Session fixture from conftest.py.
            framework_test_config: Config fixture from conftest.py.
        """
        import copy

        cfg_a = self._make_wm_config(framework_test_config)
        cfg_b = copy.deepcopy(cfg_a)
        cfg_b["pipeline"]["job_name"] = f"TEST_WM_B_{uuid.uuid4().hex[:6].upper()}"

        wm_a = WatermarkManager(spark, cfg_a)
        wm_b = WatermarkManager(spark, cfg_b)

        source = "SHARED_SOURCE"
        col = "UPDATED_TS"

        wm_a.update_watermark(source, col, "2025-01-01")
        wm_b.update_watermark(source, col, "2025-06-01")

        assert wm_a.get_watermark(source, col) == "2025-01-01"
        assert wm_b.get_watermark(source, col) == "2025-06-01"
