"""Databricks runtime context auto-detection.

Extracts job metadata (job_id, workflow_name, run_id, cluster_id, etc.)
from the Databricks notebook context when running on a cluster. Returns
empty dict gracefully when running locally or in tests.

Shared by DatabricksJobLogger and AuditLogger so both get identical
context enrichment without duplicating the detection logic.

Usage:
    from framework.utils.databricks_context import get_databricks_context
    ctx = get_databricks_context()
    # ctx = {"job_id": "1001", "run_id": "abc", "cluster_id": "xyz", ...}
"""

from __future__ import annotations

from typing import Any, Optional


def get_databricks_context() -> dict[str, Any]:
    """Extract runtime context from the Databricks notebook environment.

    Reads tags from the notebook context object exposed by dbutils.
    Each field is read defensively — if any tag is unavailable (e.g.,
    running locally, interactive notebook, or older DBR version),
    that field is simply omitted from the result.

    Returns:
        Dict with available context keys. May include:
        - job_id, run_id, task_run_id, job_name
        - notebook_path, cluster_id, user_name
        - workflow_name (derived from job_name if available)
    """
    context: dict[str, Any] = {}

    try:
        # dbutils is injected by Databricks runtime — not importable
        dbutils_obj = globals().get("dbutils")
        if dbutils_obj is None:
            # Try the common fallback for Databricks notebooks
            import builtins

            dbutils_obj = getattr(builtins, "dbutils", None)

        if dbutils_obj is None:
            return context

        notebook_context = (
            dbutils_obj.notebook.entry_point
            .getDbutils().notebook().getContext()
        )

        def _safe_get(tag_name: str) -> Optional[str]:
            """Read a tag from notebook context, returning None on failure."""
            try:
                return notebook_context.tags().get(tag_name).get()
            except Exception:
                return None

        def _safe_option(accessor) -> Optional[str]:
            """Read a Scala Option, returning None if empty."""
            try:
                return accessor.get() if accessor.isDefined() else None
            except Exception:
                return None

        context["job_id"] = _safe_get("jobId")
        context["run_id"] = _safe_get("runId")
        context["task_run_id"] = _safe_get("multitaskParentRunId")
        context["job_name"] = _safe_get("jobName")
        context["notebook_path"] = _safe_option(notebook_context.notebookPath())
        context["cluster_id"] = _safe_get("clusterId")
        context["user_name"] = _safe_get("user")

        # Derive workflow_name from job_name if not separately available
        if context.get("job_name") and "workflow_name" not in context:
            context["workflow_name"] = context["job_name"]

        # Remove None values for cleaner dict
        context = {k: v for k, v in context.items() if v is not None}

    except Exception:
        # Running outside Databricks (local dev, pytest) — return empty
        pass

    return context
