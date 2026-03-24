# Databricks notebook source
# MAGIC %pip install pyyaml chispa delta-spark

# COMMAND ----------

import sys
import os

# Ensure the project root is on the Python path so framework and jobs packages are importable
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

# Widget definitions — override at runtime via Databricks job parameters or notebook UI
dbutils.widgets.text("job_name", "CREW_J_TE_EMPLOYEE_PHONE_DW", "Job Name")
dbutils.widgets.dropdown("run_mode", "full_refresh", ["full_refresh", "incremental"], "Run Mode")

# COMMAND ----------

from jobs.crew_j_te_employee_phone_dw.run_pipeline import CrewEmployeePhoneDwPipeline

job_name = dbutils.widgets.get("job_name")
run_mode = dbutils.widgets.get("run_mode")

pipeline = CrewEmployeePhoneDwPipeline(spark, job_name=job_name)
result = pipeline.run()

print(result)
