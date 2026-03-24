"""Pipeline runner for CREW_J_TE_EMPLOYEE_PHONE_DW.

Extends PipelineRunner. Only implements transform() — everything else
(audit, DQ, logging, write) is handled by the base class.

Migrated from IBM DataStage: CREW_J_TE_EMPLOYEE_PHONE_DW.
Source: CREW_WSTELE_LND (Teradata).
Target: TE_EMPLOYEE_PHONE (via work table TE_EMPLOYEE_PHONE_NEW).
Error: TE_EMPLOYEE_PHONE_ERR (invalid EMP_NBR records).
"""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as F  # noqa: F401  (available for ad-hoc use in subclasses)
from pyspark.sql import DataFrame, SparkSession

from framework.pipeline_runner import PipelineRunner
from jobs.crew_j_te_employee_phone_dw.transformations import (
    add_away_seq_offset,
    add_home_seq_offset,
    adjust_priority,
    aggregate_max_priority,
    calculate_new_priority,
    enrich_basic_records,
    funnel_sequences,
    increment_priority,
    join_home_records,
    join_max_priority,
    merge_all_records,
    pivot_away_sequence,
    pivot_basic_phones,
    pivot_home_sequence,
    route_by_phone_type,
    split_basic_by_home_away,
    validate_and_finalize,
)


class CrewEmployeePhoneDwPipeline(PipelineRunner):
    """Pipeline migrated from DataStage: CREW_J_TE_EMPLOYEE_PHONE_DW.

    Source: CREW_WSTELE_LND (Teradata JDBC).
    Main target: TE_EMPLOYEE_PHONE_NEW (truncate_load) → promoted to TE_EMPLOYEE_PHONE via After-SQL.
    Error target: TE_EMPLOYEE_PHONE_ERR (append) — records with invalid EMP_NBR.

    Because this pipeline has two output targets, transform() returns a dict keyed
    by "main" and "error".  PipelineRunner.run() handles routing each key to its
    configured target write.
    """

    def transform(self, source_df: DataFrame, config: dict[str, Any]) -> dict[str, DataFrame]:
        """Apply full CREW_J_TE_EMPLOYEE_PHONE_DW transformation chain.

        Implements the complete DataStage pipeline graph:
          Step 1  — SEPARATE_PHTYPE_TFM: 6-way route (emergency/temp/basic/home/away/error)
          Step 2A — BASIC branch: pivot → enrich → 5-way split
          Step 2B — HOME branch: 3x pivot → offset TFMs → funnel → increment → join → new priority
          Step 2C — AWAY branch: mirror of HOME branch
          Step 3A — HOME max priority: aggregate → join basic → adjust CALL_PRTY
          Step 3B — AWAY max priority: aggregate → join basic → adjust CALL_PRTY
          Step 4  — ALL_REC_FNL: 7-stream union
          Step 5  — NULL_VAL_TFM: filter + finalize → TE_EMPLOYEE_PHONE target schema

        Args:
            source_df: Source DataFrame from CREW_WSTELE_LND (post-DQ checks).
            config: Full pipeline config dict loaded from etl_job_config Delta table.

        Returns:
            Dict with keys:
                "main"  — final DataFrame matching TE_EMPLOYEE_PHONE target schema (14 columns).
                "error" — pass-through DataFrame of invalid-EMP_NBR records for error table.
        """
        # ------------------------------------------------------------------
        # Step 1: Route by phone type (SEPARATE_PHTYPE_TFM — 6-way split)
        # ------------------------------------------------------------------
        emergency_df, temp_df, basic_raw_df, home_raw_df, away_raw_df, error_df = route_by_phone_type(
            source_df, config
        )

        # ------------------------------------------------------------------
        # Step 2A: BASIC phone pipeline
        #   BASIC_TYPE_PVT → BASIC_REC_TFM → 5-way split
        # ------------------------------------------------------------------
        basic_pivoted_df = pivot_basic_phones(basic_raw_df, config)
        basic_enriched_df = enrich_basic_records(basic_pivoted_df, config)
        all_basic_df, basic_home_jnr_df, basic_home_max_df, basic_away_jnr_df, basic_away_max_df = (
            split_basic_by_home_away(basic_enriched_df, config)
        )

        # ------------------------------------------------------------------
        # Step 2B: HOME phone pipeline
        #   HOME_CPY → HOME_SEQ1/2/3_PVT → HOME_SEQ2/3_TFM → HOME_SEQ_FNL
        #   → INC_PRTY_TFM → HOME_BASIC_JNR → NEW_PRTY_TFM
        # ------------------------------------------------------------------
        home_seq1_df = pivot_home_sequence(home_raw_df, seq_num=1, config=config)
        home_seq2_df = add_home_seq_offset(
            pivot_home_sequence(home_raw_df, seq_num=2, config=config), offset=5, config=config
        )
        home_seq3_df = add_home_seq_offset(
            pivot_home_sequence(home_raw_df, seq_num=3, config=config), offset=10, config=config
        )
        home_seq_df = funnel_sequences(home_seq1_df, home_seq2_df, home_seq3_df, config)

        # INC_PRTY_TFM: filter valid EMP_NBR + LKP_CALL_PRTY, increment CALL_PRTY, set CALL_LIST
        home_inc_df = increment_priority(home_seq_df, config).withColumn("CALL_LIST", F.lit("HOME"))

        # HOME_BASIC_JNR → NEW_PRTY_TFM
        home_joined_df = join_home_records(home_inc_df, basic_home_jnr_df, config)
        home_priority_df, home_cprty_df = calculate_new_priority(home_joined_df, config)

        # ------------------------------------------------------------------
        # Step 2C: AWAY phone pipeline (mirror of HOME)
        #   AWAY_CPY → AWAY_SEQ1/2/3_PVT → AWAY_SEQ2/3_TFM → AWAY_SEQ_FNL
        #   → INC_AWAY_PRTY_TFM → AWAY_BASIC_JNR → ABREC_NEW_PRTY_TFM
        # ------------------------------------------------------------------
        away_seq1_df = pivot_away_sequence(away_raw_df, seq_num=1, config=config)
        away_seq2_df = add_away_seq_offset(
            pivot_away_sequence(away_raw_df, seq_num=2, config=config), offset=5, config=config
        )
        away_seq3_df = add_away_seq_offset(
            pivot_away_sequence(away_raw_df, seq_num=3, config=config), offset=10, config=config
        )
        away_seq_df = funnel_sequences(away_seq1_df, away_seq2_df, away_seq3_df, config)

        # INC_AWAY_PRTY_TFM: same filter + increment + CALL_LIST='AWAY'
        away_inc_df = increment_priority(away_seq_df, config).withColumn("CALL_LIST", F.lit("AWAY"))

        # AWAY_BASIC_JNR → ABREC_NEW_PRTY_TFM
        away_joined_df = join_home_records(away_inc_df, basic_away_jnr_df, config)
        away_priority_df, away_cprty_df = calculate_new_priority(away_joined_df, config)

        # ------------------------------------------------------------------
        # Step 3A: HOME max priority chain
        #   MAX_PRTY_AGG → BASIC_MAX_JNR → ADJUSTING_PRTY_TFM
        # ------------------------------------------------------------------
        home_max_df = aggregate_max_priority(home_cprty_df, config)
        home_max_joined_df = join_max_priority(basic_home_max_df, home_max_df, config)
        adj_home_df = adjust_priority(home_max_joined_df, config).withColumn("CALL_LIST", F.lit("HOME"))

        # ------------------------------------------------------------------
        # Step 3B: AWAY max priority chain
        #   MAX_ABPRTY_AGG → AWAY_MAX_JNR → ADJUST_PRTY_TFM
        # ------------------------------------------------------------------
        away_max_df = aggregate_max_priority(away_cprty_df, config)
        away_max_joined_df = join_max_priority(basic_away_max_df, away_max_df, config)
        adj_away_df = adjust_priority(away_max_joined_df, config).withColumn("CALL_LIST", F.lit("AWAY"))

        # ------------------------------------------------------------------
        # Step 4: Merge all streams (ALL_REC_FNL — 7 inputs)
        # ------------------------------------------------------------------
        all_records_df = merge_all_records(
            emergency_df,
            temp_df,
            all_basic_df,
            home_priority_df,
            away_priority_df,
            adj_home_df,
            adj_away_df,
            config=config,
        )

        # ------------------------------------------------------------------
        # Step 5: Final validation + null handling (NULL_VAL_TFM)
        # ------------------------------------------------------------------
        final_df = validate_and_finalize(all_records_df, config)

        return {"main": final_df, "error": error_df}


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    CrewEmployeePhoneDwPipeline(spark, job_name="CREW_J_TE_EMPLOYEE_PHONE_DW").run()
