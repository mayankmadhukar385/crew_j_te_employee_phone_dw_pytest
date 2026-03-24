-- Config deployment for: CREW_J_TE_EMPLOYEE_PHONE_DW
-- Run ONCE during first deployment. Config stored in etl_job_config Delta table.
-- After that, use SQL UPDATE or ConfigManager.update_config_key().
--
-- Prerequisites:
--   1. etl_job_config Delta table must already exist (created by framework DDL script).
--   2. Substitute ${UNITY_CATALOG} and ${UNITY_SCHEMA} with actual Unity Catalog values
--      before execution, OR use Databricks variable substitution (dbutils.widgets / job params).
--
-- Source: CREW_WSTELE_LND (Teradata TeradataConnectorPX)
-- Main target: TE_EMPLOYEE_PHONE_NEW (truncate_load) → promoted to TE_EMPLOYEE_PHONE via After-SQL
-- Error target: TE_EMPLOYEE_PHONE_ERR (append)
-- DataStage job: CREW_J_TE_EMPLOYEE_PHONE_DW

INSERT INTO ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_config
  (job_name, config_json, version, is_active, created_by, created_ts, description)
VALUES (
  'CREW_J_TE_EMPLOYEE_PHONE_DW',
  '{"pipeline":{"job_name":"CREW_J_TE_EMPLOYEE_PHONE_DW","workflow_name":"CREW_J_TE_EMPLOYEE_PHONE_DW","run_mode":"full_refresh","catalog":"${UNITY_CATALOG}","schema":"${UNITY_SCHEMA}"},"source":{"type":"jdbc","table":"CREW_WSTELE_LND","jdbc_options":{"url":"${TERADATA_JDBC_URL}","dbtable":"(SELECT TELE_EMP_NBR AS EMP_NBR, TELE_LAST_UPDATED_BY AS USER_ID, TELE_LAST_UPDATED_DATE, TELE_LAST_UPDATED_TIME, TELE_EMGR_PH_NAME AS PH_COMMENTS, TRIM(BOTH '' '' FROM TELE_EMGR_PH_AREA_CD)||TRIM(BOTH '' '' FROM TELE_EMGR_PH_PREFIX)||TRIM(BOTH '' '' FROM TELE_EMGR_PH_NUM) AS PH_NBR, TRIM(BOTH '' '' FROM TELE_TEMP_PH_AREA_CD)||TRIM(BOTH '' '' FROM TELE_TEMP_PH_PREFIX)||TRIM(BOTH '' '' FROM TELE_TEMP_PH_NUM) AS TEMP_PH_NBR, TELE_TEMP_PH_ACCESS AS TEMP_PH_ACCESS, TELE_TEMP_PH_COMMENT AS TEMP_PH_COMMENTS, TELE_TEMP_PH_DATE, TELE_TEMP_PH_TIME, TRIM(BOTH '' '' FROM TELE_PH_AREA_CD)||TRIM(BOTH '' '' FROM TELE_PH_PREFIX)||TRIM(BOTH '' '' FROM TELE_PH_NUM) AS BASIC_PH_NBR_1, TELE_PH_ACCESS AS BASIC_PH_ACCESS_1, TELE_PH_COMMENT AS BASIC_PH_COMMENTS_1, TELE_PH_TYPE AS BASIC_PH_TYPE_1, TELE_PH_UNLIST_CD AS BASIC_PH_UNLIST_CD_1, TELE_PH_HOME_AWAY_CD AS BASIC_PH_HOME_AWAY_CD_1, TRIM(BOTH '' '' FROM TELE_PH_AREA_CD_2)||TRIM(BOTH '' '' FROM TELE_PH_PREFIX_2)||TRIM(BOTH '' '' FROM TELE_PH_NUM_2) AS BASIC_PH_NBR_2, TELE_PH_ACCESS_2 AS BASIC_PH_ACCESS_2, TELE_PH_COMMENT_2 AS BASIC_PH_COMMENTS_2, TELE_PH_TYPE_2 AS BASIC_PH_TYPE_2, TELE_PH_UNLIST_CD_2 AS BASIC_PH_UNLIST_CD_2, TELE_PH_HOME_AWAY_CD_2 AS BASIC_PH_HOME_AWAY_CD_2, TRIM(BOTH '' '' FROM TELE_PH_AREA_CD_3)||TRIM(BOTH '' '' FROM TELE_PH_PREFIX_3)||TRIM(BOTH '' '' FROM TELE_PH_NUM_3) AS BASIC_PH_NBR_3, TELE_PH_ACCESS_3 AS BASIC_PH_ACCESS_3, TELE_PH_COMMENT_3 AS BASIC_PH_COMMENTS_3, TELE_PH_TYPE_3 AS BASIC_PH_TYPE_3, TELE_PH_UNLIST_CD_3 AS BASIC_PH_UNLIST_CD_3, TELE_PH_HOME_AWAY_CD_3 AS BASIC_PH_HOME_AWAY_CD_3, TRIM(BOTH '' '' FROM TELE_PH_AREA_CD_4)||TRIM(BOTH '' '' FROM TELE_PH_PREFIX_4)||TRIM(BOTH '' '' FROM TELE_PH_NUM_4) AS BASIC_PH_NBR_4, TELE_PH_ACCESS_4 AS BASIC_PH_ACCESS_4, TELE_PH_COMMENT_4 AS BASIC_PH_COMMENTS_4, TELE_PH_TYPE_4 AS BASIC_PH_TYPE_4, TELE_PH_UNLIST_CD_4 AS BASIC_PH_UNLIST_CD_4, TELE_PH_HOME_AWAY_CD_4 AS BASIC_PH_HOME_AWAY_CD_4, TRIM(BOTH '' '' FROM TELE_PH_AREA_CD_5)||TRIM(BOTH '' '' FROM TELE_PH_PREFIX_5)||TRIM(BOTH '' '' FROM TELE_PH_NUM_5) AS BASIC_PH_NBR_5, TELE_PH_ACCESS_5 AS BASIC_PH_ACCESS_5, TELE_PH_COMMENT_5 AS BASIC_PH_COMMENTS_5, TELE_PH_TYPE_5 AS BASIC_PH_TYPE_5, TELE_PH_UNLIST_CD_5 AS BASIC_PH_UNLIST_CD_5, TELE_PH_HOME_AWAY_CD_5 AS BASIC_PH_HOME_AWAY_CD_5, TELE_HOME_PRI_FROM AS TELE_HOME_PRI_FROM_1, TELE_HOME_PRI_TO AS TELE_HOME_PRI_TO_1, TELE_HOME_PRI_SEQ AS TELE_HOME_PRI_SEQ_1_1, TELE_HOME_PRI_SEQ_2 AS TELE_HOME_PRI_SEQ_2_1, TELE_HOME_PRI_SEQ_3 AS TELE_HOME_PRI_SEQ_3_1, TELE_HOME_PRI_SEQ_4 AS TELE_HOME_PRI_SEQ_4_1, TELE_HOME_PRI_SEQ_5 AS TELE_HOME_PRI_SEQ_5_1, TELE_HOME_PRI_FROM_2, TELE_HOME_PRI_TO_2, TELE_HOME_PRI_SEQ_6 AS TELE_HOME_PRI_SEQ_1_2, TELE_HOME_PRI_SEQ_2_2, TELE_HOME_PRI_SEQ_3_2, TELE_HOME_PRI_SEQ_4_2, TELE_HOME_PRI_SEQ_5_2, TELE_HOME_PRI_FROM_3, TELE_HOME_PRI_TO_3, TELE_HOME_PRI_SEQ_7 AS TELE_HOME_PRI_SEQ_1_3, TELE_HOME_PRI_SEQ_2_3, TELE_HOME_PRI_SEQ_3_3, TELE_HOME_PRI_SEQ_4_3, TELE_HOME_PRI_SEQ_5_3, TELE_AWAY_PRI_FROM AS TELE_AWAY_PRI_FROM_1, TELE_AWAY_PRI_TO AS TELE_AWAY_PRI_TO_1, TELE_AWAY_PRI_SEQ AS TELE_AWAY_PRI_SEQ_1_1, TELE_AWAY_PRI_SEQ_2 AS TELE_AWAY_PRI_SEQ_2_1, TELE_AWAY_PRI_SEQ_3 AS TELE_AWAY_PRI_SEQ_3_1, TELE_AWAY_PRI_SEQ_4 AS TELE_AWAY_PRI_SEQ_4_1, TELE_AWAY_PRI_SEQ_5 AS TELE_AWAY_PRI_SEQ_5_1, TELE_AWAY_PRI_FROM_2, TELE_AWAY_PRI_TO_2, TELE_AWAY_PRI_SEQ_6 AS TELE_AWAY_PRI_SEQ_1_2, TELE_AWAY_PRI_SEQ_2_2, TELE_AWAY_PRI_SEQ_3_2, TELE_AWAY_PRI_SEQ_4_2, TELE_AWAY_PRI_SEQ_5_2, TELE_AWAY_PRI_FROM_3, TELE_AWAY_PRI_TO_3, TELE_AWAY_PRI_SEQ_7 AS TELE_AWAY_PRI_SEQ_1_3, TELE_AWAY_PRI_SEQ_2_3, TELE_AWAY_PRI_SEQ_3_3, TELE_AWAY_PRI_SEQ_4_3, TELE_AWAY_PRI_SEQ_5_3 FROM ${TD_WORK_DB}.CREW_WSTELE_LND) AS src","driver":"com.teradata.jdbc.TeraDriver","fetchsize":"10000"}},"targets":{"main":{"table":"TE_EMPLOYEE_PHONE_NEW","write_mode":"truncate_load","business_keys":["EMP_NBR","PH_NBR"]},"error":{"table":"TE_EMPLOYEE_PHONE_ERR","write_mode":"append"}},"target":{"table":"TE_EMPLOYEE_PHONE_NEW","write_mode":"truncate_load","business_keys":["EMP_NBR","PH_NBR"]},"delta":{"optimize_after_write":true,"vacuum_retention_hours":168},"logging":{"level":"INFO","buffer_limit":1000},"dq_checks":[]}',
  1,
  'Y',
  'deployment',
  current_timestamp(),
  'Initial deployment — migrated from DataStage CREW_J_TE_EMPLOYEE_PHONE_DW'
);

-- ---------------------------------------------------------------------------
-- Verification query — run after INSERT to confirm config was stored correctly
-- ---------------------------------------------------------------------------
-- SELECT job_name, version, is_active, created_ts, description
-- FROM ${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_config
-- WHERE job_name = 'CREW_J_TE_EMPLOYEE_PHONE_DW';

-- ---------------------------------------------------------------------------
-- Target table DDL (run before first pipeline execution)
-- ---------------------------------------------------------------------------

-- Work table (receives truncate+load on every run)
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE_NEW (
  EMP_NBR        INT           NOT NULL COMMENT 'Employee number (int32)',
  EMP_NO         VARCHAR(9)    NOT NULL COMMENT 'Employee number zero-padded to 9 chars',
  PH_LIST        VARCHAR(20)   NOT NULL COMMENT 'Phone list type: EMERGENCY/TEMP/BASIC/HOME/AWAY',
  PH_PRTY        INT           NOT NULL COMMENT 'Phone call priority (sequential per EMP_NBR)',
  EFF_START_TM   VARCHAR(8)    NOT NULL COMMENT 'Effective start time HH:MM:SS',
  EFF_END_TM     VARCHAR(8)    NOT NULL COMMENT 'Effective end time HH:MM:SS',
  PH_NBR         BIGINT        NOT NULL COMMENT 'Phone number (int64)',
  PH_ACCESS      VARCHAR(100)           COMMENT 'Phone access code',
  PH_COMMENTS    VARCHAR(500)           COMMENT 'Phone comments / name',
  PH_TYPE        VARCHAR(10)            COMMENT 'Phone type code',
  UNLISTD_IND    VARCHAR(1)             COMMENT 'Unlisted indicator',
  HOME_AWAY_IND  VARCHAR(1)             COMMENT 'Home/Away indicator (H/A/B)',
  TEMP_PH_EXP_TS TIMESTAMP              COMMENT 'Temporary phone expiry timestamp',
  TD_LD_TS       TIMESTAMP     NOT NULL COMMENT 'Teradata load timestamp (current_timestamp at load)'
)
USING DELTA
COMMENT 'Work table for TE_EMPLOYEE_PHONE — truncate+loaded on every run of CREW_J_TE_EMPLOYEE_PHONE_DW';

-- Live table (promoted from work table via After-SQL equivalent in pipeline)
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE (
  EMP_NBR        INT           NOT NULL COMMENT 'Employee number (int32)',
  EMP_NO         VARCHAR(9)    NOT NULL COMMENT 'Employee number zero-padded to 9 chars',
  PH_LIST        VARCHAR(20)   NOT NULL COMMENT 'Phone list type: EMERGENCY/TEMP/BASIC/HOME/AWAY',
  PH_PRTY        INT           NOT NULL COMMENT 'Phone call priority (sequential per EMP_NBR)',
  EFF_START_TM   VARCHAR(8)    NOT NULL COMMENT 'Effective start time HH:MM:SS',
  EFF_END_TM     VARCHAR(8)    NOT NULL COMMENT 'Effective end time HH:MM:SS',
  PH_NBR         BIGINT        NOT NULL COMMENT 'Phone number (int64)',
  PH_ACCESS      VARCHAR(100)           COMMENT 'Phone access code',
  PH_COMMENTS    VARCHAR(500)           COMMENT 'Phone comments / name',
  PH_TYPE        VARCHAR(10)            COMMENT 'Phone type code',
  UNLISTD_IND    VARCHAR(1)             COMMENT 'Unlisted indicator',
  HOME_AWAY_IND  VARCHAR(1)             COMMENT 'Home/Away indicator (H/A/B)',
  TEMP_PH_EXP_TS TIMESTAMP              COMMENT 'Temporary phone expiry timestamp',
  TD_LD_TS       TIMESTAMP     NOT NULL COMMENT 'Teradata load timestamp (current_timestamp at load)'
)
USING DELTA
COMMENT 'Live employee phone table — migrated from DataStage CREW_J_TE_EMPLOYEE_PHONE_DW';

-- Error table (appended to on each run for invalid EMP_NBR records)
CREATE TABLE IF NOT EXISTS ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE_ERR (
  EMP_NBR        VARCHAR(100)           COMMENT 'Raw EMP_NBR value (invalid — not castable to int32)',
  PH_NBR         VARCHAR(100)           COMMENT 'Raw PH_NBR value',
  TEMP_PH_NBR    VARCHAR(100)           COMMENT 'Raw TEMP_PH_NBR value',
  BASIC_PH_NBR_1 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_1 value',
  BASIC_PH_NBR_2 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_2 value',
  BASIC_PH_NBR_3 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_3 value',
  BASIC_PH_NBR_4 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_4 value',
  BASIC_PH_NBR_5 VARCHAR(100)           COMMENT 'Raw BASIC_PH_NBR_5 value',
  _error_ts      TIMESTAMP     NOT NULL COMMENT 'Timestamp when error record was written'
)
USING DELTA
COMMENT 'Error records from CREW_J_TE_EMPLOYEE_PHONE_DW — rows with invalid EMP_NBR (original DataStage: EMPLOYEE_PHONE.err)';
