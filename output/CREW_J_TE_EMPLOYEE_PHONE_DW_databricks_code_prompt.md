# Databricks Code Generation Prompt — CREW_J_TE_EMPLOYEE_PHONE_DW

> **Zero-context document**: everything a code generator needs to produce production-grade
> PySpark for this pipeline lives in this file. No other file need be consulted.

---

## 1. Pipeline Identity

| Field | Value |
|---|---|
| Job name | CREW_J_TE_EMPLOYEE_PHONE_DW |
| Source system | Teradata (TeradataConnectorPX) |
| Source table | `#ENV.$TD_WORK_DB#.CREW_WSTELE_LND` |
| Target system | Delta Lake (Databricks) |
| Work table | `TE_EMPLOYEE_PHONE_NEW` (truncate+insert) |
| Live table | `TE_EMPLOYEE_PHONE` (promoted via After-SQL) |
| Error target | `WSSEN_ERR_SEQ` → `#ENV.$LOG_DIR#/EMPLOYEE_PHONE.err` |
| Total stages | 34 |
| Total edges | 47 |
| Target columns (TE_EMPLOYEE_PHONE) | 14 (EMP_NBR, EMP_NO, PH_LIST, PH_PRTY, EFF_START_TM, EFF_END_TM, PH_NBR, PH_ACCESS, PH_COMMENTS, PH_TYPE, UNLISTD_IND, HOME_AWAY_IND, TEMP_PH_EXP_TS, TD_LD_TS) |
| Error columns (WSSEN_ERR_SEQ) | 10 (EMP_NBR, PH_NBR, TEMP_PH_NBR, BASIC_PH_NBR_1..5) |
| Job description | Reads CREW_WSTELE_LND landing table, routes records by phone type (EMERGENCY / TEMP / BASIC / HOME / AWAY), builds call-priority sequence numbers, and writes to TE_EMPLOYEE_PHONE. |

---

## 2. Stage Types and PySpark Mappings

Only stage types present in this pipeline are listed.

| DataStage Stage Type | Count | PySpark Equivalent |
|---|---|---|
| `TeradataConnectorPX` (Source) | 1 | `spark.read.format("jdbc")` with Teradata driver |
| `TeradataConnectorPX` (Target) | 1 | Delta `truncate + insert` (TableAction=3, WriteMode=0); After-SQL promotes to live table |
| `CTransformerStage` | 13 | `df.withColumn()` / `df.select()` with `F.when`, `F.coalesce`, `F.lit`, `F.trim`, `F.length`, `F.substring`, `F.concat` |
| `PxPivot` | 7 | `F.expr("stack(N, col1, col2, ...)")` — unpivots N wide columns into one tall column per output row |
| `PxCopy` | 2 | Python variable reference reuse — same DataFrame assigned to multiple downstream variables |
| `PxFunnel` | 3 | `df.unionByName(other, allowMissingColumns=True)` chained for 3+ inputs |
| `PxJoin` | 4 | `df.join(other, on=[keys], how="inner")` |
| `PxAggregator` | 2 | `df.groupBy("EMP_NBR").agg(F.max("CALL_PRTY").alias("MAX_CALL_PRTY"))` |
| `PxSequentialFile` (Target) | 1 | `df.write.format("delta").mode("overwrite")` to error Delta table |

---

## 3. Complete Transformation Chain

Stages are listed in Sequence_No order from the Stage_Sequence sheet.

### 3.1 Seq 1 — WSTELE_LND_TDC (TeradataConnectorPX, Source)

**Link out**: `LNK_OUT_TDC` → SEPARATE_PHTYPE_TFM

**Source SQL** (verbatim from XML SelectStatement CDATA):

```sql
SELECT

/*EMERGENCY*/

 TELE_EMP_NBR AS EMP_NBR
,TELE_LAST_UPDATED_BY AS USER_ID
,TELE_LAST_UPDATED_DATE
,TELE_LAST_UPDATED_TIME
,TELE_EMGR_PH_NAME AS PH_COMMENTS
,TRIM(BOTH ' ' FROM TELE_EMGR_PH_AREA_CD) ||  TRIM(BOTH ' ' FROM TELE_EMGR_PH_PREFIX)  ||  TRIM(BOTH ' ' FROM TELE_EMGR_PH_NUM) AS PH_NBR

/*TEMP*/
,TRIM(BOTH ' ' FROM TELE_TEMP_PH_AREA_CD) ||  TRIM(BOTH ' ' FROM TELE_TEMP_PH_PREFIX)  ||  TRIM(BOTH ' ' FROM TELE_TEMP_PH_NUM) AS TEMP_PH_NBR
,TELE_TEMP_PH_ACCESS AS TEMP_PH_ACCESS
,TELE_TEMP_PH_COMMENT AS TEMP_PH_COMMENTS
,TELE_TEMP_PH_DATE
,TELE_TEMP_PH_TIME

/*BASIC*/
,TRIM(BOTH ' ' FROM TELE_PH_AREA_CD)  ||  TRIM(BOTH ' ' FROM TELE_PH_PREFIX)  ||  TRIM(BOTH ' ' FROM TELE_PH_NUM)             AS BASIC_PH_NBR_1
,TELE_PH_ACCESS     AS BASIC_PH_ACCESS_1
,TELE_PH_COMMENT     AS BASIC_PH_COMMENTS_1
,TELE_PH_TYPE    AS BASIC_PH_TYPE_1
,TELE_PH_UNLIST_CD AS BASIC_PH_UNLIST_CD_1
,TELE_PH_HOME_AWAY_CD     AS BASIC_PH_HOME_AWAY_CD_1

,TRIM(BOTH ' ' FROM TELE_PH_AREA_CD_2)  ||  TRIM(BOTH ' ' FROM TELE_PH_PREFIX_2)  ||  TRIM(BOTH ' ' FROM TELE_PH_NUM_2) AS BASIC_PH_NBR_2
,TELE_PH_ACCESS_2 AS BASIC_PH_ACCESS_2
,TELE_PH_COMMENT_2 AS BASIC_PH_COMMENTS_2
,TELE_PH_TYPE_2 AS BASIC_PH_TYPE_2
,TELE_PH_UNLIST_CD_2 AS BASIC_PH_UNLIST_CD_2
,TELE_PH_HOME_AWAY_CD_2 AS BASIC_PH_HOME_AWAY_CD_2

,TRIM(BOTH ' ' FROM TELE_PH_AREA_CD_3)  ||  TRIM(BOTH ' ' FROM TELE_PH_PREFIX_3)  ||  TRIM(BOTH ' ' FROM TELE_PH_NUM_3) AS BASIC_PH_NBR_3
,TELE_PH_ACCESS_3 AS BASIC_PH_ACCESS_3
,TELE_PH_COMMENT_3 AS BASIC_PH_COMMENTS_3
,TELE_PH_TYPE_3 AS BASIC_PH_TYPE_3
,TELE_PH_UNLIST_CD_3 AS BASIC_PH_UNLIST_CD_3
,TELE_PH_HOME_AWAY_CD_3 AS BASIC_PH_HOME_AWAY_CD_3

,TRIM(BOTH ' ' FROM TELE_PH_AREA_CD_4)  ||  TRIM(BOTH ' ' FROM TELE_PH_PREFIX_4)  ||  TRIM(BOTH ' ' FROM TELE_PH_NUM_4) AS BASIC_PH_NBR_4
,TELE_PH_ACCESS_4 AS BASIC_PH_ACCESS_4
,TELE_PH_COMMENT_4 AS BASIC_PH_COMMENTS_4
,TELE_PH_TYPE_4 AS BASIC_PH_TYPE_4
,TELE_PH_UNLIST_CD_4 AS BASIC_PH_UNLIST_CD_4
,TELE_PH_HOME_AWAY_CD_4 AS BASIC_PH_HOME_AWAY_CD_4

,TRIM(BOTH ' ' FROM TELE_PH_AREA_CD_5)  ||  TRIM(BOTH ' ' FROM TELE_PH_PREFIX_5)  ||  TRIM(BOTH ' ' FROM TELE_PH_NUM_5) AS BASIC_PH_NBR_5
,TELE_PH_ACCESS_5 AS BASIC_PH_ACCESS_5
,TELE_PH_COMMENT_5 AS BASIC_PH_COMMENTS_5
,TELE_PH_TYPE_5 AS BASIC_PH_TYPE_5
,TELE_PH_UNLIST_CD_5 AS BASIC_PH_UNLIST_CD_5
,TELE_PH_HOME_AWAY_CD_5 AS BASIC_PH_HOME_AWAY_CD_5

/*HOME*/
,TELE_HOME_PRI_FROM  AS TELE_HOME_PRI_FROM_1
,TELE_HOME_PRI_TO       AS TELE_HOME_PRI_TO_1
,TELE_HOME_PRI_SEQ     AS TELE_HOME_PRI_SEQ_1_1
,TELE_HOME_PRI_SEQ_2 AS TELE_HOME_PRI_SEQ_2_1
,TELE_HOME_PRI_SEQ_3 AS TELE_HOME_PRI_SEQ_3_1
,TELE_HOME_PRI_SEQ_4 AS TELE_HOME_PRI_SEQ_4_1
,TELE_HOME_PRI_SEQ_5 AS TELE_HOME_PRI_SEQ_5_1

,TELE_HOME_PRI_FROM_2
,TELE_HOME_PRI_TO_2
,TELE_HOME_PRI_SEQ_6           AS  TELE_HOME_PRI_SEQ_1_2
,TELE_HOME_PRI_SEQ_2_2
,TELE_HOME_PRI_SEQ_3_2
,TELE_HOME_PRI_SEQ_4_2
,TELE_HOME_PRI_SEQ_5_2

,TELE_HOME_PRI_FROM_3
,TELE_HOME_PRI_TO_3
,TELE_HOME_PRI_SEQ_7           AS  TELE_HOME_PRI_SEQ_1_3
,TELE_HOME_PRI_SEQ_2_3
,TELE_HOME_PRI_SEQ_3_3
,TELE_HOME_PRI_SEQ_4_3
,TELE_HOME_PRI_SEQ_5_3

/*AWAY*/
,TELE_AWAY_PRI_FROM  AS TELE_AWAY_PRI_FROM_1
,TELE_AWAY_PRI_TO       AS TELE_AWAY_PRI_TO_1
,TELE_AWAY_PRI_SEQ     AS TELE_AWAY_PRI_SEQ_1_1
,TELE_AWAY_PRI_SEQ_2 AS TELE_AWAY_PRI_SEQ_2_1
,TELE_AWAY_PRI_SEQ_3 AS TELE_AWAY_PRI_SEQ_3_1
,TELE_AWAY_PRI_SEQ_4 AS TELE_AWAY_PRI_SEQ_4_1
,TELE_AWAY_PRI_SEQ_5 AS TELE_AWAY_PRI_SEQ_5_1

,TELE_AWAY_PRI_FROM_2
,TELE_AWAY_PRI_TO_2
,TELE_AWAY_PRI_SEQ_6           AS  TELE_AWAY_PRI_SEQ_1_2
,TELE_AWAY_PRI_SEQ_2_2
,TELE_AWAY_PRI_SEQ_3_2
,TELE_AWAY_PRI_SEQ_4_2
,TELE_AWAY_PRI_SEQ_5_2

,TELE_AWAY_PRI_FROM_3
,TELE_AWAY_PRI_TO_3
,TELE_AWAY_PRI_SEQ_7           AS  TELE_AWAY_PRI_SEQ_1_3
,TELE_AWAY_PRI_SEQ_2_3
,TELE_AWAY_PRI_SEQ_3_3
,TELE_AWAY_PRI_SEQ_4_3
,TELE_AWAY_PRI_SEQ_5_3

FROM
#ENV.$TD_WORK_DB#.CREW_WSTELE_LND
```

**PySpark**: Read via JDBC. Replace `#ENV.$TD_WORK_DB#` with config value `${TD_WORK_DB}`.

---

### 3.2 Seq 2 — SEPARATE_PHTYPE_TFM (CTransformerStage)

**Link in**: `LNK_OUT_TDC`
**Links out** (6 outputs):

| Output Link | Destination | Constraint (verbatim from XML) |
|---|---|---|
| `EMERGENCY_IN_TFM` | ALL_REC_FNL | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR) and isvalid("int64",LNK_OUT_TDC.PH_NBR)` |
| `TEMP_TYPE_TFM` | ALL_REC_FNL | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR) and isvalid("int64",LNK_OUT_TDC.TEMP_PH_NBR)` |
| `BASIC_TYPE_TFM` | BASIC_TYPE_PVT | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR)` |
| `HOME_TYPE_CPY` | HOME_CPY | (no constraint — all valid EMP_NBR records with HOME data) |
| `AWAY_TYPE_CPY` | AWAY_CPY | (no constraint — all valid EMP_NBR records with AWAY data) |
| `LNK_OUT_ERR_SEQ` | WSSEN_ERR_SEQ | `Not (IsValid("int32", (LNK_OUT_TDC.EMP_NBR)))` |

**Column derivations per output link (verbatim from XML):**

**EMERGENCY_IN_TFM output:**
```
EMP_NBR    = LNK_OUT_TDC.EMP_NBR
CALL_LIST  = 'EMERGENCY'
CALL_PRTY  = 1
EFF_START_TM = '0000'
EFF_END_TM   = '2359'
PH_ACCESS  = SETNULL()
PH_COMMENTS = if trimleadingtrailing(LNK_OUT_TDC.PH_COMMENTS)='' then setnull() else LNK_OUT_TDC.PH_COMMENTS
PH_TYPE    = SETNULL()
UNLISTD_IND = SETNULL()
HOME_AWAY_IND = SETNULL()
TEMP_PH_EXP_TS = SETNULL()
PH_NBR     = LNK_OUT_TDC.PH_NBR
USER_ID    = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.USER_ID)='' THEN SETNULL() ELSE LNK_OUT_TDC.USER_ID
UPD_TS     = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.TELE_LAST_UPDATED_DATE)='' OR TRIMLEADINGTRAILING(LNK_OUT_TDC.TELE_LAST_UPDATED_TIME)='' THEN SETNULL() ELSE IF NOT(ISVALID("TIMESTAMP",'20':LEFT(LNK_OUT_TDC.TELE_LAST_UPDATED_DATE,2):'-':RIGHT(LEFT(LNK_OUT_TDC.TELE_LAST_UPDATED_DATE,4),2):'-':RIGHT(LNK_OUT_TDC.TELE_LAST_UPDATED_DATE,2):' ':LEFT(LNK_OUT_TDC.TELE_LAST_UPDATED_TIME,2):':':RIGHT(LNK_OUT_TDC.TELE_LAST_UPDATED_TIME,2):':00')) THEN SETNULL() ELSE '20':LEFT(LNK_OUT_TDC.TELE_LAST_UPDATED_DATE,2):'-':RIGHT(LEFT(LNK_OUT_TDC.TELE_LAST_UPDATED_DATE,4),2):'-':RIGHT(LNK_OUT_TDC.TELE_LAST_UPDATED_DATE,2):' ':LEFT(LNK_OUT_TDC.TELE_LAST_UPDATED_TIME,2):':':RIGHT(LNK_OUT_TDC.TELE_LAST_UPDATED_TIME,2):':00'
```

**TEMP_TYPE_TFM output:**
```
EMP_NBR    = LNK_OUT_TDC.EMP_NBR
CALL_LIST  = 'TEMP'
CALL_PRTY  = 1
EFF_START_TM = '0000'
EFF_END_TM   = '2359'
PH_ACCESS  = if trimleadingtrailing(LNK_OUT_TDC.TEMP_PH_ACCESS)='' then setnull() else LNK_OUT_TDC.TEMP_PH_ACCESS
PH_COMMENTS = if trimleadingtrailing(LNK_OUT_TDC.TEMP_PH_COMMENTS)='' then setnull() else LNK_OUT_TDC.TEMP_PH_COMMENTS
PH_TYPE    = SETNULL()
UNLISTD_IND = SETNULL()
HOME_AWAY_IND = SETNULL()
TEMP_PH_EXP_TS = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.TELE_TEMP_PH_DATE)='' OR TRIMLEADINGTRAILING(LNK_OUT_TDC.TELE_TEMP_PH_TIME)='' THEN SETNULL() ELSE IF NOT(ISVALID("TIMESTAMP",'20':LEFT(LNK_OUT_TDC.TELE_TEMP_PH_DATE,2):'-':RIGHT(LEFT(LNK_OUT_TDC.TELE_TEMP_PH_DATE,4),2):'-':RIGHT(LNK_OUT_TDC.TELE_TEMP_PH_DATE,2):' ':LEFT(LNK_OUT_TDC.TELE_TEMP_PH_TIME,2):':':RIGHT(LNK_OUT_TDC.TELE_TEMP_PH_TIME,2):':00')) THEN SETNULL() ELSE '20':LEFT(LNK_OUT_TDC.TELE_TEMP_PH_DATE,2):'-':RIGHT(LEFT(LNK_OUT_TDC.TELE_TEMP_PH_DATE,4),2):'-':RIGHT(LNK_OUT_TDC.TELE_TEMP_PH_DATE,2):' ':LEFT(LNK_OUT_TDC.TELE_TEMP_PH_TIME,2):':':RIGHT(LNK_OUT_TDC.TELE_TEMP_PH_TIME,2):':00'
PH_NBR     = LNK_OUT_TDC.TEMP_PH_NBR
USER_ID    = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.USER_ID)='' THEN SETNULL() ELSE LNK_OUT_TDC.USER_ID
UPD_TS     = (same construction as EMERGENCY — from TELE_LAST_UPDATED_DATE/TIME)
```

**BASIC_TYPE_TFM output (feeds BASIC_TYPE_PVT):**
```
EMP_NBR    = LNK_OUT_TDC.EMP_NBR
CALL_LIST  = 'BASIC'
EFF_START_TM = '0001'
EFF_END_TM   = '2359'
BASIC_PH_NBR_1   = LNK_OUT_TDC.BASIC_PH_NBR_1
BASIC_PH_ACCESS_1  = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.BASIC_PH_ACCESS_1)='' THEN SETNULL() ELSE LNK_OUT_TDC.BASIC_PH_ACCESS_1
BASIC_PH_COMMENT_1 = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.BASIC_PH_COMMENTS_1)='' THEN SETNULL() ELSE LNK_OUT_TDC.BASIC_PH_COMMENTS_1
BASIC_PH_TYPE_1    = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.BASIC_PH_TYPE_1)='' THEN SETNULL() ELSE LNK_OUT_TDC.BASIC_PH_TYPE_1
BASIC_PH_UNLIST_CD_1   = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.BASIC_PH_UNLIST_CD_1)='' THEN SETNULL() ELSE LNK_OUT_TDC.BASIC_PH_UNLIST_CD_1
BASIC_PH_HOME_AWAY_CD_1 = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.BASIC_PH_HOME_AWAY_CD_1)='' THEN SETNULL() ELSE LNK_OUT_TDC.BASIC_PH_HOME_AWAY_CD_1
TEMP_PH_EXP_TS_1 = SETNULL()
... (same pattern for _2 through _5)
USER_ID    = IF TRIMLEADINGTRAILING(LNK_OUT_TDC.USER_ID)='' THEN SETNULL() ELSE LNK_OUT_TDC.USER_ID
UPD_TS     = (same construction from TELE_LAST_UPDATED_DATE/TIME)
```

**HOME_TYPE_CPY output (feeds HOME_CPY):**
```
EMP_NBR = LNK_OUT_TDC.EMP_NBR
TELE_HOME_PRI_FROM_1 = LNK_OUT_TDC.TELE_HOME_PRI_FROM_1
TELE_HOME_PRI_TO_1   = LNK_OUT_TDC.TELE_HOME_PRI_TO_1
TELE_HOME_PRI_SEQ_1_1 .. TELE_HOME_PRI_SEQ_5_1 = pass-through
TELE_HOME_PRI_FROM_2..3, TELE_HOME_PRI_TO_2..3 = pass-through
TELE_HOME_PRI_SEQ_1_2..5_2, TELE_HOME_PRI_SEQ_1_3..5_3 = pass-through
```

**AWAY_TYPE_CPY output (feeds AWAY_CPY):**
```
EMP_NBR = LNK_OUT_TDC.EMP_NBR
TELE_HOME_PRI_FROM_1 = LNK_OUT_TDC.TELE_AWAY_PRI_FROM_1   (note: remapped from AWAY to HOME column names)
TELE_HOME_PRI_TO_1   = LNK_OUT_TDC.TELE_AWAY_PRI_TO_1
TELE_HOME_PRI_SEQ_1_1 = LNK_OUT_TDC.TELE_AWAY_PRI_SEQ_1_1
TELE_HOME_PRI_SEQ_2_1 = LNK_OUT_TDC.TELE_AWAY_PRI_SEQ_2_1
... (all AWAY SEQ columns remapped to TELE_HOME_PRI_SEQ_* naming)
```

**LNK_OUT_ERR_SEQ output (feeds WSSEN_ERR_SEQ):**
All source columns pass through verbatim. No transformation. Filter: `Not (IsValid("int32", (LNK_OUT_TDC.EMP_NBR)))`.

---

### 3.3 Seq 3A — AWAY_CPY (PxCopy)

**Link in**: `AWAY_TYPE_CPY`
**Links out**: `AWAY_SEQ1_CPY` → AWAY_SEQ1_PVT, `AWAY_SEQ2_CPY` → AWAY_SEQ2_PVT, `AWAY_SEQ3_CPY` → AWAY_SEQ3_PVT

No transformation. Same DataFrame is fed to three downstream pivot stages.

---

### 3.4 Seq 3B — WSSEN_ERR_SEQ (PxSequentialFile, Target)

**Link in**: `LNK_OUT_ERR_SEQ`
File path: `#ENV.$LOG_DIR#/EMPLOYEE_PHONE.err` (overwrite mode)
In Databricks: write to Delta error table `TE_EMPLOYEE_PHONE_ERR`.

---

### 3.5 Seq 3C — BASIC_TYPE_PVT (PxPivot)

**Link in**: `BASIC_TYPE_TFM`
**Link out**: `BREC_OUT_PVT` → BASIC_REC_TFM

**Pivot operation** (verbatim from XML PivotCustomOSH):
5 wide BASIC phone columns per employee are pivoted into individual rows.
Pass-through columns (unchanged): `EMP_NBR`, `CALL_LIST`, `EFF_START_TM`, `EFF_END_TM`, `USER_ID`, `UPD_TS`
Index column generated: `CALL_PRTY` (0-based pivot index, incremented to 1-based in BASIC_REC_TFM)
Pivoted column groups (5 → 1 row each):
```
PH_ACCESS  from BASIC_PH_ACCESS_1..5
PH_COMMENTS from BASIC_PH_COMMENT_1..5
PH_TYPE    from BASIC_PH_TYPE_1..5
UNLISTD_IND from BASIC_PH_UNLIST_CD_1..5
HOME_AWAY_IND from BASIC_PH_HOME_AWAY_CD_1..5
TEMP_PH_EXP_TS from TEMP_PH_EXP_TS_1..5
PH_NBR     from BASIC_PH_NBR_1..5
```

**PySpark implementation**:
```python
from pyspark.sql import functions as F

basic_stack_expr = """
stack(5,
  BASIC_PH_NBR_1, BASIC_PH_ACCESS_1, BASIC_PH_COMMENT_1, BASIC_PH_TYPE_1,
    BASIC_PH_UNLIST_CD_1, BASIC_PH_HOME_AWAY_CD_1, TEMP_PH_EXP_TS_1, 0,
  BASIC_PH_NBR_2, BASIC_PH_ACCESS_2, BASIC_PH_COMMENT_2, BASIC_PH_TYPE_2,
    BASIC_PH_UNLIST_CD_2, BASIC_PH_HOME_AWAY_CD_2, TEMP_PH_EXP_TS_2, 1,
  BASIC_PH_NBR_3, BASIC_PH_ACCESS_3, BASIC_PH_COMMENT_3, BASIC_PH_TYPE_3,
    BASIC_PH_UNLIST_CD_3, BASIC_PH_HOME_AWAY_CD_3, TEMP_PH_EXP_TS_3, 2,
  BASIC_PH_NBR_4, BASIC_PH_ACCESS_4, BASIC_PH_COMMENT_4, BASIC_PH_TYPE_4,
    BASIC_PH_UNLIST_CD_4, BASIC_PH_HOME_AWAY_CD_4, TEMP_PH_EXP_TS_4, 3,
  BASIC_PH_NBR_5, BASIC_PH_ACCESS_5, BASIC_PH_COMMENT_5, BASIC_PH_TYPE_5,
    BASIC_PH_UNLIST_CD_5, BASIC_PH_HOME_AWAY_CD_5, TEMP_PH_EXP_TS_5, 4
) AS (PH_NBR, PH_ACCESS, PH_COMMENTS, PH_TYPE, UNLISTD_IND, HOME_AWAY_IND, TEMP_PH_EXP_TS, CALL_PRTY)
"""
pivoted = basic_df.select(
    "EMP_NBR","CALL_LIST","EFF_START_TM","EFF_END_TM","USER_ID","UPD_TS",
    F.expr(basic_stack_expr)
)
```

---

### 3.6 Seq 3D — HOME_CPY (PxCopy)

**Link in**: `HOME_TYPE_CPY`
**Links out**: `HOME_SEQ1_CPY` → HOME_SEQ1_PVT, `HOME_SEQ2_CPY` → HOME_SEQ2_PVT, `HOME_SEQ3_CPY` → HOME_SEQ3_PVT

No transformation. Same DataFrame assigned to three downstream pivot branches.

---

### 3.7 Seq 4A — AWAY_SEQ1_PVT (PxPivot)

**Link in**: `AWAY_SEQ1_CPY`
**Link out**: `ASEQ1_OUT_PVT` → AWAY_SEQ_FNL

Pivots 5 AWAY SEQ1 priority sequence columns into rows.
Pass-through: `EMP_NBR`, `TELE_HOME_PRI_FROM_1`, `TELE_HOME_PRI_TO_1`
Pivoted column (verbatim from XML PivotCustomOSH):
```
LKP_CALL_PRTY  from TELE_HOME_PRI_SEQ_1_1, TELE_HOME_PRI_SEQ_2_1,
                    TELE_HOME_PRI_SEQ_3_1, TELE_HOME_PRI_SEQ_4_1, TELE_HOME_PRI_SEQ_5_1
```
Index column: `CALL_PRTY` (0-based)

**PySpark**:
```python
away_seq1_pivoted = away_df.select(
    "EMP_NBR",
    F.col("TELE_HOME_PRI_FROM_1").alias("TELE_HOME_PRI_FROM"),
    F.col("TELE_HOME_PRI_TO_1").alias("TELE_HOME_PRI_TO"),
    F.expr("stack(5, TELE_HOME_PRI_SEQ_1_1,0, TELE_HOME_PRI_SEQ_2_1,1, TELE_HOME_PRI_SEQ_3_1,2, TELE_HOME_PRI_SEQ_4_1,3, TELE_HOME_PRI_SEQ_5_1,4) AS (LKP_CALL_PRTY, CALL_PRTY)")
)
```

---

### 3.8 Seq 4B — AWAY_SEQ2_PVT (PxPivot)

**Link in**: `AWAY_SEQ2_CPY`
**Link out**: `ASEQ2_OUT_PVT` → AWAY_SEQ2_TFM

Pivots 5 AWAY SEQ2 priority sequence columns into rows.
Pass-through: `EMP_NBR`, `TELE_HOME_PRI_FROM_2` → `TELE_HOME_PRI_FROM`, `TELE_HOME_PRI_TO_2` → `TELE_HOME_PRI_TO`
Pivoted (verbatim): `LKP_CALL_PRTY` from `TELE_HOME_PRI_SEQ_1_2..5_2`

---

### 3.9 Seq 4C — AWAY_SEQ3_PVT (PxPivot)

**Link in**: `AWAY_SEQ3_CPY`
**Link out**: `ASEQ3_OUT_PVT` → AWAY_SEQ3_TFM

Pivots 5 AWAY SEQ3 priority sequence columns into rows.
Pass-through: `EMP_NBR`, `TELE_HOME_PRI_FROM_3` → `TELE_HOME_PRI_FROM`, `TELE_HOME_PRI_TO_3` → `TELE_HOME_PRI_TO`
Pivoted (verbatim): `LKP_CALL_PRTY` from `TELE_HOME_PRI_SEQ_1_3..5_3`

---

### 3.10 Seq 4D — BASIC_REC_TFM (CTransformerStage)

**Link in**: `BREC_OUT_PVT`
**Links out** (5 outputs):

| Output Link | Destination | Constraint (verbatim from XML) |
|---|---|---|
| `ALL_BREC_OUT_TFM` | ALL_REC_FNL | `isvalid("int64",BREC_OUT_PVT.PH_NBR)` |
| `CALL_PRIORITY_RIGHT_JNR` | HOME_BASIC_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='H')` |
| `BREC_OUT_TFM` | BASIC_MAX_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='H')` |
| `CALL_PRI_RIGHT_JNR` | AWAY_BASIC_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='A')` |
| `BASIC_REC_OUT_TFM` | AWAY_MAX_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='A')` |

**Column derivations (verbatim — same for all 5 outputs except extra LKP_CALL_PRTY on join outputs):**
```
EMP_NBR       = BREC_OUT_PVT.EMP_NBR
CALL_LIST     = BREC_OUT_PVT.CALL_LIST           (= 'BASIC')
CALL_PRTY     = BREC_OUT_PVT.CALL_PRTY+1         (convert 0-based pivot index to 1-based)
EFF_START_TM  = BREC_OUT_PVT.EFF_START_TM
EFF_END_TM    = BREC_OUT_PVT.EFF_END_TM
PH_ACCESS     = BREC_OUT_PVT.PH_ACCESS
PH_COMMENTS   = BREC_OUT_PVT.PH_COMMENTS
PH_TYPE       = BREC_OUT_PVT.PH_TYPE
UNLISTD_IND   = BREC_OUT_PVT.UNLISTD_IND
HOME_AWAY_IND = BREC_OUT_PVT.HOME_AWAY_IND
TEMP_PH_EXP_TS = BREC_OUT_PVT.TEMP_PH_EXP_TS
PH_NBR        = BREC_OUT_PVT.PH_NBR
USER_ID       = BREC_OUT_PVT.USER_ID
UPD_TS        = BREC_OUT_PVT.UPD_TS
```
For `CALL_PRIORITY_RIGHT_JNR` and `CALL_PRI_RIGHT_JNR` additionally:
```
LKP_CALL_PRTY = BREC_OUT_PVT.CALL_PRTY+1
```

---

### 3.11 Seq 4E — HOME_SEQ1_PVT (PxPivot)

**Link in**: `HOME_SEQ1_CPY`
**Link out**: `HSEQ1_OUT_PVT` → HOME_SEQ_FNL

Pivots 5 HOME SEQ1 priority sequence columns into rows.
Pass-through: `EMP_NBR`, `TELE_HOME_PRI_FROM_1` → `TELE_HOME_PRI_FROM`, `TELE_HOME_PRI_TO_1` → `TELE_HOME_PRI_TO`
Pivoted (verbatim from XML): `LKP_CALL_PRTY` from `TELE_HOME_PRI_SEQ_1_1, TELE_HOME_PRI_SEQ_2_1, TELE_HOME_PRI_SEQ_3_1, TELE_HOME_PRI_SEQ_4_1, TELE_HOME_PRI_SEQ_5_1`

---

### 3.12 Seq 4F — HOME_SEQ2_PVT (PxPivot)

**Link in**: `HOME_SEQ2_CPY`
**Link out**: `HSEQ2_OUT_PVT` → HOME_SEQ2_TFM

Pivots 5 HOME SEQ2 priority sequence columns into rows.
Pass-through: `EMP_NBR`, `TELE_HOME_PRI_FROM_2` → `TELE_HOME_PRI_FROM`, `TELE_HOME_PRI_TO_2` → `TELE_HOME_PRI_TO`
Pivoted (verbatim): `LKP_CALL_PRTY` from `TELE_HOME_PRI_SEQ_1_2..5_2`

---

### 3.13 Seq 4G — HOME_SEQ3_PVT (PxPivot)

**Link in**: `HOME_SEQ3_CPY`
**Link out**: `HSEQ3_OUT_PVT` → HOME_SEQ3_TFM

Pivots 5 HOME SEQ3 priority sequence columns into rows.
Pass-through: `EMP_NBR`, `TELE_HOME_PRI_FROM_3` → `TELE_HOME_PRI_FROM`, `TELE_HOME_PRI_TO_3` → `TELE_HOME_PRI_TO`
Pivoted (verbatim): `LKP_CALL_PRTY` from `TELE_HOME_PRI_SEQ_1_3..5_3`

---

### 3.14 Seq 5A — AWAY_SEQ2_TFM (CTransformerStage)

**Link in**: `ASEQ2_OUT_PVT`
**Link out**: `ASEQ2_OUT_TFM` → AWAY_SEQ_FNL

**Derivations (verbatim from XML):**
```
EMP_NBR           = ASEQ2_OUT_PVT.EMP_NBR
CALL_PRTY         = ASEQ2_OUT_PVT.CALL_PRTY+5
TELE_HOME_PRI_FROM = ASEQ2_OUT_PVT.TELE_HOME_PRI_FROM
TELE_HOME_PRI_TO   = ASEQ2_OUT_PVT.TELE_HOME_PRI_TO
LKP_CALL_PRTY     = ASEQ2_OUT_PVT.LKP_CALL_PRTY
```

---

### 3.15 Seq 5B — AWAY_SEQ3_TFM (CTransformerStage)

**Link in**: `ASEQ3_OUT_PVT`
**Link out**: `ASEQ3_OUT_TFM` → AWAY_SEQ_FNL

**Derivations (verbatim from XML):**
```
EMP_NBR           = ASEQ3_OUT_PVT.EMP_NBR
CALL_PRTY         = ASEQ3_OUT_PVT.CALL_PRTY+10
TELE_HOME_PRI_FROM = ASEQ3_OUT_PVT.TELE_HOME_PRI_FROM
TELE_HOME_PRI_TO   = ASEQ3_OUT_PVT.TELE_HOME_PRI_TO
LKP_CALL_PRTY     = ASEQ3_OUT_PVT.LKP_CALL_PRTY
```

---

### 3.16 Seq 5C — HOME_SEQ2_TFM (CTransformerStage)

**Link in**: `HSEQ2_OUT_PVT`
**Link out**: `HSEQ2_OUT_TFM` → HOME_SEQ_FNL

**Derivations (verbatim from XML):**
```
EMP_NBR           = HSEQ2_OUT_PVT.EMP_NBR
CALL_PRTY         = HSEQ2_OUT_PVT.CALL_PRTY+5
TELE_HOME_PRI_FROM = HSEQ2_OUT_PVT.TELE_HOME_PRI_FROM
TELE_HOME_PRI_TO   = HSEQ2_OUT_PVT.TELE_HOME_PRI_TO
LKP_CALL_PRTY     = HSEQ2_OUT_PVT.LKP_CALL_PRTY
```

---

### 3.17 Seq 5D — HOME_SEQ3_TFM (CTransformerStage)

**Link in**: `HSEQ3_OUT_PVT`
**Link out**: `HSEQ3_OUT_TFM` → HOME_SEQ_FNL

**Derivations (verbatim from XML):**
```
EMP_NBR           = HSEQ3_OUT_PVT.EMP_NBR
CALL_PRTY         = HSEQ3_OUT_PVT.CALL_PRTY+10
TELE_HOME_PRI_FROM = HSEQ3_OUT_PVT.TELE_HOME_PRI_FROM
TELE_HOME_PRI_TO   = HSEQ3_OUT_PVT.TELE_HOME_PRI_TO
LKP_CALL_PRTY     = HSEQ3_OUT_PVT.LKP_CALL_PRTY
```

---

### 3.18 Seq 6A — AWAY_SEQ_FNL (PxFunnel)

**Links in**: `ASEQ1_OUT_PVT`, `ASEQ2_OUT_TFM`, `ASEQ3_OUT_TFM`
**Link out**: `ASEQ_OUT_FNL` → INC_AWAY_PRTY_TFM

`df = aseq1.unionByName(aseq2_tfm, allowMissingColumns=True).unionByName(aseq3_tfm, allowMissingColumns=True)`

---

### 3.19 Seq 6B — HOME_SEQ_FNL (PxFunnel)

**Links in**: `HSEQ1_OUT_PVT`, `HSEQ2_OUT_TFM`, `HSEQ3_OUT_TFM`
**Link out**: `HSEQ_OUT_FNL` → INC_PRTY_TFM

`df = hseq1.unionByName(hseq2_tfm, allowMissingColumns=True).unionByName(hseq3_tfm, allowMissingColumns=True)`

---

### 3.20 Seq 7A — INC_AWAY_PRTY_TFM (CTransformerStage)

**Link in**: `ASEQ_OUT_FNL`
**Link out**: `CALL_PRIORITY_LEFT_JNR` → AWAY_BASIC_JNR

**Constraint (verbatim from XML):** `isvalid("int32", ASEQ_OUT_FNL.EMP_NBR) and isvalid("int32", ASEQ_OUT_FNL.LKP_CALL_PRTY)`

**Derivations (verbatim from XML):**
```
EMP_NBR           = ASEQ_OUT_FNL.EMP_NBR
CALL_PRTY         = ASEQ_OUT_FNL.CALL_PRTY + 1
TELE_HOME_PRI_FROM = if not(isvalid("time",left(ASEQ_OUT_FNL.TELE_HOME_PRI_FROM,2):':':right(ASEQ_OUT_FNL.TELE_HOME_PRI_FROM,2):':00')) then setnull() else trimleadingtrailing(ASEQ_OUT_FNL.TELE_HOME_PRI_FROM)
TELE_HOME_PRI_TO   = if not(isvalid("time",left(ASEQ_OUT_FNL.TELE_HOME_PRI_TO,2):':':right(ASEQ_OUT_FNL.TELE_HOME_PRI_TO,2):':00')) then setnull() else trimleadingtrailing(ASEQ_OUT_FNL.TELE_HOME_PRI_TO)
LKP_CALL_PRTY     = ASEQ_OUT_FNL.LKP_CALL_PRTY
CALL_LIST         = 'AWAY'
```

---

### 3.21 Seq 7B — INC_PRTY_TFM (CTransformerStage)

**Link in**: `HSEQ_OUT_FNL`
**Link out**: `CALL_PRIORITY_LEFT_JNR` → HOME_BASIC_JNR

**Constraint (verbatim from XML):** `isvalid("int32", HSEQ_OUT_FNL.EMP_NBR) and isvalid("int32", HSEQ_OUT_FNL.LKP_CALL_PRTY)`

**Derivations (verbatim from XML):**
```
EMP_NBR           = HSEQ_OUT_FNL.EMP_NBR
CALL_PRTY         = HSEQ_OUT_FNL.CALL_PRTY + 1
TELE_HOME_PRI_FROM = if not(isvalid("time",left(HSEQ_OUT_FNL.TELE_HOME_PRI_FROM,2):':':right(HSEQ_OUT_FNL.TELE_HOME_PRI_FROM,2):':00')) then setnull() else trimleadingtrailing(HSEQ_OUT_FNL.TELE_HOME_PRI_FROM)
TELE_HOME_PRI_TO   = if not(isvalid("time",left(HSEQ_OUT_FNL.TELE_HOME_PRI_TO,2):':':right(HSEQ_OUT_FNL.TELE_HOME_PRI_TO,2):':00')) then setnull() else trimleadingtrailing(HSEQ_OUT_FNL.TELE_HOME_PRI_TO)
LKP_CALL_PRTY     = HSEQ_OUT_FNL.LKP_CALL_PRTY
CALL_LIST         = 'HOME'
```

---

### 3.22 Seq 8A — AWAY_BASIC_JNR (PxJoin)

**Links in**: LEFT=`CALL_PRIORITY_LEFT_JNR` (INC_AWAY_PRTY_TFM), RIGHT=`CALL_PRI_RIGHT_JNR` (BASIC_REC_TFM)
**Link out**: `ABREC_OUT_JNR` → ABREC_NEW_PRTY_TFM

**Join type (verbatim from XML):** `innerjoin`
**Join keys (verbatim from XML):** `EMP_NBR`, `LKP_CALL_PRTY`

```python
abrec_df = inc_away_df.join(basic_away_right_df, on=["EMP_NBR", "LKP_CALL_PRTY"], how="inner")
```

---

### 3.23 Seq 8B — HOME_BASIC_JNR (PxJoin)

**Links in**: LEFT=`CALL_PRIORITY_LEFT_JNR` (INC_PRTY_TFM), RIGHT=`CALL_PRIORITY_RIGHT_JNR` (BASIC_REC_TFM)
**Link out**: `HBREC_OUT_JNR` → NEW_PRTY_TFM

**Join type (verbatim from XML):** `innerjoin`
**Join keys (verbatim from XML):** `EMP_NBR`, `LKP_CALL_PRTY`

```python
hbrec_df = inc_home_df.join(basic_home_right_df, on=["EMP_NBR", "LKP_CALL_PRTY"], how="inner")
```

---

### 3.24 Seq 9A — ABREC_NEW_PRTY_TFM (CTransformerStage)

**Link in**: `ABREC_OUT_JNR`
**Links out**: `ABNEW_PRTY_OUT_TFM` → ALL_REC_FNL, `ABCPRTY_OUT_TFM` → MAX_ABPRTY_AGG

**Stage variables (verbatim from XML):**
```
svEmpNbrNew = ABREC_OUT_JNR.EMP_NBR
svCalcNew   = if svEmpNbrNew <> svEmpNbrOld then 1 else svCalcOld + 1
svCalcOld   = svCalcNew
svEmpNbrOld = ABREC_OUT_JNR.EMP_NBR
```
Note: `svCalcNew` implements a per-EMP_NBR row counter (dense_rank starting at 1 when EMP_NBR changes).

**ABNEW_PRTY_OUT_TFM derivations (verbatim from XML):**
```
EMP_NBR       = ABREC_OUT_JNR.EMP_NBR
CALL_LIST     = ABREC_OUT_JNR.CALL_LIST
CALL_PRTY     = svCalcNew
EFF_START_TM  = ABREC_OUT_JNR.TELE_HOME_PRI_FROM
EFF_END_TM    = ABREC_OUT_JNR.TELE_HOME_PRI_TO
PH_ACCESS     = ABREC_OUT_JNR.PH_ACCESS
PH_COMMENTS   = ABREC_OUT_JNR.PH_COMMENTS
PH_TYPE       = ABREC_OUT_JNR.PH_TYPE
UNLISTD_IND   = ABREC_OUT_JNR.UNLISTD_IND
HOME_AWAY_IND = ABREC_OUT_JNR.HOME_AWAY_IND
TEMP_PH_EXP_TS = ABREC_OUT_JNR.TEMP_PH_EXP_TS
PH_NBR        = ABREC_OUT_JNR.PH_NBR
USER_ID       = ABREC_OUT_JNR.USER_ID
UPD_TS        = ABREC_OUT_JNR.UPD_TS
```
**ABCPRTY_OUT_TFM derivations (verbatim from XML):**
```
EMP_NBR   = ABREC_OUT_JNR.EMP_NBR
CALL_PRTY = svCalcNew
```

**PySpark implementation of svCalcNew**: Use `F.dense_rank().over(Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY"))` — produces 1-based sequential number per EMP_NBR group.

---

### 3.25 Seq 9B — NEW_PRTY_TFM (CTransformerStage)

**Link in**: `HBREC_OUT_JNR`
**Links out**: `NPRTY_OUT_TFM` → ALL_REC_FNL, `CPRTY_OUT_TFM` → MAX_PRTY_AGG

**Stage variables (verbatim from XML):**
```
svEmpNbrNew = HBREC_OUT_JNR.EMP_NBR
svCalcNew   = if svEmpNbrNew <> svEmpNbrOld then 1 else svCalcOld + 1
svCalcOld   = svCalcNew
svEmpNbrOld = HBREC_OUT_JNR.EMP_NBR
```

**NPRTY_OUT_TFM derivations (verbatim from XML):**
```
EMP_NBR       = HBREC_OUT_JNR.EMP_NBR
CALL_LIST     = HBREC_OUT_JNR.CALL_LIST
CALL_PRTY     = svCalcNew
EFF_START_TM  = HBREC_OUT_JNR.TELE_HOME_PRI_FROM
EFF_END_TM    = HBREC_OUT_JNR.TELE_HOME_PRI_TO
PH_ACCESS     = HBREC_OUT_JNR.PH_ACCESS
PH_COMMENTS   = HBREC_OUT_JNR.PH_COMMENTS
PH_TYPE       = HBREC_OUT_JNR.PH_TYPE
UNLISTD_IND   = HBREC_OUT_JNR.UNLISTD_IND
HOME_AWAY_IND = HBREC_OUT_JNR.HOME_AWAY_IND
TEMP_PH_EXP_TS = HBREC_OUT_JNR.TEMP_PH_EXP_TS
PH_NBR        = HBREC_OUT_JNR.PH_NBR
USER_ID       = HBREC_OUT_JNR.USER_ID
UPD_TS        = HBREC_OUT_JNR.UPD_TS
```
**CPRTY_OUT_TFM derivations (verbatim from XML):**
```
EMP_NBR   = HBREC_OUT_JNR.EMP_NBR
CALL_PRTY = svCalcNew
```

---

### 3.26 Seq 10A — MAX_ABPRTY_AGG (PxAggregator)

**Link in**: `ABCPRTY_OUT_TFM`
**Link out**: `AMAX_OUT_AGG` → AWAY_MAX_JNR

**Group by (verbatim from XML):** `EMP_NBR`
**Aggregate (verbatim from XML):** `MAX(CALL_PRTY) AS MAX_CALL_PRTY`

```python
away_max_df = abcprty_df.groupBy("EMP_NBR").agg(F.max("CALL_PRTY").alias("MAX_CALL_PRTY"))
```

---

### 3.27 Seq 10B — MAX_PRTY_AGG (PxAggregator)

**Link in**: `CPRTY_OUT_TFM`
**Link out**: `HMAX_OUT_AGG` → BASIC_MAX_JNR

**Group by (verbatim from XML):** `EMP_NBR`
**Aggregate (verbatim from XML):** `MAX(CALL_PRTY) AS MAX_CALL_PRTY`

```python
home_max_df = cprty_df.groupBy("EMP_NBR").agg(F.max("CALL_PRTY").alias("MAX_CALL_PRTY"))
```

---

### 3.28 Seq 11A — AWAY_MAX_JNR (PxJoin)

**Links in**: LEFT=`AMAX_OUT_AGG` (MAX_ABPRTY_AGG), RIGHT=`BASIC_REC_OUT_TFM` (BASIC_REC_TFM)
**Link out**: `DSLink85` → ADJUST_PRTY_TFM

**Join type (verbatim from XML):** `innerjoin`
**Join key (verbatim from XML):** `EMP_NBR`

```python
dslink85_df = away_max_df.join(basic_away_max_right_df, on=["EMP_NBR"], how="inner")
```

---

### 3.29 Seq 11B — BASIC_MAX_JNR (PxJoin)

**Links in**: LEFT=`HMAX_OUT_AGG` (MAX_PRTY_AGG), RIGHT=`BREC_OUT_TFM` (BASIC_REC_TFM)
**Link out**: `BMAX_OUT_JNR` → ADJUSTING_PRTY_TFM

**Join type (verbatim from XML):** `innerjoin`
**Join key (verbatim from XML):** `EMP_NBR`

```python
bmax_df = home_max_df.join(basic_home_max_right_df, on=["EMP_NBR"], how="inner")
```

---

### 3.30 Seq 12A — ADJUST_PRTY_TFM (CTransformerStage)

**Link in**: `DSLink85`
**Link out**: `ADJUST_PRTY_OUT_TFM` → ALL_REC_FNL

**Stage variables (verbatim from XML):**
```
svEmpNbrNew = DSLink85.EMP_NBR
svCalcNew   = if svEmpNbrNew <> svEmpNbrOld then DSLink85.MAX_CALL_PRTY + 1 else svCalcOld + 1
svCalcOld   = svCalcNew
svEmpNbrOld = DSLink85.EMP_NBR
```

**Derivations (verbatim from XML):**
```
EMP_NBR       = DSLink85.EMP_NBR
CALL_LIST     = 'AWAY'
CALL_PRTY     = svCalcNew
EFF_START_TM  = '0001'
EFF_END_TM    = '2359'
PH_ACCESS     = DSLink85.PH_ACCESS
PH_COMMENTS   = DSLink85.PH_COMMENTS
PH_TYPE       = DSLink85.PH_TYPE
UNLISTD_IND   = DSLink85.UNLISTD_IND
HOME_AWAY_IND = DSLink85.HOME_AWAY_IND
TEMP_PH_EXP_TS = DSLink85.TEMP_PH_EXP_TS
PH_NBR        = DSLink85.PH_NBR
USER_ID       = DSLink85.USER_ID
UPD_TS        = DSLink85.UPD_TS
```

**PySpark note on svCalcNew**: When EMP_NBR changes → `MAX_CALL_PRTY + 1`; otherwise `previous + 1`. Use `Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY")` with `F.row_number()` offset from `MAX_CALL_PRTY`:
```python
from pyspark.sql import Window
w = Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY")
adj_away = dslink85_df.withColumn(
    "CALL_PRTY",
    F.col("MAX_CALL_PRTY") + F.row_number().over(w)
)
```

---

### 3.31 Seq 12B — ADJUSTING_PRTY_TFM (CTransformerStage)

**Link in**: `BMAX_OUT_JNR`
**Link out**: `ADJ_PRTY_OUT_TFM` → ALL_REC_FNL

**Stage variables (verbatim from XML):**
```
svEmpNbrNew = BMAX_OUT_JNR.EMP_NBR
svCalcNew   = if svEmpNbrNew <> svEmpNbrOld then BMAX_OUT_JNR.MAX_CALL_PRTY + 1 else svCalcOld + 1
svCalcOld   = svCalcNew
svEmpNbrOld = BMAX_OUT_JNR.EMP_NBR
```

**Derivations (verbatim from XML):**
```
EMP_NBR       = BMAX_OUT_JNR.EMP_NBR
CALL_LIST     = 'HOME'
CALL_PRTY     = svCalcNew
EFF_START_TM  = '0001'
EFF_END_TM    = '2359'
PH_ACCESS     = BMAX_OUT_JNR.PH_ACCESS
PH_COMMENTS   = BMAX_OUT_JNR.PH_COMMENTS
PH_TYPE       = BMAX_OUT_JNR.PH_TYPE
UNLISTD_IND   = BMAX_OUT_JNR.UNLISTD_IND
HOME_AWAY_IND = BMAX_OUT_JNR.HOME_AWAY_IND
TEMP_PH_EXP_TS = BMAX_OUT_JNR.TEMP_PH_EXP_TS
PH_NBR        = BMAX_OUT_JNR.PH_NBR
USER_ID       = BMAX_OUT_JNR.USER_ID
UPD_TS        = BMAX_OUT_JNR.UPD_TS
```

Same PySpark pattern as ADJUST_PRTY_TFM:
```python
adj_home = bmax_df.withColumn("CALL_PRTY", F.col("MAX_CALL_PRTY") + F.row_number().over(w))
```

---

### 3.32 Seq 13 — ALL_REC_FNL (PxFunnel)

**Links in** (7 inputs):
1. `EMERGENCY_IN_TFM` from SEPARATE_PHTYPE_TFM
2. `TEMP_TYPE_TFM` from SEPARATE_PHTYPE_TFM
3. `ALL_BREC_OUT_TFM` from BASIC_REC_TFM
4. `NPRTY_OUT_TFM` from NEW_PRTY_TFM
5. `ABNEW_PRTY_OUT_TFM` from ABREC_NEW_PRTY_TFM
6. `ADJ_PRTY_OUT_TFM` from ADJUSTING_PRTY_TFM
7. `ADJUST_PRTY_OUT_TFM` from ADJUST_PRTY_TFM

**Link out**: `ALL_REC_OUT_FNL` → NULL_VAL_TFM

```python
all_df = (emergency_df
    .unionByName(temp_df, allowMissingColumns=True)
    .unionByName(basic_all_df, allowMissingColumns=True)
    .unionByName(home_new_prty_df, allowMissingColumns=True)
    .unionByName(away_new_prty_df, allowMissingColumns=True)
    .unionByName(adj_home_df, allowMissingColumns=True)
    .unionByName(adj_away_df, allowMissingColumns=True)
)
```

---

### 3.33 Seq 14 — NULL_VAL_TFM (CTransformerStage)

**Link in**: `ALL_REC_OUT_FNL`
**Link out**: `LNK_IN_DW` → TE_EMPLOYEE_PHONE_DW

**Constraint (verbatim from XML):** `isvalid("int64",ALL_REC_OUT_FNL.PH_NBR)`

**Derivations (verbatim from XML):**
```
EMP_NBR    = ALL_REC_OUT_FNL.EMP_NBR
EMP_NO     = If Len(TrimLeadingTrailing(ALL_REC_OUT_FNL.EMP_NBR))=9
             Then TrimLeadingTrailing(ALL_REC_OUT_FNL.EMP_NBR)
             Else Str('0',9-Len(TrimLeadingTrailing(ALL_REC_OUT_FNL.EMP_NBR))):TrimLeadingTrailing(ALL_REC_OUT_FNL.EMP_NBR)
PH_LIST    = ALL_REC_OUT_FNL.PH_LIST
PH_PRTY    = ALL_REC_OUT_FNL.PH_PRTY
EFF_START_TM = left(ALL_REC_OUT_FNL.EFF_START_TM,2):':':right(ALL_REC_OUT_FNL.EFF_START_TM,2):':00'
EFF_END_TM   = left(ALL_REC_OUT_FNL.EFF_END_TM,2):':':right(ALL_REC_OUT_FNL.EFF_END_TM,2):':00'
PH_NBR     = ALL_REC_OUT_FNL.PH_NBR
PH_ACCESS  = if TrimLeadingTrailing(nulltovalue(ALL_REC_OUT_FNL.PH_ACCESS,''))='' or TrimLeadingTrailing(nulltovalue(ALL_REC_OUT_FNL.PH_ACCESS,'')) < ' '
             then setnull()
             else TrimLeadingTrailing(nulltovalue(ALL_REC_OUT_FNL.PH_ACCESS,''))
PH_COMMENTS = if TrimLeadingTrailing(nulltovalue(ALL_REC_OUT_FNL.PH_COMMENTS,''))='' or TrimLeadingTrailing(nulltovalue(ALL_REC_OUT_FNL.PH_COMMENTS,'')) < ' '
              then setnull()
              else TrimLeadingTrailing(nulltovalue(ALL_REC_OUT_FNL.PH_COMMENTS,''))
PH_TYPE    = ALL_REC_OUT_FNL.PH_TYPE
UNLISTD_IND = ALL_REC_OUT_FNL.UNLISTD_IND
HOME_AWAY_IND = ALL_REC_OUT_FNL.HOME_AWAY_IND
TEMP_PH_EXP_TS = ALL_REC_OUT_FNL.TEMP_PH_EXP_TS
TD_LD_TS   = CurrentTimestamp()
```
Note: `PH_PRTY` = `CALL_PRTY` renamed for target. `PH_LIST` = `CALL_LIST` renamed for target.

---

### 3.34 Seq 15 — TE_EMPLOYEE_PHONE_DW (TeradataConnectorPX, Target)

**Link in**: `LNK_IN_DW`
**TableAction (verbatim from XML):** `3` = Truncate + Insert
**WriteMode (verbatim from XML):** `0`
**Work table:** `TE_EMPLOYEE_PHONE_NEW`

**After-SQL (verbatim from XML):**
```sql
DELETE FROM #ENV.$TD_DB#.TE_EMPLOYEE_PHONE;
INSERT INTO #ENV.$TD_DB#.TE_EMPLOYEE_PHONE
  SELECT EMP_NBR,EMP_NO,PH_LIST,PH_PRTY,EFF_START_TM,EFF_END_TM,PH_NBR,
         PH_ACCESS,PH_COMMENTS,PH_TYPE,UNLISTD_IND,HOME_AWAY_IND,TEMP_PH_EXP_TS,TD_LD_TS
  FROM #ENV.$TD_WORK_DB#.TE_EMPLOYEE_PHONE_NEW;
```

In Databricks: write to `TE_EMPLOYEE_PHONE_NEW` (truncate+overwrite), then run After-SQL equivalent as Spark SQL.

---

## 4. DataStage Expression Dictionary

Only functions actually present in this pipeline's expressions are listed.

| DataStage Expression | PySpark Equivalent |
|---|---|
| `If X Then Y Else Z` | `F.when(X, Y).otherwise(Z)` |
| `IsValid("int32", col)` | `F.col("col").cast("int").isNotNull()` |
| `IsValid("int64", col)` | `F.col("col").cast("long").isNotNull()` |
| `IsValid("timestamp", expr)` | `F.to_timestamp(expr).isNotNull()` |
| `IsValid("time", expr)` | `F.to_timestamp(expr, "HH:mm:ss").isNotNull()` |
| `Not(expr)` | `~(expr)` |
| `IsNull(col)` | `F.col("col").isNull()` |
| `SetNull()` | `F.lit(None).cast(target_type)` |
| `NullToValue(col, val)` | `F.coalesce(F.col("col"), F.lit(val))` |
| `TrimLeadingTrailing(col)` | `F.trim(F.col("col"))` |
| `TRIM(BOTH ' ' FROM col)` | `F.trim(F.col("col"))` |
| `Len(expr)` | `F.length(expr)` |
| `Left(col, n)` | `F.substring(F.col("col"), 1, n)` |
| `Right(col, n)` | `F.expr(f"right(col, {n})")` or `F.substring(F.col("col"), -n, n)` |
| `Str('0', n)` | `F.repeat(F.lit("0"), n)` |
| `X : Y` (concatenation) | `F.concat(X, Y)` |
| `CurrentTimestamp()` | `F.current_timestamp()` |
| `If Len(TrimLeadingTrailing(EMP_NBR))=9 Then ... Else Str('0',9-Len(...)):...` | `F.when(F.length(F.trim(F.col("EMP_NBR")))==9, F.trim(F.col("EMP_NBR"))).otherwise(F.concat(F.repeat(F.lit("0"), F.lit(9)-F.length(F.trim(F.col("EMP_NBR")))), F.trim(F.col("EMP_NBR"))))` |
| `svCalcNew` (DataStage stage variable row counter) | `F.dense_rank().over(Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY"))` or `F.row_number().over(...)` |
| `if TrimLeadingTrailing(nulltovalue(col,''))='' or ... < ' ' then setnull() else TrimLeadingTrailing(nulltovalue(col,''))` | `F.when((F.trim(F.coalesce(F.col("col"), F.lit("")))=="") \| (F.trim(F.coalesce(F.col("col"), F.lit(""))) < F.lit(" ")), F.lit(None).cast("string")).otherwise(F.trim(F.coalesce(F.col("col"), F.lit(""))))` |

---

## 5. Column Mappings

### 5.1 Target: TE_EMPLOYEE_PHONE (via TE_EMPLOYEE_PHONE_DW)

| # | Target Column | Source Column | Mapping Type | Derivation Logic |
|---|---|---|---|---|
| 1 | EMP_NBR | TELE_EMP_NBR (aliased EMP_NBR in SQL) | Pass-through | `ALL_REC_OUT_FNL.EMP_NBR` |
| 2 | EMP_NO | EMP_NBR | Derived | `If Len(TrimLeadingTrailing(EMP_NBR))=9 Then TrimLeadingTrailing(EMP_NBR) Else Str('0',9-Len(TrimLeadingTrailing(EMP_NBR))):TrimLeadingTrailing(EMP_NBR)` — pad to 9 chars with leading zeros |
| 3 | PH_LIST | CALL_LIST constant per phone type | Constant | EMERGENCY='EMERGENCY', TEMP='TEMP', BASIC='BASIC', HOME='HOME', AWAY='AWAY' |
| 4 | PH_PRTY | CALL_PRTY (multi-stage priority chain) | Derived | Complex: pivot index +1 → join → density-ranked sequence number |
| 5 | EFF_START_TM | EFF_START_TM (4-char HHMM string) | Derived | `left(EFF_START_TM,2):':':right(EFF_START_TM,2):':00'` → HH:MM:SS |
| 6 | EFF_END_TM | EFF_END_TM (4-char HHMM string) | Derived | `left(EFF_END_TM,2):':':right(EFF_END_TM,2):':00'` → HH:MM:SS |
| 7 | PH_NBR | TELE_EMGR_PH_AREA_CD\|\|PREFIX\|\|NUM etc. | Derived | Source SQL concatenation: `TRIM(BOTH ' ' FROM area)\|\|TRIM(prefix)\|\|TRIM(num)` |
| 8 | PH_ACCESS | TELE_PH_ACCESS (via BASIC pivot) | Derived | `if TrimLeadingTrailing(nulltovalue(PH_ACCESS,''))='' or ... < ' ' then setnull() else TrimLeadingTrailing(nulltovalue(PH_ACCESS,''))` |
| 9 | PH_COMMENTS | TELE_EMGR_PH_NAME / TELE_PH_COMMENT | Derived | Same null/trim logic as PH_ACCESS |
| 10 | PH_TYPE | TELE_PH_TYPE (via BASIC pivot); SETNULL for EMERGENCY/TEMP | Pass-through | `ALL_REC_OUT_FNL.PH_TYPE` |
| 11 | UNLISTD_IND | TELE_PH_UNLIST_CD (via BASIC pivot); SETNULL for EMERGENCY/TEMP | Pass-through | `ALL_REC_OUT_FNL.UNLISTD_IND` |
| 12 | HOME_AWAY_IND | TELE_PH_HOME_AWAY_CD (via BASIC pivot); SETNULL for EMERGENCY/TEMP | Pass-through | `ALL_REC_OUT_FNL.HOME_AWAY_IND` |
| 13 | TEMP_PH_EXP_TS | TELE_TEMP_PH_DATE + TELE_TEMP_PH_TIME (TEMP branch only); SETNULL otherwise | Pass-through | `ALL_REC_OUT_FNL.TEMP_PH_EXP_TS` |
| 14 | TD_LD_TS | (none — system clock) | Constant | `CurrentTimestamp()` |

Additional columns present in the pipeline but not in final TE_EMPLOYEE_PHONE:

| # | Column | Used in | Purpose |
|---|---|---|---|
| 15 | USER_ID | All branches | Passed through; derived from TELE_LAST_UPDATED_BY; NULL if blank |
| 16 | UPD_TS | All branches | Timestamp from TELE_LAST_UPDATED_DATE+TIME; NULL if blank/invalid |

### 5.2 Target: Error sequential file (via WSSEN_ERR_SEQ)

Constraint: `Not (IsValid("int32", (LNK_OUT_TDC.EMP_NBR)))` — records with invalid EMP_NBR.

| # | Target Column | Source Column | Mapping Type | Derivation Logic |
|---|---|---|---|---|
| 1 | EMP_NBR | TELE_EMP_NBR | Pass-through | `LNK_OUT_TDC.EMP_NBR` |
| 2 | PH_NBR | TELE_EMGR_PH_AREA_CD\|\|PREFIX\|\|NUM | Derived | SQL-derived concatenated phone number |
| 3 | TEMP_PH_NBR | TELE_TEMP_PH_AREA_CD\|\|PREFIX\|\|NUM | Derived | SQL-derived TEMP phone number |
| 4 | BASIC_PH_NBR_1 | TELE_PH_AREA_CD\|\|PREFIX\|\|NUM | Derived | SQL-derived basic phone 1 |
| 5 | BASIC_PH_NBR_2 | TELE_PH_AREA_CD_2\|\|PREFIX_2\|\|NUM_2 | Derived | SQL-derived basic phone 2 |
| 6 | BASIC_PH_NBR_3 | TELE_PH_AREA_CD_3\|\|PREFIX_3\|\|NUM_3 | Derived | SQL-derived basic phone 3 |
| 7 | BASIC_PH_NBR_4 | TELE_PH_AREA_CD_4\|\|PREFIX_4\|\|NUM_4 | Derived | SQL-derived basic phone 4 |
| 8 | BASIC_PH_NBR_5 | TELE_PH_AREA_CD_5\|\|PREFIX_5\|\|NUM_5 | Derived | SQL-derived basic phone 5 |

---

## 6. Routing / Branching Logic

### 6.1 SEPARATE_PHTYPE_TFM — 6-way router

All constraints are **verbatim from XML** (`ParsedConstraint` / `Constraint` properties):

| Output Link | Destination | Verbatim Constraint |
|---|---|---|
| `EMERGENCY_IN_TFM` | ALL_REC_FNL | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR) and isvalid("int64",LNK_OUT_TDC.PH_NBR)` |
| `TEMP_TYPE_TFM` | ALL_REC_FNL | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR) and isvalid("int64",LNK_OUT_TDC.TEMP_PH_NBR)` |
| `BASIC_TYPE_TFM` | BASIC_TYPE_PVT | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR)` |
| `HOME_TYPE_CPY` | HOME_CPY | (no explicit constraint — subset with HOME priority data) |
| `AWAY_TYPE_CPY` | AWAY_CPY | (no explicit constraint — subset with AWAY priority data; AWAY columns remapped to TELE_HOME_PRI_* names) |
| `LNK_OUT_ERR_SEQ` | WSSEN_ERR_SEQ | `Not (IsValid("int32", (LNK_OUT_TDC.EMP_NBR)))` |

**Note**: HOME and AWAY branches receive all valid-EMP_NBR records; pivot stages will naturally produce null rows for employees with no HOME/AWAY sequence data.

### 6.2 BASIC_REC_TFM — 5-way router

All constraints are **verbatim from XML**:

| Output Link | Destination | Verbatim Constraint |
|---|---|---|
| `ALL_BREC_OUT_TFM` | ALL_REC_FNL | `isvalid("int64",BREC_OUT_PVT.PH_NBR)` |
| `CALL_PRIORITY_RIGHT_JNR` | HOME_BASIC_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='H')` |
| `BREC_OUT_TFM` | BASIC_MAX_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='H')` |
| `CALL_PRI_RIGHT_JNR` | AWAY_BASIC_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='A')` |
| `BASIC_REC_OUT_TFM` | AWAY_MAX_JNR | `isvalid("int64",BREC_OUT_PVT.PH_NBR) and not(isnull(BREC_OUT_PVT.HOME_AWAY_IND)) and (((BREC_OUT_PVT.HOME_AWAY_IND))='B' or ((BREC_OUT_PVT.HOME_AWAY_IND))='A')` |

### 6.3 NEW_PRTY_TFM — 2-way router

| Output Link | Destination | Verbatim Constraint |
|---|---|---|
| `NPRTY_OUT_TFM` | ALL_REC_FNL | (no constraint — all rows go to ALL_REC_FNL) |
| `CPRTY_OUT_TFM` | MAX_PRTY_AGG | (no constraint — same rows also go to aggregator) |

Both outputs receive the same rows (DataFrame is split to two consumers).

### 6.4 ABREC_NEW_PRTY_TFM — 2-way router

| Output Link | Destination | Verbatim Constraint |
|---|---|---|
| `ABNEW_PRTY_OUT_TFM` | ALL_REC_FNL | (no constraint — all rows) |
| `ABCPRTY_OUT_TFM` | MAX_ABPRTY_AGG | (no constraint — same rows also go to aggregator) |

Both outputs receive the same rows (DataFrame cached and reused).

### 6.5 INC_PRTY_TFM — output constraint

**Verbatim from XML:** `isvalid("int32", HSEQ_OUT_FNL.EMP_NBR) and isvalid("int32", HSEQ_OUT_FNL.LKP_CALL_PRTY)`

### 6.6 INC_AWAY_PRTY_TFM — output constraint

**Verbatim from XML:** `isvalid("int32", ASEQ_OUT_FNL.EMP_NBR) and isvalid("int32", ASEQ_OUT_FNL.LKP_CALL_PRTY)`

### 6.7 NULL_VAL_TFM — output constraint

**Verbatim from XML:** `isvalid("int64",ALL_REC_OUT_FNL.PH_NBR)`

---

## 7. Pipeline Config Template

```yaml
pipeline:
  job_name: CREW_J_TE_EMPLOYEE_PHONE_DW
  run_mode: full_refresh
  catalog: "${UNITY_CATALOG}"
  schema: "${UNITY_SCHEMA}"

source:
  type: jdbc
  jdbc_options:
    url: "${TERADATA_JDBC_URL}"
    dbtable: "(SELECT TELE_EMP_NBR AS EMP_NBR, TELE_LAST_UPDATED_BY AS USER_ID, TELE_LAST_UPDATED_DATE, TELE_LAST_UPDATED_TIME, TELE_EMGR_PH_NAME AS PH_COMMENTS, TRIM(BOTH ' ' FROM TELE_EMGR_PH_AREA_CD)||TRIM(BOTH ' ' FROM TELE_EMGR_PH_PREFIX)||TRIM(BOTH ' ' FROM TELE_EMGR_PH_NUM) AS PH_NBR, ... FROM ${TD_WORK_DB}.CREW_WSTELE_LND) t"
    driver: "com.teradata.jdbc.TeraDriver"
    fetchsize: 10000
    numPartitions: 4
    partitionColumn: "EMP_NBR"
  td_work_db: "${TD_WORK_DB}"

target:
  work_table: "${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE_NEW"
  live_table: "${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE"
  write_mode: truncate_insert
  after_sql: |
    DELETE FROM ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE;
    INSERT INTO ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE
      SELECT EMP_NBR, EMP_NO, PH_LIST, PH_PRTY, EFF_START_TM, EFF_END_TM,
             PH_NBR, PH_ACCESS, PH_COMMENTS, PH_TYPE, UNLISTD_IND,
             HOME_AWAY_IND, TEMP_PH_EXP_TS, TD_LD_TS
      FROM ${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE_NEW;

error:
  enabled: true
  table: "${UNITY_CATALOG}.${UNITY_SCHEMA}.TE_EMPLOYEE_PHONE_ERR"
  original_file: "${LOG_DIR}/EMPLOYEE_PHONE.err"
  write_mode: overwrite

logging:
  log_table: "${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_job_log"
  audit_table: "${UNITY_CATALOG}.${UNITY_SCHEMA}.etl_audit_log"
```

---

## 8. Module Structure

```
jobs/CREW_J_TE_EMPLOYEE_PHONE_DW/
├── transformations.py      ← all transform functions (pure functions)
├── run_pipeline.py         ← orchestrator (extends PipelineRunner)
└── tests/
    ├── test_transformations.py
    └── fixtures/
        └── sample_input.py
```

### `transformations.py` — function signatures and responsibilities

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from typing import Dict, Any, Tuple

def read_source(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """Read CREW_WSTELE_LND via JDBC with full source SQL.
    Applies all TRIM(BOTH ' ' FROM ...) concatenations at source.
    Returns wide DataFrame with all BASIC_PH_NBR_1..5, HOME/AWAY seq columns.
    """

def route_by_phone_type(
    df: DataFrame, config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """Implements SEPARATE_PHTYPE_TFM 6-way routing.
    Returns: (emergency_df, temp_df, basic_df, home_df, away_df, error_df)
    Applies verbatim constraints from XML.
    Sets CALL_LIST constants, CALL_PRTY=1, EFF_START_TM/EFF_END_TM constants.
    Derives UPD_TS and TEMP_PH_EXP_TS from date+time columns.
    """

def pivot_basic_phones(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Implements BASIC_TYPE_PVT (PxPivot).
    Unpivots BASIC_PH_NBR_1..5 and associated columns into one row per phone.
    Uses stack() expression. Returns DataFrame with CALL_PRTY as 0-based index.
    """

def apply_basic_rec_tfm(df: DataFrame, config: Dict[str, Any]) -> Dict[str, DataFrame]:
    """Implements BASIC_REC_TFM 5-way split.
    Input: pivot output (CALL_PRTY 0-based).
    Applies CALL_PRTY+1 (1-based), filters by verbatim constraints.
    Returns dict with keys: 'all_rec', 'home_jnr_right', 'home_max_right',
                             'away_jnr_right', 'away_max_right'
    """

def pivot_sequence_phones(
    df: DataFrame, seq_num: int, phone_type: str, config: Dict[str, Any]
) -> DataFrame:
    """Generic pivot for HOME/AWAY SEQ1/2/3 stages.
    seq_num: 1, 2, or 3. phone_type: 'HOME' or 'AWAY'.
    Pivots TELE_HOME_PRI_SEQ_1_N..5_N into LKP_CALL_PRTY rows.
    """

def apply_seq_offset_tfm(
    df: DataFrame, offset: int, config: Dict[str, Any]
) -> DataFrame:
    """Implements HOME/AWAY_SEQ2_TFM (+5) and SEQ3_TFM (+10).
    Adds offset to CALL_PRTY column.
    """

def build_home_pipeline(
    home_df: DataFrame,
    basic_home_jnr_right_df: DataFrame,
    config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame]:
    """Implements HOME branch: HOME_SEQ1/2/3_PVT → SEQ2/3_TFM → HOME_SEQ_FNL
    → INC_PRTY_TFM → HOME_BASIC_JNR → NEW_PRTY_TFM.
    Returns: (nprty_df_for_all_rec, cprty_df_for_max_agg)
    """

def build_away_pipeline(
    away_df: DataFrame,
    basic_away_jnr_right_df: DataFrame,
    config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame]:
    """Implements AWAY branch: AWAY_SEQ1/2/3_PVT → SEQ2/3_TFM → AWAY_SEQ_FNL
    → INC_AWAY_PRTY_TFM → AWAY_BASIC_JNR → ABREC_NEW_PRTY_TFM.
    Returns: (abnew_prty_df_for_all_rec, abcprty_df_for_max_agg)
    """

def calc_home_max_priority(
    cprty_df: DataFrame,
    basic_home_max_right_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Implements MAX_PRTY_AGG → BASIC_MAX_JNR → ADJUSTING_PRTY_TFM.
    Groups by EMP_NBR, MAX(CALL_PRTY).
    Inner joins on EMP_NBR.
    Applies svCalcNew: MAX_CALL_PRTY + row_number() per EMP_NBR.
    Returns adj_home_df for ALL_REC_FNL.
    """

def calc_away_max_priority(
    abcprty_df: DataFrame,
    basic_away_max_right_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Implements MAX_ABPRTY_AGG → AWAY_MAX_JNR → ADJUST_PRTY_TFM.
    Groups by EMP_NBR, MAX(CALL_PRTY).
    Inner joins on EMP_NBR.
    Applies svCalcNew: MAX_CALL_PRTY + row_number() per EMP_NBR.
    Returns adj_away_df for ALL_REC_FNL.
    """

def merge_all_records(*dfs: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Implements ALL_REC_FNL (7-stream unionByName).
    Accepts: emergency_df, temp_df, basic_all_df, home_new_prty_df,
             away_new_prty_df, adj_home_df, adj_away_df
    """

def validate_and_finalize(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Implements NULL_VAL_TFM.
    Filter: isvalid("int64", PH_NBR) — cast PH_NBR to long, keep non-null.
    Derives EMP_NO (zero-pad to 9 chars).
    Formats EFF_START_TM/EFF_END_TM from HHMM to HH:MM:SS.
    Cleans PH_ACCESS and PH_COMMENTS (null if blank or < ' ').
    Adds TD_LD_TS = F.current_timestamp().
    Renames CALL_LIST → PH_LIST, CALL_PRTY → PH_PRTY.
    """

def write_target(spark: SparkSession, df: DataFrame, config: Dict[str, Any]) -> None:
    """Write to work table TE_EMPLOYEE_PHONE_NEW (truncate+insert).
    Execute After-SQL to promote to TE_EMPLOYEE_PHONE live table.
    """

def write_errors(spark: SparkSession, error_df: DataFrame, config: Dict[str, Any]) -> None:
    """Write error records to TE_EMPLOYEE_PHONE_ERR Delta table.
    Original DataStage target: EMPLOYEE_PHONE.err sequential file.
    """
```

---

## 9. Test Specifications

### File structure

```
jobs/CREW_J_TE_EMPLOYEE_PHONE_DW/tests/
├── test_transformations.py
└── fixtures/
    └── sample_input.py
```

### Fixture schema (`sample_input.py`)

Source DataFrame schema (from SQL aliases):
```python
SOURCE_SCHEMA = [
    "EMP_NBR",           # int (TELE_EMP_NBR)
    "USER_ID",           # string (TELE_LAST_UPDATED_BY)
    "TELE_LAST_UPDATED_DATE",  # string YYMMDD
    "TELE_LAST_UPDATED_TIME",  # string HHMM
    "PH_COMMENTS",       # string (TELE_EMGR_PH_NAME for EMERGENCY)
    "PH_NBR",            # string (area+prefix+num concatenated)
    "TEMP_PH_NBR",       # string
    "TEMP_PH_ACCESS",    # string
    "TEMP_PH_COMMENTS",  # string
    "TELE_TEMP_PH_DATE", # string
    "TELE_TEMP_PH_TIME", # string
    "BASIC_PH_NBR_1",    # string
    "BASIC_PH_ACCESS_1", # string
    "BASIC_PH_COMMENTS_1", # string
    "BASIC_PH_TYPE_1",   # string
    "BASIC_PH_UNLIST_CD_1", # string
    "BASIC_PH_HOME_AWAY_CD_1", # string
    # ... BASIC_PH_NBR_2..5 with same pattern
    "TELE_HOME_PRI_FROM_1", "TELE_HOME_PRI_TO_1",
    "TELE_HOME_PRI_SEQ_1_1","TELE_HOME_PRI_SEQ_2_1","TELE_HOME_PRI_SEQ_3_1",
    "TELE_HOME_PRI_SEQ_4_1","TELE_HOME_PRI_SEQ_5_1",
    # ... FROM/TO/SEQ for _2 and _3 groups
    "TELE_AWAY_PRI_FROM_1", "TELE_AWAY_PRI_TO_1",
    "TELE_AWAY_PRI_SEQ_1_1", # ... through _5_3
]
```

### Test cases (`test_transformations.py`)

```python
from chispa.dataframe_comparer import assert_df_equality

class TestRoutByPhoneType:
    def test_emergency_routing(self, spark):
        """Valid EMP_NBR (int32), valid PH_NBR (int64) → emergency_df.
        EMP_NBR=12345, PH_NBR='4085551234' → CALL_LIST='EMERGENCY', CALL_PRTY=1, EFF_START_TM='0000'"""

    def test_temp_routing(self, spark):
        """Valid EMP_NBR, valid TEMP_PH_NBR → temp_df.
        CALL_LIST='TEMP', CALL_PRTY=1, EFF_START_TM='0000'"""

    def test_basic_routing(self, spark):
        """Valid EMP_NBR → basic_df (even if PH_NBR invalid).
        CALL_LIST='BASIC', EFF_START_TM='0001'"""

    def test_error_routing(self, spark):
        """EMP_NBR='ABC' (not valid int32) → error_df.
        Not (IsValid("int32", EMP_NBR)) satisfied."""

    def test_null_emp_nbr_to_error(self, spark):
        """null EMP_NBR → error_df (IsValid("int32", null) = False)"""

    def test_emergency_sets_null_fields(self, spark):
        """EMERGENCY records: PH_ACCESS=null, PH_TYPE=null, UNLISTD_IND=null,
        HOME_AWAY_IND=null, TEMP_PH_EXP_TS=null"""

    def test_temp_ph_exp_ts_valid(self, spark):
        """TEMP record with valid date '251231' time '1430' →
        TEMP_PH_EXP_TS='2025-12-31 14:30:00'"""

    def test_temp_ph_exp_ts_blank_date(self, spark):
        """TEMP record with blank TELE_TEMP_PH_DATE → TEMP_PH_EXP_TS=null"""

    def test_upd_ts_valid(self, spark):
        """Valid TELE_LAST_UPDATED_DATE='251231' TIME='1430' →
        UPD_TS='2025-12-31 14:30:00'"""

    def test_upd_ts_invalid_timestamp(self, spark):
        """TELE_LAST_UPDATED_DATE='991301' (invalid month 13) → UPD_TS=null"""

class TestPivotBasicPhones:
    def test_five_phones_produce_five_rows(self, spark):
        """1 employee with 5 BASIC phones → 5 rows, CALL_PRTY 0..4"""

    def test_null_phone_produces_null_row(self, spark):
        """Employee with BASIC_PH_NBR_3=null → row 3 has PH_NBR=null"""

    def test_pass_through_columns_preserved(self, spark):
        """EMP_NBR, CALL_LIST, EFF_START_TM, EFF_END_TM same on all 5 rows"""

class TestApplyBasicRecTfm:
    def test_call_prty_incremented(self, spark):
        """CALL_PRTY input=0 → output=1; input=4 → output=5"""

    def test_home_away_b_in_both_branches(self, spark):
        """HOME_AWAY_IND='B' → appears in both home_jnr_right AND away_jnr_right"""

    def test_home_away_h_only_in_home_branch(self, spark):
        """HOME_AWAY_IND='H' → only in home_jnr_right, not away"""

    def test_home_away_a_only_in_away_branch(self, spark):
        """HOME_AWAY_IND='A' → only in away_jnr_right, not home"""

    def test_invalid_ph_nbr_excluded_from_all_rec(self, spark):
        """PH_NBR not castable to int64 → excluded from all_rec output"""

    def test_lkp_call_prty_equals_call_prty(self, spark):
        """LKP_CALL_PRTY = CALL_PRTY (after +1 increment)"""

class TestSeqPivot:
    def test_home_seq1_pivot_5_rows(self, spark):
        """5 TELE_HOME_PRI_SEQ columns → 5 rows with LKP_CALL_PRTY values"""

    def test_seq2_offset_adds_5(self, spark):
        """CALL_PRTY=0 input → after SEQ2_TFM: CALL_PRTY=5"""

    def test_seq3_offset_adds_10(self, spark):
        """CALL_PRTY=0 input → after SEQ3_TFM: CALL_PRTY=10"""

class TestIncPrtyTfm:
    def test_call_prty_incremented_by_1(self, spark):
        """CALL_PRTY+1 applied; CALL_LIST='HOME'"""

    def test_invalid_emp_nbr_filtered(self, spark):
        """isvalid("int32", EMP_NBR) fails → row excluded"""

    def test_invalid_lkp_call_prty_filtered(self, spark):
        """isvalid("int32", LKP_CALL_PRTY) fails → row excluded"""

    def test_valid_home_pri_from_preserved(self, spark):
        """TELE_HOME_PRI_FROM='0830' → validated time format, kept"""

    def test_invalid_home_pri_from_nulled(self, spark):
        """TELE_HOME_PRI_FROM='9999' → not valid time → set to null"""

class TestNewPrtyTfm:
    def test_svCalcNew_resets_per_emp_nbr(self, spark):
        """EMP_NBR changes → CALL_PRTY resets to 1"""

    def test_svCalcNew_increments_within_group(self, spark):
        """Same EMP_NBR, multiple rows → CALL_PRTY = 1, 2, 3, ..."""

    def test_nprty_and_cprty_same_rows(self, spark):
        """Both NPRTY and CPRTY outputs have same rows (DataFrame split)"""

class TestMaxPriorityAgg:
    def test_group_by_emp_nbr(self, spark):
        """2 employees, 3 rows each → 2 rows in output"""

    def test_max_call_prty_correct(self, spark):
        """EMP_NBR=1: CALL_PRTY=1,2,3 → MAX_CALL_PRTY=3"""

class TestAdjustPrtyTfm:
    def test_adj_prty_starts_at_max_plus_1(self, spark):
        """First row for EMP_NBR: CALL_PRTY = MAX_CALL_PRTY + 1"""

    def test_adj_prty_increments_within_group(self, spark):
        """Second row: MAX_CALL_PRTY + 2"""

    def test_call_list_set_to_home(self, spark):
        """ADJUSTING_PRTY_TFM sets CALL_LIST='HOME'"""

    def test_call_list_set_to_away(self, spark):
        """ADJUST_PRTY_TFM sets CALL_LIST='AWAY'"""

class TestValidateAndFinalize:
    def test_invalid_ph_nbr_filtered(self, spark):
        """PH_NBR='ABC' (not int64) → row excluded"""

    def test_emp_no_9_chars_no_pad(self, spark):
        """EMP_NBR='123456789' (9 chars) → EMP_NO='123456789'"""

    def test_emp_no_7_chars_padded(self, spark):
        """EMP_NBR='1234567' (7 chars) → EMP_NO='001234567'"""

    def test_eff_start_tm_formatted(self, spark):
        """EFF_START_TM='0000' → '00:00:00'; '0830' → '08:30:00'"""

    def test_eff_end_tm_formatted(self, spark):
        """EFF_END_TM='2359' → '23:59:00'"""

    def test_ph_access_blank_nulled(self, spark):
        """PH_ACCESS='' → null"""

    def test_ph_access_preserved_if_valid(self, spark):
        """PH_ACCESS='  5551  ' → '5551' (trimmed)"""

    def test_ph_comments_blank_nulled(self, spark):
        """PH_COMMENTS=' ' → null"""

    def test_td_ld_ts_is_timestamp(self, spark):
        """TD_LD_TS is not null and is a timestamp"""

    def test_call_list_renamed_to_ph_list(self, spark):
        """Output column PH_LIST present, CALL_LIST absent"""

    def test_call_prty_renamed_to_ph_prty(self, spark):
        """Output column PH_PRTY present, CALL_PRTY absent"""
```

### Test data pattern (fixtures/sample_input.py)

```python
# Use source.type="test_dataframe" — register as Spark temp views

VALID_EMP_NBR = 12345
VALID_PH_NBR = "4085551234"        # 10-char string, castable to int64
VALID_TEMP_PH_NBR = "4085559999"
INVALID_EMP_NBR = "ABC"            # fails IsValid("int32")
INVALID_PH_NBR = "NOTANUMBER"      # fails IsValid("int64")
BLANK_PH_ACCESS = "   "
HOME_AWAY_IND_HOME = "H"
HOME_AWAY_IND_AWAY = "A"
HOME_AWAY_IND_BOTH = "B"

def create_source_view(spark, rows):
    df = spark.createDataFrame(rows, schema=SOURCE_SCHEMA)
    df.createOrReplaceTempView("source_view")
    return df
```

---

## 10. Execution Order

```python
from jobs.CREW_J_TE_EMPLOYEE_PHONE_DW.transformations import (
    read_source, route_by_phone_type, pivot_basic_phones,
    apply_basic_rec_tfm, pivot_sequence_phones, apply_seq_offset_tfm,
    build_home_pipeline, build_away_pipeline,
    calc_home_max_priority, calc_away_max_priority,
    merge_all_records, validate_and_finalize,
    write_target, write_errors,
)

# Step 1: Read source (WSTELE_LND_TDC)
source_df = read_source(spark, config)
logger.info(f"source_df row count: {source_df.count()}")

# Step 2: Route by phone type (SEPARATE_PHTYPE_TFM — 6-way split)
emergency_df, temp_df, basic_raw_df, home_df, away_df, error_df = \
    route_by_phone_type(source_df, config)

# Step 3A: Write errors immediately (WSSEN_ERR_SEQ)
write_errors(spark, error_df, config)

# Step 3B: BASIC branch
#   BASIC_TYPE_PVT (pivot 5 phones → rows)
basic_pivoted_df = pivot_basic_phones(basic_raw_df, config)
#   BASIC_REC_TFM (5-way split)
basic_branches = apply_basic_rec_tfm(basic_pivoted_df, config)
basic_all_df         = basic_branches["all_rec"]          # → ALL_REC_FNL
basic_home_jnr_right = basic_branches["home_jnr_right"]   # → HOME_BASIC_JNR RIGHT
basic_home_max_right = basic_branches["home_max_right"]   # → BASIC_MAX_JNR RIGHT
basic_away_jnr_right = basic_branches["away_jnr_right"]   # → AWAY_BASIC_JNR RIGHT
basic_away_max_right = basic_branches["away_max_right"]   # → AWAY_MAX_JNR RIGHT

# Step 3C: HOME branch (HOME_CPY → 3x PxPivot → SEQ2/3_TFM → HOME_SEQ_FNL
#           → INC_PRTY_TFM → HOME_BASIC_JNR → NEW_PRTY_TFM)
home_new_prty_df, home_cprty_df = build_home_pipeline(
    home_df, basic_home_jnr_right, config
)

# Step 3D: AWAY branch (AWAY_CPY → 3x PxPivot → SEQ2/3_TFM → AWAY_SEQ_FNL
#           → INC_AWAY_PRTY_TFM → AWAY_BASIC_JNR → ABREC_NEW_PRTY_TFM)
away_new_prty_df, away_cprty_df = build_away_pipeline(
    away_df, basic_away_jnr_right, config
)

# Step 4A: HOME max priority chain
#   MAX_PRTY_AGG → BASIC_MAX_JNR → ADJUSTING_PRTY_TFM
adj_home_df = calc_home_max_priority(home_cprty_df, basic_home_max_right, config)
logger.info(f"adj_home_df row count: {adj_home_df.count()}")

# Step 4B: AWAY max priority chain
#   MAX_ABPRTY_AGG → AWAY_MAX_JNR → ADJUST_PRTY_TFM
adj_away_df = calc_away_max_priority(away_cprty_df, basic_away_max_right, config)
logger.info(f"adj_away_df row count: {adj_away_df.count()}")

# Step 5: Merge all 7 streams (ALL_REC_FNL)
all_df = merge_all_records(
    emergency_df,
    temp_df,
    basic_all_df,
    home_new_prty_df,
    away_new_prty_df,
    adj_home_df,
    adj_away_df,
    config=config,
)
logger.info(f"all_df row count: {all_df.count()}")

# Step 6: Final validation and formatting (NULL_VAL_TFM)
#   Filter: isvalid("int64", PH_NBR)
#   Derive: EMP_NO, EFF_START_TM/EFF_END_TM formatting
#   Null-check: PH_ACCESS, PH_COMMENTS
#   Add: TD_LD_TS = CurrentTimestamp()
final_df = validate_and_finalize(all_df, config)
logger.info(f"final_df row count: {final_df.count()}")

# Step 7: Write to target (TE_EMPLOYEE_PHONE_DW)
#   Truncate TE_EMPLOYEE_PHONE_NEW, insert, then After-SQL promotes to live
write_target(spark, final_df, config)
```

---

## 11. Project Requirements

- PySpark only — no pandas UDFs unless absolutely necessary
- Delta Lake format exclusively — no Parquet/CSV for intermediate or target tables
- Unity Catalog compatible — all tables use `catalog.schema.table` from config dict
- Config-driven — no hardcoded table names, column names, or connection strings anywhere in code
- Type annotations required on all function signatures
- Google-style docstrings on all modules and public functions
- Ruff linting: `line-length=120`, `select=["E","F","I","N","W","UP","S","B","A","C4","SIM"]`
- Black formatting: `line-length=120`
- Pytest with chispa: minimum 80% code coverage gate
- Pure functions: every transform takes `(DataFrame, config: Dict[str, Any]) -> DataFrame` or `-> Tuple[DataFrame, ...]`
- Logging: `logger.info()` at each stage boundary with row counts using `DatabricksJobLogger`
- Error handling: `try/except` per phase, write partial results to error Delta table, mark audit as FAILED
- Tests use `source.type="test_dataframe"` — register synthetic data as Spark temp views, no JDBC required
- `DataFrame.cache()` must be applied to DataFrames that feed multiple downstream consumers (e.g., BASIC_REC_TFM outputs, NEW_PRTY_TFM/ABREC_NEW_PRTY_TFM both-outputs)
- Column renaming at final step only: `CALL_LIST → PH_LIST`, `CALL_PRTY → PH_PRTY` in `validate_and_finalize()`
- The `svCalcNew` DataStage stage variable pattern is a sequential row counter that resets per `EMP_NBR` group; implement with `Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY")` and `F.row_number()` or `F.dense_rank()`
- For `ADJUSTING_PRTY_TFM` and `ADJUST_PRTY_TFM`, the sequential number offsets from `MAX_CALL_PRTY`: use `F.col("MAX_CALL_PRTY") + F.row_number().over(Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY"))`

---

## Appendix A: Key XML Values Summary

| Item | Verbatim XML Value |
|---|---|
| Source table | `#ENV.$TD_WORK_DB#.CREW_WSTELE_LND` |
| Target work table | `TE_EMPLOYEE_PHONE_NEW` |
| Target live table | `#ENV.$TD_DB#.TE_EMPLOYEE_PHONE` |
| TableAction | `3` (Truncate + Insert) |
| WriteMode | `0` |
| Error file | `#ENV.$LOG_DIR#/EMPLOYEE_PHONE.err` (overwrite) |
| EMERGENCY constraint | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR) and isvalid("int64",LNK_OUT_TDC.PH_NBR)` |
| TEMP constraint | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR) and isvalid("int64",LNK_OUT_TDC.TEMP_PH_NBR)` |
| BASIC constraint | `ISVALID("INT32", LNK_OUT_TDC.EMP_NBR)` |
| ERROR constraint | `Not (IsValid("int32", (LNK_OUT_TDC.EMP_NBR)))` |
| BASIC pivot count | 5 phone records per employee |
| HOME/AWAY sequence groups | 3 (SEQ1, SEQ2, SEQ3) × 5 priorities each = 15 pivot rows max |
| HOME_BASIC_JNR join keys | `EMP_NBR`, `LKP_CALL_PRTY` (innerjoin) |
| AWAY_BASIC_JNR join keys | `EMP_NBR`, `LKP_CALL_PRTY` (innerjoin) |
| BASIC_MAX_JNR join key | `EMP_NBR` (innerjoin) |
| AWAY_MAX_JNR join key | `EMP_NBR` (innerjoin) |
| MAX_PRTY_AGG | GROUP BY `EMP_NBR`, `MAX(CALL_PRTY) AS MAX_CALL_PRTY` |
| MAX_ABPRTY_AGG | GROUP BY `EMP_NBR`, `MAX(CALL_PRTY) AS MAX_CALL_PRTY` |
| NULL_VAL_TFM filter | `isvalid("int64",ALL_REC_OUT_FNL.PH_NBR)` |
| EMP_NO derivation | `If Len(TrimLeadingTrailing(EMP_NBR))=9 Then TrimLeadingTrailing(EMP_NBR) Else Str('0',9-Len(TrimLeadingTrailing(EMP_NBR))):TrimLeadingTrailing(EMP_NBR)` |
| EFF_START_TM derivation | `left(EFF_START_TM,2):':':right(EFF_START_TM,2):':00'` |
| EFF_END_TM derivation | `left(EFF_END_TM,2):':':right(EFF_END_TM,2):':00'` |
| TD_LD_TS derivation | `CurrentTimestamp()` |
