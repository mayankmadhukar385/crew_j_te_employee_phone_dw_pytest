# Step 0: XML Discovery — Detect Job Structure and Plan Execution

## Your Role

You are the first stage of a DataStage-to-Databricks migration pipeline. Before any
lineage extraction or code generation happens, you must analyze the XML and determine
what it contains and how to process it.

---

## Your Input

The DataStage XML export file(s) in the `input/` folder.

## Your Output

A single file: `output/discovery.json`

---

## What to Do

### 1. Parse Every `<Job>` Element in the XML

A DataStage XML export can contain one or more `<Job>` elements. For EACH job found:

```python
for job in root.findall('Job'):
    job_name = job.get('Identifier')
```

### 2. Classify Each Job

For each job, examine its `<Record>` elements to determine the job type:

**Sequence Job** — contains ANY of these Record types:
- `JSJobActivity` (calls child jobs)
- `JSCondition` (conditional branching)
- `JSExceptionHandler` (error handling)
- `JSTerminatorActivity` (terminator)
- `JSUserVarsActivity` (user variables)
- `JSExecCmdActivity` (command execution)
- `JSRoutineActivity` (routine calls)
- `JSNotificationActivity` (notifications)

**Parallel Job** — contains ANY of these Record types:
- `CustomStage` (PxJoin, PxFunnel, PxPivot, PxCopy, TeradataConnectorPX, etc.)
- `TransformerStage` (CTransformerStage)

**Server Job** — contains:
- `CCustomStage`, server-side stage types
(Treat server jobs the same as parallel jobs for lineage purposes.)

A single XML file can contain:
- Scenario A: One parallel job only
- Scenario B: One sequence job + its child parallel jobs (most common for exports)
- Scenario C: Multiple unrelated parallel jobs (batch export)
- Scenario D: Nested sequences (sequence calling other sequences)

### 3. For Sequence Jobs — Extract Orchestration Logic

If a sequence job is found, extract:

**Activity list** — every `JSJobActivity` Record:
- Activity name (Property Name="Name")
- Which child job it calls (from stage properties)

**Condition nodes** — every `JSCondition` Record:
- Condition name
- Output links and their conditions (from JSActivityOutput properties)

**Links between activities** — from JSActivityInput/JSActivityOutput Records:
- Build the execution graph: which activity runs after which
- Extract link conditions (these determine execution order and branching)

**Exception handling** — JSExceptionHandler, JSTerminatorActivity paths

**Job parameters** — sequence-level parameters that control branching
(e.g., `$pFull_Load_EVOLUTIONPARMS = 'Y'` → run full load job)

### 4. For Parallel Jobs — Count Stages and Links

For each parallel job, do a quick inventory:
- Count of stages by type
- Count of links
- Source stage names and types
- Target stage names and types

### 5. Build the Execution Plan

Determine the processing order:

**For Scenario A (single parallel job):**
```json
{
  "scenario": "single_parallel_job",
  "jobs_to_process": ["JOB_NAME"],
  "orchestration": null
}
```

**For Scenario B (sequence + children):**
```json
{
  "scenario": "sequence_with_children",
  "sequence_job": "SEQ_JOB_NAME",
  "parallel_jobs": ["CHILD_1", "CHILD_2", ...],
  "orchestration": {
    "activities": [...],
    "conditions": [...],
    "execution_graph": [...],
    "exception_handling": [...]
  },
  "jobs_to_process": ["CHILD_1", "CHILD_2", ...]
}
```

**For Scenario C (multiple unrelated jobs):**
```json
{
  "scenario": "multiple_parallel_jobs",
  "jobs_to_process": ["JOB_1", "JOB_2", ...],
  "orchestration": null
}
```

---

## Output File: `output/discovery.json`

Write a JSON file with this exact structure:

```json
{
  "xml_file": "<filename>",
  "scenario": "single_parallel_job | sequence_with_children | multiple_parallel_jobs",
  "total_jobs_found": <N>,

  "jobs": [
    {
      "job_name": "<name>",
      "job_type": "sequence | parallel | server",
      "stage_count": <N>,
      "stage_types": {"<type>": <count>, ...},
      "source_stages": ["<name> (<type>)"],
      "target_stages": ["<name> (<type>)"],
      "link_count": <N>
    }
  ],

  "orchestration": {
    "sequence_job_name": "<name or null>",
    "activities": [
      {
        "activity_name": "<name>",
        "calls_job": "<child job name>",
        "activity_type": "JSJobActivity | JSCondition | JSExceptionHandler | ..."
      }
    ],
    "execution_graph": [
      {
        "from_activity": "<name>",
        "to_activity": "<name>",
        "link_name": "<name>",
        "condition": "<expression or null>"
      }
    ],
    "exception_handling": [
      {
        "handler": "<name>",
        "actions": ["<activity_name>", ...]
      }
    ],
    "parameters": [
      {
        "name": "<param_name>",
        "default_value": "<value>",
        "used_in_conditions": true
      }
    ]
  },

  "execution_plan": {
    "jobs_to_process": ["<job_name>", ...],
    "processing_order": [
      {
        "order": 1,
        "job_name": "<name>",
        "job_type": "parallel",
        "depends_on": [],
        "note": "Process independently"
      }
    ],
    "generates_orchestration": true,
    "orchestration_type": "databricks_workflow"
  }
}
```

---

## Also Print a Summary

After writing the JSON, print a human-readable summary:

```
╔══════════════════════════════════════════════════════════════╗
  DISCOVERY SUMMARY
╠══════════════════════════════════════════════════════════════╣
  XML File    : NBME0019_SEQ.xml
  Scenario    : Sequence job with 5 child parallel jobs
  
  SEQUENCE ORCHESTRATION: NBME0019_SEQ
    1. Jb_load_last_process_ts_dtst  →  initializes timestamp
    2. nc_extracttype                →  branches on $pFull_Load
       ├─ Y → Jb_LMIS_Evolutionparms_Full_Ld
       └─ N → Jb_LMIS_Evolutionparms_Incr_Ld
    3. Jb_load_reset_process_ts      →  after full load
    4. Jb_load_last_process_ts       →  after incremental load
  
  PARALLEL JOBS TO PROCESS:
    1. Jb_LMIS_Evolutionparms_Full_Ld    (3 stages, Teradata→Teradata)
    2. Jb_LMIS_Evolutionparms_Incr_Ld    (3 stages, DB2→Teradata)
    3. Jb_load_last_process_ts_dtst       (3 stages, RowGen→DataSet)
    4. Jb_load_last_process_ts            (3 stages, DataSet→SeqFile)
    5. Jb_load_reset_process_ts           (3 stages, RowGen→SeqFile)
  
  EXECUTION PLAN:
    → Generate lineage for each of 5 parallel jobs
    → Generate Databricks Workflow for sequence orchestration
╚══════════════════════════════════════════════════════════════╝
```

---

## Critical Rules

- Parse the XML — do not guess job types from names
- A job is a sequence job ONLY if it contains JSJobActivity/JSCondition records
- A job is a parallel job ONLY if it contains CustomStage/TransformerStage records
- Some XMLs may contain BOTH types — classify each job independently
- Extract condition expressions from sequence links VERBATIM
- The execution_plan.jobs_to_process must list ONLY parallel/server jobs
  (sequence jobs are not processed for lineage — they become Databricks Workflows)
