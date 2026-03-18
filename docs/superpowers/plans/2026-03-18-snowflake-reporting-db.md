# Snowflake Reporting Database Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load three CommCare CSV exports into Snowflake and create two reporting views — one tracking client status changes over time, one flagging potential duplicate clients.

**Architecture:** Raw CSV data loaded into a `RAW` schema via internal stage and `COPY INTO`. JSON form data stored as `VARIANT` and parsed at query time in views. All SQL lives in version-controlled `.sql` files organized by concern. A Python runner script executes the SQL against Snowflake.

**Tech Stack:** Snowflake SQL, Python (snowflake-connector-python for execution)

**Spec:** `docs/superpowers/specs/2026-03-18-snowflake-reporting-db-design.md`

---

## File Structure

```
sql/
  01_setup_schema.sql        -- Schema, file formats, stage creation
  02_create_tables.sql       -- Table DDL for all 3 tables
  03_load_data.sql           -- PUT + COPY INTO for all 3 CSVs
  04_validate_load.sql       -- Row count checks
  05_view_status_changes.sql -- V_CLIENT_STATUS_CHANGES view
  06_view_duplicates.sql     -- V_POTENTIAL_DUPLICATE_CLIENTS view
  07_validate_views.sql      -- Queries to verify view output
run.py                       -- Executes SQL files in order against Snowflake
```

---

### Task 1: Project scaffolding and Snowflake runner

**Files:**
- Create: `sql/` directory
- Create: `run.py`

- [ ] **Step 1: Create `run.py`**

Python script that connects to Snowflake and executes `.sql` files in order. Uses `snowflake-connector-python`. Reads connection params from environment or hardcoded defaults for this exercise. Supports running a single file or all files in sequence.

```python
#!/usr/bin/env python3
"""Execute SQL files against Snowflake in order."""
import sys
import os
import snowflake.connector

SNOWFLAKE_CONFIG = {
    "account": "zozlgyg-dimagi_data_analytics",
    "user": "candidate_abd_user",
    "password": "vCWigj0z6lOUBNJh",
    "database": "DATA_ENGINEER_EXERCISE_ABD_DB",
    "role": "CANDIDATE_ABD_ROLE",
    "warehouse": "COMPUTE_WH",
}

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")


def run_sql_file(cursor, filepath):
    """Execute all statements in a SQL file."""
    with open(filepath, "r") as f:
        content = f.read()

    # Split on semicolons but respect quoted strings
    statements = [s.strip() for s in content.split(";") if s.strip()]
    for stmt in statements:
        if stmt.startswith("--") and "\n" not in stmt:
            continue
        print(f"  Executing: {stmt[:80]}...")
        cursor.execute(stmt)
        try:
            results = cursor.fetchall()
            if results:
                for row in results:
                    print(f"    {row}")
        except snowflake.connector.errors.ProgrammingError:
            pass


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    try:
        if target:
            filepath = os.path.join(SQL_DIR, target)
            print(f"Running {target}...")
            run_sql_file(cursor, filepath)
        else:
            sql_files = sorted(f for f in os.listdir(SQL_DIR) if f.endswith(".sql"))
            for sql_file in sql_files:
                filepath = os.path.join(SQL_DIR, sql_file)
                print(f"\n{'='*60}")
                print(f"Running {sql_file}...")
                print(f"{'='*60}")
                run_sql_file(cursor, filepath)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Copy CSV data files into project**

Copy the 3 CSV files from Downloads into `data/` directory in the project.

- [ ] **Step 3: Verify runner connects**

```bash
python3 run.py 01_setup_schema.sql
```

Expected: Connects successfully, runs whatever is in the file.

- [ ] **Step 4: Commit**

```bash
git add run.py sql/ data/
git commit -m "feat: add project scaffolding with Snowflake SQL runner"
```

---

### Task 2: Schema, stage, and file formats

**Files:**
- Create: `sql/01_setup_schema.sql`

- [ ] **Step 1: Write schema setup SQL**

```sql
USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- File format for the two simple CSVs (case_client, case_alias)
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = NONE
  NULL_IF = ('');

-- File format for forms_raw.csv (contains multiline JSON in a quoted field)
CREATE OR REPLACE FILE FORMAT csv_json_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = NONE
  NULL_IF = ('');

-- Internal stage for uploading CSV files
CREATE OR REPLACE STAGE csv_stage
  FILE_FORMAT = csv_format;
```

- [ ] **Step 2: Run and verify**

```bash
python3 run.py 01_setup_schema.sql
```

Expected: Schema `RAW` created, file formats and stage created.

- [ ] **Step 3: Commit**

```bash
git add sql/01_setup_schema.sql
git commit -m "feat: add schema, file formats, and internal stage"
```

---

### Task 3: Create tables

**Files:**
- Create: `sql/02_create_tables.sql`

- [ ] **Step 1: Write table DDL**

All columns as `VARCHAR` except `FORMS_RAW.JSON` which needs special handling. The JSON column will be loaded as `VARCHAR` first, then parsed to `VARIANT` in the view (or we can use a `VARIANT` column and parse during COPY). Given the multiline JSON in quoted CSV fields, loading as `VARCHAR` and using `PARSE_JSON()` in views is more reliable.

```sql
USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE CASE_CLIENT (
  DOMAIN VARCHAR,
  ID VARCHAR,
  LAST_UPDATED VARCHAR,
  FULL_NAME VARCHAR,
  GENDER VARCHAR,
  INSURANCE_INFORMATION VARCHAR,
  LAST_NAME VARCHAR,
  LEVEL_OF_CARE_NEEDED VARCHAR,
  LOCK_IN_STATUS VARCHAR,
  LOCK_OUT_STATUS VARCHAR,
  LOCK_STATUS VARCHAR,
  MEDICAID_ID VARCHAR,
  MIDDLE_INITIAL VARCHAR,
  MIDDLE_NAME VARCHAR,
  NO_SOCIAL_SECURITY_NUMBER_REASON VARCHAR,
  NO_SOCIAL_SECURITY_NUMBER_REASON_OTHER VARCHAR,
  NON_DUPLICATE_CASE_IDS VARCHAR,
  OPEN_TS VARCHAR,
  OWNER_ID VARCHAR,
  POTENTIAL_DUPLICATE_CASE_IDS VARCHAR,
  POTENTIAL_DUPLICATE_INDEX_CASE_IDS VARCHAR,
  PREEXISTING_MEDICAL_CONDITIONS VARCHAR,
  PRESCRIPTION VARCHAR,
  PRIORITY_POPULATION VARCHAR,
  PROFILE_CHECKS VARCHAR,
  PROVISIONAL_DIAGNOSIS VARCHAR,
  REASON_FOR_SEEKING_CARE VARCHAR,
  REFERRER_HQ_USER_ID VARCHAR,
  REFERRER_NAME VARCHAR,
  SOCIAL_SECURITY_NUMBER VARCHAR,
  SYMPTOMS VARCHAR,
  TYPE_OF_CARE VARCHAR,
  WAITING_FOR_A_REFERRAL_SINCE_DATE VARCHAR,
  WITHDRAWN_TS VARCHAR,
  RESOURCE_URI VARCHAR,
  SERVER_DATE_MODIFIED VARCHAR,
  SERVER_DATE_OPENED VARCHAR,
  USER_ID VARCHAR,
  CASE_ID VARCHAR,
  CLOSED VARCHAR,
  CLOSED_BY VARCHAR,
  DATE_CLOSED VARCHAR,
  DATE_MODIFIED VARCHAR,
  INDEXED_ON VARCHAR,
  PARENT_CASE_TYPE VARCHAR,
  PARENT_RELATIONSHIP VARCHAR,
  OPENED_BY VARCHAR,
  ACTIVE_ADMISSION_CLINIC_ID VARCHAR,
  ADDITIONAL_DETAILS VARCHAR,
  ADDRESS_CITY VARCHAR,
  ADDRESS_COUNTY VARCHAR,
  ADDRESS_FULL VARCHAR,
  ADDRESS_LATITUDE VARCHAR,
  ADDRESS_LONGITUDE VARCHAR,
  ADDRESS_MAP_COORDINATES VARCHAR,
  ADDRESS_STATE VARCHAR,
  ADDRESS_STREET VARCHAR,
  ADDRESS_ZIP VARCHAR,
  ADMISSION_DATE VARCHAR,
  ADMISSION_TIME VARCHAR,
  AGE VARCHAR,
  AGE_RANGE VARCHAR,
  CARE_COORDINATION_ESCALATION VARCHAR,
  CASE_NAME VARCHAR,
  CASE_TYPE VARCHAR,
  CENTRAL_REGISTRY VARCHAR,
  CLIENT_ID VARCHAR,
  CLIENT_PLACED_TS VARCHAR,
  CLOSED_TS VARCHAR,
  COMMCARE_EMAIL_ADDRESS VARCHAR,
  CONSENT_COLLECTED VARCHAR,
  CONTACT_PHONE_NUMBER VARCHAR,
  CURRENT_MEDICATIONS VARCHAR,
  CURRENT_STATUS VARCHAR,
  DATE_OPENED VARCHAR,
  DECEASED VARCHAR,
  DECEASED_NOTES VARCHAR,
  DISCHARGE_DATE VARCHAR,
  DISCHARGE_TIME VARCHAR,
  DOB VARCHAR,
  DUAL_ADMISSION_ATTEMPT_COUNT VARCHAR,
  DUAL_ADMISSION_ATTEMPT_MOST_RECENT_DATE VARCHAR,
  DUAL_ADMISSION_ATTEMPT_MOST_RECENT_TIME VARCHAR,
  DUAL_ADMISSION_ATTEMPTED VARCHAR,
  DUPLICATE_CASE_IDS VARCHAR,
  DUPLICATE_PRIMARY_CASE_ID VARCHAR,
  ESCALATED_TS VARCHAR,
  EXTERNAL_ID VARCHAR,
  FIRST_NAME VARCHAR,
  PARENT_CASE_ID VARCHAR,
  CLIENT_PLACED_DATE VARCHAR,
  MEDICATION_ASSISTED_TREATMENT_DETAILS VARCHAR,
  SUBSTANCE_USE_INFORMATION VARCHAR,
  WITHDRAWAL_SYMPTOMS VARCHAR,
  ASAM_LEVEL VARCHAR,
  WITHDRAWN_TS_DATE VARCHAR,
  CLOSED_TS_DATE VARCHAR,
  CLIENT_PLACED_TS_DATE VARCHAR,
  LOCK_IN_CLINIC_IDS VARCHAR,
  LOCK_OUT_CLINIC_IDS VARCHAR,
  LOCK_OUT_CASE_IDS VARCHAR,
  LOCK_IN_CASE_IDS VARCHAR,
  REFERRER_ADDITIONAL_CONTACT_DETAILS VARCHAR,
  REFERRER_PHONE_NUMBER_EXT VARCHAR,
  REFERRER_PHONE_NUMBER VARCHAR
);

CREATE OR REPLACE TABLE CASE_ALIAS (
  DOMAIN VARCHAR,
  ID VARCHAR,
  LAST_UPDATED VARCHAR,
  CASE_ID VARCHAR,
  CLOSED VARCHAR,
  CLOSED_BY VARCHAR,
  DATE_CLOSED VARCHAR,
  DATE_MODIFIED VARCHAR,
  INDEXED_ON VARCHAR,
  PARENT_CASE_ID VARCHAR,
  PARENT_CASE_TYPE VARCHAR,
  PARENT_RELATIONSHIP VARCHAR,
  OPENED_BY VARCHAR,
  CASE_NAME VARCHAR,
  CASE_TYPE VARCHAR,
  DATE_OPENED VARCHAR,
  DOB VARCHAR,
  EXTERNAL_ID VARCHAR,
  FIRST_NAME VARCHAR,
  LAST_NAME VARCHAR,
  MEDICAID_ID VARCHAR,
  MIDDLE_NAME VARCHAR,
  OWNER_ID VARCHAR,
  SOCIAL_SECURITY_NUMBER VARCHAR,
  RESOURCE_URI VARCHAR,
  SERVER_DATE_MODIFIED VARCHAR,
  SERVER_DATE_OPENED VARCHAR,
  USER_ID VARCHAR
);

CREATE OR REPLACE TABLE FORMS_RAW (
  DOMAIN VARCHAR,
  METADATA VARCHAR,
  METADATA_FILENAME VARCHAR,
  ID VARCHAR,
  SYSTEM_QUERY_TS VARCHAR,
  SYSTEM_CREATE_TS VARCHAR,
  TASK_ID VARCHAR,
  EXECUTION_ID VARCHAR,
  JSON VARCHAR
);
```

- [ ] **Step 2: Run and verify**

```bash
python3 run.py 02_create_tables.sql
```

Expected: All 3 tables created.

- [ ] **Step 3: Commit**

```bash
git add sql/02_create_tables.sql
git commit -m "feat: add table DDL for case_client, case_alias, forms_raw"
```

---

### Task 4: Load CSV data

**Files:**
- Create: `sql/03_load_data.sql`
- Create: `sql/04_validate_load.sql`

- [ ] **Step 1: Write data loading SQL**

Note: `PUT` requires the local file path. The runner script will need to handle this — `PUT` uses `file://` protocol. We'll use the Python connector's `put` method via the cursor.

```sql
USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

-- Upload files to stage
PUT file://{DATA_DIR}/case_client.csv @csv_stage/case_client AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://{DATA_DIR}/case_alias.csv @csv_stage/case_alias AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://{DATA_DIR}/forms_raw.csv @csv_stage/forms_raw AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Load case_client
COPY INTO CASE_CLIENT
FROM @csv_stage/case_client
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Load case_alias
COPY INTO CASE_ALIAS
FROM @csv_stage/case_alias
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Load forms_raw
COPY INTO FORMS_RAW
FROM @csv_stage/forms_raw
FILE_FORMAT = csv_json_format
ON_ERROR = 'CONTINUE';
```

Note: The `{DATA_DIR}` placeholder in PUT statements needs to be resolved by `run.py`. Update `run.py` to do string replacement before executing.

- [ ] **Step 2: Write validation SQL**

```sql
USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

-- Expected: 23 rows
SELECT 'CASE_CLIENT' AS table_name, COUNT(*) AS row_count FROM CASE_CLIENT;

-- Expected: 36 rows
SELECT 'CASE_ALIAS' AS table_name, COUNT(*) AS row_count FROM CASE_ALIAS;

-- Expected: 12 rows
SELECT 'FORMS_RAW' AS table_name, COUNT(*) AS row_count FROM FORMS_RAW;

-- Verify JSON parses correctly
SELECT ID, PARSE_JSON(JSON):form:"@name"::STRING AS form_name
FROM FORMS_RAW
LIMIT 5;
```

- [ ] **Step 3: Run load and validate**

```bash
python3 run.py 03_load_data.sql
python3 run.py 04_validate_load.sql
```

Expected: Row counts match (23, 36, 12). JSON parses to valid form names.

- [ ] **Step 4: Commit**

```bash
git add sql/03_load_data.sql sql/04_validate_load.sql
git commit -m "feat: add data loading and validation SQL"
```

---

### Task 5: View 1 — Client Status Changes

**Files:**
- Create: `sql/05_view_status_changes.sql`

- [ ] **Step 1: Write the view SQL**

```sql
USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

CREATE OR REPLACE VIEW V_CLIENT_STATUS_CHANGES AS
WITH form_status_updates AS (
  SELECT
    f.ID AS FORM_ID,
    PARSE_JSON(f.JSON) AS form_json,
    PARSE_JSON(f.JSON):form:"@name"::STRING AS form_name,
    PARSE_JSON(f.JSON):form:client_profile_group
      :client_profile_save_to_case
      :create_client_profile
      :case:"@case_id"::STRING AS client_case_id,
    PARSE_JSON(f.JSON):form:client_profile_group
      :client_profile_save_to_case
      :create_client_profile
      :case:update
      :current_status::STRING AS status_value,
    PARSE_JSON(f.JSON):form:client_profile_group
      :client_profile_save_to_case
      :create_client_profile
      :case:"@date_modified"::TIMESTAMP_NTZ AS status_change_ts
  FROM FORMS_RAW f
)
SELECT
  c.CASE_ID,
  c.CASE_NAME,
  c.FIRST_NAME,
  c.LAST_NAME,
  fsu.status_value AS CURRENT_STATUS,
  fsu.status_change_ts AS STATUS_CHANGE_DATE,
  fsu.FORM_ID
FROM form_status_updates fsu
JOIN CASE_CLIENT c
  ON c.CASE_ID = fsu.client_case_id
WHERE fsu.form_name = 'Create Profile and Refer'
  AND LOWER(c.CLOSED) = 'false'
ORDER BY c.CASE_ID, fsu.status_change_ts;
```

- [ ] **Step 2: Run and verify**

```bash
python3 run.py 05_view_status_changes.sql
```

Then query the view to check results.

- [ ] **Step 3: Commit**

```bash
git add sql/05_view_status_changes.sql
git commit -m "feat: add V_CLIENT_STATUS_CHANGES view"
```

---

### Task 6: View 2 — Potential Duplicate Clients

**Files:**
- Create: `sql/06_view_duplicates.sql`

- [ ] **Step 1: Write the view SQL**

```sql
USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

CREATE OR REPLACE VIEW V_POTENTIAL_DUPLICATE_CLIENTS AS
WITH all_names AS (
  -- Client names (open clients only)
  SELECT
    CASE_ID AS CLIENT_CASE_ID,
    LOWER(TRIM(FIRST_NAME)) AS FIRST_NAME,
    LOWER(TRIM(LAST_NAME)) AS LAST_NAME,
    'client' AS NAME_SOURCE
  FROM CASE_CLIENT
  WHERE LOWER(CLOSED) = 'false'
    AND TRIM(COALESCE(FIRST_NAME, '')) != ''
    AND TRIM(COALESCE(LAST_NAME, '')) != ''

  UNION ALL

  -- Alias names (exclude aliases whose parent client is closed)
  SELECT
    a.PARENT_CASE_ID AS CLIENT_CASE_ID,
    LOWER(TRIM(a.FIRST_NAME)) AS FIRST_NAME,
    LOWER(TRIM(a.LAST_NAME)) AS LAST_NAME,
    'alias' AS NAME_SOURCE
  FROM CASE_ALIAS a
  LEFT JOIN CASE_CLIENT c ON c.CASE_ID = a.PARENT_CASE_ID
  WHERE (c.CASE_ID IS NULL OR LOWER(c.CLOSED) = 'false')
    AND TRIM(COALESCE(a.FIRST_NAME, '')) != ''
    AND TRIM(COALESCE(a.LAST_NAME, '')) != ''
),
duplicate_pairs AS (
  SELECT
    a.CLIENT_CASE_ID AS CLIENT_A_CASE_ID,
    a.FIRST_NAME || ' ' || a.LAST_NAME AS CLIENT_A_NAME,
    a.NAME_SOURCE AS CLIENT_A_NAME_SOURCE,
    b.CLIENT_CASE_ID AS CLIENT_B_CASE_ID,
    b.FIRST_NAME || ' ' || b.LAST_NAME AS CLIENT_B_NAME,
    b.NAME_SOURCE AS CLIENT_B_NAME_SOURCE,
    CASE
      WHEN a.FIRST_NAME = b.FIRST_NAME AND a.LAST_NAME = b.LAST_NAME
        THEN 'exact'
      ELSE 'fuzzy'
    END AS MATCH_TYPE
  FROM all_names a
  JOIN all_names b
    ON a.CLIENT_CASE_ID < b.CLIENT_CASE_ID  -- Deduplicate pairs + exclude self
    AND (
      -- Exact match
      (a.FIRST_NAME = b.FIRST_NAME AND a.LAST_NAME = b.LAST_NAME)
      OR
      -- Fuzzy match with minimum name length of 3
      (
        LENGTH(a.FIRST_NAME) >= 3 AND LENGTH(b.FIRST_NAME) >= 3
        AND LENGTH(a.LAST_NAME) >= 3 AND LENGTH(b.LAST_NAME) >= 3
        AND EDITDISTANCE(a.FIRST_NAME, b.FIRST_NAME) <= 2
        AND EDITDISTANCE(a.LAST_NAME, b.LAST_NAME) <= 2
      )
    )
)
SELECT DISTINCT *
FROM duplicate_pairs
ORDER BY CLIENT_A_CASE_ID, CLIENT_B_CASE_ID;
```

- [ ] **Step 2: Run and verify**

```bash
python3 run.py 06_view_duplicates.sql
```

Then query the view to check results.

- [ ] **Step 3: Commit**

```bash
git add sql/06_view_duplicates.sql
git commit -m "feat: add V_POTENTIAL_DUPLICATE_CLIENTS view"
```

---

### Task 7: Validation queries

**Files:**
- Create: `sql/07_validate_views.sql`

- [ ] **Step 1: Write validation queries**

```sql
USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

-- View 1: Check status changes view
SELECT * FROM V_CLIENT_STATUS_CHANGES;

-- View 2: Check duplicate detection
SELECT * FROM V_POTENTIAL_DUPLICATE_CLIENTS;

-- Sanity check: all form case_ids match a client
SELECT fsu.client_case_id, c.CASE_ID IS NOT NULL AS has_client_match
FROM (
  SELECT PARSE_JSON(JSON):form:client_profile_group
    :client_profile_save_to_case
    :create_client_profile
    :case:"@case_id"::STRING AS client_case_id
  FROM FORMS_RAW
  WHERE PARSE_JSON(JSON):form:"@name"::STRING = 'Create Profile and Refer'
) fsu
LEFT JOIN CASE_CLIENT c ON c.CASE_ID = fsu.client_case_id;
```

- [ ] **Step 2: Run and review output**

```bash
python3 run.py 07_validate_views.sql
```

- [ ] **Step 3: Final commit**

```bash
git add sql/07_validate_views.sql
git commit -m "feat: add validation queries for both views"
```
