# Behavioral Health Referral — Snowflake Reporting Database

A reporting database built on Snowflake for tracking client referrals in a behavioral health coordination system. Loads CommCare case and form data, then surfaces two analytical views: one for monitoring client status changes over time, and one for detecting potential duplicate client records.

## What's in Snowflake

**Database:** `DATA_ENGINEER_EXERCISE_ABD_DB` | **Schema:** `RAW`

### Tables

| Table | Rows | Description |
|-------|------|-------------|
| `CASE_CLIENT` | 23 (14 unique) | Client case records — demographics, status, referral details |
| `CASE_ALIAS` | 36 | Alias records linked to clients via `PARENT_CASE_ID` |
| `FORMS_RAW` | 12 | Form submissions with full JSON payloads stored as VARCHAR |

### Views

**`V_CLIENT_STATUS_CHANGES`** — Tracks how each client's `current_status` changes over time by parsing "Create Profile and Refer" form submissions. Joins form data to open (non-closed) clients, deduplicating source data that contains exact duplicate rows.

```sql
SELECT * FROM V_CLIENT_STATUS_CHANGES;
-- CASE_ID | CLIENT_ID | CASE_NAME | FIRST_NAME | LAST_NAME | CURRENT_STATUS | STATUS_CHANGE_DATE | FORM_ID
```

**`V_POTENTIAL_DUPLICATE_CLIENTS`** — Flags potential duplicate clients by comparing names across both client records and alias records. Uses exact matching and fuzzy matching via Snowflake's `EDITDISTANCE` (Levenshtein distance <= 2, minimum name length 3).

```sql
SELECT * FROM V_POTENTIAL_DUPLICATE_CLIENTS;
-- CLIENT_A_CASE_ID | CLIENT_A_NAME | CLIENT_A_NAME_SOURCE | CLIENT_B_CASE_ID | CLIENT_B_NAME | CLIENT_B_NAME_SOURCE | MATCH_TYPE
```

## Project Structure

```
sql/
  01_setup_schema.sql        Schema, file format, internal stage
  02_create_tables.sql       Table DDL (all VARCHAR columns)
  03_load_data.sql           PUT + COPY INTO for all 3 CSVs
  04_validate_load.sql       Row count checks + JSON parse verification
  05_view_status_changes.sql V_CLIENT_STATUS_CHANGES definition
  06_view_duplicates.sql     V_POTENTIAL_DUPLICATE_CLIENTS definition
  07_validate_views.sql      View output + sanity checks
data/
  case_client.csv            Source client data
  case_alias.csv             Source alias data
  forms_raw.csv              Source form data (contains nested JSON)
run.py                       SQL runner — executes files against Snowflake
```

## Running

Execute all SQL files in order:

```bash
python3 run.py
```

Or run a single file:

```bash
python3 run.py 05_view_status_changes.sql
```

Requires `snowflake-connector-python`:

```bash
pip install snowflake-connector-python
```

## Design Decisions

- **All VARCHAR columns** — Raw/staging tables mirror the CSV exactly. Type casting happens in views where it's explicit.
- **JSON stored as VARCHAR, parsed with `PARSE_JSON()` at query time** — Preserves raw data fidelity. Fine at this scale; at production volume, consider VARIANT or materialized views.
- **Single schema** — With 3 tables and 2 views, a medallion architecture (raw/staging/analytics) adds overhead without benefit.
- **`SELECT DISTINCT` for deduplication** — Source data contains exact duplicate rows (9 of 14 clients). Handled in view CTEs rather than at load time to preserve the raw data.
- **`ON_ERROR = 'ABORT_STATEMENT'`** — Strict loading; any parse error stops the load rather than silently skipping rows.

## Data Quality Notes

1. **Source duplicates** — `case_client.csv` has 23 rows but only 14 unique `CASE_ID` values (9 exact duplicates)
2. **Sparse name fields** — Many clients have NULL `FIRST_NAME`/`LAST_NAME`, particularly those created by "Create Profile and Refer" forms
3. **Orphaned aliases** — 85% of alias `PARENT_CASE_ID` values don't match any client in `case_client.csv`, suggesting the client export is a subset
4. **Single status per form type** — All "Create Profile and Refer" forms set `current_status = 'open'` (they're creation forms); subsequent status changes come from other form types not in scope

## Docs

- [Design Spec](docs/superpowers/specs/2026-03-18-snowflake-reporting-db-design.md) — Architecture, approach rationale, view definitions
- [Implementation Plan](docs/superpowers/plans/2026-03-18-snowflake-reporting-db.md) — Step-by-step build plan with exact SQL
