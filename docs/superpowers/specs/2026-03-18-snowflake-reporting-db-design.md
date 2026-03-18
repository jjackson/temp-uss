# Snowflake Reporting Database — Design Spec

## Context

Three CSV exports from a CommCare behavioral health referral application need to be loaded into Snowflake and transformed into reporting views. The data tracks clients being referred between facilities, with status changes driven by form submissions.

### Source Data

| File | Rows | Description |
|------|------|-------------|
| `case_client.csv` | 23 | Client cases with ~90 columns. Each has a unique `CASE_ID` and a `CURRENT_STATUS` that changes over time. `CLOSED` indicates whether the case is still active. |
| `case_alias.csv` | 36 | Alias records linked to clients via `PARENT_CASE_ID`. Contains alternate `FIRST_NAME` / `LAST_NAME` for a client. |
| `forms_raw.csv` | 12 | Form submissions with 9 columns including a `JSON` column containing the full CommCare form payload as nested JSON. |

### Form JSON Structure (relevant paths)

For "Create Profile and Refer" forms, the status update lives at:

```
form
  .client_profile_group
    .client_profile_save_to_case
      .create_client_profile
        .case
          .@case_id          → which client was updated
          .@date_modified    → when the update happened
          .update
            .current_status  → the new status value
```

## Design Decisions

### Why raw tables + SQL views (not pre-processed in Python)

Three approaches were considered:

1. **Raw tables + SQL views** — load CSVs as-is, parse JSON at query time in views
2. **Pre-process JSON in Python** — flatten JSON locally, load flat tables, simpler views
3. **Heavy Python, minimal Snowflake** — do transformations outside Snowflake

**Chosen: Approach 1.** Keeping JSON as a `VARIANT` column and parsing it in views is the standard Snowflake pattern for semi-structured data. It preserves the raw data for auditability, avoids lossy pre-processing, and keeps all transformation logic in SQL where it's visible and version-controllable.

### Why a single RAW schema (not raw + transformed)

With only 3 source tables and 2 views, a multi-schema medallion architecture adds overhead without benefit. A single `RAW` schema with tables and views colocated is appropriate for this data volume.

## Architecture

### Schema: `RAW`

**Tables** (loaded via internal stage + `COPY INTO`):

- `CASE_CLIENT` — all columns from `case_client.csv`, typed as strings (Snowflake will handle casting in views)
- `CASE_ALIAS` — all columns from `case_alias.csv`
- `FORMS_RAW` — 9 columns, with `JSON` stored as `VARIANT`

**Loading approach:**
1. Create named internal stage in `RAW` schema
2. `PUT` CSV files to stage
3. `COPY INTO` each table with appropriate file format (CSV with header skip, field delimiter `,`, field optionally enclosed by `"`)
4. `FORMS_RAW.JSON` column requires special handling — the CSV contains multiline JSON strings, so the file format needs `FIELD_OPTIONALLY_ENCLOSED_BY = '"'` and proper escape handling

### View 1: `V_CLIENT_STATUS_CHANGES`

Tracks how each client's `current_status` changes over time.

**Logic:**
1. Filter `FORMS_RAW` to rows where `form.@name = 'Create Profile and Refer'`
2. Parse JSON to extract `case_id`, `current_status`, and `date_modified` from the path documented above
3. Join to `CASE_CLIENT` on `CASE_ID` where `CLOSED = 'false'`
4. Order by client, then chronologically

**Output columns:**

| Column | Source | Description |
|--------|--------|-------------|
| `CASE_ID` | form JSON `@case_id` | The client case that was updated |
| `CASE_NAME` | `CASE_CLIENT.CASE_NAME` | Human-readable client identifier |
| `FIRST_NAME` | `CASE_CLIENT.FIRST_NAME` | Client first name |
| `LAST_NAME` | `CASE_CLIENT.LAST_NAME` | Client last name |
| `CURRENT_STATUS` | form JSON `update.current_status` | Status value written by this form |
| `STATUS_CHANGE_DATE` | form JSON `@date_modified` | Timestamp of the change |
| `FORM_ID` | `FORMS_RAW.ID` | The form submission that made this change |

### View 2: `V_POTENTIAL_DUPLICATE_CLIENTS`

Flags potential duplicate clients by comparing names across client and alias records.

**Data notes:**
- Many aliases in `CASE_ALIAS` reference `PARENT_CASE_ID` values not present in `CASE_CLIENT`. These orphaned aliases are included in duplicate detection using a LEFT JOIN — their names are still valid for matching even if the parent client isn't in this dataset.
- NULL or empty `FIRST_NAME`/`LAST_NAME` values are excluded from the unified name list to avoid spurious matches.
- The `CLOSED = 'false'` filter applies to `CASE_CLIENT` records. Aliases whose parent client is closed are also excluded (via the join). Orphaned aliases (no matching parent) are included since we cannot determine their closed status.

**Logic:**
1. Build a unified name list: client `FIRST_NAME`/`LAST_NAME` from `CASE_CLIENT` (where `CLOSED = 'false'`) unioned with alias `FIRST_NAME`/`LAST_NAME` from `CASE_ALIAS` (with resolved `CLIENT_CASE_ID` = `PARENT_CASE_ID` or the alias's own parent if orphaned)
2. Exclude rows where `FIRST_NAME` or `LAST_NAME` is NULL or empty
3. Self-join excluding same-client pairs
4. Flag as potential duplicate where:
   - Exact match on `LOWER(FIRST_NAME)` and `LOWER(LAST_NAME)`, OR
   - `EDITDISTANCE(LOWER(first), LOWER(other_first)) <= 2` AND `EDITDISTANCE(LOWER(last), LOWER(other_last)) <= 2`, with a minimum name length of 3 characters to avoid false positives on very short names
5. Deduplicate pairs (A,B) = (B,A)

**Output columns:**

| Column | Description |
|--------|-------------|
| `CLIENT_A_CASE_ID` | First client in the potential duplicate pair |
| `CLIENT_A_NAME` | Name (or alias) that matched |
| `CLIENT_A_NAME_SOURCE` | `'client'` or `'alias'` |
| `CLIENT_B_CASE_ID` | Second client in the pair |
| `CLIENT_B_NAME` | Name (or alias) that matched |
| `CLIENT_B_NAME_SOURCE` | `'client'` or `'alias'` |
| `MATCH_TYPE` | `'exact'` or `'fuzzy'` |

## Implementation Steps

1. Create schema `RAW`
2. Create tables `CASE_CLIENT`, `CASE_ALIAS`, `FORMS_RAW`
3. Create internal stage and file formats
4. Upload and load CSV data
5. Validate row counts match source files
6. Create `V_CLIENT_STATUS_CHANGES`
7. Validate view returns expected data
8. Create `V_POTENTIAL_DUPLICATE_CLIENTS`
9. Validate duplicate detection results
