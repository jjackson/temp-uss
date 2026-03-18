# Interview Prep Guide

## How to Demo

1. Open Snowflake web UI at https://zozlgyg-dimagi_data_analytics.snowflakecomputing.com
2. Set context: `USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB; USE WAREHOUSE COMPUTE_WH; USE SCHEMA RAW;`
3. Show the tables: `SELECT COUNT(*) FROM CASE_CLIENT;` (23 rows), `CASE_ALIAS` (36), `FORMS_RAW` (12)
4. Query View 1: `SELECT * FROM V_CLIENT_STATUS_CHANGES;`
5. Query View 2: `SELECT * FROM V_POTENTIAL_DUPLICATE_CLIENTS;`
6. Walk through the SQL files in order (01-07) to explain the approach

---

## Expected Questions & Answers

### Data Loading

**Q: How did you load the data into Snowflake?**

I created an internal named stage (`csv_stage`), uploaded the CSV files with `PUT`, then used `COPY INTO` to load each table. I used a single CSV file format with `FIELD_OPTIONALLY_ENCLOSED_BY = '"'` — Snowflake's CSV parser natively handles multiline content within quoted fields, so the same format works for both the simple CSVs and the forms CSV that has multiline JSON.

**Q: Why did you store all columns as VARCHAR?**

For a raw/staging layer, keeping everything as VARCHAR avoids type-casting errors during load and preserves the source data exactly as-is. Any type conversions (like casting `@date_modified` to `TIMESTAMP_NTZ`) happen in the view layer where they're explicit and visible.

**Q: Why VARCHAR for the JSON column instead of VARIANT?**

Either approach works. I chose VARCHAR so the raw table is a faithful mirror of the CSV — the JSON is stored exactly as it arrived. I use `PARSE_JSON()` in the views to access it. At this data volume (12 rows), there's no meaningful performance difference. In production with millions of rows, I'd consider storing it as VARIANT or creating a materialized view to avoid repeated parsing.

**Q: What does `ON_ERROR = 'ABORT_STATEMENT'` do and why did you choose it?**

It stops the entire COPY INTO if any row fails to load, rather than silently skipping bad rows. With only 23/36/12 rows, I want to know immediately if something's wrong. In a production pipeline with messy data, you might use `CONTINUE` with error logging, but for a clean dataset this is the safer choice.

---

### View 1: Status Changes

**Q: Walk me through the status changes view.**

It uses two CTEs:
1. `deduplicated_clients` — `SELECT DISTINCT *` from `CASE_CLIENT` where `CLOSED = 'false'`. The source data has exact duplicate rows (9 out of 14 unique clients appear twice), so this removes them.
2. `form_status_updates` — Parses the JSON column from `FORMS_RAW`, filtering to only "Create Profile and Refer" forms. Extracts three fields from the nested JSON path: the `@case_id` (which client), `current_status` (the new status), and `@date_modified` (when).

Then it joins these on `CASE_ID` to get the final result ordered by client and timestamp.

**Q: How does the JSON path navigation work?**

Snowflake uses colon notation to traverse semi-structured data. `PARSE_JSON(f.JSON):form:client_profile_group:client_profile_save_to_case:create_client_profile:case:update:current_status::STRING` means: parse the JSON, navigate down through nested keys, and cast the leaf value to STRING. Keys with special characters (like `@name`) need double quotes.

**Q: Why do all the status values show "open"?**

The exercise specifically asks to only include "Create Profile and Refer" forms. These are the initial client profile creation forms, so they always set status to "open." Other form types (like "Outgoing Referral Details" or "Escalated Referral Details") set subsequent statuses like "client_placed" or "withdrawn," but those are different form types not included in scope.

If you wanted a complete status history, you'd need to parse multiple form types, each with different JSON paths for their status updates.

**Q: Why are FIRST_NAME and LAST_NAME NULL?**

These particular clients don't have name data in the `CASE_CLIENT` table — their names were only captured within the form JSON submission but not persisted to the case properties. The `CLIENT_ID` field (e.g., "Polar-Express-00C665") is the available identifier. This is a data quality characteristic of the test data, not a bug in the query.

**Q: You found duplicate rows in the source data. How did you handle that?**

The `case_client.csv` has 23 rows but only 14 unique `CASE_ID` values. The duplicates are exact (every column matches). I used `SELECT DISTINCT *` in a CTE rather than deduplicating at load time, which preserves the raw data exactly as provided while preventing the duplicates from inflating join results.

---

### View 2: Duplicate Detection

**Q: How does the duplicate detection work?**

Three steps:
1. **Build a unified name list** — UNION ALL of client names and alias names, all lowercased and trimmed. Client names filtered to open cases. Aliases use a LEFT JOIN to the client table so orphaned aliases (whose parent isn't in our dataset) are still included.
2. **Self-join for matching** — Join the name list against itself where `CLIENT_CASE_ID < other_CLIENT_CASE_ID` (this deduplicates pairs and prevents self-matches). Match condition is either exact name match OR fuzzy match using `EDITDISTANCE <= 2` on both first and last name.
3. **Output the pairs** with match type classification (exact vs fuzzy).

**Q: What is EDITDISTANCE and why threshold of 2?**

`EDITDISTANCE` (Levenshtein distance) counts the minimum number of single-character edits (insert, delete, substitute) to transform one string into another. A threshold of 2 catches common variations like typos, phonetic similarities ("Justin" vs "Agustin"), and minor spelling differences while keeping false positives manageable. I also require a minimum name length of 3 characters to prevent very short names from producing spurious matches.

**Q: Why LEFT JOIN for aliases instead of INNER JOIN?**

Only 3 out of 20 unique `PARENT_CASE_ID` values in the alias table match a `CASE_ID` in the client table. The other aliases are "orphaned" — their parent client isn't in this dataset. Using an INNER JOIN would drop 85% of aliases. Since we can't determine whether those parent clients are closed, I include orphaned aliases (they could reveal duplicates) but exclude aliases whose parent client is explicitly closed.

**Q: What are the limitations of this approach?**

- **Edit distance is character-based, not phonetic** — it won't catch "Smith" vs "Smythe" (distance 2, works) but would miss "Catherine" vs "Katherine" (distance 1, works) vs "Cathy" (distance > 2, missed). SOUNDEX could complement it.
- **Self-join is O(n^2)** — fine for dozens of records, problematic for millions. At scale you'd use blocking (compare only within same first-letter or zip code groups) or probabilistic approaches.
- **No weighting** — an exact match on a common name like "John Smith" is treated the same as an exact match on "Xerxes Pemberton." In practice you'd want frequency-based scoring.

---

### Architecture & Design Decisions

**Q: Why a single schema instead of raw + transformed (medallion architecture)?**

With 3 tables and 2 views, the overhead of multiple schemas isn't justified. In production I'd use a medallion pattern (raw/staging/analytics) but that's YAGNI here. The views serve as the transformation layer.

**Q: Why SQL views instead of materialized views or tables?**

Views are appropriate because:
- The data is static (no incremental loads)
- The dataset is tiny (no performance concern)
- Views always reflect the current state of the underlying tables
- No need for the maintenance overhead of materialized view refresh schedules

In production with large datasets and frequent queries, I'd consider materialized views or dbt models.

**Q: Why not use dbt for this?**

dbt would be the right choice for a production data pipeline — it gives you version control, testing, documentation, and dependency management for SQL transformations. For a single-shot exercise with 3 tables and 2 views, it's unnecessary overhead. But if asked "how would you productionize this," dbt is the answer.

**Q: How would you handle this if the data were being loaded incrementally?**

I'd add:
- A `LOADED_AT` timestamp column to track when each row was ingested
- Snowpipe or a scheduled task for continuous ingestion
- An incremental merge strategy (MERGE INTO) instead of full reload
- Change data capture for the client table to track status changes natively rather than relying on form submissions

---

### Data Quality Observations

These are good things to proactively mention during the demo:

1. **Source duplicates** — 9 of 14 clients in `case_client.csv` are exact duplicates. Handled with DISTINCT.
2. **Sparse name data** — Many clients have NULL `FIRST_NAME`/`LAST_NAME`, particularly the ones created by "Create Profile and Refer" forms.
3. **Orphaned aliases** — 85% of alias records reference parent clients not in the client dataset. This suggests the client export is a subset, not the full table.
4. **Test data artifacts** — Names like "16th aym" and "30th Adams" are clearly test data, not real names. The duplicate detection correctly handles these.
5. **Single status value** — All "Create Profile and Refer" forms set `current_status = 'open'` because they're creation forms. A full status history would require parsing additional form types.
