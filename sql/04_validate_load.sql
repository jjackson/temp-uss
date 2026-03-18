USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

-- Expected: 23 rows
SELECT 'CASE_CLIENT' AS table_name, COUNT(*) AS row_count FROM CASE_CLIENT;

-- Expected: 36 rows
SELECT 'CASE_ALIAS' AS table_name, COUNT(*) AS row_count FROM CASE_ALIAS;

-- Expected: 12 rows
SELECT 'FORMS_RAW' AS table_name, COUNT(*) AS row_count FROM FORMS_RAW;

-- Verify JSON column parses correctly
SELECT ID, PARSE_JSON(JSON):form:"@name"::STRING AS form_name
FROM FORMS_RAW
LIMIT 5;
