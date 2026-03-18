USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA RAW;

-- Upload CSV files to internal stage
PUT file://{DATA_DIR}/case_client.csv @csv_stage/case_client AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://{DATA_DIR}/case_alias.csv @csv_stage/case_alias AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://{DATA_DIR}/forms_raw.csv @csv_stage/forms_raw AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Load case_client (23 rows expected)
COPY INTO CASE_CLIENT
FROM @csv_stage/case_client
FILE_FORMAT = csv_format
ON_ERROR = 'ABORT_STATEMENT';

-- Load case_alias (36 rows expected)
COPY INTO CASE_ALIAS
FROM @csv_stage/case_alias
FILE_FORMAT = csv_format
ON_ERROR = 'ABORT_STATEMENT';

-- Load forms_raw (12 rows expected)
COPY INTO FORMS_RAW
FROM @csv_stage/forms_raw
FILE_FORMAT = csv_format
ON_ERROR = 'ABORT_STATEMENT';
