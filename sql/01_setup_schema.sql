USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- Single CSV file format for all files.
-- Snowflake's CSV parser natively handles multiline fields within
-- FIELD_OPTIONALLY_ENCLOSED_BY quotes, so forms_raw.csv (which has
-- multiline JSON) loads correctly with the same format.
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = NONE
  NULL_IF = ('');

-- Internal stage for CSV uploads
CREATE OR REPLACE STAGE csv_stage
  FILE_FORMAT = csv_format;
