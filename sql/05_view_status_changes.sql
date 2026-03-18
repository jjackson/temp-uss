USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA RAW;

CREATE OR REPLACE VIEW V_CLIENT_STATUS_CHANGES AS
WITH deduplicated_clients AS (
  SELECT DISTINCT *
  FROM CASE_CLIENT
  WHERE LOWER(CLOSED) = 'false'
),
form_status_updates AS (
  SELECT
    f.ID AS FORM_ID,
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
  WHERE PARSE_JSON(f.JSON):form:"@name"::STRING = 'Create Profile and Refer'
)
SELECT
  c.CASE_ID,
  c.CLIENT_ID,
  c.CASE_NAME,
  c.FIRST_NAME,
  c.LAST_NAME,
  fsu.status_value AS CURRENT_STATUS,
  fsu.status_change_ts AS STATUS_CHANGE_DATE,
  fsu.FORM_ID
FROM form_status_updates fsu
JOIN deduplicated_clients c
  ON c.CASE_ID = fsu.client_case_id
ORDER BY c.CASE_ID, fsu.status_change_ts;
