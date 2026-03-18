USE DATABASE DATA_ENGINEER_EXERCISE_ABD_DB;
USE SCHEMA RAW;

-- View 1: All status changes
SELECT * FROM V_CLIENT_STATUS_CHANGES;

-- View 2: All potential duplicates
SELECT * FROM V_POTENTIAL_DUPLICATE_CLIENTS;

-- Sanity: verify all Create Profile and Refer forms match a client
SELECT
  PARSE_JSON(f.JSON):form:client_profile_group
    :client_profile_save_to_case
    :create_client_profile
    :case:"@case_id"::STRING AS client_case_id,
  c.CASE_ID IS NOT NULL AS has_client_match,
  c.CLOSED
FROM FORMS_RAW f
LEFT JOIN CASE_CLIENT c
  ON c.CASE_ID = PARSE_JSON(f.JSON):form:client_profile_group
    :client_profile_save_to_case
    :create_client_profile
    :case:"@case_id"::STRING
WHERE PARSE_JSON(f.JSON):form:"@name"::STRING = 'Create Profile and Refer';
