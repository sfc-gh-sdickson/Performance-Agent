-- ============================================================================
-- Infrastructure Setup
-- ============================================================================
CREATE DATABASE IF NOT EXISTS PERFORMANCE_OPTI_APP;
USE DATABASE PERFORMANCE_OPTI_APP;
CREATE SCHEMA IF NOT EXISTS CORE;
USE SCHEMA CORE;

-- Tables
CREATE OR REPLACE TABLE APP_LOGS (
    LOG_ID INTEGER AUTOINCREMENT,
    TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    AGENT_NAME STRING,
    ACTION_TYPE STRING,
    MESSAGE STRING,
    DETAILS VARIANT,
    QUERY_ID STRING
);

CREATE OR REPLACE TABLE QUERY_RECOMMENDATIONS (
    RECOMMENDATION_ID STRING DEFAULT UUID_STRING(),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    TARGET_QUERY_ID STRING,
    QUERY_TEXT STRING,
    EXECUTION_TIME_SECONDS FLOAT,
    ANALYSIS_TEXT STRING,
    PROPOSED_DDL STRING,
    STATUS STRING DEFAULT 'PENDING', -- PENDING, VALIDATING, VALIDATED, DEPLOYED, REVERTED
    VALIDATION_RESULT VARIANT,
    DEPLOYMENT_ID STRING,
    REVERT_DDL STRING
);

-- Logging Helper
CREATE OR REPLACE PROCEDURE LOG_ACTION(
    AGENT_NAME STRING,
    ACTION_TYPE STRING,
    MESSAGE STRING,
    DETAILS VARIANT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'log_entry'
AS
$$
def log_entry(session, agent_name, action_type, message, details):
    import json
    try:
        # Using simple SQL insertion to avoid potential dataframe overhead for single row
        # Escaping is minimal here; param binding in pure SQL is preferred but this is a helper.
        # We will use session.create_dataframe for safer insertion if needed, but SQL is faster for logs.
        # Let's use parameterized insert via session.sql
        query = "INSERT INTO APP_LOGS (AGENT_NAME, ACTION_TYPE, MESSAGE, DETAILS) VALUES (?, ?, ?, ?)"
        session.sql(query, params=[agent_name, action_type, message, details]).collect()
        return "Logged"
    except Exception as e:
        return f"Log Error: {str(e)}"
$$;
