-- ============================================================================
-- Infrastructure Setup for Performance Optimization App
-- ============================================================================

-- Database and Schema Setup
CREATE DATABASE IF NOT EXISTS PERFORMANCE_OPTI_APP;
USE DATABASE PERFORMANCE_OPTI_APP;
CREATE SCHEMA IF NOT EXISTS CORE;
USE SCHEMA CORE;


-- ============================================================================
-- Logging Table
-- Purpose: Track every action taken by the agents for audit and UI display
-- ============================================================================
CREATE OR REPLACE TABLE APP_LOGS (
    LOG_ID INTEGER AUTOINCREMENT,
    TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    AGENT_NAME STRING,
    ACTION_TYPE STRING, -- 'ANALYSIS', 'VALIDATION', 'DEPLOYMENT', 'ERROR'
    MESSAGE STRING,
    DETAILS VARIANT, -- JSON details about the action
    QUERY_ID STRING
);

-- ============================================================================
-- Recommendations Table
-- Purpose: Store the findings and proposed steps from the Collection Agent
-- ============================================================================
CREATE OR REPLACE TABLE QUERY_RECOMMENDATIONS (
    RECOMMENDATION_ID STRING DEFAULT UUID_STRING(),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    TARGET_QUERY_ID STRING,
    QUERY_TEXT STRING,
    EXECUTION_TIME_SECONDS FLOAT,
    ANALYSIS_TEXT STRING, -- The LLM's explanation
    PROPOSED_DDL STRING, -- The specific SQL suggested (e.g., CREATE INDEX)
    STATUS STRING DEFAULT 'PENDING', -- PENDING, VALIDATING, VALIDATED, DEPLOYED, REJECTED
    VALIDATION_RESULT VARIANT,
    DEPLOYMENT_ID STRING
);

-- ============================================================================
-- Helper Procedure: Log Action
-- ============================================================================
CREATE OR REPLACE PROCEDURE LOG_ACTION(
    AGENT_NAME STRING,
    ACTION_TYPE STRING,
    MESSAGE STRING,
    DETAILS VARIANT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'log_entry'
AS
$$
def log_entry(session, agent_name, action_type, message, details):
    import json
    
    # Escape single quotes in message for SQL safety if not using parameters effectively in simple strings
    # But using session.sql with params is better.
    
    insert_sql = """
    INSERT INTO APP_LOGS (AGENT_NAME, ACTION_TYPE, MESSAGE, DETAILS)
    SELECT ?, ?, ?, ?
    """
    try:
        session.sql(insert_sql, params=[agent_name, action_type, message, details]).collect()
        return "Logged successfully"
    except Exception as e:
        return f"Logging failed: {str(e)}"
$$;

