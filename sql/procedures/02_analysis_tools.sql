USE DATABASE PERFORMANCE_OPTI_APP;
USE SCHEMA CORE;

-- ============================================================================
-- Tool: Get Slow Queries
-- Description: Scans QUERY_HISTORY for queries exceeding the threshold.
--              Filters for DML and Selects.
-- ============================================================================
CREATE OR REPLACE PROCEDURE GET_SLOW_QUERIES(
    MIN_EXECUTION_TIME_SECONDS FLOAT,
    LOOKBACK_HOURS INT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_slow'
COMMENT = 'Finds slow SELECT/DML queries to analyze'
AS
$$
import json

def get_slow(session, min_seconds, lookback_hours):
    min_ms = min_seconds * 1000
    
    query = f"""
    SELECT 
        QUERY_ID, 
        QUERY_TEXT, 
        DATABASE_NAME,
        SCHEMA_NAME,
        QUERY_TYPE,
        EXECUTION_TIME / 1000.0 AS EXECUTION_SECONDS, 
        BYTES_SCANNED, 
        PARTITIONS_SCANNED, 
        TOTAL_PARTITIONS,
        START_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE EXECUTION_TIME >= {min_ms}
      AND START_TIME >= DATEADD('hour', -{lookback_hours}, CURRENT_TIMESTAMP())
      AND EXECUTION_STATUS = 'SUCCESS'
      AND QUERY_TYPE IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE')
      AND QUERY_TEXT NOT LIKE '%QUERY_HISTORY%' -- Exclude self
    ORDER BY EXECUTION_TIME DESC
    LIMIT 20
    """
    
    try:
        df = session.sql(query).to_pandas()
        
        # Log this check
        session.call("LOG_ACTION", "PERFORMANCE_COLLECTOR", "ANALYSIS", 
                     f"Scanned for queries > {min_seconds}s in last {lookback_hours}h", 
                     json.dumps({"count": len(df)}))
                     
        return df.to_json(orient='records')
    except Exception as e:
        return json.dumps({"error": str(e)})
$$;

-- ============================================================================
-- Tool: Save Recommendation
-- Description: Persists the Agent's analysis to the table.
-- ============================================================================
CREATE OR REPLACE PROCEDURE SAVE_RECOMMENDATION(
    QUERY_ID STRING,
    QUERY_TEXT STRING,
    EXECUTION_TIME FLOAT,
    ANALYSIS STRING,
    PROPOSED_DDL STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'save_rec'
AS
$$
import json

def save_rec(session, query_id, query_text, exec_time, analysis, ddl):
    insert_sql = """
    INSERT INTO QUERY_RECOMMENDATIONS 
    (TARGET_QUERY_ID, QUERY_TEXT, EXECUTION_TIME_SECONDS, ANALYSIS_TEXT, PROPOSED_DDL, STATUS)
    VALUES (?, ?, ?, ?, ?, 'PENDING')
    """
    try:
        session.sql(insert_sql, params=[query_id, query_text, exec_time, analysis, ddl]).collect()
        
        session.call("LOG_ACTION", "PERFORMANCE_COLLECTOR", "RECOMMENDATION", 
                     f"Generated recommendation for Query {query_id}", 
                     json.dumps({"analysis_snippet": analysis[:100]}))
                     
        return "Recommendation saved successfully."
    except Exception as e:
        return f"Error saving recommendation: {str(e)}"
$$;

