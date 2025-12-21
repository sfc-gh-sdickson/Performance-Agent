USE DATABASE PERFORMANCE_OPTI_APP;
USE SCHEMA CORE;

-- ============================================================================
-- Analysis Tool
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
AS
$$
import json
from snowflake.snowpark.functions import col, lit

def get_slow(session, min_seconds, lookback_hours):
    min_ms = min_seconds * 1000
    
    # Use Snowpark DataFrame for safer, cleaner query construction
    # But direct SQL is often easier for system views requiring specific latency/permissions
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
      AND QUERY_TEXT NOT LIKE '%QUERY_HISTORY%' 
      AND DATABASE_NAME IS NOT NULL
    ORDER BY EXECUTION_TIME DESC
    LIMIT 20
    """
    
    try:
        df = session.sql(query).to_pandas()
        return df.to_json(orient='records')
    except Exception as e:
        return json.dumps({"error": str(e)})
$$;

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
    try:
        query = """
        INSERT INTO QUERY_RECOMMENDATIONS 
        (TARGET_QUERY_ID, QUERY_TEXT, EXECUTION_TIME_SECONDS, ANALYSIS_TEXT, PROPOSED_DDL, STATUS)
        VALUES (?, ?, ?, ?, ?, 'PENDING')
        """
        session.sql(query, params=[query_id, query_text, exec_time, analysis, ddl]).collect()
        return "Saved"
    except Exception as e:
        return f"Error: {str(e)}"
$$;
