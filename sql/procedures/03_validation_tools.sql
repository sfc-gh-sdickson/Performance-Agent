USE DATABASE PERFORMANCE_OPTI_APP;
USE SCHEMA CORE;

-- ============================================================================
-- Tool: Validate Recommendation
-- Description: 
-- 1. Clones the target database to a sandbox (DB_CLONE_<UUID>).
-- 2. Applies the Optimization DDL.
-- 3. Runs the Query (or Explain Plan) to verify improvement.
-- 4. Logs results and cleans up.
-- ============================================================================
CREATE OR REPLACE PROCEDURE VALIDATE_RECOMMENDATION(
    RECOMMENDATION_ID STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'validate_rec'
COMMENT = 'Orchestrates the cloning, application, and testing of a recommendation'
AS
$$
import json
import time

def validate_rec(session, rec_id):
    # 1. Fetch Recommendation Details
    rec = session.sql(f"""
        SELECT TARGET_QUERY_ID, QUERY_TEXT, PROPOSED_DDL, EXECUTION_TIME_SECONDS 
        FROM QUERY_RECOMMENDATIONS WHERE RECOMMENDATION_ID = '{rec_id}'
    """).collect()
    
    if not rec:
        return "Recommendation not found."
    
    row = rec[0]
    original_query = row['QUERY_TEXT']
    ddl = row['PROPOSED_DDL']
    original_time = row['EXECUTION_TIME_SECONDS']
    
    # 2. Setup Sandbox
    # We assume the query runs in the current DB context or we need to know the source DB.
    # For this generalized tool, we'll try to determine the current DB from context or assume 'MICROCHIP_INTELLIGENCE' 
    # (Since we need a concrete target, usually the query history shows the DB).
    # We will try to parse the DB from the session context of the original query if available, 
    # but QUERY_HISTORY 'DATABASE_NAME' column would be better.
    # For now, let's assume the user configures the TARGET_DATABASE in the app or we pass it.
    # We'll use a placeholder 'TARGET_DB' variable for the logic.
    
    # Let's verify the DB from the query history if possible, but here we might just clone the "current" DB 
    # or the one specified. 
    # SIMPLIFICATION: We will clone the DB where the app is installed or pass it as a param? 
    # No, the agent finds the query. The query belongs to a DB. 
    # We'll add logic to find the DB name from the query ID.
    
    try:
        q_info = session.sql(f"""
            SELECT DATABASE_NAME, SCHEMA_NAME 
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE QUERY_ID = '{row['TARGET_QUERY_ID']}'
        """).collect()
        
        if not q_info or not q_info[0]['DATABASE_NAME']:
            return "Could not identify target database for the query."
            
        target_db = q_info[0]['DATABASE_NAME']
        target_schema = q_info[0]['SCHEMA_NAME']
        
        clone_db_name = f"{target_db}_PERF_TEST_{rec_id.replace('-', '_')}"
        
        # Log start
        session.call("LOG_ACTION", "PERFORMANCE_VALIDATOR", "VALIDATION_START", 
                     f"Cloning {target_db} to {clone_db_name}", json.dumps({"rec_id": rec_id}))

        # Create Clone
        session.sql(f"CREATE DATABASE {clone_db_name} CLONE {target_db}").collect()
        
        # 3. Apply DDL in Clone
        # We need to make sure the DDL runs in the clone context
        session.sql(f"USE DATABASE {clone_db_name}").collect()
        if target_schema:
            session.sql(f"USE SCHEMA {target_schema}").collect()
            
        # Execute the optimization (e.g., Create Materialized View, Index, etc.)
        start_apply = time.time()
        session.sql(ddl).collect()
        apply_time = time.time() - start_apply
        
        # 4. Run Test (Execute Query)
        # Note: If the query has fully qualified names (DB.SCHEMA), it will hit the original DB!
        # We must attempt to rewrite DB references or rely on context.
        # Simple string replace of the DB name:
        test_query = original_query.replace(target_db, clone_db_name)
        
        start_run = time.time()
        # We run the query. If it's huge, this might be slow. 
        # Ideally we might want to just run EXPLAIN or a limited version.
        # But the user asked to "analyze performance", so we run it.
        session.sql(test_query).collect()
        new_time = time.time() - start_run
        
        # 5. Cleanup
        session.sql(f"DROP DATABASE IF EXISTS {clone_db_name}").collect()
        
        # 6. Record Results
        improvement = original_time - new_time
        result_json = json.dumps({
            "status": "SUCCESS",
            "original_time": original_time,
            "new_time": new_time,
            "improvement_seconds": improvement,
            "clone_used": clone_db_name
        })
        
        update_sql = f"""
        UPDATE QUERY_RECOMMENDATIONS 
        SET STATUS = 'VALIDATED', 
            VALIDATION_RESULT = PARSE_JSON('{result_json}')
        WHERE RECOMMENDATION_ID = '{rec_id}'
        """
        # Switch back to App DB to update
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        session.sql(update_sql).collect()
        
        session.call("LOG_ACTION", "PERFORMANCE_VALIDATOR", "VALIDATION_COMPLETE", 
                     f"Validation finished. Improvement: {improvement}s", json.dumps(json.loads(result_json)))
        
        return f"Validation successful. New time: {new_time}s (Original: {original_time}s)"

    except Exception as e:
        # cleanup if failed
        try:
             session.sql(f"DROP DATABASE IF EXISTS {clone_db_name}").collect()
        except:
            pass
            
        error_msg = str(e)
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        session.sql(f"""
            UPDATE QUERY_RECOMMENDATIONS 
            SET STATUS = 'ERROR', 
                VALIDATION_RESULT = PARSE_JSON('{json.dumps({"error": error_msg})}')
            WHERE RECOMMENDATION_ID = '{rec_id}'
        """).collect()
        
        session.call("LOG_ACTION", "PERFORMANCE_VALIDATOR", "ERROR", 
                     "Validation failed", json.dumps({"error": error_msg}))
                     
        return f"Validation failed: {error_msg}"
$$;

