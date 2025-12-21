USE DATABASE PERFORMANCE_OPTI_APP;
USE SCHEMA CORE;

CREATE OR REPLACE PROCEDURE VALIDATE_RECOMMENDATION(
    RECOMMENDATION_ID STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'validate_rec'
AS
$$
import json
import time

def validate_rec(session, rec_id):
    # 1. Fetch Details
    rec_df = session.sql(f"""
        SELECT TARGET_QUERY_ID, QUERY_TEXT, PROPOSED_DDL, EXECUTION_TIME_SECONDS 
        FROM QUERY_RECOMMENDATIONS WHERE RECOMMENDATION_ID = '{rec_id}'
    """).collect()
    
    if not rec_df:
        return "Recommendation not found."
    
    row = rec_df[0]
    original_query = row['QUERY_TEXT']
    ddl = row['PROPOSED_DDL']
    original_time = row['EXECUTION_TIME_SECONDS']
    
    # 2. Get DB Context
    q_info = session.sql(f"""
        SELECT DATABASE_NAME, SCHEMA_NAME 
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE QUERY_ID = '{row['TARGET_QUERY_ID']}'
    """).collect()
    
    if not q_info or not q_info[0]['DATABASE_NAME']:
        # Fallback if query history is too slow or missing: Parse from DDL if possible, or fail
        return "Target DB not found in history."
        
    target_db = q_info[0]['DATABASE_NAME']
    target_schema = q_info[0]['SCHEMA_NAME']
    
    clone_db = f"{target_db}_PERF_TEST_{rec_id.replace('-', '_')}"
    
    try:
        # 3. Clone
        session.sql(f"CREATE DATABASE {clone_db} CLONE {target_db}").collect()
        
        # 4. Apply DDL
        session.sql(f"USE DATABASE {clone_db}").collect()
        if target_schema:
            session.sql(f"USE SCHEMA {target_schema}").collect()
            
        session.sql(ddl).collect()
        
        # 5. Test
        # Simple text replacement for DB name in query
        test_query = original_query.replace(target_db, clone_db)
        
        start_run = time.time()
        session.sql(test_query).collect()
        new_time = time.time() - start_run
        
        # 6. Cleanup
        session.sql(f"DROP DATABASE IF EXISTS {clone_db}").collect()
        
        # 7. Update
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        
        improvement = original_time - new_time
        res_json = json.dumps({
            "status": "SUCCESS",
            "original": original_time,
            "new": new_time,
            "improvement": improvement
        })
        
        update_q = """
        UPDATE QUERY_RECOMMENDATIONS 
        SET STATUS = 'VALIDATED', VALIDATION_RESULT = PARSE_JSON(?)
        WHERE RECOMMENDATION_ID = ?
        """
        session.sql(update_q, params=[res_json, rec_id]).collect()
        
        return f"Validation Complete. Improvement: {improvement:.2f}s"
        
    except Exception as e:
        # Try cleanup
        try: session.sql(f"DROP DATABASE IF EXISTS {clone_db}").collect()
        except: pass
        
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        
        err_json = json.dumps({"error": str(e)})
        session.sql("UPDATE QUERY_RECOMMENDATIONS SET STATUS = 'ERROR', VALIDATION_RESULT = PARSE_JSON(?) WHERE RECOMMENDATION_ID = ?", 
                   params=[err_json, rec_id]).collect()
                   
        return f"Validation Failed: {str(e)}"
$$;
