USE DATABASE PERFORMANCE_OPTI_APP;
USE SCHEMA CORE;

-- Add REVERT_DDL column if it doesn't exist (Idempotent)
BEGIN
    ALTER TABLE QUERY_RECOMMENDATIONS ADD COLUMN IF NOT EXISTS REVERT_DDL STRING;
EXCEPTION
    WHEN OTHER THEN NULL;
END;

-- ============================================================================
-- Tool: Deploy Recommendation
-- Description: Applies the validated DDL to Production.
-- ============================================================================
CREATE OR REPLACE PROCEDURE DEPLOY_RECOMMENDATION(
    RECOMMENDATION_ID STRING,
    REVERT_SCRIPT STRING -- Agent must provide the script to undo this change
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'deploy_rec'
COMMENT = 'Deploys the optimization to the production database'
AS
$$
import json

def deploy_rec(session, rec_id, revert_script):
    # 1. Fetch Details
    rec = session.sql(f"""
        SELECT TARGET_QUERY_ID, PROPOSED_DDL, STATUS 
        FROM QUERY_RECOMMENDATIONS WHERE RECOMMENDATION_ID = '{rec_id}'
    """).collect()
    
    if not rec:
        return "Recommendation not found."
    
    row = rec[0]
    ddl = row['PROPOSED_DDL']
    
    if row['STATUS'] != 'VALIDATED':
        return f"Cannot deploy. Current status is {row['STATUS']}, must be VALIDATED."

    # 2. Identify Target DB (Same logic as validation, should persist this actually)
    # We really should have stored the target DB in the rec table.
    # We will fetch it again from Query History.
    try:
        q_info = session.sql(f"""
            SELECT DATABASE_NAME, SCHEMA_NAME 
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE QUERY_ID = '{row['TARGET_QUERY_ID']}'
        """).collect()
        
        target_db = q_info[0]['DATABASE_NAME']
        target_schema = q_info[0]['SCHEMA_NAME']
        
        # 3. Apply Change
        session.sql(f"USE DATABASE {target_db}").collect()
        if target_schema:
            session.sql(f"USE SCHEMA {target_schema}").collect()
            
        session.sql(ddl).collect()
        
        # 4. Update Status and Save Revert Script
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        
        # Use parameterized query for safety with scripts
        update_sql = """
        UPDATE QUERY_RECOMMENDATIONS 
        SET STATUS = 'DEPLOYED', 
            REVERT_DDL = ?,
            DEPLOYMENT_ID = UUID_STRING()
        WHERE RECOMMENDATION_ID = ?
        """
        session.sql(update_sql, params=[revert_script, rec_id]).collect()
        
        session.call("LOG_ACTION", "PRODUCTION_DEPLOYER", "DEPLOYMENT_SUCCESS", 
                     f"Deployed change for Rec {rec_id}", json.dumps({"ddl": ddl}))
        
        return "Deployment successful."
        
    except Exception as e:
        session.call("LOG_ACTION", "PRODUCTION_DEPLOYER", "DEPLOYMENT_ERROR", 
                     f"Failed to deploy Rec {rec_id}", json.dumps({"error": str(e)}))
        return f"Deployment failed: {str(e)}"
$$;

-- ============================================================================
-- Tool: Revert Recommendation
-- Description: Undoes the deployment using the REVERT_DDL.
-- ============================================================================
CREATE OR REPLACE PROCEDURE REVERT_RECOMMENDATION(
    RECOMMENDATION_ID STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'revert_rec'
AS
$$
import json

def revert_rec(session, rec_id):
    rec = session.sql(f"""
        SELECT TARGET_QUERY_ID, REVERT_DDL, STATUS 
        FROM QUERY_RECOMMENDATIONS WHERE RECOMMENDATION_ID = '{rec_id}'
    """).collect()
    
    if not rec:
        return "Recommendation not found."
    
    row = rec[0]
    revert_ddl = row['REVERT_DDL']
    
    if row['STATUS'] != 'DEPLOYED':
        return "Cannot revert. Change is not currently DEPLOYED."
        
    try:
        # Get Context
        q_info = session.sql(f"""
            SELECT DATABASE_NAME, SCHEMA_NAME 
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE QUERY_ID = '{row['TARGET_QUERY_ID']}'
        """).collect()
        
        target_db = q_info[0]['DATABASE_NAME']
        target_schema = q_info[0]['SCHEMA_NAME']
        
        # Apply Revert
        session.sql(f"USE DATABASE {target_db}").collect()
        if target_schema:
            session.sql(f"USE SCHEMA {target_schema}").collect()
            
        session.sql(revert_ddl).collect()
        
        # Update Status
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        
        session.sql(f"""
        UPDATE QUERY_RECOMMENDATIONS 
        SET STATUS = 'REVERTED'
        WHERE RECOMMENDATION_ID = '{rec_id}'
        """).collect()
        
        session.call("LOG_ACTION", "PRODUCTION_DEPLOYER", "REVERT_SUCCESS", 
                     f"Reverted change for Rec {rec_id}", json.dumps({"revert_ddl": revert_ddl}))
                     
        return "Revert successful."
        
    except Exception as e:
         return f"Revert failed: {str(e)}"
$$;

