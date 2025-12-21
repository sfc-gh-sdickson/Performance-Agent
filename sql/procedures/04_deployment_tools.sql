USE DATABASE PERFORMANCE_OPTI_APP;
USE SCHEMA CORE;

CREATE OR REPLACE PROCEDURE DEPLOY_RECOMMENDATION(
    RECOMMENDATION_ID STRING,
    REVERT_SCRIPT STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'deploy'
AS
$$
import json

def deploy(session, rec_id, revert_script):
    rec = session.sql(f"SELECT TARGET_QUERY_ID, PROPOSED_DDL, STATUS FROM QUERY_RECOMMENDATIONS WHERE RECOMMENDATION_ID = '{rec_id}'").collect()
    
    if not rec or rec[0]['STATUS'] != 'VALIDATED':
        return "Not validated or not found."
    
    ddl = rec[0]['PROPOSED_DDL']
    
    # Get DB
    q_info = session.sql(f"SELECT DATABASE_NAME, SCHEMA_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = '{rec[0]['TARGET_QUERY_ID']}'").collect()
    target_db = q_info[0]['DATABASE_NAME']
    target_schema = q_info[0]['SCHEMA_NAME']
    
    try:
        session.sql(f"USE DATABASE {target_db}").collect()
        if target_schema: session.sql(f"USE SCHEMA {target_schema}").collect()
        
        session.sql(ddl).collect()
        
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        
        session.sql("UPDATE QUERY_RECOMMENDATIONS SET STATUS = 'DEPLOYED', REVERT_DDL = ?, DEPLOYMENT_ID = UUID_STRING() WHERE RECOMMENDATION_ID = ?", 
                   params=[revert_script, rec_id]).collect()
                   
        return "Deployed"
    except Exception as e:
        return f"Deploy Failed: {str(e)}"
$$;

CREATE OR REPLACE PROCEDURE REVERT_RECOMMENDATION(
    RECOMMENDATION_ID STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'revert'
AS
$$
import json

def revert(session, rec_id):
    rec = session.sql(f"SELECT TARGET_QUERY_ID, REVERT_DDL, STATUS FROM QUERY_RECOMMENDATIONS WHERE RECOMMENDATION_ID = '{rec_id}'").collect()
    
    if not rec or rec[0]['STATUS'] != 'DEPLOYED':
        return "Not deployed or not found."
        
    revert_ddl = rec[0]['REVERT_DDL']
    
    # Get DB
    q_info = session.sql(f"SELECT DATABASE_NAME, SCHEMA_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_ID = '{rec[0]['TARGET_QUERY_ID']}'").collect()
    target_db = q_info[0]['DATABASE_NAME']
    target_schema = q_info[0]['SCHEMA_NAME']
    
    try:
        session.sql(f"USE DATABASE {target_db}").collect()
        if target_schema: session.sql(f"USE SCHEMA {target_schema}").collect()
        
        session.sql(revert_ddl).collect()
        
        session.sql("USE DATABASE PERFORMANCE_OPTI_APP").collect()
        session.sql("USE SCHEMA CORE").collect()
        
        session.sql(f"UPDATE QUERY_RECOMMENDATIONS SET STATUS = 'REVERTED' WHERE RECOMMENDATION_ID = '{rec_id}'").collect()
        
        return "Reverted"
    except Exception as e:
        return f"Revert Failed: {str(e)}"
$$;
