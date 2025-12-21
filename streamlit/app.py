import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import pandas as pd

# Set page config
st.set_page_config(page_title="Performance Optimizer", layout="wide")

# Get Session
try:
    session = get_active_session()
except Exception:
    st.error("Could not get active Snowflake session. Make sure you are running this in Streamlit in Snowflake.")
    st.stop()

# ==============================================================================
# Helper Functions
# ==============================================================================

def run_query(query):
    return session.sql(query).collect()

def get_df(query):
    return session.sql(query).to_pandas()

def ask_agent(agent_name, question):
    """
    Simulates sending a prompt to the Snowflake Agent.
    """
    st.info(f"🤖 **{agent_name}** is processing...")
    
    try:
        if agent_name == "PERFORMANCE_COLLECTION_AGENT":
            # Call GET_SLOW_QUERIES directly
            # Defaulting to 2.0s threshold and 24h lookback
            res = session.call("PERFORMANCE_OPTI_APP.CORE.GET_SLOW_QUERIES", 2.0, 24)
            data = json.loads(res)
            
            if isinstance(data, list) and len(data) > 0:
                count = 0
                for item in data[:5]: # Analyze top 5
                    q_id = item.get('QUERY_ID')
                    q_text = item.get('QUERY_TEXT')
                    exec_time = item.get('EXECUTION_SECONDS')
                    
                    # Simulated Analysis
                    analysis = f"Query {q_id} took {exec_time}s. Recommendation: Review partition pruning."
                    ddl = f"-- Example Optimization for {q_id}\n-- ALTER TABLE {item.get('DATABASE_NAME', 'TARGET_DB')}.{item.get('SCHEMA_NAME', 'PUBLIC')}.TABLE_NAME CLUSTER BY (DATE_COL);" 
                    
                    session.call("PERFORMANCE_OPTI_APP.CORE.SAVE_RECOMMENDATION", 
                                 q_id, q_text, float(exec_time), analysis, ddl)
                    count += 1
                return f"Scanned query history. Found and analyzed {count} slow queries."
            else:
                return "No slow queries found exceeding the threshold."

        elif agent_name == "PERFORMANCE_VALIDATION_AGENT":
            # Extract ID from question "Validate recommendation ID <UUID>"
            parts = question.split("ID ")
            if len(parts) < 2: return "Please provide a Recommendation ID."
            rec_id = parts[1].strip()
            
            res = session.call("PERFORMANCE_OPTI_APP.CORE.VALIDATE_RECOMMENDATION", rec_id)
            return res

        elif agent_name == "PRODUCTION_IMPLEMENTATION_AGENT":
             parts = question.split("ID ")
             if len(parts) < 2: return "Please provide a Recommendation ID."
             rec_id = parts[1].split(" ")[0].strip()
             
             if "revert" in question.lower():
                 res = session.call("PERFORMANCE_OPTI_APP.CORE.REVERT_RECOMMENDATION", rec_id)
                 return res
             else:
                 # Deploy
                 revert_script = f"-- Revert script for {rec_id}"
                 res = session.call("PERFORMANCE_OPTI_APP.CORE.DEPLOY_RECOMMENDATION", rec_id, revert_script)
                 return res
                 
    except Exception as e:
        return f"Error executing agent action: {str(e)}"
                 
    return "Agent received request."

# ==============================================================================
# UI Layout
# ==============================================================================

st.title("🚀 Snowflake Performance Optimizer")

tab1, tab2, tab3 = st.tabs(["🔍 Analysis", "✅ Validation", "🚢 Deployment"])

# ------------------------------------------------------------------------------
# Tab 1: Analysis
# ------------------------------------------------------------------------------
with tab1:
    st.subheader("Performance Collection")
    if st.button("Run Performance Scan"):
        with st.spinner("Analyzing Query History..."):
            response = ask_agent("PERFORMANCE_COLLECTION_AGENT", "Find slow queries")
            st.success(response)
    
    st.divider()
    st.subheader("Recommendations")
    try:
        df_recs = get_df("SELECT * FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS ORDER BY CREATED_AT DESC")
        st.dataframe(df_recs, use_container_width=True)
    except:
        st.caption("No recommendations table found yet.")

# ------------------------------------------------------------------------------
# Tab 2: Validation
# ------------------------------------------------------------------------------
with tab2:
    st.subheader("Validation Sandbox")
    
    try:
        pending_recs = get_df("SELECT RECOMMENDATION_ID, TARGET_QUERY_ID, EXECUTION_TIME_SECONDS FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS WHERE STATUS = 'PENDING'")
        
        if not pending_recs.empty:
            rec_id = st.selectbox("Select Recommendation", pending_recs["RECOMMENDATION_ID"])
            if st.button("Validate Selected"):
                with st.spinner("Cloning and Testing..."):
                    response = ask_agent("PERFORMANCE_VALIDATION_AGENT", f"Validate recommendation ID {rec_id}")
                    st.success(response)
                    time.sleep(1)
                    st.experimental_rerun()
        else:
            st.info("No pending recommendations.")
    except:
        pass

# ------------------------------------------------------------------------------
# Tab 3: Deployment
# ------------------------------------------------------------------------------
with tab3:
    st.subheader("Production Deployment")
    
    try:
        ready_recs = get_df("SELECT RECOMMENDATION_ID, TARGET_QUERY_ID, VALIDATION_RESULT FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS WHERE STATUS = 'VALIDATED'")
        
        if not ready_recs.empty:
            deploy_id = st.selectbox("Select Validated Fix", ready_recs["RECOMMENDATION_ID"])
            if st.button("Deploy to Production"):
                with st.spinner("Deploying..."):
                    response = ask_agent("PRODUCTION_IMPLEMENTATION_AGENT", f"Deploy recommendation ID {deploy_id}")
                    st.success(response)
                    time.sleep(1)
                    st.experimental_rerun()
        else:
            st.info("No validated fixes ready for deployment.")
            
        st.divider()
        st.write("Deployed Fixes (Rollback Available)")
        deployed_recs = get_df("SELECT RECOMMENDATION_ID, TARGET_QUERY_ID FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS WHERE STATUS = 'DEPLOYED'")
        if not deployed_recs.empty:
            revert_id = st.selectbox("Select Deployment to Revert", deployed_recs["RECOMMENDATION_ID"])
            if st.button("⚠️ Revert Change"):
                 with st.spinner("Reverting..."):
                    response = ask_agent("PRODUCTION_IMPLEMENTATION_AGENT", f"Revert recommendation ID {revert_id}")
                    st.success(response)
                    time.sleep(1)
                    st.experimental_rerun()
    except:
        pass
