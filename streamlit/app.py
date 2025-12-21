import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import pandas as pd
import time
import re

# Set page config
st.set_page_config(page_title="Snowflake Performance Optimizer", layout="wide")

# Get Session
session = get_active_session()

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
    In a real environment with Cortex Agents enabled, this would utilize the 
    Agent API or SQL function like:
    SELECT * FROM TABLE(EVALUATE_AGENT(:agent_name, :question))
    
    For this app, we will simulate the Agent's orchestration logic by calling 
    the underlying tools directly based on the 'intent' of the question, 
    to ensure the app is functional even if the Agent Service is not fully provisioned.
    """
    st.info(f"🤖 **{agent_name}** is processing: '{question}'...")
    
    # SIMULATION LOGIC
    if agent_name == "PERFORMANCE_COLLECTION_AGENT":
        # The agent would decide to call GET_SLOW_QUERIES
        # We'll parse the prompt to extract params or default
        min_exec = 2.0
        hours = 24
        
        try:
             # Extract time (e.g. "2.0s")
             time_match = re.search(r'execution time > (\d+\.?\d*)', question)
             if time_match:
                 min_exec = float(time_match.group(1))
                 
             # Extract hours (e.g. "last 24 hours")
             hours_match = re.search(r'last (\d+) hours', question)
             if hours_match:
                 hours = int(hours_match.group(1))
                 
             st.caption(f"Agent decided to call tool: `GetSlowQueries({min_exec}, {hours})`")
             res = session.call("PERFORMANCE_OPTI_APP.CORE.GET_SLOW_QUERIES", float(min_exec), int(hours))
        except Exception as e:
             st.error(f"Error executing agent tool: {e}")
             res = "[]"

        data = json.loads(res)
        
        # Then the agent would analyze each and call SAVE_RECOMMENDATION
        # We will simulate this loop for the first few items
        if isinstance(data, list) and len(data) > 0:
            for item in data[:3]: # Limit to 3 for demo speed
                q_id = item.get('QUERY_ID')
                q_text = item.get('QUERY_TEXT')
                exec_time = item.get('EXECUTION_SECONDS')
                
                # Mock LLM Analysis (In reality, the Agent does this)
                analysis = f"Query {q_id} is slow ({exec_time}s) due to full table scan."
                ddl = f"ALTER TABLE {item.get('DATABASE_NAME', 'DB')}.{item.get('SCHEMA_NAME', 'SCH')}.TABLE_NAME CLUSTER BY (DATE_COL);" 
                # Note: Real agent would generate valid DDL based on query analysis
                
                st.caption(f"Agent decided to call tool: `SaveRecommendation({q_id}...)`")
                session.call("PERFORMANCE_OPTI_APP.CORE.SAVE_RECOMMENDATION", 
                             q_id, q_text, float(exec_time), analysis, ddl)
            return "I have scanned the query history and saved recommendations for the slowest queries."
        else:
            return "I found no queries exceeding the threshold."

    elif agent_name == "PERFORMANCE_VALIDATION_AGENT":
        # Question format: "Validate recommendation X"
        # Extract ID (Simplified)
        # In reality agent parses natural language
        try:
            rec_id = question.split("ID ")[1].strip()
            st.caption(f"Agent decided to call tool: `ValidateRecommendation({rec_id})`")
            res = session.call("PERFORMANCE_OPTI_APP.CORE.VALIDATE_RECOMMENDATION", rec_id)
            return res
        except:
             return "Could not parse ID."

    elif agent_name == "PRODUCTION_IMPLEMENTATION_AGENT":
        if "revert" in question.lower():
             rec_id = question.split("ID ")[1].strip()
             st.caption(f"Agent decided to call tool: `RevertRecommendation({rec_id})`")
             res = session.call("PERFORMANCE_OPTI_APP.CORE.REVERT_RECOMMENDATION", rec_id)
             return res
        else:
             # Deploy
             try:
                 parts = question.split("ID ")
                 rec_id = parts[1].split(" ")[0].strip()
                 # Agent generates revert script
                 revert = "DROP TABLE ... -- Agent Generated Revert"
                 st.caption(f"Agent decided to call tool: `DeployRecommendation({rec_id}, ...)`")
                 res = session.call("PERFORMANCE_OPTI_APP.CORE.DEPLOY_RECOMMENDATION", rec_id, revert)
                 return res
             except:
                 return "Could not parse ID."
                 
    return "Agent response simulated."

# ==============================================================================
# UI Layout
# ==============================================================================

st.title("🚀 Snowflake Performance Optimizer")
st.markdown("Automated Performance Analysis, Validation, and Deployment using **Snowflake Agents**.")

tab1, tab2, tab3, tab4 = st.tabs(["🔍 Analysis", "✅ Validation", "🚢 Deployment", "📜 Logs"])

# ------------------------------------------------------------------------------
# Tab 1: Analysis
# ------------------------------------------------------------------------------
with tab1:
    st.header("Performance Collection Agent")
    st.write("Ask the agent to scan your account for slow queries.")
    
    col_input1, col_input2 = st.columns(2)
    with col_input1:
        min_exec_input = st.number_input("Min Execution Time (seconds)", min_value=0.1, value=2.0, step=0.5)
    with col_input2:
        lookback_input = st.number_input("Lookback Period (hours)", min_value=1, value=24, step=1)
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Run Performance Scan"):
            with st.spinner("Agent is working..."):
                prompt = f"Find slow queries from the last {lookback_input} hours with execution time > {min_exec_input}s and analyze them."
                response = ask_agent("PERFORMANCE_COLLECTION_AGENT", prompt)
                st.success(response)
    
    st.subheader("Current Recommendations")
    df_recs = get_df("SELECT * FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS ORDER BY CREATED_AT DESC")
    st.dataframe(df_recs, use_container_width=True)

# ------------------------------------------------------------------------------
# Tab 2: Validation
# ------------------------------------------------------------------------------
with tab2:
    st.header("Performance Validation Agent")
    st.write("Select a recommendation to validate in a cloned sandbox.")
    
    # Filter for PENDING
    pending_recs = get_df("SELECT RECOMMENDATION_ID, TARGET_QUERY_ID, EXECUTION_TIME_SECONDS FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS WHERE STATUS = 'PENDING'")
    
    if not pending_recs.empty:
        rec_id = st.selectbox("Select Recommendation to Validate", pending_recs["RECOMMENDATION_ID"])
        
        if st.button("Validate Recommendation"):
            with st.spinner("Agent is cloning environment and testing..."):
                response = ask_agent("PERFORMANCE_VALIDATION_AGENT", f"Validate recommendation ID {rec_id}")
                st.success(response)
                st.rerun()
    else:
        st.info("No pending recommendations found.")
        
    st.subheader("Validation Results")
    validated_recs = get_df("SELECT RECOMMENDATION_ID, STATUS, VALIDATION_RESULT FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS WHERE STATUS IN ('VALIDATED', 'ERROR')")
    st.dataframe(validated_recs, use_container_width=True)

# ------------------------------------------------------------------------------
# Tab 3: Deployment
# ------------------------------------------------------------------------------
with tab3:
    st.header("Production Implementation Agent")
    st.write("Deploy validated changes to Production.")
    
    # Filter for VALIDATED
    ready_recs = get_df("SELECT RECOMMENDATION_ID, TARGET_QUERY_ID, VALIDATION_RESULT FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS WHERE STATUS = 'VALIDATED'")
    
    if not ready_recs.empty:
        deploy_id = st.selectbox("Select Recommendation to Deploy", ready_recs["RECOMMENDATION_ID"])
        
        if st.button("Deploy to Production"):
            with st.spinner("Agent is deploying change..."):
                response = ask_agent("PRODUCTION_IMPLEMENTATION_AGENT", f"Deploy recommendation ID {deploy_id}")
                st.success(response)
                st.rerun()
    else:
        st.info("No validated recommendations ready for deployment.")

    st.divider()
    st.subheader("Rollback / Revert")
    
    deployed_recs = get_df("SELECT RECOMMENDATION_ID, TARGET_QUERY_ID FROM PERFORMANCE_OPTI_APP.CORE.QUERY_RECOMMENDATIONS WHERE STATUS = 'DEPLOYED'")
    
    if not deployed_recs.empty:
        revert_id = st.selectbox("Select Deployment to Revert", deployed_recs["RECOMMENDATION_ID"])
        
        if st.button("⚠️ Revert Change"):
             with st.spinner("Agent is reverting change..."):
                response = ask_agent("PRODUCTION_IMPLEMENTATION_AGENT", f"Revert recommendation ID {revert_id}")
                st.success(response)
                st.rerun()

# ------------------------------------------------------------------------------
# Tab 4: Logs
# ------------------------------------------------------------------------------
with tab4:
    st.header("Agent Activity Logs")
    if st.button("Refresh Logs"):
        st.rerun()
        
    logs = get_df("SELECT * FROM PERFORMANCE_OPTI_APP.CORE.APP_LOGS ORDER BY TIMESTAMP DESC LIMIT 100")
    st.dataframe(logs, use_container_width=True)

