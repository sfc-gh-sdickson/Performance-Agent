<img src="Snowflake_Logo.svg" width="200">

# Snowflake Performance Optimizer

A Streamlit in Snowflake (SiS) application powered by Snowflake Intelligence Agents to automatically detect, analyze, validate, and deploy query performance improvements.

## Architecture

1.  **Performance Collection Agent**: Scans `QUERY_HISTORY` for slow queries and uses LLM to suggest DDL improvements.
2.  **Performance Validation Agent**: Creates a zero-copy clone of the target database, applies the fix, and runs the query to verify performance gains.
3.  **Production Implementation Agent**: Deploys the validated fix to production and handles rollback (revert) if needed.
4.  **Streamlit UI**: Provides the interface for the user to trigger these agents and view logs.

## Setup Instructions

### 1. Database & Infrastructure
Run the following SQL scripts in your Snowflake account (e.g., via Snowsight Worksheets) in order:

1.  `sql/setup/01_infrastructure.sql` - Creates Database, Schema, Tables.
2.  `sql/procedures/02_analysis_tools.sql` - Creates Analysis tools.
3.  `sql/procedures/03_validation_tools.sql` - Creates Validation logic (Cloning).
4.  `sql/procedures/04_deployment_tools.sql` - Creates Deployment/Revert logic.
5.  `sql/setup/05_agents.sql` - (Optional*) Defines the Cortex Agents.

*> Note: Step 5 requires Snowflake Cortex Agents (Project Polaris) enabled in your account. The Streamlit app simulates the agent behavior by calling tools directly if the Agent Service is not fully configured.*

### 2. Streamlit App
1.  Open **Snowsight** -> **Streamlit**.
2.  Create a New Streamlit App.
3.  Select the `PERFORMANCE_OPTI_APP` database and `CORE` schema (created in step 1).
4.  Paste the contents of `streamlit/app.py` into the editor.
5.  Run the app.

## Usage

1.  **Analysis**: Go to the "Analysis" tab and click "Run Performance Scan". The agent will identify slow queries.
2.  **Validation**: Go to the "Validation" tab. Select a recommendation. The agent will clone your DB, apply the fix, and test it.
3.  **Deployment**: Go to the "Deployment" tab. Deploy validated fixes to production. You can also Revert changes here.

