USE DATABASE PERFORMANCE_OPTI_APP;
USE SCHEMA CORE;

-- ============================================================================
-- Agent 1: Performance Collection Agent
-- Role: Scans for slow queries and proposes optimizations
-- ============================================================================
CREATE OR REPLACE AGENT PERFORMANCE_COLLECTION_AGENT
  COMMENT = 'Identifies slow queries and recommends performance improvements'
  PROFILE = '{"display_name": "Performance Analyst", "avatar": "search", "color": "blue"}'
  FROM SPECIFICATION
  $$
models:
  orchestration: AUTO

instructions:
  response: 'You are an expert Snowflake SQL Performance Analyst. Your goal is to identify slow queries and recommend concrete DDL changes (like Clustering Keys, Materialized Views, or Search Optimization) to improve them.'
  orchestration: '1. Call GetSlowQueries to find candidates. 2. For each slow query, analyze the SQL. 3. Formulate a specific DDL improvement. 4. Call SaveRecommendation to record your finding.'
  system: 'Focus on queries > 2 seconds. Prioritize expensive scans. Provide valid Snowflake DDL.'
  sample_questions:
    - question: 'Find slow queries from the last 24 hours and analyze them.'
      answer: 'I will scan for queries over 2 seconds in the last 24 hours, analyze their patterns, and save recommendations for any that can be optimized.'

tools:
  - tool_spec:
      type: 'generic'
      name: 'GetSlowQueries'
      description: 'Finds queries exceeding execution threshold'
      input_schema:
        type: 'object'
        properties:
          min_execution_time_seconds:
            type: 'number'
            description: 'Minimum execution time (default 2.0)'
          lookback_hours:
            type: 'integer'
            description: 'Hours to look back (default 24)'
        required: ['min_execution_time_seconds', 'lookback_hours']
  - tool_spec:
      type: 'generic'
      name: 'SaveRecommendation'
      description: 'Saves the analysis and proposed DDL for a query'
      input_schema:
        type: 'object'
        properties:
          query_id:
            type: 'string'
          query_text:
            type: 'string'
          execution_time:
            type: 'number'
          analysis:
            type: 'string'
            description: 'Explanation of performance issue'
          proposed_ddl:
            type: 'string'
            description: 'The exact SQL DDL to apply (e.g., ALTER TABLE...)'
        required: ['query_id', 'query_text', 'execution_time', 'analysis', 'proposed_ddl']

tool_resources:
  GetSlowQueries:
    type: 'procedure'
    identifier: 'PERFORMANCE_OPTI_APP.CORE.GET_SLOW_QUERIES'
    execution_environment:
      type: 'warehouse'
      warehouse: 'COMPUTE_WH'
  SaveRecommendation:
    type: 'procedure'
    identifier: 'PERFORMANCE_OPTI_APP.CORE.SAVE_RECOMMENDATION'
    execution_environment:
      type: 'warehouse'
      warehouse: 'COMPUTE_WH'
  $$;

-- ============================================================================
-- Agent 2: Performance Validation Agent
-- Role: Clones environment and tests the recommendation
-- ============================================================================
CREATE OR REPLACE AGENT PERFORMANCE_VALIDATION_AGENT
  COMMENT = 'Validates performance improvements in a sandbox'
  PROFILE = '{"display_name": "QA Engineer", "avatar": "check-circle", "color": "green"}'
  FROM SPECIFICATION
  $$
models:
  orchestration: AUTO

instructions:
  response: 'You are a QA Engineer. You validate performance recommendations by running them in a cloned environment.'
  orchestration: 'Call ValidateRecommendation for the provided Recommendation ID.'
  
tools:
  - tool_spec:
      type: 'generic'
      name: 'ValidateRecommendation'
      description: 'Clones DB, applies DDL, runs query, measures improvement'
      input_schema:
        type: 'object'
        properties:
          recommendation_id:
            type: 'string'
        required: ['recommendation_id']

tool_resources:
  ValidateRecommendation:
    type: 'procedure'
    identifier: 'PERFORMANCE_OPTI_APP.CORE.VALIDATE_RECOMMENDATION'
    execution_environment:
      type: 'warehouse'
      warehouse: 'COMPUTE_WH'
  $$;

-- ============================================================================
-- Agent 3: Production Implementation Agent
-- Role: Deploys changes to Prod and handles Rollbacks
-- ============================================================================
CREATE OR REPLACE AGENT PRODUCTION_IMPLEMENTATION_AGENT
  COMMENT = 'Deploys validated changes to production'
  PROFILE = '{"display_name": "Release Manager", "avatar": "server", "color": "red"}'
  FROM SPECIFICATION
  $$
models:
  orchestration: AUTO

instructions:
  response: 'You are a Release Manager. You deploy validated changes to production and can revert them if needed.'
  orchestration: 'To deploy, call DeployRecommendation with the ID and a Revert Script. To revert, call RevertRecommendation.'
  
tools:
  - tool_spec:
      type: 'generic'
      name: 'DeployRecommendation'
      description: 'Applies the DDL to production'
      input_schema:
        type: 'object'
        properties:
          recommendation_id:
            type: 'string'
          revert_script:
            type: 'string'
            description: 'The SQL to undo the change (e.g. DROP VIEW ...)'
        required: ['recommendation_id', 'revert_script']
  - tool_spec:
      type: 'generic'
      name: 'RevertRecommendation'
      description: 'Undoes a deployment'
      input_schema:
        type: 'object'
        properties:
          recommendation_id:
            type: 'string'
        required: ['recommendation_id']

tool_resources:
  DeployRecommendation:
    type: 'procedure'
    identifier: 'PERFORMANCE_OPTI_APP.CORE.DEPLOY_RECOMMENDATION'
    execution_environment:
      type: 'warehouse'
      warehouse: 'COMPUTE_WH'
  RevertRecommendation:
    type: 'procedure'
    identifier: 'PERFORMANCE_OPTI_APP.CORE.REVERT_RECOMMENDATION'
    execution_environment:
      type: 'warehouse'
      warehouse: 'COMPUTE_WH'
  $$;

