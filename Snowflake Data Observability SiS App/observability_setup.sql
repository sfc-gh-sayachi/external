-- =============================================================================
-- DATA OBSERVABILITY - COMPLETE SETUP SCRIPT
-- =============================================================================
-- This script sets up ALL components for the Data Observability application:
--   1. Database & Schema
--   2. Data Freshness Monitoring
--   3. KPI Monitoring  
--   4. Pipeline Health Monitoring
--   5. Slack Alert Integrations
--   6. Alert Procedures
--
-- PREREQUISITES:
--   - ACCOUNTADMIN role (for notification integrations)
--   - A warehouse (e.g., COMPUTE_WH)
--
-- USAGE:
--   1. Configure your Slack webhook URLs in STEP 1
--   2. Run this entire script
--   3. Deploy the Streamlit app
--   4. Use the app wizard to create monitoring tasks
--
-- =============================================================================


-- =============================================================================
-- STEP 1: CONFIGURATION - EDIT THESE VALUES
-- =============================================================================
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ ⚠️  CONFIGURE YOUR SETTINGS BELOW BEFORE RUNNING                         │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Database and schema where observability objects will be created
SET TARGET_DB = 'DATA_QUALITY_MONITORING_DB';
SET TARGET_SCHEMA = 'OBSERVABILITY';

-- Slack webhook secret (the part after /services/ in your webhook URL)
-- Get this from: https://api.slack.com/apps → Incoming Webhooks
-- Your URL looks like: https://hooks.slack.com/services/XXXXX/XXXXX/XXXXX
-- The secret is: XXXXX/XXXXX/XXXXX (everything after /services/)
SET SLACK_WEBHOOK_SECRET = 'XXXXXXX/XXXXXXXXXXX/XXXXXXXXXXXXXXXXX';

-- Dashboard URL (for alert buttons)
SET DASHBOARD_URL = 'https://app.snowflake.com/yv93160/ml89966/#/streamlit-apps/DATA_QUALITY_MONITORING_DB.OBSERVABILITY.PBX3UPJVJ6HKF6D7';


-- =============================================================================
-- STEP 2: CREATE DATABASE & SCHEMA
-- =============================================================================
CREATE DATABASE IF NOT EXISTS IDENTIFIER($TARGET_DB);
SET FULL_SCHEMA_PATH = $TARGET_DB || '.' || $TARGET_SCHEMA;
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($FULL_SCHEMA_PATH);

USE DATABASE IDENTIFIER($TARGET_DB);
USE SCHEMA IDENTIFIER($TARGET_SCHEMA);


-- =============================================================================
-- STEP 3: SLACK NOTIFICATION INTEGRATIONS
-- =============================================================================
-- These are shared by all monitoring types (Data Freshness, KPI, Pipeline)
-- Reference: https://docs.snowflake.com/en/user-guide/notifications/webhook-notifications

-- First, create a SECRET to store the Slack webhook secret
SET SECRET_PATH = $TARGET_DB || '.' || $TARGET_SCHEMA || '.SLACK_WEBHOOK_SECRET';
CREATE OR REPLACE SECRET IDENTIFIER($SECRET_PATH)
    TYPE = GENERIC_STRING
    SECRET_STRING = $SLACK_WEBHOOK_SECRET;

-- Grant usage on the secret
GRANT USAGE ON SECRET IDENTIFIER($SECRET_PATH) TO ROLE PUBLIC;
GRANT READ ON SECRET IDENTIFIER($SECRET_PATH) TO ROLE PUBLIC;

-- Critical alerts integration (for severe issues)
CREATE OR REPLACE NOTIFICATION INTEGRATION data_freshness_slack_critical_int
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = DATA_QUALITY_MONITORING_DB.OBSERVABILITY.SLACK_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "🚨 *CRITICAL - Data Freshness Alert*\\nSNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type' = 'application/json')
    COMMENT = 'Critical data freshness alerts';

CREATE OR REPLACE NOTIFICATION INTEGRATION data_freshness_slack_warning_int
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = DATA_QUALITY_MONITORING_DB.OBSERVABILITY.SLACK_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "⚠️ *WARNING - Data Freshness Alert*\\nSNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type' = 'application/json')
    COMMENT = 'Warning data freshness alerts';

CREATE OR REPLACE NOTIFICATION INTEGRATION kpi_slack_critical_int
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = DATA_QUALITY_MONITORING_DB.OBSERVABILITY.SLACK_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "🚨 *CRITICAL - KPI Alert*\\nSNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type' = 'application/json')
    COMMENT = 'Critical KPI alerts';

CREATE OR REPLACE NOTIFICATION INTEGRATION kpi_slack_warning_int
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = DATA_QUALITY_MONITORING_DB.OBSERVABILITY.SLACK_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "⚠️ *WARNING - KPI Alert*\\nSNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type' = 'application/json')
    COMMENT = 'Warning KPI alerts';

CREATE OR REPLACE NOTIFICATION INTEGRATION pipe_health_slack_critical_int
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = DATA_QUALITY_MONITORING_DB.OBSERVABILITY.SLACK_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "🚨 *CRITICAL - Pipeline Health Alert*\\nSNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type' = 'application/json')
    COMMENT = 'Critical pipeline health alerts';

CREATE OR REPLACE NOTIFICATION INTEGRATION pipe_health_slack_warning_int
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = DATA_QUALITY_MONITORING_DB.OBSERVABILITY.SLACK_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "⚠️ *WARNING - Pipeline Health Alert*\\nSNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type' = 'application/json')
    COMMENT = 'Warning pipeline health alerts';


-- =============================================================================
-- STEP 4: ALERT TRACKING TABLES
-- =============================================================================
-- These tables track sent alerts to avoid duplicate notifications

CREATE TABLE IF NOT EXISTS DATA_FRESHNESS_ALERTS_SENT (
    ALERT_ID            VARCHAR(100) NOT NULL PRIMARY KEY,
    ALERT_TYPE          VARCHAR(20) DEFAULT 'ALL',
    ALERT_DATE          DATE NOT NULL,
    NOTIFICATION_TIME   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    TOTAL_ISSUES_COUNT  NUMBER,
    SCHEMAS_AFFECTED    VARCHAR(4000),
    TABLES_AFFECTED     VARCHAR(4000),
    MESSAGE_SENT        VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS KPI_ALERTS_SENT (
    ALERT_ID            VARCHAR(100) NOT NULL PRIMARY KEY,
    ALERT_TYPE          VARCHAR(20) DEFAULT 'ALL',
    ALERT_DATE          DATE NOT NULL,
    NOTIFICATION_TIME   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    TOTAL_ISSUES_COUNT  NUMBER,
    KPIS_AFFECTED       VARCHAR(4000),
    ANOMALY_TYPES       VARCHAR(500),
    MESSAGE_SENT        VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS PIPE_HEALTH_ALERTS_SENT (
    ALERT_ID            VARCHAR(100) NOT NULL PRIMARY KEY,
    ALERT_TYPE          VARCHAR(20) DEFAULT 'ALL',
    ALERT_DATE          DATE NOT NULL,
    NOTIFICATION_TIME   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    TOTAL_ISSUES_COUNT  NUMBER,
    ISSUE_TYPES         VARCHAR(500),
    PIPES_AFFECTED      VARCHAR(4000),
    MESSAGE_SENT        VARCHAR(4000)
);


-- =============================================================================
-- STEP 5: CONFIGURATION TABLES
-- =============================================================================
-- These tables store monitoring configurations (thresholds, enabled flags, etc.)

-- Schema-level threshold configuration for Data Freshness
CREATE TABLE IF NOT EXISTS SCHEMA_THRESHOLD_CONFIG (
    DATABASE_NAME           VARCHAR(255) NOT NULL,
    SCHEMA_NAME             VARCHAR(255) NOT NULL,
    WARN_THRESHOLD_MINUTES  NUMBER DEFAULT 1440,  -- 24 hours
    ALERT_THRESHOLD_MINUTES NUMBER DEFAULT 2880,  -- 48 hours
    IS_MONITORED            BOOLEAN DEFAULT TRUE,
    CRITICAL_INTEGRATION    VARCHAR(255),  -- Notification integration for critical alerts (NULL = use default)
    WARNING_INTEGRATION     VARCHAR(255),  -- Notification integration for warning alerts (NULL = use default)
    NOTES                   VARCHAR(1000),
    CREATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (DATABASE_NAME, SCHEMA_NAME)
);

-- Add columns if table already exists (migration)
ALTER TABLE SCHEMA_THRESHOLD_CONFIG ADD COLUMN IF NOT EXISTS CRITICAL_INTEGRATION VARCHAR(255);
ALTER TABLE SCHEMA_THRESHOLD_CONFIG ADD COLUMN IF NOT EXISTS WARNING_INTEGRATION VARCHAR(255);

-- Global alert integration configuration (for Pipeline Health, KPI, etc.)
CREATE TABLE IF NOT EXISTS ALERT_INTEGRATION_CONFIG (
    ALERT_TYPE              VARCHAR(50) PRIMARY KEY,
    CRITICAL_INTEGRATION    VARCHAR(255),
    WARNING_INTEGRATION     VARCHAR(255),
    UPDATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table-level configuration for Data Freshness (overrides schema defaults)
CREATE TABLE IF NOT EXISTS TABLE_MONITOR_CONFIG (
    TABLE_FQN               VARCHAR(500) NOT NULL PRIMARY KEY,
    DATABASE_NAME           VARCHAR(255),
    SCHEMA_NAME             VARCHAR(255),
    TABLE_NAME              VARCHAR(255),
    IS_MONITORED            BOOLEAN DEFAULT TRUE,
    IS_CRITICAL             BOOLEAN DEFAULT FALSE,
    WARN_THRESHOLD_MINUTES  NUMBER,  -- NULL = use schema default
    ALERT_THRESHOLD_MINUTES NUMBER,  -- NULL = use schema default
    FRESHNESS_COLUMN        VARCHAR(255),
    MODE                    VARCHAR(50) DEFAULT 'FULL_REFRESH',
    NOTES                   VARCHAR(1000),
    CREATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- KPI configuration
CREATE TABLE IF NOT EXISTS KPI_CONFIG (
    KPI_NAME            VARCHAR(255) NOT NULL PRIMARY KEY,
    KPI_DESCRIPTION     VARCHAR(1000),
    DISPLAY_NAME        VARCHAR(255),
    METRIC_SQL          VARCHAR(10000) NOT NULL,
    EXPECTED_MODE       VARCHAR(50) DEFAULT 'THRESHOLD',
    THRESHOLD_PCT       NUMBER DEFAULT 20,
    FIXED_EXPECTED_VALUE NUMBER,
    BASELINE_DAYS       NUMBER DEFAULT 30,
    DATE_OFFSET         NUMBER DEFAULT 1,
    DASHBOARD_URL       VARCHAR(1000),
    IS_ENABLED          BOOLEAN DEFAULT TRUE,
    IS_MONITORED        BOOLEAN DEFAULT TRUE,
    ALERT_ON_ANOMALY    BOOLEAN DEFAULT TRUE,
    ANOMALY_DIRECTION   VARCHAR(10) DEFAULT 'BOTH',
    CRITICAL_INTEGRATION VARCHAR(255),
    WARNING_INTEGRATION VARCHAR(255),
    CREATED_AT          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add integration columns if table already exists (migration)
ALTER TABLE KPI_CONFIG ADD COLUMN IF NOT EXISTS CRITICAL_INTEGRATION VARCHAR(255);
ALTER TABLE KPI_CONFIG ADD COLUMN IF NOT EXISTS WARNING_INTEGRATION VARCHAR(255);

-- Pipeline monitoring configuration
CREATE TABLE IF NOT EXISTS PIPE_MONITOR_CONFIG (
    PIPE_NAME               VARCHAR(500) NOT NULL PRIMARY KEY,
    DATABASE_NAME           VARCHAR(255),
    SCHEMA_NAME             VARCHAR(255),
    IS_MONITORED            BOOLEAN DEFAULT TRUE,
    RUNS_DAILY              BOOLEAN DEFAULT TRUE,
    ALERT_ON_MISSING        BOOLEAN DEFAULT TRUE,
    ALERT_ON_VOLUME_DROP    BOOLEAN DEFAULT TRUE,
    VOLUME_THRESHOLD_PCT    NUMBER DEFAULT 50,
    NOTES                   VARCHAR(1000),
    CREATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =============================================================================
-- STEP 6: HELPER FUNCTIONS
-- =============================================================================

-- Helper function for Data Freshness filter building
CREATE OR REPLACE FUNCTION BUILD_FRESHNESS_FILTERS(config STRING)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS
$$
    var cfg = JSON.parse(CONFIG);
    var tableFilters = [];
    var fqnFilters = [];
    
    for (var dbName in cfg) {
        var schemas = cfg[dbName];
        var dbUpper = dbName.toUpperCase();
        
        if (schemas.length === 1 && (schemas[0] === '*' || schemas[0].toUpperCase() === 'ALL')) {
            tableFilters.push("(TABLE_CATALOG = '" + dbUpper + "')");
            fqnFilters.push("(SPLIT_PART(UPPER(f.value:objectName::STRING), '.', 1) = '" + dbUpper + "')");
        } else {
            var schemaList = schemas.map(function(s) { return "'" + s.toUpperCase() + "'"; }).join(",");
            tableFilters.push("(TABLE_CATALOG = '" + dbUpper + "' AND TABLE_SCHEMA IN (" + schemaList + "))");
            fqnFilters.push("(SPLIT_PART(UPPER(f.value:objectName::STRING), '.', 1) = '" + dbUpper + "' AND SPLIT_PART(UPPER(f.value:objectName::STRING), '.', 2) IN (" + schemaList + "))");
        }
    }
    
    return {
        "table_filter": tableFilters.join(" OR "),
        "fqn_filter": fqnFilters.join(" OR ")
    };
$$;


-- =============================================================================
-- STEP 7: DATA FRESHNESS PROCEDURES
-- =============================================================================

-- Refresh Data Freshness metrics (incremental - only updates specified schemas)
CREATE OR REPLACE PROCEDURE REFRESH_DATA_FRESHNESS_TABLES(
    P_MONITOR_CONFIG STRING,
    P_BASELINE_DAYS NUMBER DEFAULT 30
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_daily_table STRING;
    v_metrics_table STRING;
    v_sql STRING;
    v_lookback_days NUMBER;
    v_table_filter STRING;
    v_fqn_filter STRING;
    v_filter_obj OBJECT;
    v_db_schema_delete_filter STRING;
    v_metrics_delete_filter STRING;
BEGIN
    v_daily_table := CURRENT_DATABASE() || '.' || CURRENT_SCHEMA() || '.DATA_FRESHNESS_DAILY_VOLUME';
    v_metrics_table := CURRENT_DATABASE() || '.' || CURRENT_SCHEMA() || '.DATA_FRESHNESS_TABLE_METRICS';
    v_lookback_days := GREATEST(P_BASELINE_DAYS, 7) + 7;
    
    v_filter_obj := BUILD_FRESHNESS_FILTERS(P_MONITOR_CONFIG);
    v_table_filter := v_filter_obj:table_filter::STRING;
    v_fqn_filter := v_filter_obj:fqn_filter::STRING;
    
    -- Build delete filter for daily_volume (uses FQN column)
    v_db_schema_delete_filter := REPLACE(v_table_filter, 'TABLE_CATALOG', 'UPPER(SPLIT_PART(FQN, ''.'', 1))');
    v_db_schema_delete_filter := REPLACE(v_db_schema_delete_filter, 'TABLE_SCHEMA', 'UPPER(SPLIT_PART(FQN, ''.'', 2))');
    
    -- Create tables if they don't exist
    v_sql := '
    CREATE TABLE IF NOT EXISTS ' || v_daily_table || ' (
        FQN STRING,
        ACTIVITY_DATE DATE,
        ROWS_INSERTED NUMBER,
        ROWS_UPDATED NUMBER,
        ROWS_DELETED NUMBER,
        NET_ROW_CHANGE NUMBER,
        WRITE_OPERATIONS NUMBER,
        DATA_SOURCES STRING
    )';
    EXECUTE IMMEDIATE v_sql;
    
    v_sql := '
    CREATE TABLE IF NOT EXISTS ' || v_metrics_table || ' (
        DATABASE_NAME STRING,
        SCHEMA_NAME STRING,
        TABLE_NAME STRING,
        FQN STRING,
        CURRENT_ROWS NUMBER,
        CURRENT_BYTES NUMBER,
        TABLE_CREATED TIMESTAMP_NTZ,
        LAST_ALTERED TIMESTAMP_NTZ,
        LAST_MODIFIED_DATE DATE,
        HOURS_SINCE_WRITE NUMBER,
        TODAY_ROWS_MODIFIED NUMBER,
        RECENT_INSERTS NUMBER,
        ACTIVE_DAYS NUMBER,
        DATA_SOURCES STRING,
        HISTORY_DAYS NUMBER,
        BASELINE_DAILY_INSERTS FLOAT,
        MEDIAN_DAILY_INSERTS FLOAT,
        AVG_DAILY_GROWTH FLOAT,
        MEDIAN_DAILY_GROWTH FLOAT,
        BASELINE_TOTAL_INSERTS NUMBER,
        BASELINE_NET_CHANGE NUMBER,
        GROWTH_DAYS NUMBER,
        SHRINK_DAYS NUMBER,
        CHURN_DAYS NUMBER,
        QUIET_DAYS NUMBER,
        TODAY_INSERTS NUMBER,
        TODAY_NET_CHANGE NUMBER,
        YESTERDAY_INSERTS NUMBER,
        YESTERDAY_NET_CHANGE NUMBER,
        PCT_OF_BASELINE FLOAT,
        LOAD_PATTERN STRING,
        INGEST_PATTERN STRING,
        TABLE_AGE_DAYS NUMBER,
        DAYS_SINCE_ALTERED NUMBER,
        BASELINE_ROWS NUMBER,
        REFRESHED_AT TIMESTAMP_NTZ
    )';
    EXECUTE IMMEDIATE v_sql;
    
    -- Delete existing data for the specified schemas only
    v_sql := 'DELETE FROM ' || v_daily_table || ' WHERE ' || v_db_schema_delete_filter;
    EXECUTE IMMEDIATE v_sql;
    
    -- Build delete filter for metrics table (uses DATABASE_NAME and SCHEMA_NAME columns)
    v_metrics_delete_filter := REPLACE(v_table_filter, 'TABLE_CATALOG', 'UPPER(DATABASE_NAME)');
    v_metrics_delete_filter := REPLACE(v_metrics_delete_filter, 'TABLE_SCHEMA', 'UPPER(SCHEMA_NAME)');
    
    v_sql := 'DELETE FROM ' || v_metrics_table || ' WHERE ' || v_metrics_delete_filter;
    EXECUTE IMMEDIATE v_sql;
    
    -- Insert fresh data for specified schemas into daily volume
    v_sql := '
    INSERT INTO ' || v_daily_table || '
    WITH ACCESS_HISTORY_RAW AS (
        SELECT 
            f.value:objectName::STRING AS FQN,
            DATE_TRUNC(''day'', ah.QUERY_START_TIME) AS ACTIVITY_DATE,
            SUM(COALESCE(f.value:rowsInserted::NUMBER, 0)) AS ROWS_INSERTED,
            SUM(COALESCE(f.value:rowsUpdated::NUMBER, 0)) AS ROWS_UPDATED,
            SUM(COALESCE(f.value:rowsDeleted::NUMBER, 0)) AS ROWS_DELETED,
            COUNT(DISTINCT ah.QUERY_ID) AS WRITE_OPERATIONS
        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
             LATERAL FLATTEN(input => ah.OBJECTS_MODIFIED) f
        WHERE ah.QUERY_START_TIME >= DATEADD(''day'', -' || v_lookback_days || ', CURRENT_DATE())
          AND f.value:objectDomain::STRING = ''Table''
          AND (' || v_fqn_filter || ')
        GROUP BY 1, 2
    )
    SELECT 
        FQN,
        ACTIVITY_DATE,
        ROWS_INSERTED,
        ROWS_UPDATED,
        ROWS_DELETED,
        (ROWS_INSERTED - ROWS_DELETED) AS NET_ROW_CHANGE,
        WRITE_OPERATIONS,
        CASE 
            WHEN ROWS_INSERTED > 0 AND ROWS_DELETED > 0 AND ROWS_UPDATED > 0 THEN ''INSERT,UPDATE,DELETE''
            WHEN ROWS_INSERTED > 0 AND ROWS_DELETED > 0 THEN ''INSERT,DELETE''
            WHEN ROWS_INSERTED > 0 AND ROWS_UPDATED > 0 THEN ''INSERT,UPDATE''
            WHEN ROWS_INSERTED > 0 THEN ''INSERT''
            WHEN ROWS_UPDATED > 0 THEN ''UPDATE''
            WHEN ROWS_DELETED > 0 THEN ''DELETE''
            ELSE ''OTHER''
        END AS DATA_SOURCES
    FROM ACCESS_HISTORY_RAW';
    
    EXECUTE IMMEDIATE v_sql;
    
    -- Insert fresh metrics for specified schemas
    v_sql := '
    INSERT INTO ' || v_metrics_table || '
    WITH DAILY_VOLUME AS (
        SELECT * FROM ' || v_daily_table || '
    ),
    TABLES AS (
        SELECT 
            TABLE_CATALOG AS DATABASE_NAME, 
            TABLE_SCHEMA AS SCHEMA_NAME, 
            TABLE_NAME,
            ROW_COUNT AS CURRENT_ROWS, 
            BYTES AS CURRENT_BYTES,
            CREATED AS TABLE_CREATED, 
            LAST_ALTERED,
            UPPER(TABLE_CATALOG || ''.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME) AS FQN
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
        WHERE DELETED IS NULL
          AND (' || v_table_filter || ')
    ),
    LAST_WRITES AS (
        SELECT 
            FQN,
            MAX(ACTIVITY_DATE) AS LAST_ACTIVITY_DATE,
            SUM(CASE WHEN ACTIVITY_DATE = CURRENT_DATE() THEN ROWS_INSERTED + ROWS_UPDATED ELSE 0 END) AS TODAY_ROWS_MODIFIED,
            SUM(CASE WHEN ACTIVITY_DATE >= DATEADD(''day'', -1, CURRENT_DATE()) THEN ROWS_INSERTED ELSE 0 END) AS RECENT_INSERTS,
            COUNT(DISTINCT ACTIVITY_DATE) AS ACTIVE_DAYS,
            LISTAGG(DISTINCT DATA_SOURCES, '','') AS ALL_SOURCES
        FROM DAILY_VOLUME
        GROUP BY FQN
    ),
    LAST_WRITE_TIME AS (
        SELECT FQN, MAX(ACTIVITY_DATE) AS LAST_MODIFIED_DATE
        FROM DAILY_VOLUME
        GROUP BY FQN
    ),
    VOLUME_PERCENTILES AS (
        SELECT 
            FQN,
            ACTIVITY_DATE,
            ROWS_INSERTED,
            ROWS_UPDATED,
            ROWS_DELETED,
            NET_ROW_CHANGE,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY ROWS_INSERTED) OVER (PARTITION BY FQN) AS P10_INSERTS,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ROWS_INSERTED) OVER (PARTITION BY FQN) AS P90_INSERTS,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY NET_ROW_CHANGE) OVER (PARTITION BY FQN) AS P10_NET,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY NET_ROW_CHANGE) OVER (PARTITION BY FQN) AS P90_NET
        FROM DAILY_VOLUME
        WHERE ACTIVITY_DATE >= DATEADD(''day'', -' || P_BASELINE_DAYS || ', CURRENT_DATE())
          AND ACTIVITY_DATE < CURRENT_DATE()
    ),
    VOLUME_BASELINES AS (
        SELECT 
            FQN,
            COUNT(*) AS HISTORY_DAYS,
            AVG(ROWS_INSERTED) AS AVG_DAILY_INSERTS,
            MEDIAN(ROWS_INSERTED) AS MEDIAN_DAILY_INSERTS,
            AVG(NET_ROW_CHANGE) AS AVG_NET_CHANGE,
            MEDIAN(NET_ROW_CHANGE) AS MEDIAN_NET_CHANGE,
            SUM(ROWS_INSERTED) AS TOTAL_INSERTS,
            SUM(NET_ROW_CHANGE) AS TOTAL_NET_CHANGE,
            SUM(CASE WHEN NET_ROW_CHANGE > 0 THEN 1 ELSE 0 END) AS GROWTH_DAYS,
            SUM(CASE WHEN NET_ROW_CHANGE < 0 THEN 1 ELSE 0 END) AS SHRINK_DAYS,
            SUM(CASE WHEN ROWS_INSERTED > 0 AND ROWS_DELETED > 0 THEN 1 ELSE 0 END) AS CHURN_DAYS,
            SUM(CASE WHEN ROWS_INSERTED = 0 AND ROWS_UPDATED = 0 AND ROWS_DELETED = 0 THEN 1 ELSE 0 END) AS QUIET_DAYS
        FROM VOLUME_PERCENTILES
        WHERE ROWS_INSERTED BETWEEN P10_INSERTS AND P90_INSERTS
        GROUP BY FQN
    ),
    YESTERDAY_VOLUME AS (
        SELECT FQN, ROWS_INSERTED AS YESTERDAY_INSERTS, NET_ROW_CHANGE AS YESTERDAY_NET_CHANGE
        FROM DAILY_VOLUME WHERE ACTIVITY_DATE = CURRENT_DATE() - 1
    ),
    TODAY_VOLUME AS (
        SELECT FQN, ROWS_INSERTED AS TODAY_INSERTS, NET_ROW_CHANGE AS TODAY_NET_CHANGE
        FROM DAILY_VOLUME WHERE ACTIVITY_DATE = CURRENT_DATE()
    )
    SELECT 
        t.DATABASE_NAME,
        t.SCHEMA_NAME,
        t.TABLE_NAME,
        t.FQN,
        t.CURRENT_ROWS,
        t.CURRENT_BYTES,
        t.TABLE_CREATED,
        t.LAST_ALTERED,
        lwt.LAST_MODIFIED_DATE,
        DATEDIFF(''hour'', COALESCE(lwt.LAST_MODIFIED_DATE, t.LAST_ALTERED, t.TABLE_CREATED), CURRENT_TIMESTAMP()) AS HOURS_SINCE_WRITE,
        lw.TODAY_ROWS_MODIFIED,
        lw.RECENT_INSERTS,
        lw.ACTIVE_DAYS,
        lw.ALL_SOURCES AS DATA_SOURCES,
        vb.HISTORY_DAYS,
        vb.AVG_DAILY_INSERTS AS BASELINE_DAILY_INSERTS,
        vb.MEDIAN_DAILY_INSERTS,
        vb.AVG_NET_CHANGE AS AVG_DAILY_GROWTH,
        vb.MEDIAN_NET_CHANGE AS MEDIAN_DAILY_GROWTH,
        vb.TOTAL_INSERTS AS BASELINE_TOTAL_INSERTS,
        vb.TOTAL_NET_CHANGE AS BASELINE_NET_CHANGE,
        vb.GROWTH_DAYS,
        vb.SHRINK_DAYS,
        vb.CHURN_DAYS,
        vb.QUIET_DAYS,
        tv.TODAY_INSERTS,
        tv.TODAY_NET_CHANGE,
        yv.YESTERDAY_INSERTS,
        yv.YESTERDAY_NET_CHANGE,
        CASE WHEN vb.AVG_DAILY_INSERTS > 0 THEN ROUND(100.0 * COALESCE(tv.TODAY_INSERTS, 0) / vb.AVG_DAILY_INSERTS, 1) ELSE NULL END AS PCT_OF_BASELINE,
        CASE
            WHEN vb.HISTORY_DAYS IS NULL OR vb.HISTORY_DAYS < 3 THEN ''INSUFFICIENT_HISTORY''
            WHEN vb.GROWTH_DAYS > vb.SHRINK_DAYS * 3 AND vb.CHURN_DAYS < vb.HISTORY_DAYS * 0.2 THEN ''APPEND_ONLY''
            WHEN vb.CHURN_DAYS > vb.HISTORY_DAYS * 0.5 THEN ''HIGH_CHURN''
            WHEN vb.QUIET_DAYS > vb.HISTORY_DAYS * 0.7 THEN ''MOSTLY_STATIC''
            WHEN vb.SHRINK_DAYS > vb.GROWTH_DAYS THEN ''SHRINKING''
            ELSE ''MIXED''
        END AS LOAD_PATTERN,
        CASE
            WHEN vb.HISTORY_DAYS IS NULL THEN
                CASE
                    WHEN ABS(DATEDIFF(''hour'', t.TABLE_CREATED, t.LAST_ALTERED)) < 24 AND t.CURRENT_ROWS > 0 THEN ''CTAS/SNAP''
                    WHEN t.CURRENT_ROWS > 0 AND DATEDIFF(''day'', t.LAST_ALTERED, CURRENT_TIMESTAMP()) > 7 THEN ''BATCH''
                    WHEN t.CURRENT_ROWS > 0 THEN ''EXTERNAL''
                    WHEN t.CURRENT_ROWS = 0 OR t.CURRENT_ROWS IS NULL THEN ''EMPTY''
                    ELSE ''BATCH''
                END
            ELSE ''BATCH''
        END AS INGEST_PATTERN,
        DATEDIFF(''day'', t.TABLE_CREATED, CURRENT_TIMESTAMP()) AS TABLE_AGE_DAYS,
        DATEDIFF(''day'', t.LAST_ALTERED, CURRENT_TIMESTAMP()) AS DAYS_SINCE_ALTERED,
        t.CURRENT_ROWS AS BASELINE_ROWS,
        CURRENT_TIMESTAMP() AS REFRESHED_AT
    FROM TABLES t 
    LEFT JOIN LAST_WRITES lw ON lw.FQN = t.FQN
    LEFT JOIN LAST_WRITE_TIME lwt ON lwt.FQN = t.FQN
    LEFT JOIN VOLUME_BASELINES vb ON vb.FQN = t.FQN
    LEFT JOIN YESTERDAY_VOLUME yv ON yv.FQN = t.FQN
    LEFT JOIN TODAY_VOLUME tv ON tv.FQN = t.FQN';
    
    EXECUTE IMMEDIATE v_sql;
    
    RETURN 'Successfully refreshed tables at ' || CURRENT_TIMESTAMP()::STRING || 
           '. Monitoring config: ' || P_MONITOR_CONFIG ||
           '. Tables updated in: ' || CURRENT_DATABASE() || '.' || CURRENT_SCHEMA();
END;
$$;


-- Send Data Freshness Alert procedure
CREATE OR REPLACE PROCEDURE SEND_DATA_FRESHNESS_ALERT(
    P_CRITICAL_INTEGRATION VARCHAR DEFAULT 'data_freshness_slack_critical_int',
    P_WARNING_INTEGRATION VARCHAR DEFAULT 'data_freshness_slack_warning_int'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_alert'
EXECUTE AS OWNER
AS $$
import snowflake.snowpark as snowpark
from datetime import datetime, date
import json

def send_alert(session, P_CRITICAL_INTEGRATION, P_WARNING_INTEGRATION):
    TARGET_DB = "DATA_QUALITY_MONITORING_DB"
    TARGET_SCHEMA = "OBSERVABILITY"
    DEFAULT_CRITICAL_INTEGRATION = P_CRITICAL_INTEGRATION
    DEFAULT_WARNING_INTEGRATION = P_WARNING_INTEGRATION
    DEFAULT_WARN_HOURS = 24
    DEFAULT_ALERT_HOURS = 48
    BASELINE_DAYS = 30
    APP_URL = "https://app.snowflake.com/yv93160/ml89966/#/streamlit-apps/DATA_QUALITY_MONITORING_DB.OBSERVABILITY.PBX3UPJVJ6HKF6D7"
    
    # Refresh data first - fail if refresh fails
    schema_config = session.sql(f"""
        SELECT DATABASE_NAME, SCHEMA_NAME, CRITICAL_INTEGRATION, WARNING_INTEGRATION 
        FROM {TARGET_DB}.{TARGET_SCHEMA}.SCHEMA_THRESHOLD_CONFIG 
        WHERE IS_MONITORED = TRUE
    """).collect()
    if not schema_config:
        raise Exception("No schemas configured for monitoring. Add schemas to SCHEMA_THRESHOLD_CONFIG first.")
    
    # Build schema config with integrations
    db_schemas = {}
    schema_integrations = {}  # key: "DB.SCHEMA" -> {critical: ..., warning: ...}
    for row in schema_config:
        db = row["DATABASE_NAME"]
        schema = row["SCHEMA_NAME"]
        schema_key = f"{db}.{schema}"
        if db not in db_schemas:
            db_schemas[db] = []
        db_schemas[db].append(schema)
        schema_integrations[schema_key] = {
            "critical": row["CRITICAL_INTEGRATION"] or DEFAULT_CRITICAL_INTEGRATION,
            "warning": row["WARNING_INTEGRATION"] or DEFAULT_WARNING_INTEGRATION
        }
    config_json = json.dumps(db_schemas)
    
    # This will raise an exception if refresh fails
    session.sql(f"CALL {TARGET_DB}.{TARGET_SCHEMA}.REFRESH_DATA_FRESHNESS_TABLES('{config_json}', {BASELINE_DAYS})").collect()
    
    today_str = date.today().isoformat()
    
    # Get tables already alerted today (to detect NEW tables in error)
    def get_alerted_tables(alert_type):
        try:
            result = session.sql(f"""
                SELECT TABLES_AFFECTED FROM {TARGET_DB}.{TARGET_SCHEMA}.DATA_FRESHNESS_ALERTS_SENT 
                WHERE ALERT_DATE = CURRENT_DATE() AND ALERT_TYPE = '{alert_type}'
            """).collect()
            if result and result[0][0]:
                return set(t.strip() for t in result[0][0].split(',') if t.strip())
            return set()
        except:
            return set()
    
    critical_alerted_tables = get_alerted_tables('CRITICAL')
    warning_alerted_tables = get_alerted_tables('WARNING')
    
    issues_query = f"""
    WITH TABLE_THRESHOLDS AS (
        SELECT 
            m.DATABASE_NAME, m.SCHEMA_NAME, m.TABLE_NAME, m.FQN,
            DATEDIFF('hour', GREATEST(COALESCE(m.LAST_MODIFIED_DATE, '1900-01-01'::TIMESTAMP), COALESCE(m.LAST_ALTERED, '1900-01-01'::TIMESTAMP)), CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE,
            COALESCE(tc.WARN_THRESHOLD_MINUTES, sc.WARN_THRESHOLD_MINUTES, {DEFAULT_WARN_HOURS * 60}) / 60 AS WARN_THRESHOLD_HOURS,
            COALESCE(tc.ALERT_THRESHOLD_MINUTES, sc.ALERT_THRESHOLD_MINUTES, {DEFAULT_ALERT_HOURS * 60}) / 60 AS ALERT_THRESHOLD_HOURS,
            COALESCE(tc.IS_CRITICAL, FALSE) AS IS_CRITICAL
        FROM {TARGET_DB}.{TARGET_SCHEMA}.DATA_FRESHNESS_TABLE_METRICS m
        LEFT JOIN {TARGET_DB}.{TARGET_SCHEMA}.TABLE_MONITOR_CONFIG tc ON m.FQN = tc.TABLE_FQN
        LEFT JOIN {TARGET_DB}.{TARGET_SCHEMA}.SCHEMA_THRESHOLD_CONFIG sc ON m.DATABASE_NAME = sc.DATABASE_NAME AND m.SCHEMA_NAME = sc.SCHEMA_NAME
        WHERE COALESCE(tc.IS_MONITORED, FALSE) = TRUE
    )
    SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FQN, HOURS_SINCE_UPDATE, WARN_THRESHOLD_HOURS, ALERT_THRESHOLD_HOURS, IS_CRITICAL,
        CASE WHEN HOURS_SINCE_UPDATE >= ALERT_THRESHOLD_HOURS THEN 'CRITICAL' WHEN HOURS_SINCE_UPDATE >= WARN_THRESHOLD_HOURS THEN 'WARNING' ELSE NULL END AS ALERT_LEVEL
    FROM TABLE_THRESHOLDS WHERE HOURS_SINCE_UPDATE >= WARN_THRESHOLD_HOURS
    ORDER BY IS_CRITICAL DESC, HOURS_SINCE_UPDATE DESC
    """
    
    try:
        issues = session.sql(issues_query).collect()
    except Exception as e:
        return f"ERROR: {str(e)}"
    
    if len(issues) == 0:
        return "Data refreshed. No freshness issues detected."
    
    # Separate issues by alert level
    all_critical_issues = [row for row in issues if row["ALERT_LEVEL"] == "CRITICAL"]
    all_warning_issues = [row for row in issues if row["ALERT_LEVEL"] == "WARNING"]
    
    # Filter to only NEW tables (not already alerted today)
    all_critical_fqns = set(row["FQN"] for row in all_critical_issues)
    all_warning_fqns = set(row["FQN"] for row in all_warning_issues)
    
    new_critical_fqns = all_critical_fqns - critical_alerted_tables
    new_warning_fqns = all_warning_fqns - warning_alerted_tables
    
    critical_issues = [row for row in all_critical_issues if row["FQN"] in new_critical_fqns]
    warning_issues = [row for row in all_warning_issues if row["FQN"] in new_warning_fqns]
    
    results = []
    
    def build_message(issue_list, alert_type, is_new=False, schema_filter=None):
        if schema_filter:
            issue_list = [r for r in issue_list if r["SCHEMA_NAME"] == schema_filter]
        total = len(issue_list)
        if total == 0:
            return None, {}
        schema_counts = {}
        for row in issue_list:
            schema = row["SCHEMA_NAME"]
            schema_counts[schema] = schema_counts.get(schema, 0) + 1
        schema_summary = ", ".join([f"{s}({c})" for s, c in list(schema_counts.items())[:5]])
        new_prefix = "🆕 " if is_new else ""
        emoji = "🚨" if alert_type == "CRITICAL" else "⚠️"
        msg = f"{emoji} {new_prefix}*{total} table(s) {('CRITICALLY stale' if alert_type == 'CRITICAL' else 'need attention')}*"
        msg += f" | *Schemas:* {schema_summary}"
        top_items = []
        for row in issue_list[:5]:
            table = row["TABLE_NAME"][:25]
            hours = int(row["HOURS_SINCE_UPDATE"])
            top_items.append(f"{table}: {hours}h")
        msg += " | *Tables:* " + ", ".join(top_items)
        if total > 5:
            msg += f" (+{total - 5} more)"
        msg += f" | <{APP_URL}|📊 View Dashboard>"
        return msg, schema_counts
    
    def send_notification(message, integration):
        msg_escaped = message.replace("'", "''")
        session.sql(f"""
            CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
                    SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT('{msg_escaped}')
                ),
                SNOWFLAKE.NOTIFICATION.INTEGRATION('{integration}')
            )
        """).collect()
    
    # Group issues by integration to avoid duplicate notifications
    def get_issues_by_integration(issue_list, alert_type):
        integration_issues = {}
        for row in issue_list:
            schema_key = f"{row['DATABASE_NAME']}.{row['SCHEMA_NAME']}"
            integration = schema_integrations.get(schema_key, {}).get(
                "critical" if alert_type == "CRITICAL" else "warning",
                DEFAULT_CRITICAL_INTEGRATION if alert_type == "CRITICAL" else DEFAULT_WARNING_INTEGRATION
            )
            if integration not in integration_issues:
                integration_issues[integration] = []
            integration_issues[integration].append(row)
        return integration_issues
    
    # Process CRITICAL alerts by integration
    if critical_issues:
        is_first_alert = len(critical_alerted_tables) == 0
        critical_by_integration = get_issues_by_integration(critical_issues, "CRITICAL")
        
        for integration, issues_for_int in critical_by_integration.items():
            message, schemas = build_message(issues_for_int, "CRITICAL", is_new=not is_first_alert)
            if message:
                try:
                    send_notification(message, integration)
                    results.append(f"CRITICAL: {len(issues_for_int)} NEW ({integration[:20]})")
                except Exception as e:
                    results.append(f"CRITICAL failed ({integration[:20]}): {str(e)[:50]}")
        
        # Update tracking table
        all_alerted = critical_alerted_tables | all_critical_fqns
        fqn_list = ','.join(sorted(all_alerted))[:4000]
        critical_alert_id = f"DATA_FRESHNESS_CRITICAL_{today_str}"
        try:
            session.sql(f"DELETE FROM {TARGET_DB}.{TARGET_SCHEMA}.DATA_FRESHNESS_ALERTS_SENT WHERE ALERT_ID = '{critical_alert_id}'").collect()
            session.sql(f"""
                INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.DATA_FRESHNESS_ALERTS_SENT 
                (ALERT_ID, ALERT_TYPE, ALERT_DATE, TOTAL_ISSUES_COUNT, SCHEMAS_AFFECTED, TABLES_AFFECTED, MESSAGE_SENT) 
                VALUES ('{critical_alert_id}', 'CRITICAL', CURRENT_DATE(), {len(all_critical_issues)}, 
                        '{','.join(set(r["SCHEMA_NAME"] for r in all_critical_issues))[:4000]}', '{fqn_list}', 'Sent to multiple integrations')
            """).collect()
        except:
            pass
    
    # Process WARNING alerts by integration
    if warning_issues:
        is_first_alert = len(warning_alerted_tables) == 0
        warning_by_integration = get_issues_by_integration(warning_issues, "WARNING")
        
        for integration, issues_for_int in warning_by_integration.items():
            message, schemas = build_message(issues_for_int, "WARNING", is_new=not is_first_alert)
            if message:
                try:
                    send_notification(message, integration)
                    results.append(f"WARNING: {len(issues_for_int)} NEW ({integration[:20]})")
                except Exception as e:
                    results.append(f"WARNING failed ({integration[:20]}): {str(e)[:50]}")
        
        # Update tracking table
        all_alerted = warning_alerted_tables | all_warning_fqns
        fqn_list = ','.join(sorted(all_alerted))[:4000]
        warning_alert_id = f"DATA_FRESHNESS_WARNING_{today_str}"
        try:
            session.sql(f"DELETE FROM {TARGET_DB}.{TARGET_SCHEMA}.DATA_FRESHNESS_ALERTS_SENT WHERE ALERT_ID = '{warning_alert_id}'").collect()
            session.sql(f"""
                INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.DATA_FRESHNESS_ALERTS_SENT 
                (ALERT_ID, ALERT_TYPE, ALERT_DATE, TOTAL_ISSUES_COUNT, SCHEMAS_AFFECTED, TABLES_AFFECTED, MESSAGE_SENT) 
                VALUES ('{warning_alert_id}', 'WARNING', CURRENT_DATE(), {len(all_warning_issues)}, 
                        '{','.join(set(r["SCHEMA_NAME"] for r in all_warning_issues))[:4000]}', '{fqn_list}', 'Sent to multiple integrations')
            """).collect()
        except:
            pass
    
    if results:
        return " | ".join(results)
    elif all_critical_issues or all_warning_issues:
        return f"All {len(all_critical_issues)} critical + {len(all_warning_issues)} warning tables already alerted today. No NEW issues."
    else:
        return "Data refreshed. No freshness issues detected."
$$;


-- =============================================================================
-- STEP 8: KPI MONITORING PROCEDURES
-- =============================================================================

-- Refresh KPI metrics
CREATE OR REPLACE PROCEDURE REFRESH_KPI_METRICS(
    P_TARGET_DB STRING DEFAULT 'DATA_QUALITY_MONITORING_DB',
    P_TARGET_SCHEMA STRING DEFAULT 'OBSERVABILITY',
    P_LOOKBACK_DAYS NUMBER DEFAULT 7
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'refresh_kpi_metrics'
EXECUTE AS CALLER
AS $$
def refresh_kpi_metrics(session, P_TARGET_DB, P_TARGET_SCHEMA, P_LOOKBACK_DAYS):
    from datetime import date, timedelta
    
    daily_table = f"{P_TARGET_DB}.{P_TARGET_SCHEMA}.KPI_DAILY_METRICS"
    summary_table = f"{P_TARGET_DB}.{P_TARGET_SCHEMA}.KPI_HEALTH_SUMMARY"
    config_table = f"{P_TARGET_DB}.{P_TARGET_SCHEMA}.KPI_CONFIG"
    
    # Create daily metrics table if not exists
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {daily_table} (
            KPI_NAME VARCHAR(255) NOT NULL,
            METRIC_DATE DATE NOT NULL,
            METRIC_VALUE NUMBER(38,6),
            PRIMARY KEY (KPI_NAME, METRIC_DATE)
        )
    """).collect()
    
    # Get KPIs - simple query that works
    kpis = session.sql(f"SELECT KPI_NAME, METRIC_SQL FROM {config_table}").collect()
    
    if not kpis:
        return "No KPIs found in config table"
    
    insert_count = 0
    error_count = 0
    errors_detail = []
    
    for kpi in kpis:
        kpi_name = kpi['KPI_NAME']
        metric_sql = kpi['METRIC_SQL']
        
        for day_offset in range(int(P_LOOKBACK_DAYS) + 1):
            result_date = date.today() - timedelta(days=(1 + day_offset))
            result_date_str = result_date.strftime('%Y-%m-%d')
            query_sql = metric_sql.replace('{DATE}', result_date_str)
            
            try:
                result = session.sql(query_sql).collect()
                metric_value = result[0][0] if result else None
                
                if metric_value is not None:
                    session.sql(f"""
                        MERGE INTO {daily_table} t
                        USING (SELECT '{kpi_name}' AS KPI_NAME, '{result_date_str}'::DATE AS METRIC_DATE, {metric_value} AS METRIC_VALUE) s
                        ON t.KPI_NAME = s.KPI_NAME AND t.METRIC_DATE = s.METRIC_DATE
                        WHEN MATCHED THEN UPDATE SET METRIC_VALUE = s.METRIC_VALUE
                        WHEN NOT MATCHED THEN INSERT (KPI_NAME, METRIC_DATE, METRIC_VALUE) VALUES (s.KPI_NAME, s.METRIC_DATE, s.METRIC_VALUE)
                    """).collect()
                    insert_count += 1
            except Exception as e:
                error_count += 1
                if len(errors_detail) < 3:
                    errors_detail.append(f"{kpi_name}/{result_date_str}: {str(e)[:80]}")
    
    # Create summary table with columns expected by the app
    session.sql(f"""
    CREATE OR REPLACE TABLE {summary_table} AS
    WITH LATEST AS (
        SELECT KPI_NAME, METRIC_VALUE AS LATEST_VALUE, METRIC_DATE AS LATEST_DATE,
               ROW_NUMBER() OVER (PARTITION BY KPI_NAME ORDER BY METRIC_DATE DESC) AS RN
        FROM {daily_table}
    ),
    YESTERDAY AS (
        SELECT KPI_NAME, METRIC_VALUE AS YESTERDAY_VALUE
        FROM {daily_table}
        WHERE METRIC_DATE = CURRENT_DATE() - 2
    ),
    BASELINES AS (
        SELECT KPI_NAME, 
               AVG(METRIC_VALUE) AS EXPECTED_VALUE, 
               STDDEV(METRIC_VALUE) AS BASELINE_STDDEV,
               COUNT(*) AS HISTORY_DAYS
        FROM {daily_table}
        WHERE METRIC_DATE >= DATEADD('day', -30, CURRENT_DATE()) AND METRIC_DATE < CURRENT_DATE()
        GROUP BY KPI_NAME
    )
    SELECT 
        l.KPI_NAME,
        l.LATEST_VALUE,
        b.EXPECTED_VALUE,
        CASE WHEN b.EXPECTED_VALUE > 0 THEN ROUND(100.0 * (l.LATEST_VALUE - b.EXPECTED_VALUE) / b.EXPECTED_VALUE, 2) ELSE 0 END AS DEVIATION_PCT,
        CASE WHEN y.YESTERDAY_VALUE > 0 THEN ROUND(100.0 * (l.LATEST_VALUE - y.YESTERDAY_VALUE) / y.YESTERDAY_VALUE, 2) ELSE 0 END AS DAY_OVER_DAY_PCT,
        20 AS THRESHOLD_PCT,
        COALESCE(b.HISTORY_DAYS, 0) AS HISTORY_DAYS,
        l.LATEST_DATE,
        CASE 
            WHEN ABS(CASE WHEN b.EXPECTED_VALUE > 0 THEN 100.0 * (l.LATEST_VALUE - b.EXPECTED_VALUE) / b.EXPECTED_VALUE ELSE 0 END) > 50 THEN 'CRITICAL'
            WHEN ABS(CASE WHEN b.EXPECTED_VALUE > 0 THEN 100.0 * (l.LATEST_VALUE - b.EXPECTED_VALUE) / b.EXPECTED_VALUE ELSE 0 END) > 25 THEN 'WARNING'
            ELSE 'OK' 
        END AS STATUS,
        CASE WHEN ABS(CASE WHEN b.EXPECTED_VALUE > 0 THEN 100.0 * (l.LATEST_VALUE - b.EXPECTED_VALUE) / b.EXPECTED_VALUE ELSE 0 END) > 25 THEN TRUE ELSE FALSE END AS IS_ANOMALY,
        CASE 
            WHEN ABS(CASE WHEN b.EXPECTED_VALUE > 0 THEN 100.0 * (l.LATEST_VALUE - b.EXPECTED_VALUE) / b.EXPECTED_VALUE ELSE 0 END) > 50 THEN 5
            WHEN ABS(CASE WHEN b.EXPECTED_VALUE > 0 THEN 100.0 * (l.LATEST_VALUE - b.EXPECTED_VALUE) / b.EXPECTED_VALUE ELSE 0 END) > 25 THEN 3
            ELSE 1 
        END AS SEVERITY,
        NULL AS ANOMALY_REASON,
        CURRENT_TIMESTAMP() AS REFRESHED_AT
    FROM LATEST l
    LEFT JOIN BASELINES b ON l.KPI_NAME = b.KPI_NAME
    LEFT JOIN YESTERDAY y ON l.KPI_NAME = y.KPI_NAME
    WHERE l.RN = 1
    ORDER BY SEVERITY DESC, l.KPI_NAME
    """).collect()
    
    error_msg = f" | Errors: {'; '.join(errors_detail)}" if errors_detail else ""
    return f"Refreshed {insert_count} KPI metrics with {error_count} errors{error_msg}"
$$;


-- Send KPI Alert procedure (per-KPI integration support)
CREATE OR REPLACE PROCEDURE SEND_KPI_ALERT(
    P_DEFAULT_CRITICAL_INTEGRATION VARCHAR DEFAULT 'kpi_slack_critical_int',
    P_DEFAULT_WARNING_INTEGRATION VARCHAR DEFAULT 'kpi_slack_warning_int'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_alert'
EXECUTE AS OWNER
AS $$
import snowflake.snowpark as snowpark
from datetime import datetime, date, timedelta
from collections import defaultdict

def send_alert(session, P_DEFAULT_CRITICAL_INTEGRATION, P_DEFAULT_WARNING_INTEGRATION):
    TARGET_DB = "DATA_QUALITY_MONITORING_DB"
    TARGET_SCHEMA = "OBSERVABILITY"
    CRITICAL_DEVIATION = 50
    WARNING_DEVIATION = 25
    LOOKBACK_DAYS = 7
    APP_URL = "https://app.snowflake.com/yv93160/ml89966/#/streamlit-apps/DATA_QUALITY_MONITORING_DB.OBSERVABILITY.PBX3UPJVJ6HKF6D7"
    
    # Get global default integrations from ALERT_INTEGRATION_CONFIG (fallback)
    GLOBAL_CRITICAL = P_DEFAULT_CRITICAL_INTEGRATION
    GLOBAL_WARNING = P_DEFAULT_WARNING_INTEGRATION
    try:
        config_df = session.sql(f"""
            SELECT CRITICAL_INTEGRATION, WARNING_INTEGRATION 
            FROM {TARGET_DB}.{TARGET_SCHEMA}.ALERT_INTEGRATION_CONFIG 
            WHERE ALERT_TYPE = 'KPI'
        """).collect()
        if config_df:
            row = config_df[0]
            if row["CRITICAL_INTEGRATION"]:
                GLOBAL_CRITICAL = row["CRITICAL_INTEGRATION"]
            if row["WARNING_INTEGRATION"]:
                GLOBAL_WARNING = row["WARNING_INTEGRATION"]
    except:
        pass
    
    # Refresh KPI metrics first
    try:
        session.sql(f"CALL {TARGET_DB}.{TARGET_SCHEMA}.REFRESH_KPI_METRICS('{TARGET_DB}', '{TARGET_SCHEMA}', {LOOKBACK_DAYS})").collect()
    except:
        pass
    
    today_str = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    
    # Get all KPI issues with their per-KPI integrations
    issues_query = f"""
    WITH KPI_BASELINE AS (
        SELECT KPI_NAME, AVG(METRIC_VALUE) AS BASELINE_AVG, STDDEV(METRIC_VALUE) AS BASELINE_STDDEV, COUNT(*) AS BASELINE_DAYS
        FROM {TARGET_DB}.{TARGET_SCHEMA}.KPI_DAILY_METRICS
        WHERE METRIC_DATE >= DATEADD('day', -30, CURRENT_DATE()) AND METRIC_DATE < CURRENT_DATE() - 1
        GROUP BY KPI_NAME HAVING COUNT(*) >= 7
    ),
    YESTERDAY_METRICS AS (
        SELECT m.KPI_NAME, m.METRIC_VALUE, 
               COALESCE(c.DISPLAY_NAME, c.KPI_NAME) AS DISPLAY_NAME,
               c.ALERT_ON_ANOMALY, c.IS_ENABLED,
               c.CRITICAL_INTEGRATION, c.WARNING_INTEGRATION
        FROM {TARGET_DB}.{TARGET_SCHEMA}.KPI_DAILY_METRICS m
        JOIN {TARGET_DB}.{TARGET_SCHEMA}.KPI_CONFIG c ON m.KPI_NAME = c.KPI_NAME
        WHERE m.METRIC_DATE = '{yesterday}' AND c.IS_ENABLED = TRUE AND c.ALERT_ON_ANOMALY = TRUE
    )
    SELECT y.KPI_NAME, y.DISPLAY_NAME, y.METRIC_VALUE AS YESTERDAY_VALUE, b.BASELINE_AVG,
        y.CRITICAL_INTEGRATION, y.WARNING_INTEGRATION,
        CASE WHEN b.BASELINE_AVG = 0 THEN CASE WHEN y.METRIC_VALUE = 0 THEN 0 ELSE 100 END
             ELSE ABS((y.METRIC_VALUE - b.BASELINE_AVG) / b.BASELINE_AVG) * 100 END AS DEVIATION_PCT,
        CASE WHEN y.METRIC_VALUE > b.BASELINE_AVG THEN 'HIGHER' WHEN y.METRIC_VALUE < b.BASELINE_AVG THEN 'LOWER' ELSE 'SAME' END AS DIRECTION,
        CASE WHEN ABS((y.METRIC_VALUE - b.BASELINE_AVG) / NULLIF(b.BASELINE_AVG, 0)) * 100 >= {CRITICAL_DEVIATION} THEN 'CRITICAL'
             WHEN ABS((y.METRIC_VALUE - b.BASELINE_AVG) / NULLIF(b.BASELINE_AVG, 0)) * 100 >= {WARNING_DEVIATION} THEN 'WARNING'
             WHEN y.METRIC_VALUE > b.BASELINE_AVG + (3 * b.BASELINE_STDDEV) THEN 'CRITICAL'
             WHEN y.METRIC_VALUE < b.BASELINE_AVG - (3 * b.BASELINE_STDDEV) THEN 'CRITICAL'
             WHEN y.METRIC_VALUE > b.BASELINE_AVG + (2 * b.BASELINE_STDDEV) THEN 'WARNING'
             WHEN y.METRIC_VALUE < b.BASELINE_AVG - (2 * b.BASELINE_STDDEV) THEN 'WARNING' ELSE NULL END AS ALERT_LEVEL
    FROM YESTERDAY_METRICS y JOIN KPI_BASELINE b ON y.KPI_NAME = b.KPI_NAME
    WHERE ABS((y.METRIC_VALUE - b.BASELINE_AVG) / NULLIF(b.BASELINE_AVG, 0)) * 100 >= {WARNING_DEVIATION}
       OR y.METRIC_VALUE > b.BASELINE_AVG + (2 * b.BASELINE_STDDEV) OR y.METRIC_VALUE < b.BASELINE_AVG - (2 * b.BASELINE_STDDEV)
    ORDER BY DEVIATION_PCT DESC
    """
    
    try:
        issues = session.sql(issues_query).collect()
    except Exception as e:
        return f"ERROR: {str(e)}"
    
    if len(issues) == 0:
        return f"No KPI anomalies detected for {yesterday}."
    
    # Group issues by integration (using per-KPI or global fallback)
    critical_by_integration = defaultdict(list)
    warning_by_integration = defaultdict(list)
    
    for row in issues:
        alert_level = row["ALERT_LEVEL"]
        if alert_level == "CRITICAL":
            integration = row["CRITICAL_INTEGRATION"] or GLOBAL_CRITICAL
            critical_by_integration[integration].append(row)
        elif alert_level == "WARNING":
            integration = row["WARNING_INTEGRATION"] or GLOBAL_WARNING
            warning_by_integration[integration].append(row)
    
    results = []
    
    def build_message(issue_list, alert_type):
        total = len(issue_list)
        msg = f"*{total} {('CRITICAL KPI anomaly(s)' if alert_type == 'CRITICAL' else 'KPI warning(s)')} detected*"
        top_items = []
        for row in issue_list[:5]:
            kpi = (row["DISPLAY_NAME"] or row["KPI_NAME"])[:30]
            direction = "📈" if row["DIRECTION"] == "HIGHER" else "📉" if row["DIRECTION"] == "LOWER" else "➡️"
            dev = row["DEVIATION_PCT"]
            top_items.append(f"{direction} {kpi}: {dev:.0f}%")
        msg += " | " + " | ".join(top_items)
        if total > 5:
            msg += f" (+{total - 5} more)"
        msg += f" | <{APP_URL}|📊 View Dashboard>"
        return msg
    
    def send_notification(integration, issue_list, alert_type):
        alert_id = f"KPI_{alert_type}_{integration}_{today_str}"
        
        # Check if already sent
        try:
            already_sent = session.sql(f"SELECT COUNT(*) FROM {TARGET_DB}.{TARGET_SCHEMA}.KPI_ALERTS_SENT WHERE ALERT_ID = '{alert_id}'").collect()[0][0] > 0
            if already_sent:
                return f"{alert_type}({integration}): already sent"
        except:
            pass
        
        message = build_message(issue_list, alert_type)
        msg_escaped = message.replace("'", "''")
        
        try:
            session.sql(f"""
                CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                    SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
                        SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT('{msg_escaped}')
                    ),
                    SNOWFLAKE.NOTIFICATION.INTEGRATION('{integration}')
                )
            """).collect()
            
            kpis = ", ".join([row["KPI_NAME"] for row in issue_list[:20]])
            session.sql(f"""
                INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.KPI_ALERTS_SENT 
                (ALERT_ID, ALERT_TYPE, ALERT_DATE, TOTAL_ISSUES_COUNT, KPIS_AFFECTED, MESSAGE_SENT) 
                VALUES ('{alert_id}', '{alert_type}', CURRENT_DATE(), {len(issue_list)}, 
                        '{kpis[:4000]}', '{message[:4000].replace(chr(39), chr(39)+chr(39))}')
            """).collect()
            
            return f"{alert_type}({integration}): {len(issue_list)} KPI(s)"
        except Exception as e:
            return f"{alert_type}({integration}) failed: {str(e)[:50]}"
    
    # Send notifications grouped by integration
    for integration, issue_list in critical_by_integration.items():
        results.append(send_notification(integration, issue_list, "CRITICAL"))
    
    for integration, issue_list in warning_by_integration.items():
        results.append(send_notification(integration, issue_list, "WARNING"))
    
    return " | ".join(results) if results else "No new alerts"
$$;


-- =============================================================================
-- STEP 9: PIPELINE HEALTH PROCEDURES
-- =============================================================================

-- Refresh Pipeline Health metrics
CREATE OR REPLACE PROCEDURE REFRESH_PIPE_HEALTH_TABLES(
    P_TARGET_DB STRING DEFAULT 'DATA_QUALITY_MONITORING_DB',
    P_TARGET_SCHEMA STRING DEFAULT 'OBSERVABILITY',
    P_HISTORY_DAYS NUMBER DEFAULT 30,
    P_LOOKBACK_DAYS NUMBER DEFAULT 45,
    P_OUTLIER_THRESHOLD NUMBER DEFAULT 2.0
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_metrics_table STRING;
    v_history_table STRING;
BEGIN
    v_metrics_table := P_TARGET_DB || '.' || P_TARGET_SCHEMA || '.PIPE_HEALTH_METRICS';
    v_history_table := P_TARGET_DB || '.' || P_TARGET_SCHEMA || '.PIPE_HEALTH_HISTORY';
    
    -- Create history table
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE TABLE ' || v_history_table || ' AS
    SELECT 
        PIPE_CATALOG_NAME || ''.'' || PIPE_SCHEMA_NAME || ''.'' || PIPE_NAME AS PIPE_NAME,
        PIPE_CATALOG_NAME AS DATABASE_NAME,
        PIPE_SCHEMA_NAME AS SCHEMA_NAME,
        DATE_TRUNC(''day'', LAST_LOAD_TIME) AS LOAD_DATE,
        COUNT(*) AS FILES_LOADED,
        SUM(ROW_COUNT) AS ROWS_LOADED,
        SUM(ERROR_COUNT) AS ERRORS,
        AVG(ROW_COUNT) AS AVG_ROWS_PER_FILE,
        MAX(LAST_LOAD_TIME) AS LAST_LOAD_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
    WHERE LAST_LOAD_TIME >= DATEADD(''day'', -' || P_LOOKBACK_DAYS || ', CURRENT_DATE())
      AND PIPE_CATALOG_NAME IS NOT NULL
    GROUP BY 1, 2, 3, 4';
    
    -- Create metrics table
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE TABLE ' || v_metrics_table || ' AS
    WITH DAILY_STATS AS (
        SELECT * FROM ' || v_history_table || '
    ),
    BASELINES AS (
        SELECT 
            PIPE_NAME,
            AVG(FILES_LOADED) AS AVG_FILES,
            AVG(ROWS_LOADED) AS AVG_ROWS,
            AVG(AVG_ROWS_PER_FILE) AS AVG_ROWS_PER_FILE,
            STDDEV(FILES_LOADED) AS STDDEV_FILES,
            COUNT(*) AS HISTORY_DAYS,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(HOUR FROM LAST_LOAD_TIME)) AS P95_LOAD_HOUR
        FROM DAILY_STATS
        WHERE LOAD_DATE >= DATEADD(''day'', -' || P_HISTORY_DAYS || ', CURRENT_DATE())
          AND LOAD_DATE < CURRENT_DATE()
        GROUP BY PIPE_NAME
    ),
    YESTERDAY AS (
        SELECT * FROM DAILY_STATS WHERE LOAD_DATE = CURRENT_DATE() - 1
    ),
    TODAY AS (
        SELECT * FROM DAILY_STATS WHERE LOAD_DATE = CURRENT_DATE()
    )
    SELECT 
        b.PIPE_NAME,
        COALESCE(y.DATABASE_NAME, t.DATABASE_NAME) AS DATABASE_NAME,
        COALESCE(y.SCHEMA_NAME, t.SCHEMA_NAME) AS SCHEMA_NAME,
        y.LOAD_DATE AS YESTERDAY_DATE,
        y.FILES_LOADED AS YESTERDAY_FILES,
        y.ROWS_LOADED AS YESTERDAY_ROWS,
        y.ERRORS AS YESTERDAY_ERRORS,
        y.AVG_ROWS_PER_FILE AS YESTERDAY_ROWS_PER_FILE,
        CASE WHEN b.STDDEV_FILES > 0 AND ABS(y.FILES_LOADED - b.AVG_FILES) > ' || P_OUTLIER_THRESHOLD || ' * b.STDDEV_FILES THEN TRUE ELSE FALSE END AS YESTERDAY_IS_OUTLIER,
        t.LOAD_DATE AS TODAY_DATE,
        t.FILES_LOADED AS TODAY_FILES,
        t.ROWS_LOADED AS TODAY_ROWS,
        ROUND(b.AVG_FILES) AS EXPECTED_FILES,
        ROUND(b.AVG_ROWS) AS EXPECTED_ROWS,
        ROUND(b.AVG_ROWS_PER_FILE, 2) AS EXPECTED_ROWS_PER_FILE,
        b.HISTORY_DAYS,
        b.P95_LOAD_HOUR,
        CASE WHEN b.AVG_FILES > 0 THEN ROUND(100 * (b.AVG_FILES - COALESCE(y.FILES_LOADED, 0)) / b.AVG_FILES, 1) ELSE 0 END AS FILES_SHORT_PCT,
        CASE WHEN b.AVG_ROWS > 0 THEN ROUND(100 * (b.AVG_ROWS - COALESCE(y.ROWS_LOADED, 0)) / b.AVG_ROWS, 1) ELSE 0 END AS ROWS_SHORT_PCT,
        CASE WHEN b.AVG_ROWS_PER_FILE > 0 THEN ROUND(100 * (b.AVG_ROWS_PER_FILE - COALESCE(y.AVG_ROWS_PER_FILE, 0)) / b.AVG_ROWS_PER_FILE, 1) ELSE 0 END AS ROWS_PER_FILE_SHORT_PCT,
        CURRENT_TIMESTAMP() AS REFRESHED_AT
    FROM BASELINES b
    LEFT JOIN YESTERDAY y ON b.PIPE_NAME = y.PIPE_NAME
    LEFT JOIN TODAY t ON b.PIPE_NAME = t.PIPE_NAME';
    
    RETURN 'Refreshed pipe health metrics at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;


-- Send Pipeline Health Alert procedure
CREATE OR REPLACE PROCEDURE SEND_PIPE_HEALTH_ALERT(
    P_CRITICAL_INTEGRATION VARCHAR DEFAULT 'pipe_health_slack_critical_int',
    P_WARNING_INTEGRATION VARCHAR DEFAULT 'pipe_health_slack_warning_int'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_alert'
EXECUTE AS OWNER
AS $$
import snowflake.snowpark as snowpark
from datetime import datetime, date

def send_alert(session, P_CRITICAL_INTEGRATION, P_WARNING_INTEGRATION):
    TARGET_DB = "DATA_QUALITY_MONITORING_DB"
    TARGET_SCHEMA = "OBSERVABILITY"
    HISTORY_DAYS = 30
    LOOKBACK_DAYS = 45
    OUTLIER_THRESHOLD = 2.0
    APP_URL = "https://app.snowflake.com/yv93160/ml89966/#/streamlit-apps/DATA_QUALITY_MONITORING_DB.OBSERVABILITY.PBX3UPJVJ6HKF6D7"
    
    # Try to read integration config from table, fallback to parameters
    CRITICAL_INTEGRATION = P_CRITICAL_INTEGRATION
    WARNING_INTEGRATION = P_WARNING_INTEGRATION
    try:
        config_df = session.sql(f"""
            SELECT CRITICAL_INTEGRATION, WARNING_INTEGRATION 
            FROM {TARGET_DB}.{TARGET_SCHEMA}.ALERT_INTEGRATION_CONFIG 
            WHERE ALERT_TYPE = 'PIPE_HEALTH'
        """).collect()
        if config_df:
            row = config_df[0]
            if row["CRITICAL_INTEGRATION"]:
                CRITICAL_INTEGRATION = row["CRITICAL_INTEGRATION"]
            if row["WARNING_INTEGRATION"]:
                WARNING_INTEGRATION = row["WARNING_INTEGRATION"]
    except:
        pass  # Use parameter defaults if table doesn't exist
    
    # Refresh data first
    try:
        session.sql(f"CALL {TARGET_DB}.{TARGET_SCHEMA}.REFRESH_PIPE_HEALTH_TABLES('{TARGET_DB}', '{TARGET_SCHEMA}', {HISTORY_DAYS}, {LOOKBACK_DAYS}, {OUTLIER_THRESHOLD})").collect()
    except Exception as e:
        return f"ERROR: Failed to refresh: {str(e)}"
    
    today_str = date.today().isoformat()
    critical_alert_id = f"PIPE_HEALTH_CRITICAL_{today_str}"
    warning_alert_id = f"PIPE_HEALTH_WARNING_{today_str}"
    
    try:
        critical_already_sent = session.sql(f"SELECT COUNT(*) FROM {TARGET_DB}.{TARGET_SCHEMA}.PIPE_HEALTH_ALERTS_SENT WHERE ALERT_ID = '{critical_alert_id}'").collect()[0][0] > 0
    except:
        critical_already_sent = False
    
    try:
        warning_already_sent = session.sql(f"SELECT COUNT(*) FROM {TARGET_DB}.{TARGET_SCHEMA}.PIPE_HEALTH_ALERTS_SENT WHERE ALERT_ID = '{warning_alert_id}'").collect()[0][0] > 0
    except:
        warning_already_sent = False
    
    if critical_already_sent and warning_already_sent:
        return f"Both alerts already sent today ({today_str}). Skipping."
    
    issues_query = f"""
    WITH PIPE_ALERTS AS (
        SELECT H.PIPE_NAME, H.DATABASE_NAME, H.SCHEMA_NAME, H.YESTERDAY_DATE, COALESCE(H.YESTERDAY_FILES, 0) AS YESTERDAY_FILES,
            COALESCE(H.YESTERDAY_ERRORS, 0) AS YESTERDAY_ERRORS, H.EXPECTED_FILES, COALESCE(H.TODAY_FILES, 0) AS TODAY_FILES,
            C.RUNS_DAILY, C.ALERT_ON_MISSING, C.ALERT_ON_VOLUME_DROP, C.VOLUME_THRESHOLD_PCT,
            CASE WHEN H.YESTERDAY_DATE IS NULL THEN 'NO_DATA'
                 WHEN C.RUNS_DAILY = TRUE AND H.YESTERDAY_DATE < CURRENT_DATE() - 1 AND C.ALERT_ON_MISSING = TRUE THEN 'STALE_DATA'
                 WHEN C.RUNS_DAILY = TRUE AND C.ALERT_ON_MISSING = TRUE AND COALESCE(H.TODAY_FILES, 0) = 0 AND H.EXPECTED_FILES > 0 THEN 'MISSING_TODAY'
                 WHEN C.ALERT_ON_VOLUME_DROP = TRUE AND H.EXPECTED_FILES > 0 AND COALESCE(H.YESTERDAY_FILES, 0) <= (C.VOLUME_THRESHOLD_PCT / 100.0) * H.EXPECTED_FILES THEN 'LOW_FILES'
                 WHEN COALESCE(H.YESTERDAY_ERRORS, 0) > 0 THEN 'ERRORS' ELSE NULL END AS ISSUE_TYPE,
            CASE WHEN H.YESTERDAY_DATE IS NULL THEN 5 WHEN H.YESTERDAY_DATE < CURRENT_DATE() - 1 THEN 5
                 WHEN C.RUNS_DAILY = TRUE AND COALESCE(H.TODAY_FILES, 0) = 0 AND H.EXPECTED_FILES > 0 THEN 5
                 WHEN COALESCE(H.YESTERDAY_ERRORS, 0) > 0 THEN 4 ELSE 2 END AS SEVERITY
        FROM {TARGET_DB}.{TARGET_SCHEMA}.PIPE_HEALTH_METRICS H
        JOIN {TARGET_DB}.{TARGET_SCHEMA}.PIPE_MONITOR_CONFIG C 
            ON H.PIPE_NAME = C.DATABASE_NAME || '.' || C.SCHEMA_NAME || '.' || C.PIPE_NAME
        WHERE C.IS_MONITORED = TRUE
    )
    SELECT PIPE_NAME, DATABASE_NAME, SCHEMA_NAME, ISSUE_TYPE, SEVERITY, YESTERDAY_DATE, YESTERDAY_FILES, EXPECTED_FILES, YESTERDAY_ERRORS, TODAY_FILES
    FROM PIPE_ALERTS WHERE ISSUE_TYPE IS NOT NULL ORDER BY SEVERITY DESC, ISSUE_TYPE, PIPE_NAME
    """
    
    try:
        issues = session.sql(issues_query).collect()
    except Exception as e:
        return f"ERROR: {str(e)}"
    
    if len(issues) == 0:
        return "No pipe health issues detected."
    
    critical_issues = [row for row in issues if row["SEVERITY"] == 5]
    warning_issues = [row for row in issues if row["SEVERITY"] < 5]
    results = []
    
    def build_message(issue_list, alert_type):
        total = len(issue_list)
        issue_counts = {}
        for row in issue_list:
            itype = row["ISSUE_TYPE"]
            issue_counts[itype] = issue_counts.get(itype, 0) + 1
        issue_summary = ", ".join([f"{v} {k}" for k, v in issue_counts.items()])
        msg = f"*{total} {('CRITICAL issue(s)' if alert_type == 'CRITICAL' else 'warning(s)')}:* {issue_summary}"
        top_items = []
        for row in issue_list[:5]:
            pipe_short = row["PIPE_NAME"].split(".")[-1][:20]
            itype = row["ISSUE_TYPE"]
            top_items.append(f"{pipe_short}:{itype}")
        msg += " | *Pipes:* " + ", ".join(top_items)
        if total > 5:
            msg += f" (+{total - 5} more)"
        msg += f" | <{APP_URL}|📊 View Dashboard>"
        return msg, issue_counts
    
    if critical_issues and not critical_already_sent:
        critical_message, critical_counts = build_message(critical_issues, "CRITICAL")
        try:
            msg_escaped = critical_message.replace("'", "''")
            session.sql(f"""
                CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                    SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
                        SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT('{msg_escaped}')
                    ),
                    SNOWFLAKE.NOTIFICATION.INTEGRATION('{CRITICAL_INTEGRATION}')
                )
            """).collect()
            pipes = ", ".join([row["PIPE_NAME"] for row in critical_issues[:20]])
            session.sql(f"INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.PIPE_HEALTH_ALERTS_SENT (ALERT_ID, ALERT_TYPE, ALERT_DATE, TOTAL_ISSUES_COUNT, ISSUE_TYPES, PIPES_AFFECTED, MESSAGE_SENT) VALUES ('{critical_alert_id}', 'CRITICAL', CURRENT_DATE(), {len(critical_issues)}, '{', '.join(critical_counts.keys())[:500]}', '{pipes[:4000]}', '{critical_message[:4000].replace(chr(39), chr(39)+chr(39))}')").collect()
            results.append(f"CRITICAL: {len(critical_issues)} pipe(s)")
        except Exception as e:
            results.append(f"CRITICAL failed: {str(e)[:50]}")
    
    if warning_issues and not warning_already_sent:
        warning_message, warning_counts = build_message(warning_issues, "WARNING")
        try:
            msg_escaped = warning_message.replace("'", "''")
            session.sql(f"""
                CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                    SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
                        SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT('{msg_escaped}')
                    ),
                    SNOWFLAKE.NOTIFICATION.INTEGRATION('{WARNING_INTEGRATION}')
                )
            """).collect()
            pipes = ", ".join([row["PIPE_NAME"] for row in warning_issues[:20]])
            session.sql(f"INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.PIPE_HEALTH_ALERTS_SENT (ALERT_ID, ALERT_TYPE, ALERT_DATE, TOTAL_ISSUES_COUNT, ISSUE_TYPES, PIPES_AFFECTED, MESSAGE_SENT) VALUES ('{warning_alert_id}', 'WARNING', CURRENT_DATE(), {len(warning_issues)}, '{', '.join(warning_counts.keys())[:500]}', '{pipes[:4000]}', '{warning_message[:4000].replace(chr(39), chr(39)+chr(39))}')").collect()
            results.append(f"WARNING: {len(warning_issues)} pipe(s)")
        except Exception as e:
            results.append(f"WARNING failed: {str(e)[:50]}")
    
    return " | ".join(results) if results else "No new alerts"
$$;


-- =============================================================================
-- SETUP COMPLETE
-- =============================================================================
-- 
-- Next steps:
--   1. Deploy the Streamlit app to Snowflake
--   2. Use the app's Setup Wizard to create monitoring tasks
--   3. Configure thresholds in the Data Freshness page
--   4. Add KPIs in the KPI Monitoring page
--
-- Manual testing:
--   CALL SEND_DATA_FRESHNESS_ALERT();
--   CALL SEND_KPI_ALERT();
--   CALL SEND_PIPE_HEALTH_ALERT();
--
-- =============================================================================
