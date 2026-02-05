-- ============================================================================
-- Unity Catalog Governance Configuration
-- Governed AI Lakehouse - Row-Level Security and PII Masking
-- ============================================================================

-- ============================================================================
-- CATALOG AND SCHEMA CREATION
-- ============================================================================

-- Create the main catalog for the lakehouse
CREATE CATALOG IF NOT EXISTS main
COMMENT 'Production lakehouse catalog with governed data assets';

-- Create bronze schema for raw ingestion
CREATE SCHEMA IF NOT EXISTS main.bronze
COMMENT 'Bronze layer: Raw data from source systems';

-- Create silver schema for cleaned data
CREATE SCHEMA IF NOT EXISTS main.silver
COMMENT 'Silver layer: Cleaned and validated data';

-- Create gold schema for aggregated metrics
CREATE SCHEMA IF NOT EXISTS main.gold
COMMENT 'Gold layer: Business-ready aggregated data for AI/ML';

-- ============================================================================
-- ROW-LEVEL SECURITY FUNCTIONS
-- ============================================================================

-- Function to filter sensitive data based on user role
CREATE OR REPLACE FUNCTION main.gold.filter_sensitive_data(
  user_email STRING,
  ip_address STRING,
  revenue DOUBLE
)
RETURNS STRUCT<user_email: STRING, ip_address: STRING, revenue: DOUBLE>
COMMENT 'Apply row-level security: Admins see all data, analysts see masked PII'
RETURN
  CASE
    -- Admin group sees full data
    WHEN is_account_group_member('admin') THEN
      STRUCT(user_email, ip_address, revenue)
    
    -- Analyst group sees masked PII
    WHEN is_account_group_member('analyst') THEN
      STRUCT(
        CONCAT('***@', SPLIT(user_email, '@')[1]) AS user_email,  -- Mask email prefix
        CONCAT(SUBSTRING(ip_address, 1, 7), '.***') AS ip_address,  -- Mask IP suffix
        revenue
      )
    
    -- Default: completely mask PII
    ELSE
      STRUCT(
        '[REDACTED]' AS user_email,
        '[REDACTED]' AS ip_address,
        NULL AS revenue
      )
  END;


-- Function to check if user can view revenue data
CREATE OR REPLACE FUNCTION main.gold.can_view_revenue()
RETURNS BOOLEAN
COMMENT 'Determine if current user has permission to view revenue data'
RETURN
  is_account_group_member('admin') OR 
  is_account_group_member('finance') OR
  is_account_group_member('analyst');


-- ============================================================================
-- APPLY ROW-LEVEL SECURITY TO GOLD TABLES
-- ============================================================================

-- Create secured view of user daily metrics with RLS
CREATE OR REPLACE VIEW main.gold.user_daily_metrics_secured AS
SELECT
  user_id,
  event_date,
  total_events,
  distinct_event_types,
  
  -- Apply revenue masking based on permissions
  CASE
    WHEN main.gold.can_view_revenue() THEN total_revenue
    ELSE NULL
  END AS total_revenue,
  
  CASE
    WHEN main.gold.can_view_revenue() THEN avg_revenue_per_event
    ELSE NULL
  END AS avg_revenue_per_event,
  
  _gold_timestamp
FROM main.gold.gold_user_daily_metrics;

COMMENT ON VIEW main.gold.user_daily_metrics_secured IS 
'Secured view with row-level security: Revenue visible only to authorized groups';


-- Create secured view with PII masking
CREATE OR REPLACE VIEW main.silver.events_secured AS
SELECT
  event_id,
  event_type,
  user_id,
  event_timestamp,
  event_properties,
  
  -- Apply PII masking using the security function
  main.gold.filter_sensitive_data(user_email, ip_address, revenue).user_email AS user_email,
  main.gold.filter_sensitive_data(user_email, ip_address, revenue).ip_address AS ip_address,
  main.gold.filter_sensitive_data(user_email, ip_address, revenue).revenue AS revenue,
  
  _silver_timestamp
FROM main.silver.silver_events;

COMMENT ON VIEW main.silver.events_secured IS 
'Secured view with dynamic PII masking based on user groups';


-- ============================================================================
-- COLUMN-LEVEL TAGGING FOR DATA DISCOVERY
-- ============================================================================

-- Tag PII columns for data governance and discovery
ALTER TABLE main.silver.silver_events 
ALTER COLUMN user_email 
SET TAGS ('classification' = 'PII', 'sensitivity' = 'high');

ALTER TABLE main.silver.silver_events 
ALTER COLUMN ip_address 
SET TAGS ('classification' = 'PII', 'sensitivity' = 'high');

ALTER TABLE main.gold.gold_user_daily_metrics 
ALTER COLUMN total_revenue 
SET TAGS ('classification' = 'financial', 'sensitivity' = 'medium');


-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to analyst group (limited access)
GRANT USAGE ON CATALOG main TO `analyst`;
GRANT USAGE ON SCHEMA main.silver TO `analyst`;
GRANT USAGE ON SCHEMA main.gold TO `analyst`;
GRANT SELECT ON VIEW main.silver.events_secured TO `analyst`;
GRANT SELECT ON VIEW main.gold.user_daily_metrics_secured TO `analyst`;

-- Grant permissions to admin group (full access)
GRANT ALL PRIVILEGES ON CATALOG main TO `admin`;
GRANT ALL PRIVILEGES ON SCHEMA main.bronze TO `admin`;
GRANT ALL PRIVILEGES ON SCHEMA main.silver TO `admin`;
GRANT ALL PRIVILEGES ON SCHEMA main.gold TO `admin`;

-- Grant permissions to data engineering group (write access)
GRANT USAGE ON CATALOG main TO `data_engineering`;
GRANT ALL PRIVILEGES ON SCHEMA main.bronze TO `data_engineering`;
GRANT ALL PRIVILEGES ON SCHEMA main.silver TO `data_engineering`;
GRANT ALL PRIVILEGES ON SCHEMA main.gold TO `data_engineering`;

-- ============================================================================
-- AUDIT LOGGING
-- ============================================================================

-- Enable audit logging on sensitive tables
ALTER TABLE main.silver.silver_events 
SET TBLPROPERTIES ('delta.logRetentionDuration' = '90 days');

ALTER TABLE main.gold.gold_user_daily_metrics 
SET TBLPROPERTIES ('delta.logRetentionDuration' = '90 days');


-- ============================================================================
-- DATA LINEAGE TRACKING
-- ============================================================================

-- Document table lineage in comments
COMMENT ON TABLE main.bronze.raw_events IS 
'Source: Auto Loader ingestion from S3/ADLS | Downstream: silver_events';

COMMENT ON TABLE main.silver.silver_events IS 
'Source: bronze_events | Transformations: DLT quality checks | Downstream: gold tables';

COMMENT ON TABLE main.gold.gold_user_daily_metrics IS 
'Source: silver_events | Purpose: ML feature engineering and analytics';