-- ==============================================================================
-- FB Nova - Metadata Tables for Load Tracking
-- ALIGNED WITH SOLUTION ARCHITECTURE DESIGN DOCUMENT (SADD)
-- ==============================================================================
-- Run this script in the TARGET database (synapse-fbnova-dev/sqlpoolfbnovadev)
-- before running the sync with delta loading support.
--
-- These tables implement the SADD Key Logging Dimensions:
-- - Pipeline name, job name, BatchID
-- - Start time, end time, duration
-- - Trigger type (manual, scheduled, event-based)
-- - Row counts: read, inserted, updated, rejected
-- - Source and target table names
-- - Status, Error Message
-- ==============================================================================

-- Create meta schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'meta')
    EXEC('CREATE SCHEMA meta');
GO

-- ==============================================================================
-- Table 1: nova_load_stats (SADD Key Logging Dimensions)
-- ==============================================================================
-- Tracks every sync run with detailed statistics
-- Aligned with SADD operational logging requirements
-- ==============================================================================

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats')
BEGIN
    CREATE TABLE [meta].[nova_load_stats] (
        -- Primary key
        id INT IDENTITY(1,1) NOT NULL,
        
        -- =====================================================================
        -- SADD: Pipeline/Job Identification
        -- =====================================================================
        run_id VARCHAR(50) NOT NULL,            -- Unique batch ID (BatchID per SADD)
        pipeline_name VARCHAR(128),             -- e.g., PL_NOVA_SYNC (per SADD: PL_POS, PL_FIS)
        trigger_type VARCHAR(50),               -- per SADD: manual, scheduled, event-based
        
        -- =====================================================================
        -- SADD: Source System Identification
        -- =====================================================================
        source_system_id INT,                   -- per SADD: unique id for each source system
        source_system_name VARCHAR(128),        -- e.g., 'FB_DW_PROD', 'CRM', 'POS'
        
        -- =====================================================================
        -- SADD: Timestamps (Start time, end time, duration)
        -- =====================================================================
        run_started DATETIME2 NOT NULL,         -- When this table sync started
        run_ended DATETIME2,                    -- When this table sync ended
        
        -- =====================================================================
        -- Source/Target Table Information
        -- =====================================================================
        source_schema VARCHAR(128) NOT NULL,    -- e.g., 'dwh'
        source_table VARCHAR(128) NOT NULL,     -- e.g., 'FactSales'
        target_schema VARCHAR(128) NOT NULL,    -- e.g., 'stg_mig'
        target_table VARCHAR(128) NOT NULL,     -- e.g., 'FactSales'
        
        -- =====================================================================
        -- Load Type Configuration
        -- =====================================================================
        load_type VARCHAR(20) NOT NULL,         -- 'FULL' or 'DELTA'
        delta_column VARCHAR(128),              -- Column used for delta (e.g., 'TransactionDate')
        delta_start_value VARCHAR(50),          -- Start value for delta range
        delta_end_value VARCHAR(50),            -- End value for delta range
        
        -- =====================================================================
        -- SADD: Row counts (read, inserted, updated, rejected)
        -- =====================================================================
        rows_read BIGINT DEFAULT 0,             -- Rows read from source
        rows_loaded BIGINT DEFAULT 0,           -- Rows successfully inserted to target
        rows_rejected BIGINT DEFAULT 0,         -- Rows rejected during Load Ready validation
        chunks_processed INT DEFAULT 0,         -- Number of CSV chunks processed
        
        -- =====================================================================
        -- SADD: Performance Metrics
        -- =====================================================================
        duration_ms BIGINT,                     -- Total duration in milliseconds
        
        -- =====================================================================
        -- SADD: Status and Error Message
        -- =====================================================================
        success BIT DEFAULT 0,                  -- 1 = success, 0 = failure
        error_message NVARCHAR(4000)            -- Error details if failed
    )
    WITH (
        DISTRIBUTION = ROUND_ROBIN,
        CLUSTERED COLUMNSTORE INDEX
    );
    
    PRINT 'Created table: meta.nova_load_stats (SADD-compliant)';
END
ELSE
    PRINT 'Table already exists: meta.nova_load_stats';
GO

-- Add new columns if table exists but missing SADD fields
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats')
BEGIN
    -- Add pipeline_name if missing
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats' 
                   AND COLUMN_NAME = 'pipeline_name')
        ALTER TABLE meta.nova_load_stats ADD pipeline_name VARCHAR(128);
    
    -- Add trigger_type if missing
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats' 
                   AND COLUMN_NAME = 'trigger_type')
        ALTER TABLE meta.nova_load_stats ADD trigger_type VARCHAR(50);
    
    -- Add source_system_id if missing
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats' 
                   AND COLUMN_NAME = 'source_system_id')
        ALTER TABLE meta.nova_load_stats ADD source_system_id INT;
    
    -- Add source_system_name if missing
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats' 
                   AND COLUMN_NAME = 'source_system_name')
        ALTER TABLE meta.nova_load_stats ADD source_system_name VARCHAR(128);
    
    -- Add rows_read if missing (rename from rows_exported)
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats' 
                   AND COLUMN_NAME = 'rows_read')
        ALTER TABLE meta.nova_load_stats ADD rows_read BIGINT DEFAULT 0;
    
    -- Add rows_rejected if missing
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats' 
                   AND COLUMN_NAME = 'rows_rejected')
        ALTER TABLE meta.nova_load_stats ADD rows_rejected BIGINT DEFAULT 0;
    
    PRINT 'Updated table: meta.nova_load_stats with SADD fields';
END
GO

-- ==============================================================================
-- Table 2: nova_watermark
-- ==============================================================================
-- Tracks the last successful sync point for each table
-- Used to determine where to start delta loads
-- ==============================================================================

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_watermark')
BEGIN
    CREATE TABLE [meta].[nova_watermark] (
        -- Primary key
        table_name VARCHAR(255) NOT NULL,       -- Unique table identifier
        
        -- Last load information
        last_load_type VARCHAR(20),             -- 'FULL' or 'DELTA'
        last_delta_column VARCHAR(128),         -- Column used for delta
        last_delta_value VARCHAR(50),           -- Last delta value synced
        last_max_id BIGINT,                     -- Max ID value synced (if ID-based)
        last_row_count BIGINT,                  -- Rows synced in last run
        last_batch_id VARCHAR(50),              -- Batch ID of last sync
        
        -- Timestamps
        last_success_at DATETIME2,              -- When last successful sync completed
        updated_at DATETIME2 DEFAULT GETUTCDATE()
    )
    WITH (
        DISTRIBUTION = ROUND_ROBIN,
        HEAP
    );
    
    PRINT 'Created table: meta.nova_watermark';
END
ELSE
    PRINT 'Table already exists: meta.nova_watermark';
GO

-- ==============================================================================
-- Table 3: nova_data_quality (SADD Data Quality Controls)
-- ==============================================================================
-- Per SADD: Source vs FDW Validation
-- Tracks data quality checks after each batch load
-- ==============================================================================

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_data_quality')
BEGIN
    CREATE TABLE [meta].[nova_data_quality] (
        -- Primary key
        id INT IDENTITY(1,1) NOT NULL,
        
        -- Run identification
        run_id VARCHAR(50) NOT NULL,            -- Links to nova_load_stats.run_id
        table_name VARCHAR(255) NOT NULL,       -- Table being validated
        
        -- Validation details
        check_type VARCHAR(50) NOT NULL,        -- 'ROW_COUNT', 'SUM_CHECK', 'NULL_CHECK'
        source_value VARCHAR(255),              -- Value from source
        target_value VARCHAR(255),              -- Value from target (FDW)
        difference BIGINT,                      -- Numeric difference
        difference_percent FLOAT,               -- Percentage difference
        
        -- Result
        is_valid BIT DEFAULT 0,                 -- 1 = passed, 0 = failed
        checked_at DATETIME2                    -- When check was performed
    )
    WITH (
        DISTRIBUTION = ROUND_ROBIN,
        HEAP
    );
    
    PRINT 'Created table: meta.nova_data_quality (SADD Data Quality Controls)';
END
ELSE
    PRINT 'Table already exists: meta.nova_data_quality';
GO

-- ==============================================================================
-- Table 4: nova_source_systems (SADD Source System Reference)
-- ==============================================================================
-- Per SADD: Source_system_Id reference table
-- Maps source system IDs to names
-- ==============================================================================

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_source_systems')
BEGIN
    CREATE TABLE [meta].[nova_source_systems] (
        source_system_id INT NOT NULL,          -- Unique ID per SADD
        source_system_name VARCHAR(128) NOT NULL,
        source_system_type VARCHAR(50),         -- 'DATABASE', 'FILE', 'API', 'STREAMING'
        description VARCHAR(500),
        is_active BIT DEFAULT 1,
        created_at DATETIME2 DEFAULT GETUTCDATE()
    )
    WITH (
        DISTRIBUTION = ROUND_ROBIN,
        HEAP
    );
    
    -- Insert default source systems per SADD
    INSERT INTO meta.nova_source_systems (source_system_id, source_system_name, source_system_type, description)
    VALUES 
        (1, 'FB_DW_PROD', 'DATABASE', 'Famous Brands Production Data Warehouse'),
        (2, 'POS', 'DATABASE', 'Point of Sale System'),
        (3, 'CRM', 'DATABASE', 'Customer Relationship Management'),
        (4, 'FIS', 'DATABASE', 'Financial Information System'),
        (5, 'MUNCH', 'DATABASE', 'Munch System');
    
    PRINT 'Created table: meta.nova_source_systems with default values';
END
ELSE
    PRINT 'Table already exists: meta.nova_source_systems';
GO

-- ==============================================================================
-- Useful Views (SADD Operational Monitoring)
-- ==============================================================================

-- View: Latest run status for each table
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_nova_latest_runs' AND schema_id = SCHEMA_ID('meta'))
    DROP VIEW meta.v_nova_latest_runs;
GO

CREATE VIEW meta.v_nova_latest_runs AS
WITH LatestRuns AS (
    SELECT 
        source_table,
        target_table,
        load_type,
        pipeline_name,
        trigger_type,
        source_system_id,
        rows_read,
        rows_loaded,
        rows_rejected,
        duration_ms,
        success,
        error_message,
        run_started,
        run_ended,
        run_id,
        ROW_NUMBER() OVER (PARTITION BY source_table ORDER BY run_started DESC) as rn
    FROM meta.nova_load_stats
)
SELECT 
    source_table,
    target_table,
    load_type,
    pipeline_name,
    trigger_type,
    source_system_id,
    rows_read,
    rows_loaded,
    rows_rejected,
    duration_ms / 1000.0 as duration_seconds,
    CASE WHEN success = 1 THEN 'Success' ELSE 'Failed' END as status,
    error_message,
    run_started,
    run_ended,
    run_id
FROM LatestRuns
WHERE rn = 1;
GO

PRINT 'Created view: meta.v_nova_latest_runs';
GO

-- View: Daily load summary (SADD daily email report)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_nova_daily_summary' AND schema_id = SCHEMA_ID('meta'))
    DROP VIEW meta.v_nova_daily_summary;
GO

CREATE VIEW meta.v_nova_daily_summary AS
SELECT 
    CAST(run_started AS DATE) as run_date,
    pipeline_name,
    trigger_type,
    COUNT(*) as total_tables,
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed,
    SUM(rows_read) as total_rows_read,
    SUM(rows_loaded) as total_rows_loaded,
    SUM(rows_rejected) as total_rows_rejected,
    SUM(duration_ms) / 1000.0 as total_duration_seconds,
    COUNT(DISTINCT run_id) as batch_count
FROM meta.nova_load_stats
GROUP BY CAST(run_started AS DATE), pipeline_name, trigger_type;
GO

PRINT 'Created view: meta.v_nova_daily_summary';
GO

-- View: Data Quality Issues (SADD Data Quality Monitoring)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_nova_data_quality_issues' AND schema_id = SCHEMA_ID('meta'))
    DROP VIEW meta.v_nova_data_quality_issues;
GO

CREATE VIEW meta.v_nova_data_quality_issues AS
SELECT 
    dq.run_id,
    dq.table_name,
    dq.check_type,
    dq.source_value,
    dq.target_value,
    dq.difference,
    dq.difference_percent,
    dq.checked_at,
    ls.pipeline_name,
    ls.trigger_type
FROM meta.nova_data_quality dq
LEFT JOIN meta.nova_load_stats ls ON dq.run_id = ls.run_id AND dq.table_name = CONCAT(ls.source_schema, '.', ls.source_table)
WHERE dq.is_valid = 0;
GO

PRINT 'Created view: meta.v_nova_data_quality_issues';
GO

-- View: 30-day trend (per SADD: 30-day trend of differences)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_nova_30day_trend' AND schema_id = SCHEMA_ID('meta'))
    DROP VIEW meta.v_nova_30day_trend;
GO

CREATE VIEW meta.v_nova_30day_trend AS
SELECT 
    CAST(run_started AS DATE) as run_date,
    source_table,
    load_type,
    SUM(rows_loaded) as rows_loaded,
    SUM(rows_rejected) as rows_rejected,
    AVG(duration_ms) as avg_duration_ms,
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failure_count
FROM meta.nova_load_stats
WHERE run_started >= DATEADD(day, -30, GETDATE())
GROUP BY CAST(run_started AS DATE), source_table, load_type;
GO

PRINT 'Created view: meta.v_nova_30day_trend';
GO

-- ==============================================================================
-- Sample Queries (per SADD operational requirements)
-- ==============================================================================

/*
-- Check latest run status (per SADD monitoring)
SELECT * FROM meta.v_nova_latest_runs ORDER BY run_started DESC;

-- Check daily summary (per SADD daily email report)
SELECT * FROM meta.v_nova_daily_summary ORDER BY run_date DESC;

-- Check data quality issues (per SADD Source vs FDW Validation)
SELECT * FROM meta.v_nova_data_quality_issues ORDER BY checked_at DESC;

-- Check 30-day trend (per SADD)
SELECT * FROM meta.v_nova_30day_trend ORDER BY run_date DESC, source_table;

-- Check watermarks
SELECT * FROM meta.nova_watermark ORDER BY last_success_at DESC;

-- Find failed runs with details
SELECT 
    run_id,
    pipeline_name,
    trigger_type,
    source_table,
    target_table,
    rows_read,
    rows_loaded,
    rows_rejected,
    error_message,
    run_started
FROM meta.nova_load_stats 
WHERE success = 0 
ORDER BY run_started DESC;

-- Source system reference
SELECT * FROM meta.nova_source_systems WHERE is_active = 1;

-- Performance analysis by trigger type
SELECT 
    trigger_type,
    AVG(duration_ms) as avg_duration_ms,
    SUM(rows_loaded) as total_rows,
    COUNT(*) as run_count
FROM meta.nova_load_stats
WHERE run_started >= DATEADD(day, -7, GETDATE())
GROUP BY trigger_type;
*/

PRINT '';
PRINT '==============================================================================';
PRINT 'FB Nova metadata tables created successfully!';
PRINT 'Aligned with Solution Architecture Design Document (SADD)';
PRINT '';
PRINT 'Tables:';
PRINT '  - meta.nova_load_stats      (SADD Key Logging Dimensions)';
PRINT '  - meta.nova_watermark       (Delta load tracking)';
PRINT '  - meta.nova_data_quality    (SADD Data Quality Controls)';
PRINT '  - meta.nova_source_systems  (SADD Source System Reference)';
PRINT '';
PRINT 'Views:';
PRINT '  - meta.v_nova_latest_runs        (Latest status per table)';
PRINT '  - meta.v_nova_daily_summary      (Daily aggregates for email report)';
PRINT '  - meta.v_nova_data_quality_issues (Data quality failures)';
PRINT '  - meta.v_nova_30day_trend        (30-day trend analysis)';
PRINT '==============================================================================';
GO
