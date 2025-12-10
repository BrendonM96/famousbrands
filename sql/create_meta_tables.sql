-- ==============================================================================
-- FB Nova - Metadata Tables for Load Tracking
-- ==============================================================================
-- Run this script in the TARGET database (synapse-fbnova-dev/sqlpoolfbnovadev)
-- before running the sync with delta loading support.
--
-- These tables are similar to your existing load_stats pattern but customized
-- for the Nova sync process.
-- ==============================================================================

-- Create meta schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'meta')
    EXEC('CREATE SCHEMA meta');
GO

-- ==============================================================================
-- Table 1: nova_load_stats
-- ==============================================================================
-- Tracks every sync run with detailed statistics
-- Similar to your existing load_stats table
-- ==============================================================================

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats')
BEGIN
    CREATE TABLE [meta].[nova_load_stats] (
        -- Primary key
        id INT IDENTITY(1,1) NOT NULL,
        
        -- Run identification
        run_id VARCHAR(50) NOT NULL,           -- Unique batch ID (e.g., nova_20251210_143022_a1b2c3d4)
        run_started DATETIME2 NOT NULL,         -- When this table sync started
        run_ended DATETIME2,                    -- When this table sync ended
        
        -- Table information
        source_schema VARCHAR(128) NOT NULL,    -- e.g., 'dwh'
        source_table VARCHAR(128) NOT NULL,     -- e.g., 'FactSales'
        target_schema VARCHAR(128) NOT NULL,    -- e.g., 'stg_mig'
        target_table VARCHAR(128) NOT NULL,     -- e.g., 'FactSales'
        
        -- Load type
        load_type VARCHAR(20) NOT NULL,         -- 'FULL' or 'DELTA'
        delta_column VARCHAR(128),              -- Column used for delta (e.g., 'TransactionDate')
        delta_start_value VARCHAR(50),          -- Start value for delta range
        delta_end_value VARCHAR(50),            -- End value for delta range
        
        -- Row counts
        rows_exported BIGINT DEFAULT 0,         -- Rows read from source
        rows_loaded BIGINT DEFAULT 0,           -- Rows inserted to target
        chunks_processed INT DEFAULT 0,         -- Number of CSV chunks
        
        -- Performance
        duration_ms BIGINT,                     -- Total duration in milliseconds
        
        -- Status
        success BIT DEFAULT 0,                  -- 1 = success, 0 = failure
        error_message NVARCHAR(4000)            -- Error details if failed
    )
    WITH (
        DISTRIBUTION = ROUND_ROBIN,
        CLUSTERED COLUMNSTORE INDEX
    );
    
    PRINT 'Created table: meta.nova_load_stats';
END
ELSE
    PRINT 'Table already exists: meta.nova_load_stats';
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
-- Useful Views
-- ==============================================================================

-- View: Latest run status for each table
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_nova_latest_runs')
    DROP VIEW meta.v_nova_latest_runs;
GO

CREATE VIEW meta.v_nova_latest_runs AS
WITH LatestRuns AS (
    SELECT 
        source_table,
        target_table,
        load_type,
        rows_loaded,
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
    rows_loaded,
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

-- View: Daily load summary
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_nova_daily_summary')
    DROP VIEW meta.v_nova_daily_summary;
GO

CREATE VIEW meta.v_nova_daily_summary AS
SELECT 
    CAST(run_started AS DATE) as run_date,
    COUNT(*) as total_tables,
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed,
    SUM(rows_loaded) as total_rows_loaded,
    SUM(duration_ms) / 1000.0 as total_duration_seconds,
    COUNT(DISTINCT run_id) as batch_count
FROM meta.nova_load_stats
GROUP BY CAST(run_started AS DATE);
GO

PRINT 'Created view: meta.v_nova_daily_summary';
GO

-- ==============================================================================
-- Sample Queries
-- ==============================================================================

/*
-- Check latest run status
SELECT * FROM meta.v_nova_latest_runs ORDER BY run_started DESC;

-- Check daily summary
SELECT * FROM meta.v_nova_daily_summary ORDER BY run_date DESC;

-- Check watermarks
SELECT * FROM meta.nova_watermark ORDER BY last_success_at DESC;

-- Find failed runs
SELECT * FROM meta.nova_load_stats 
WHERE success = 0 
ORDER BY run_started DESC;

-- Check specific table history
SELECT * FROM meta.nova_load_stats 
WHERE source_table = 'FactSales' 
ORDER BY run_started DESC;

-- Compare your existing load_stats with nova_load_stats
SELECT 
    'Existing' as source,
    source_table, target_table, rows_inserted, duration_ms, success, error_message
FROM stg_mig.load_stats  -- Your existing table
WHERE run_started >= DATEADD(day, -1, GETDATE())
UNION ALL
SELECT 
    'Nova' as source,
    source_table, target_table, rows_loaded, duration_ms, success, error_message
FROM meta.nova_load_stats
WHERE run_started >= DATEADD(day, -1, GETDATE());
*/

PRINT '';
PRINT '==============================================================================';
PRINT 'FB Nova metadata tables created successfully!';
PRINT '';
PRINT 'Tables:';
PRINT '  - meta.nova_load_stats   (load history, similar to your load_stats)';
PRINT '  - meta.nova_watermark    (tracks last sync point for delta loads)';
PRINT '';
PRINT 'Views:';
PRINT '  - meta.v_nova_latest_runs   (latest status per table)';
PRINT '  - meta.v_nova_daily_summary (daily aggregates)';
PRINT '==============================================================================';
GO

