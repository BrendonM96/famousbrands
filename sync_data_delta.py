"""
FB Nova - Data Sync with Delta Loading Support
Aligned with Famous Brands Solution Architecture Design Document (SADD)

Architecture Alignment:
- Staging Layer (L1): Raw data extraction from source
- Load Ready Layer: Validation before integration
- Metadata tracking per SADD specifications
- Data Quality validation (Source vs Target)
- Operational logging with SADD-compliant fields

Features:
- Full load for dimension tables (truncate + reload)
- Delta load for fact tables (date-based or ID-based)
- Load statistics tracking aligned with SADD Key Logging Dimensions
- Metadata columns (_loaded_at, _batch_id, _source_system_id)
- Service Principal authentication for automated runs
- Load Ready validation before final load
- Data Quality reconciliation reporting
- Alerting on failures (webhook support)
"""

import pyodbc
import pandas as pd
import numpy as np
import os
import sys
import uuid
import json
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

# Optional: For alerting
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# =============================================================================
# CONFIGURATION
# =============================================================================

# Load .env file if exists (for local development)
if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()

# Source (Prod)
SOURCE_SERVER = os.getenv("SOURCE_SERVER", "az-zan-sws-prod-01.sql.azuresynapse.net")
SOURCE_DATABASE = os.getenv("SOURCE_DATABASE", "FB_DW")

# Target (Dev)
TARGET_SERVER = os.getenv("TARGET_SERVER", "synapse-fbnova-dev.sql.azuresynapse.net")
TARGET_DATABASE = os.getenv("TARGET_DATABASE", "sqlpoolfbnovadev")

# Azure Storage
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "stsynfbnovadev")
STORAGE_KEY = os.getenv("STORAGE_KEY")
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "synapsedata")

# Service Principal Authentication
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")

# Legacy interactive auth
USERNAME = os.getenv("USERNAME")

# Processing settings
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500000"))  # 500K rows per chunk
DELTA_LOOKBACK_DAYS = int(os.getenv("DELTA_LOOKBACK_DAYS", "7"))  # Default 7 days for delta

# =============================================================================
# SADD-ALIGNED CONFIGURATION
# =============================================================================

# Source System ID (per SADD: unique id identifying each source system)
# 1 = POS, 2 = CRM, 3 = Munch, 4 = FIS, etc.
SOURCE_SYSTEM_ID = int(os.getenv("SOURCE_SYSTEM_ID", "1"))
SOURCE_SYSTEM_NAME = os.getenv("SOURCE_SYSTEM_NAME", "FB_DW_PROD")

# Pipeline naming (per SADD: PL_POS, PL_FIS, etc.)
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "PL_NOVA_SYNC")

# Trigger type (per SADD: manual, scheduled, event-based)
TRIGGER_TYPE = os.getenv("TRIGGER_TYPE", "manual")

# Alerting configuration
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")  # Teams/Slack webhook
ALERT_EMAIL_ENABLED = os.getenv("ALERT_EMAIL_ENABLED", "false").lower() == "true"

# Data Quality settings
ENABLE_DATA_QUALITY_CHECKS = os.getenv("ENABLE_DATA_QUALITY_CHECKS", "true").lower() == "true"
ROW_COUNT_TOLERANCE_PERCENT = float(os.getenv("ROW_COUNT_TOLERANCE_PERCENT", "0.01"))  # 1% tolerance

# =============================================================================
# BATCH TRACKING
# =============================================================================

# Generate unique batch ID for this run (per SADD: BatchID)
BATCH_ID = f"nova_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
RUN_STARTED = datetime.now()

# =============================================================================
# LOAD READY VALIDATION RULES
# =============================================================================

# Validation rules per SADD Load Ready layer
LOAD_READY_RULES = {
    'reject_null_pk': True,           # Reject rows with NULL primary key
    'reject_future_dates': True,       # Reject dates in the future
    'reject_duplicate_pk': False,      # Don't reject duplicates (let target handle)
    'validate_data_types': True,       # Validate data types match target
    'max_string_length': 4000,         # Max varchar length
}

# =============================================================================
# AUTHENTICATION
# =============================================================================

def get_auth_mode():
    """Determine authentication mode"""
    if AZURE_CLIENT_ID and AZURE_CLIENT_SECRET:
        return "ServicePrincipal"
    elif USERNAME:
        return "Interactive"
    return None

def connect_source():
    """Connect to source database"""
    auth_mode = get_auth_mode()
    print(f"  Connecting to source: {SOURCE_SERVER}...")
    print(f"  Authentication mode: {auth_mode}")
    
    if auth_mode == "ServicePrincipal":
        conn_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SOURCE_SERVER};"
            f"DATABASE={SOURCE_DATABASE};"
            f"UID={AZURE_CLIENT_ID};"
            f"PWD={AZURE_CLIENT_SECRET};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"Encrypt=yes;"
        )
    elif auth_mode == "Interactive":
        conn_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SOURCE_SERVER};"
            f"DATABASE={SOURCE_DATABASE};"
            f"UID={USERNAME};"
            f"Authentication=ActiveDirectoryInteractive;"
            f"Encrypt=yes;"
        )
    else:
        raise ValueError("No authentication credentials configured")
    
    conn = pyodbc.connect(conn_string)
    print(f"  ✓ Source connected")
    return conn

def connect_target():
    """Connect to target database"""
    auth_mode = get_auth_mode()
    print(f"  Connecting to target: {TARGET_SERVER}...")
    
    if auth_mode == "ServicePrincipal":
        conn_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={TARGET_SERVER};"
            f"DATABASE={TARGET_DATABASE};"
            f"UID={AZURE_CLIENT_ID};"
            f"PWD={AZURE_CLIENT_SECRET};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"Encrypt=yes;"
        )
    elif auth_mode == "Interactive":
        conn_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={TARGET_SERVER};"
            f"DATABASE={TARGET_DATABASE};"
            f"UID={USERNAME};"
            f"Authentication=ActiveDirectoryInteractive;"
            f"Encrypt=yes;"
        )
    else:
        raise ValueError("No authentication credentials configured")
    
    conn = pyodbc.connect(conn_string, autocommit=True)
    print(f"  ✓ Target connected")
    return conn

def get_blob_client():
    """Get blob service client"""
    if not STORAGE_CONNECTION_STRING:
        raise ValueError("STORAGE_CONNECTION_STRING not set")
    return BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

# =============================================================================
# ALERTING (per SADD: Operations | Alerting)
# =============================================================================

def send_alert(title, message, severity="warning", details=None):
    """Send alert via webhook (Teams/Slack compatible)"""
    if not ALERT_WEBHOOK_URL or not HAS_REQUESTS:
        return
    
    try:
        # Format for Microsoft Teams
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "d63333" if severity == "error" else "ffc107",
            "summary": title,
            "sections": [{
                "activityTitle": f"🔔 FB Nova Sync Alert: {title}",
                "facts": [
                    {"name": "Pipeline", "value": PIPELINE_NAME},
                    {"name": "Batch ID", "value": BATCH_ID},
                    {"name": "Trigger", "value": TRIGGER_TYPE},
                    {"name": "Severity", "value": severity.upper()},
                    {"name": "Message", "value": message}
                ],
                "markdown": True
            }]
        }
        
        if details:
            payload["sections"][0]["facts"].extend([
                {"name": k, "value": str(v)} for k, v in details.items()
            ])
        
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=10)
        print(f"    ✓ Alert sent: {title}")
    except Exception as e:
        print(f"    Warning: Could not send alert: {e}")

# =============================================================================
# CONFIG READING
# =============================================================================

def read_config():
    """Read config file with delta loading information"""
    print("Reading config file...")
    
    config_paths = [
        "config file.xlsx",
        "/app/config file.xlsx",
        os.path.join(os.path.dirname(__file__), "config file.xlsx")
    ]
    
    config_path = None
    for path in config_paths:
        if os.path.exists(path):
            config_path = path
            break
    
    if not config_path:
        raise FileNotFoundError(f"Config file not found")
    
    df = pd.read_excel(config_path)
    df.columns = df.columns.str.strip()
    
    # Find enabled column
    enabled_col = [c for c in df.columns if 'Enabled' in c][0]
    df[enabled_col] = df[enabled_col].fillna('').astype(str)
    df = df[df[enabled_col].str.strip().str.lower() == 'y']
    
    tables = []
    for _, row in df.iterrows():
        # Determine load type based on table name and config
        table_name = row['Source_table_name']
        date_column = row.get('Date_Column_Name', '')
        pk_column = row.get('PK_Column_Name', '')
        
        # Clean up column values
        date_column = str(date_column).strip() if pd.notna(date_column) and str(date_column).strip() else None
        pk_column = str(pk_column).strip() if pd.notna(pk_column) and str(pk_column).strip() else None
        
        # Determine if this should be delta or full load
        is_fact_table = table_name.startswith('Fact')
        load_type = 'DELTA' if is_fact_table and date_column else 'FULL'
        
        tables.append({
            'source_schema': row['Source_Schema'],
            'source_table': table_name,
            'target_schema': row['Target_Schema'],
            'target_table': row['Target_Table_Name'],
            'date_column': date_column,
            'pk_column': pk_column,
            'load_type': load_type
        })
    
    # Count load types
    full_count = sum(1 for t in tables if t['load_type'] == 'FULL')
    delta_count = sum(1 for t in tables if t['load_type'] == 'DELTA')
    print(f"  ✓ Found {len(tables)} enabled tables (Full: {full_count}, Delta: {delta_count})")
    
    return tables

# =============================================================================
# WATERMARK FUNCTIONS
# =============================================================================

def get_watermark(target_conn, table_name):
    """Get last watermark for a table"""
    try:
        cursor = target_conn.cursor()
        cursor.execute("""
            SELECT last_delta_value, last_max_id, last_success_at
            FROM meta.nova_watermark
            WHERE table_name = ?
        """, table_name)
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            return {
                'last_delta_value': row[0],
                'last_max_id': row[1],
                'last_success_at': row[2]
            }
    except Exception:
        pass  # Table might not exist yet
    
    return None

def update_watermark(target_conn, table_name, config, delta_value, max_id, row_count):
    """Update watermark after successful load"""
    try:
        cursor = target_conn.cursor()
        
        # Upsert watermark
        cursor.execute("""
            MERGE meta.nova_watermark AS target
            USING (SELECT ? AS table_name) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN
                UPDATE SET 
                    last_load_type = ?,
                    last_delta_column = ?,
                    last_delta_value = ?,
                    last_max_id = ?,
                    last_row_count = ?,
                    last_batch_id = ?,
                    last_success_at = GETUTCDATE(),
                    updated_at = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (table_name, last_load_type, last_delta_column, last_delta_value, 
                        last_max_id, last_row_count, last_batch_id, last_success_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETUTCDATE());
        """, 
            table_name,
            config['load_type'], config.get('date_column'), delta_value, max_id, row_count, BATCH_ID,
            table_name, config['load_type'], config.get('date_column'), delta_value, max_id, row_count, BATCH_ID
        )
        cursor.close()
    except Exception as e:
        print(f"    Warning: Could not update watermark: {e}")

def log_load_stats(target_conn, config, start_time, rows_read, rows_loaded, rows_rejected,
                   chunks, success, error=None, delta_start=None, delta_end=None):
    """
    Log load statistics per SADD Key Logging Dimensions:
    - Pipeline name, job name, BatchID
    - Start time, end time, duration
    - Trigger type (manual, scheduled, event-based)
    - Row counts: read, inserted, updated, rejected
    - Source and target table names
    - Status, Error Message
    """
    try:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        cursor = target_conn.cursor()
        
        cursor.execute("""
            INSERT INTO meta.nova_load_stats 
            (run_id, pipeline_name, trigger_type, source_system_id, source_system_name,
             run_started, run_ended, source_schema, source_table, 
             target_schema, target_table, load_type, delta_column, 
             delta_start_value, delta_end_value, rows_read, rows_loaded, rows_rejected,
             chunks_processed, duration_ms, success, error_message)
            VALUES (?, ?, ?, ?, ?, ?, GETUTCDATE(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            BATCH_ID, PIPELINE_NAME, TRIGGER_TYPE, SOURCE_SYSTEM_ID, SOURCE_SYSTEM_NAME,
            start_time,
            config['source_schema'], config['source_table'],
            config['target_schema'], config['target_table'],
            config['load_type'], config.get('date_column'),
            str(delta_start) if delta_start else None,
            str(delta_end) if delta_end else None,
            rows_read, rows_loaded, rows_rejected, chunks, duration_ms,
            1 if success else 0, error[:4000] if error else None
        )
        cursor.close()
    except Exception as e:
        print(f"    Warning: Could not log stats: {e}")

# =============================================================================
# DATA QUALITY VALIDATION (per SADD: Source vs FDW Validation)
# =============================================================================

def validate_source_target_counts(source_conn, target_conn, config, delta_start=None):
    """
    Validate row counts between source and target
    Per SADD: Source vs FDW Validation after each batch load
    """
    if not ENABLE_DATA_QUALITY_CHECKS:
        return None
    
    try:
        src = f"[{config['source_schema']}].[{config['source_table']}]"
        tgt = f"[{config['target_schema']}].[{config['target_table']}]"
        
        # Get source count
        src_cursor = source_conn.cursor()
        if config['load_type'] == 'DELTA' and delta_start:
            date_col = config['date_column']
            src_cursor.execute(f"SELECT COUNT(*) FROM {src} WHERE [{date_col}] >= ?", delta_start)
        else:
            src_cursor.execute(f"SELECT COUNT(*) FROM {src}")
        source_count = src_cursor.fetchone()[0]
        src_cursor.close()
        
        # Get target count (for this batch)
        tgt_cursor = target_conn.cursor()
        tgt_cursor.execute(f"SELECT COUNT(*) FROM {tgt} WHERE _batch_id = ?", BATCH_ID)
        target_count = tgt_cursor.fetchone()[0]
        tgt_cursor.close()
        
        # Calculate difference
        if source_count > 0:
            diff_percent = abs(source_count - target_count) / source_count
        else:
            diff_percent = 0 if target_count == 0 else 1
        
        is_valid = diff_percent <= ROW_COUNT_TOLERANCE_PERCENT
        
        validation_result = {
            'source_count': source_count,
            'target_count': target_count,
            'difference': source_count - target_count,
            'difference_percent': diff_percent * 100,
            'is_valid': is_valid,
            'tolerance_percent': ROW_COUNT_TOLERANCE_PERCENT * 100
        }
        
        if not is_valid:
            print(f"    ⚠ DATA QUALITY WARNING: Row count mismatch!")
            print(f"      Source: {source_count:,}, Target: {target_count:,}, Diff: {diff_percent*100:.2f}%")
        else:
            print(f"    ✓ Data quality check passed (diff: {diff_percent*100:.2f}%)")
        
        return validation_result
        
    except Exception as e:
        print(f"    Warning: Could not validate counts: {e}")
        return None

def log_data_quality_result(target_conn, config, validation_result):
    """Log data quality validation results"""
    if not validation_result:
        return
    
    try:
        cursor = target_conn.cursor()
        cursor.execute("""
            INSERT INTO meta.nova_data_quality
            (run_id, table_name, check_type, source_value, target_value,
             difference, difference_percent, is_valid, checked_at)
            VALUES (?, ?, 'ROW_COUNT', ?, ?, ?, ?, ?, GETUTCDATE())
        """,
            BATCH_ID,
            f"{config['source_schema']}.{config['source_table']}",
            str(validation_result['source_count']),
            str(validation_result['target_count']),
            validation_result['difference'],
            validation_result['difference_percent'],
            1 if validation_result['is_valid'] else 0
        )
        cursor.close()
    except Exception as e:
        print(f"    Warning: Could not log data quality result: {e}")

# =============================================================================
# LOAD READY VALIDATION (per SADD Load Ready Layer)
# =============================================================================

def validate_load_ready(df, config):
    """
    Validate data before final load (per SADD Load Ready Layer)
    Returns: (validated_df, rejected_count, rejection_reasons)
    """
    rejected_count = 0
    rejection_reasons = []
    
    original_count = len(df)
    
    # Rule 1: Reject NULL primary keys
    if LOAD_READY_RULES['reject_null_pk'] and config.get('pk_column'):
        pk_col = config['pk_column']
        if pk_col in df.columns:
            null_pk_mask = df[pk_col].isna()
            null_pk_count = null_pk_mask.sum()
            if null_pk_count > 0:
                df = df[~null_pk_mask]
                rejected_count += null_pk_count
                rejection_reasons.append(f"NULL {pk_col}: {null_pk_count}")
    
    # Rule 2: Reject future dates (if date column exists)
    if LOAD_READY_RULES['reject_future_dates'] and config.get('date_column'):
        date_col = config['date_column']
        if date_col in df.columns:
            try:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                future_mask = df[date_col] > datetime.now()
                future_count = future_mask.sum()
                if future_count > 0:
                    df = df[~future_mask]
                    rejected_count += future_count
                    rejection_reasons.append(f"Future {date_col}: {future_count}")
            except Exception:
                pass  # Skip if date parsing fails
    
    # Rule 3: Truncate oversized strings
    if LOAD_READY_RULES['validate_data_types']:
        max_len = LOAD_READY_RULES['max_string_length']
        for col in df.select_dtypes(include=['object']).columns:
            if col.startswith('_'):  # Skip metadata columns
                continue
            df[col] = df[col].astype(str).str[:max_len]
    
    if rejected_count > 0:
        print(f"    [LOAD READY] Validated: {len(df):,} rows, Rejected: {rejected_count:,}")
        for reason in rejection_reasons:
            print(f"      - {reason}")
    
    return df, rejected_count, rejection_reasons

# =============================================================================
# DATA EXPORT WITH DELTA SUPPORT
# =============================================================================

def fix_columns(df):
    """Fix data type issues"""
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            non_null = df[col].dropna()
            if len(non_null) > 0:
                try:
                    if (non_null == non_null.astype(int)).all():
                        df[col] = df[col].astype('Int64')
                    else:
                        df[col] = df[col].round(10)
                except (ValueError, OverflowError):
                    df[col] = df[col].round(10)
        elif df[col].dtype == 'object':
            df[col] = df[col].replace(['nan', 'None', 'NULL', 'null'], np.nan)
    return df

def add_metadata_columns(df):
    """
    Add metadata columns to dataframe per SADD requirements:
    - _loaded_at: FDW_Timestamp (time of data landing)
    - _batch_id: Batch_Id (incremental unique value)
    - _source_system_id: Source_system_Id (unique id for source system)
    """
    df['_loaded_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    df['_batch_id'] = BATCH_ID
    df['_source_system_id'] = SOURCE_SYSTEM_ID
    return df

def get_delta_query(source_conn, config, watermark):
    """Build query based on load type"""
    src = f"[{config['source_schema']}].[{config['source_table']}]"
    
    if config['load_type'] == 'FULL':
        return f"SELECT * FROM {src}", None, None
    
    # Delta load
    date_col = config['date_column']
    
    if watermark and watermark.get('last_delta_value'):
        # Use last watermark value
        delta_start = watermark['last_delta_value']
    else:
        # First delta load - use lookback period
        delta_start = (datetime.now() - timedelta(days=DELTA_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    
    delta_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    query = f"""
        SELECT * FROM {src}
        WHERE [{date_col}] >= '{delta_start}'
    """
    
    print(f"    [DELTA] Loading records where {date_col} >= '{delta_start}'")
    
    return query, delta_start, delta_end

def export_to_csv(source_conn, config, watermark):
    """Export source table to CSV with delta support and Load Ready validation"""
    table_name = config['source_table']
    base_filename = f"{table_name}"
    
    print(f"  Exporting [{config['source_schema']}].[{table_name}] to CSV...")
    print(f"    Load type: {config['load_type']}")
    
    # Get appropriate query
    query, delta_start, delta_end = get_delta_query(source_conn, config, watermark)
    
    total_rows_read = 0
    total_rows_validated = 0
    total_rows_rejected = 0
    chunk_num = 0
    max_id = None
    
    # Read in chunks
    for chunk in pd.read_sql(query, source_conn, chunksize=CHUNK_SIZE):
        chunk_num += 1
        chunk_filename = f"{base_filename}_chunk_{chunk_num:04d}.csv"
        
        rows_read = len(chunk)
        total_rows_read += rows_read
        
        # Fix data types
        chunk = fix_columns(chunk)
        
        # Add metadata columns (per SADD)
        chunk = add_metadata_columns(chunk)
        
        # LOAD READY VALIDATION (per SADD)
        chunk, rejected_count, _ = validate_load_ready(chunk, config)
        total_rows_rejected += rejected_count
        
        # Track max ID if PK column exists
        pk_col = config.get('pk_column')
        if pk_col and pk_col in chunk.columns:
            chunk_max = chunk[pk_col].max()
            if pd.notna(chunk_max):
                max_id = max(max_id or 0, int(chunk_max))
        
        # Write to CSV
        if len(chunk) > 0:
            if chunk_num == 1:
                chunk.to_csv(chunk_filename, index=False, sep='|', encoding='utf-8',
                            na_rep='', float_format='%.10f')
            else:
                chunk.to_csv(chunk_filename, index=False, sep='|', encoding='utf-8',
                            na_rep='', float_format='%.10f', header=False)
            
            # Upload immediately
            upload_chunk_to_blob(chunk_filename, table_name, chunk_num)
            os.remove(chunk_filename)
            
            total_rows_validated += len(chunk)
        
        if total_rows_read % 1000000 == 0:
            print(f"    Progress: {total_rows_read:,} rows read, {total_rows_validated:,} validated...")
    
    print(f"    Rows read: {total_rows_read:,}, Validated: {total_rows_validated:,}, Rejected: {total_rows_rejected:,}")
    
    return base_filename, total_rows_read, total_rows_validated, total_rows_rejected, chunk_num, delta_start, delta_end, max_id

# =============================================================================
# BLOB OPERATIONS
# =============================================================================

def upload_chunk_to_blob(chunk_filename, table_name, chunk_num):
    """Upload chunk to blob storage"""
    blob_service = get_blob_client()
    blob_name = f"staging/{table_name}_chunk_{chunk_num:04d}.csv"
    blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    
    with open(chunk_filename, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

def cleanup_blobs(table_name, chunk_count):
    """Delete blob chunks after load"""
    try:
        blob_service = get_blob_client()
        for chunk_num in range(1, chunk_count + 1):
            blob_name = f"staging/{table_name}_chunk_{chunk_num:04d}.csv"
            blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
            blob_client.delete_blob()
    except Exception as e:
        print(f"    Warning: Could not delete blobs: {e}")

# =============================================================================
# TARGET TABLE OPERATIONS
# =============================================================================

def ensure_metadata_columns(target_conn, config):
    """Ensure target table has metadata columns per SADD"""
    tgt = f"[{config['target_schema']}].[{config['target_table']}]"
    
    try:
        cursor = target_conn.cursor()
        
        # Check and add _loaded_at (FDW_Timestamp per SADD)
        cursor.execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{config['target_schema']}' 
                AND TABLE_NAME = '{config['target_table']}'
                AND COLUMN_NAME = '_loaded_at'
            )
            ALTER TABLE {tgt} ADD _loaded_at DATETIME2 NULL;
        """)
        
        # Check and add _batch_id (Batch_Id per SADD)
        cursor.execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{config['target_schema']}' 
                AND TABLE_NAME = '{config['target_table']}'
                AND COLUMN_NAME = '_batch_id'
            )
            ALTER TABLE {tgt} ADD _batch_id VARCHAR(50) NULL;
        """)
        
        # Check and add _source_system_id (Source_system_Id per SADD)
        cursor.execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{config['target_schema']}' 
                AND TABLE_NAME = '{config['target_table']}'
                AND COLUMN_NAME = '_source_system_id'
            )
            ALTER TABLE {tgt} ADD _source_system_id INT NULL;
        """)
        
        cursor.close()
    except Exception as e:
        print(f"    Warning: Could not add metadata columns: {e}")

def prepare_target_table(source_conn, target_conn, config):
    """Prepare target table based on load type"""
    tgt = f"[{config['target_schema']}].[{config['target_table']}]"
    
    cursor = target_conn.cursor()
    
    # Check if table exists
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{config['target_schema']}' 
        AND TABLE_NAME = '{config['target_table']}'
    """)
    exists = cursor.fetchone()[0] > 0
    
    if exists:
        if config['load_type'] == 'FULL':
            print(f"  Truncating table (FULL load)...")
            cursor.execute(f"TRUNCATE TABLE {tgt}")
        else:
            print(f"  Table exists (DELTA load - appending)...")
        
        # Ensure metadata columns exist
        ensure_metadata_columns(target_conn, config)
    else:
        print(f"  Creating table...")
        # Get column definitions from source
        src_cursor = source_conn.cursor()
        src_cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{config['source_schema']}' 
            AND TABLE_NAME = '{config['source_table']}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = []
        for row in src_cursor.fetchall():
            col_name, data_type, char_len, num_prec, num_scale, nullable = row
            
            if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                type_str = f"{data_type}({char_len})" if char_len != -1 else f"{data_type}(MAX)"
            elif data_type in ('decimal', 'numeric'):
                type_str = f"{data_type}({num_prec},{num_scale})"
            else:
                type_str = data_type
            
            columns.append(f"[{col_name}] {type_str} NULL")
        
        # Add metadata columns per SADD
        columns.append("[_loaded_at] DATETIME2 NULL")
        columns.append("[_batch_id] VARCHAR(50) NULL")
        columns.append("[_source_system_id] INT NULL")
        
        create_sql = f"CREATE TABLE {tgt} ({', '.join(columns)})"
        cursor.execute(create_sql)
        src_cursor.close()
    
    cursor.close()

def copy_into_target(target_conn, config, table_name, chunk_count):
    """Execute COPY INTO command"""
    tgt = f"[{config['target_schema']}].[{config['target_table']}]"
    
    print(f"  COPY INTO {tgt} from {chunk_count} chunks...")
    
    cursor = target_conn.cursor()
    rows_loaded = 0
    
    for chunk_num in range(1, chunk_count + 1):
        blob_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/staging/{table_name}_chunk_{chunk_num:04d}.csv"
        
        copy_sql = f"""
        COPY INTO {tgt}
        FROM '{blob_url}'
        WITH (
            FILE_TYPE = 'CSV',
            FIELDTERMINATOR = '|',
            FIRSTROW = {2 if chunk_num == 1 else 1},
            CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '{STORAGE_KEY}')
        )
        """
        
        cursor.execute(copy_sql)
        
        if chunk_num % 10 == 0:
            print(f"    Loaded {chunk_num}/{chunk_count} chunks...")
    
    # Get actual row count
    cursor.execute(f"SELECT COUNT(*) FROM {tgt} WHERE _batch_id = ?", BATCH_ID)
    rows_loaded = cursor.fetchone()[0]
    
    cursor.close()
    print(f"    ✓ COPY completed ({chunk_count} chunks, {rows_loaded:,} rows)")
    
    return rows_loaded

# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_table(source_conn, target_conn, config):
    """Process a single table with delta support and SADD compliance"""
    start_time = datetime.now()
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0
    chunk_count = 0
    delta_start = None
    delta_end = None
    max_id = None
    error = None
    success = False
    validation_result = None
    
    try:
        # Get watermark for delta loads
        watermark = None
        if config['load_type'] == 'DELTA':
            watermark = get_watermark(target_conn, config['source_table'])
        
        # Export data with Load Ready validation
        table_name, rows_read, rows_validated, rows_rejected, chunk_count, delta_start, delta_end, max_id = \
            export_to_csv(source_conn, config, watermark)
        
        if rows_validated == 0:
            print(f"    No rows to process")
            success = True
        else:
            print(f"    ✓ Exported {chunk_count} chunks")
            
            # Prepare target table
            prepare_target_table(source_conn, target_conn, config)
            
            # Load data
            rows_loaded = copy_into_target(target_conn, config, table_name, chunk_count)
            
            # Cleanup blobs
            cleanup_blobs(table_name, chunk_count)
            
            # DATA QUALITY VALIDATION (per SADD)
            validation_result = validate_source_target_counts(
                source_conn, target_conn, config, delta_start
            )
            log_data_quality_result(target_conn, config, validation_result)
            
            # Update watermark
            update_watermark(target_conn, config['source_table'], config, 
                           delta_end, max_id, rows_loaded)
            
            success = True
        
    except Exception as e:
        error = str(e)
        print(f"  ✗ ERROR: {e}")
        
        # Send alert on failure
        send_alert(
            f"Sync Failed: {config['source_table']}",
            str(e),
            severity="error",
            details={
                "Table": f"{config['source_schema']}.{config['source_table']}",
                "Load Type": config['load_type'],
                "Rows Read": rows_read
            }
        )
    
    # Log statistics (per SADD Key Logging Dimensions)
    log_load_stats(target_conn, config, start_time, rows_read, rows_loaded, rows_rejected,
                   chunk_count, success, error, delta_start, delta_end)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"  Time: {elapsed:.1f}s")
    
    return {
        'table': config['source_table'],
        'load_type': config['load_type'],
        'rows_read': rows_read,
        'rows_loaded': rows_loaded,
        'rows_rejected': rows_rejected,
        'time': elapsed,
        'status': 'Success' if success else 'FAILED',
        'error': error,
        'data_quality': validation_result
    }

def ensure_meta_tables(target_conn):
    """Ensure metadata tables exist per SADD requirements"""
    try:
        cursor = target_conn.cursor()
        
        # Create schema if not exists
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'meta')
                EXEC('CREATE SCHEMA meta')
        """)
        
        # Create enhanced load stats table (per SADD Key Logging Dimensions)
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                          WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_load_stats')
            CREATE TABLE [meta].[nova_load_stats] (
                id INT IDENTITY(1,1),
                -- Run identification (per SADD)
                run_id VARCHAR(50) NOT NULL,
                pipeline_name VARCHAR(128),
                trigger_type VARCHAR(50),
                source_system_id INT,
                source_system_name VARCHAR(128),
                -- Timestamps (per SADD)
                run_started DATETIME2 NOT NULL,
                run_ended DATETIME2,
                -- Table information
                source_schema VARCHAR(128) NOT NULL,
                source_table VARCHAR(128) NOT NULL,
                target_schema VARCHAR(128) NOT NULL,
                target_table VARCHAR(128) NOT NULL,
                -- Load type
                load_type VARCHAR(20) NOT NULL,
                delta_column VARCHAR(128),
                delta_start_value VARCHAR(50),
                delta_end_value VARCHAR(50),
                -- Row counts (per SADD: read, inserted, updated, rejected)
                rows_read BIGINT DEFAULT 0,
                rows_loaded BIGINT DEFAULT 0,
                rows_rejected BIGINT DEFAULT 0,
                chunks_processed INT DEFAULT 0,
                -- Performance
                duration_ms BIGINT,
                -- Status (per SADD)
                success BIT DEFAULT 0,
                error_message NVARCHAR(4000)
            )
        """)
        
        # Create watermark table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                          WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_watermark')
            CREATE TABLE [meta].[nova_watermark] (
                table_name VARCHAR(255) NOT NULL,
                last_load_type VARCHAR(20),
                last_delta_column VARCHAR(128),
                last_delta_value VARCHAR(50),
                last_max_id BIGINT,
                last_row_count BIGINT,
                last_batch_id VARCHAR(50),
                last_success_at DATETIME2,
                updated_at DATETIME2 DEFAULT GETUTCDATE()
            )
        """)
        
        # Create data quality table (per SADD: Data Quality controls)
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                          WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'nova_data_quality')
            CREATE TABLE [meta].[nova_data_quality] (
                id INT IDENTITY(1,1),
                run_id VARCHAR(50) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                check_type VARCHAR(50) NOT NULL,
                source_value VARCHAR(255),
                target_value VARCHAR(255),
                difference BIGINT,
                difference_percent FLOAT,
                is_valid BIT DEFAULT 0,
                checked_at DATETIME2
            )
        """)
        
        cursor.close()
        print("  ✓ Meta tables ready (SADD-compliant)")
    except Exception as e:
        print(f"  Warning: Could not create meta tables: {e}")

def validate_environment():
    """Validate required environment variables"""
    errors = []
    
    if not STORAGE_CONNECTION_STRING:
        errors.append("STORAGE_CONNECTION_STRING not set")
    if not STORAGE_KEY:
        errors.append("STORAGE_KEY not set")
    if not get_auth_mode():
        errors.append("No authentication configured")
    
    if errors:
        print("=" * 60)
        print("CONFIGURATION ERRORS")
        print("=" * 60)
        for err in errors:
            print(f"  ✗ {err}")
        return False
    
    return True

def print_sadd_compliance_info():
    """Print SADD compliance information"""
    print("\nSADD Compliance:")
    print(f"  Pipeline Name: {PIPELINE_NAME}")
    print(f"  Source System ID: {SOURCE_SYSTEM_ID} ({SOURCE_SYSTEM_NAME})")
    print(f"  Trigger Type: {TRIGGER_TYPE}")
    print(f"  Data Quality Checks: {'Enabled' if ENABLE_DATA_QUALITY_CHECKS else 'Disabled'}")
    print(f"  Alerting: {'Configured' if ALERT_WEBHOOK_URL else 'Not configured'}")

def main():
    print("=" * 60)
    print("FB NOVA - DATA SYNC WITH DELTA LOADING")
    print("Aligned with Solution Architecture Design Document (SADD)")
    print("=" * 60)
    print(f"Batch ID: {BATCH_ID}")
    print(f"Started: {RUN_STARTED}")
    print(f"Authentication: {get_auth_mode()}")
    print(f"Delta lookback: {DELTA_LOOKBACK_DAYS} days")
    
    print_sadd_compliance_info()
    print("=" * 60)
    
    if not validate_environment():
        sys.exit(1)
    
    # Read config
    print("\nSTEP 1: READ CONFIG")
    try:
        tables = read_config()
    except Exception as e:
        print(f"  ✗ ERROR: {e}")
        send_alert("Config Read Failed", str(e), severity="error")
        sys.exit(1)
    
    if len(tables) == 0:
        print("  ⚠ No tables enabled")
        sys.exit(0)
    
    # Connect
    print("\nSTEP 2: CONNECT")
    try:
        source_conn = connect_source()
        target_conn = connect_target()
    except Exception as e:
        print(f"  ✗ CONNECTION ERROR: {e}")
        send_alert("Connection Failed", str(e), severity="error")
        sys.exit(1)
    
    # Ensure meta tables exist
    print("\nSTEP 3: SETUP META TABLES (SADD-compliant)")
    ensure_meta_tables(target_conn)
    
    # Process tables
    print("\nSTEP 4: PROCESS TABLES")
    results = []
    for i, config in enumerate(tables, 1):
        load_icon = "Δ" if config['load_type'] == 'DELTA' else "▣"
        print(f"\n[{i}/{len(tables)}] {load_icon} {config['source_schema']}.{config['source_table']}")
        result = process_table(source_conn, target_conn, config)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    full_loads = [r for r in results if r['load_type'] == 'FULL']
    delta_loads = [r for r in results if r['load_type'] == 'DELTA']
    success = sum(1 for r in results if r['status'] == 'Success')
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    total_rows_loaded = sum(r['rows_loaded'] for r in results)
    total_rows_rejected = sum(r['rows_rejected'] for r in results)
    total_time = sum(r['time'] for r in results)
    
    print(f"Batch ID: {BATCH_ID}")
    print(f"Pipeline: {PIPELINE_NAME}")
    print(f"Trigger: {TRIGGER_TYPE}")
    print(f"Tables: {len(results)} (Full: {len(full_loads)}, Delta: {len(delta_loads)})")
    print(f"Status: Success: {success}, Failed: {failed}")
    print(f"Total rows loaded: {total_rows_loaded:,}")
    print(f"Total rows rejected: {total_rows_rejected:,}")
    print(f"Total time: {total_time:.1f}s")
    
    # Data Quality Summary
    dq_issues = [r for r in results if r.get('data_quality') and not r['data_quality'].get('is_valid')]
    if dq_issues:
        print(f"\n⚠ Data Quality Issues: {len(dq_issues)} tables")
        for r in dq_issues:
            dq = r['data_quality']
            print(f"  - {r['table']}: {dq['difference_percent']:.2f}% difference")
    
    if failed > 0:
        print("\nFailed tables:")
        for r in results:
            if r['status'] == 'FAILED':
                print(f"  - {r['table']}: {r['error']}")
        
        # Send summary alert
        send_alert(
            f"Sync Completed with Failures",
            f"{failed} of {len(results)} tables failed",
            severity="error",
            details={
                "Successful": success,
                "Failed": failed,
                "Total Rows": total_rows_loaded
            }
        )
    
    source_conn.close()
    target_conn.close()
    
    print(f"\nCompleted: {datetime.now()}")
    
    if failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
