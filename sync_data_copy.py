"""
FB Nova - Data Sync using COPY command (FAST)
Exports to CSV → Blob Storage → COPY INTO Synapse
"""

import pyodbc
import pandas as pd
import numpy as np
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# =============================================================================
# CONFIGURATION
# =============================================================================

# Load .env
if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()

# Source (Prod)
SOURCE_SERVER = "az-zan-sws-prod-01.sql.azuresynapse.net"
SOURCE_DATABASE = "FB_DW"

# Target (Dev)
TARGET_SERVER = "synapse-fbnova-dev.sql.azuresynapse.net"
TARGET_DATABASE = "sqlpoolfbnovadev"

# Azure Storage
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
STORAGE_ACCOUNT = "stsynfbnovadev"
CONTAINER_NAME = "synapsedata"

# Auth
USERNAME = os.getenv("USERNAME")

# =============================================================================
# CONNECTIONS
# =============================================================================

def connect_source():
    """Connect to source using Azure AD Interactive"""
    print(f"  Connecting to source: {SOURCE_SERVER}...")
    conn_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SOURCE_SERVER};"
        f"DATABASE={SOURCE_DATABASE};"
        f"UID={USERNAME};"
        f"Authentication=ActiveDirectoryInteractive;"
        f"Encrypt=yes;"
    )
    conn = pyodbc.connect(conn_string)
    print(f"  ✓ Source connected")
    return conn

def connect_target():
    """Connect to target using Azure AD Interactive"""
    print(f"  Connecting to target: {TARGET_SERVER}...")
    conn_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={TARGET_SERVER};"
        f"DATABASE={TARGET_DATABASE};"
        f"UID={USERNAME};"
        f"Authentication=ActiveDirectoryInteractive;"
        f"Encrypt=yes;"
    )
    conn = pyodbc.connect(conn_string, autocommit=True)
    print(f"  ✓ Target connected")
    return conn

def get_blob_client():
    """Get blob service client"""
    return BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

# =============================================================================
# CONFIG
# =============================================================================

def read_config():
    """Read config file, return enabled tables only"""
    print("Reading config file...")
    df = pd.read_excel("config file.xlsx")
    enabled_col = [c for c in df.columns if 'Enabled' in c][0]
    df = df[df[enabled_col].str.strip().str.lower() == 'y']
    
    tables = []
    for _, row in df.iterrows():
        tables.append({
            'source_schema': row['Source_Schema'],
            'source_table': row['Source_table_name'],
            'target_schema': row['Target_Schema'],
            'target_table': row['Target_Table_Name']
        })
    
    print(f"  ✓ Found {len(tables)} enabled tables")
    return tables

# =============================================================================
# STEP 1: EXPORT TO CSV (with chunking for large tables)
# =============================================================================

CHUNK_SIZE = 500000  # 500K rows per chunk - balances speed and memory safety

# Large table handling - sample instead of full load
LARGE_TABLE_THRESHOLD = 10000000  # 10 million rows
SAMPLE_SIZE = 1000000  # 1 million rows sample for large tables

# Tables known to be large (will use sample) - EMPTY = full load for all
LARGE_TABLES = [
    # 'FactCustomerSales',        # 36.9M
    # 'FactCustomerSalesHistory', # 17.7M
    # 'FactSales',                # 1.7B
    # 'FactSalesSummary',         # 495M
    # 'FactSalesThirdPartySummary', # 22.8M
    # 'FactTender',               # 836M
]

def fix_columns(df):
    """Fix data type issues that cause Synapse COPY INTO to fail"""
    for col in df.columns:
        if df[col].dtype == 'float64':
            # Replace infinity values with NaN (will become empty in CSV = NULL)
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            
            # Check if all non-null values are whole numbers (int disguised as float)
            non_null = df[col].dropna()
            if len(non_null) > 0:
                try:
                    if (non_null == non_null.astype(int)).all():
                        # Convert to nullable integer
                        df[col] = df[col].astype('Int64')
                    else:
                        # Keep as float but round to 10 decimal places
                        df[col] = df[col].round(10)
                except (ValueError, OverflowError):
                    df[col] = df[col].round(10)
        
        # Handle object columns that might have problematic values
        elif df[col].dtype == 'object':
            # Replace None, 'nan', 'None' strings with actual NaN
            df[col] = df[col].replace(['nan', 'None', 'NULL', 'null'], np.nan)
    
    return df

def export_to_csv(source_conn, config):
    """Export source table to local CSV file with chunking for large tables"""
    src = f"[{config['source_schema']}].[{config['source_table']}]"
    table_name = config['source_table']
    base_filename = f"{table_name}"
    
    print(f"  Exporting {src} to CSV...")
    
    # Check if this is a large table - use sample instead of full load
    if table_name in LARGE_TABLES:
        query = f"SELECT TOP {SAMPLE_SIZE} * FROM {src}"
        print(f"    [SAMPLE MODE] Loading {SAMPLE_SIZE:,} rows (table is >10M rows)")
    else:
        query = f"SELECT * FROM {src}"
    
    total_rows = 0
    chunk_num = 0
    chunk_files = []
    
    # Read in chunks to handle large tables
    for chunk in pd.read_sql(query, source_conn, chunksize=CHUNK_SIZE):
        chunk_num += 1
        chunk_filename = f"{base_filename}_chunk_{chunk_num:04d}.csv"
        
        # Fix data type issues (int/float/decimal)
        chunk = fix_columns(chunk)
        
        # Write chunk to CSV
        if chunk_num == 1:
            # First chunk includes header
            chunk.to_csv(chunk_filename, index=False, sep='|', encoding='utf-8', 
                        na_rep='', float_format='%.10f')
        else:
            # Subsequent chunks no header
            chunk.to_csv(chunk_filename, index=False, sep='|', encoding='utf-8', 
                        na_rep='', float_format='%.10f', header=False)
        
        # Upload chunk immediately to blob
        upload_chunk_to_blob(chunk_filename, table_name, chunk_num)
        
        # Delete local chunk file immediately to save disk space
        os.remove(chunk_filename)
        
        chunk_files.append(chunk_filename)
        total_rows += len(chunk)
        
        # Progress indicator for large tables
        if total_rows % 1000000 == 0:
            print(f"    Progress: {total_rows:,} rows...")
    
    print(f"    Rows: {total_rows:,}")
    
    # Return the base pattern for blob files
    return base_filename, total_rows, chunk_num

# =============================================================================
# STEP 2: UPLOAD CHUNKS TO BLOB
# =============================================================================

def upload_chunk_to_blob(chunk_filename, table_name, chunk_num):
    """Upload a single chunk file to Azure Blob Storage"""
    blob_service = get_blob_client()
    blob_name = f"staging/{table_name}_chunk_{chunk_num:04d}.csv"
    blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    
    with open(chunk_filename, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

def upload_to_blob(filename):
    """Upload file to Azure Blob Storage (kept for compatibility)"""
    print(f"  Uploading to blob storage...")
    
    blob_service = get_blob_client()
    blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=f"staging/{filename}")
    
    with open(filename, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    blob_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/staging/{filename}"
    print(f"    ✓ Uploaded to {blob_url}")
    
    return blob_url

# =============================================================================
# STEP 3: COPY INTO TARGET
# =============================================================================

def create_table_if_not_exists(source_conn, target_conn, config):
    """Create target table if it doesn't exist"""
    tgt = f"[{config['target_schema']}].[{config['target_table']}]"
    
    # Check if exists
    cursor = target_conn.cursor()
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{config['target_schema']}' 
        AND TABLE_NAME = '{config['target_table']}'
    """)
    exists = cursor.fetchone()[0] > 0
    
    if exists:
        print(f"  Table exists, truncating...")
        cursor.execute(f"TRUNCATE TABLE {tgt}")
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
            
            # Always use NULL for staging tables (source data may have NULLs despite NOT NULL constraint)
            columns.append(f"[{col_name}] {type_str} NULL")
        
        create_sql = f"CREATE TABLE {tgt} ({', '.join(columns)})"
        cursor.execute(create_sql)
        src_cursor.close()
    
    cursor.close()

def copy_into_target(target_conn, config, table_name, chunk_count):
    """Use COPY INTO to bulk load from blob chunks"""
    tgt = f"[{config['target_schema']}].[{config['target_table']}]"
    
    print(f"  COPY INTO {tgt} from {chunk_count} chunks...")
    
    cursor = target_conn.cursor()
    
    # Load each chunk file
    for chunk_num in range(1, chunk_count + 1):
        blob_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/staging/{table_name}_chunk_{chunk_num:04d}.csv"
        
        # COPY command for CSV with pipe delimiter
        copy_sql = f"""
        COPY INTO {tgt}
        FROM '{blob_url}'
        WITH (
            FILE_TYPE = 'CSV',
            FIELDTERMINATOR = '|',
            FIRSTROW = {2 if chunk_num == 1 else 1},
            CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '{os.getenv("STORAGE_KEY")}')
        )
        """
        
        cursor.execute(copy_sql)
        
        if chunk_num % 10 == 0:
            print(f"    Loaded {chunk_num}/{chunk_count} chunks...")
    
    cursor.close()
    print(f"    ✓ COPY completed ({chunk_count} chunks)")

# =============================================================================
# STEP 4: CLEANUP
# =============================================================================

def cleanup(table_name, chunk_count):
    """Delete blob chunk files"""
    try:
        blob_service = get_blob_client()
        for chunk_num in range(1, chunk_count + 1):
            blob_name = f"staging/{table_name}_chunk_{chunk_num:04d}.csv"
            blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
            blob_client.delete_blob()
    except:
        pass  # OK if delete fails

# =============================================================================
# MAIN
# =============================================================================

def process_table(source_conn, target_conn, config):
    """Full process for one table"""
    start = datetime.now()
    rows = 0
    status = "Success"
    error = None
    
    try:
        # Step 1: Export to CSV chunks (uploaded and deleted immediately)
        table_name, rows, chunk_count = export_to_csv(source_conn, config)
        print(f"    ✓ Exported and uploaded {chunk_count} chunks")
        
        # Step 2: Create table if needed
        create_table_if_not_exists(source_conn, target_conn, config)
        
        # Step 3: COPY INTO from all chunks
        copy_into_target(target_conn, config, table_name, chunk_count)
        
        # Step 4: Cleanup blob chunks
        cleanup(table_name, chunk_count)
        
    except Exception as e:
        status = "FAILED"
        error = str(e)
        print(f"  ✗ ERROR: {e}")
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"  Time: {elapsed:.1f}s")
    
    return {'table': config['source_table'], 'rows': rows, 'time': elapsed, 'status': status, 'error': error}

def main():
    print("=" * 60)
    print("FB NOVA - DATA SYNC (COPY method)")
    print(f"Started: {datetime.now()}")
    print("=" * 60)
    
    # Check storage connection
    if not STORAGE_CONNECTION_STRING:
        print("ERROR: STORAGE_CONNECTION_STRING not set in .env")
        return
    
    # Read config
    print("\nSTEP 1: READ CONFIG")
    tables = read_config()
    
    # Connect
    print("\nSTEP 2: CONNECT")
    source_conn = connect_source()
    target_conn = connect_target()
    
    # Process tables
    print("\nSTEP 3: PROCESS TABLES")
    results = []
    for i, config in enumerate(tables, 1):
        print(f"\n[{i}/{len(tables)}] {config['source_schema']}.{config['source_table']}")
        result = process_table(source_conn, target_conn, config)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    success = sum(1 for r in results if r['status'] == 'Success')
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    total_rows = sum(r['rows'] for r in results)
    total_time = sum(r['time'] for r in results)
    
    print(f"Tables: {len(results)} (Success: {success}, Failed: {failed})")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {total_time:.1f}s")
    
    if failed > 0:
        print("\nFailed tables:")
        for r in results:
            if r['status'] == 'FAILED':
                print(f"  - {r['table']}: {r['error']}")
    
    source_conn.close()
    target_conn.close()
    
    print(f"\nCompleted: {datetime.now()}")

if __name__ == "__main__":
    main()

