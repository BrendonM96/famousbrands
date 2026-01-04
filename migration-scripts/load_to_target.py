"""
Load FactSales from Blob Storage to Target Synapse
- Range-based loading with resume capability
- Slack notifications (start, hourly, milestones, completion)
- Azure Identity token authentication
"""

import os
import sys
import json
import struct
import time
import urllib.request
import pyodbc
from datetime import datetime, timedelta
from azure.identity import InteractiveBrowserCredential
from azure.storage.blob import BlobServiceClient

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

# =============================================================================
# CONFIGURATION
# =============================================================================
TARGET_SERVER = "synapse-fbnova-dev.sql.azuresynapse.net"
TARGET_DATABASE = "sqlpoolfbnovadev"
TARGET_SCHEMA = "stg_dwh"
TARGET_TABLE = "FactSales"

# Storage config - extracted from connection string
STORAGE_CONNECTION_STRING = os.getenv('STORAGE_CONNECTION_STRING')
STORAGE_KEY = os.getenv('STORAGE_KEY')
CONTAINER_NAME = "synapsedata"

def get_storage_account_name():
    if not STORAGE_CONNECTION_STRING:
        return "unknown"
    for part in STORAGE_CONNECTION_STRING.split(';'):
        if part.startswith('AccountName='):
            return part.split('=', 1)[1]
    return "unknown"

STORAGE_ACCOUNT = get_storage_account_name()

# Slack webhook
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  # Set in .env file

# Watermark file for resume
WATERMARK_FILE = "FactSales_load_watermark.json"

# =============================================================================
# SLACK NOTIFICATIONS
# =============================================================================
def send_slack_notification(message, emoji=""):
    """Send notification to Slack"""
    if not SLACK_WEBHOOK_URL:
        return
    try:
        full_message = f"{emoji} {message}" if emoji else message
        payload = json.dumps({"text": full_message}).encode('utf-8')
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=payload,
            headers={'Content-Type': 'application/json'}
        )
        urllib.request.urlopen(req, timeout=10)
        print(f"[SLACK] Notification sent")
    except Exception as e:
        print(f"[SLACK ERROR] {e}")

def format_time(seconds):
    """Format seconds to human readable"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"

# =============================================================================
# WATERMARK / RESUME
# =============================================================================
def load_watermark():
    """Load progress from watermark file"""
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, 'r') as f:
            return json.load(f)
    return {"ranges_loaded": [], "total_rows": 0, "start_time": None}

def save_watermark(data):
    """Save progress to watermark file"""
    with open(WATERMARK_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# =============================================================================
# CONNECTIONS
# =============================================================================
def get_target_connection():
    """Connect to target Synapse using Azure AD token"""
    print("Authenticating with Azure AD...")
    credential = InteractiveBrowserCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={TARGET_SERVER};DATABASE={TARGET_DATABASE};"
    return pyodbc.connect(conn_str, attrs_before={1256: token_struct}, autocommit=True)

def get_blob_service():
    """Get blob service client"""
    return BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

# =============================================================================
# BLOB OPERATIONS
# =============================================================================
def get_ranges_from_blobs():
    """Get list of ranges from blob storage"""
    blob_service = get_blob_service()
    container = blob_service.get_container_client(CONTAINER_NAME)
    
    ranges = {}
    total_files = 0
    
    for blob in container.list_blobs(name_starts_with='staging/FactSales_'):
        if blob.name.endswith('.csv'):
            total_files += 1
            # Extract range: FactSales_range00_chunk_00001.csv
            parts = blob.name.replace('staging/', '').split('_')
            if len(parts) >= 2 and parts[1].startswith('range'):
                range_num = parts[1]
                if range_num not in ranges:
                    ranges[range_num] = []
                ranges[range_num].append(blob.name)
    
    # Sort ranges and files
    sorted_ranges = {}
    for range_num in sorted(ranges.keys()):
        sorted_ranges[range_num] = sorted(ranges[range_num])
    
    return sorted_ranges, total_files

# =============================================================================
# TABLE OPERATIONS
# =============================================================================
def create_target_table(cursor):
    """Create target table with correct 42-column schema"""
    
    # Drop if exists
    try:
        cursor.execute(f'DROP TABLE [{TARGET_SCHEMA}].[{TARGET_TABLE}]')
        print("  Existing table dropped")
    except:
        pass
    
    # Create with correct schema
    create_sql = f"""
    CREATE TABLE [{TARGET_SCHEMA}].[{TARGET_TABLE}]
    (
        [FactSalesID] bigint NULL,
        [DimRestaurantID] int NULL,
        [DimCurrencyID] int NULL,
        [DimTransTypeID] int NULL,
        [DimTransDateID] int NULL,
        [DimTransTimeID] int NULL,
        [DimBusinessDateID] int NULL,
        [DimMenuItemID] int NULL,
        [DimTransLineTypeID] int NULL,
        [DimOrderMethodID] int NULL,
        [DimOrderTypeID] int NULL,
        [TransNum] int NULL,
        [POSRecordNum] varchar(20) NULL,
        [LineNum] smallint NULL,
        [ParentLineNum] smallint NULL,
        [StoreProductCode] varchar(20) NULL,
        [StoreProductDesc] varchar(30) NULL,
        [VATApplicable] tinyint NULL,
        [VATPercentage] decimal(5,5) NULL,
        [DetailTransAmount] decimal(19,6) NULL,
        [Quantity] bigint NULL,
        [StoreSellingPrice] decimal(19,2) NULL,
        [DetailDiscountAmount] decimal(19,6) NULL,
        [DetailVoucherAmount] decimal(19,6) NULL,
        [DetailStaffMealAmount] decimal(19,6) NULL,
        [StoreCostPrice] decimal(19,2) NULL,
        [DetailVATAmount] decimal(19,6) NULL,
        [DetailSalesAmountExclVAT] decimal(19,6) NULL,
        [DetailSalesAmountExclDiscount] decimal(19,6) NULL,
        [DetailSalesAmountDeclarable] decimal(19,6) NULL,
        [StoreSellingPriceExclVAT] decimal(19,6) NULL,
        [SalesValue] decimal(19,6) NULL,
        [Usage] decimal(19,6) NULL,
        [BeverageQuantity] bigint NULL,
        [FoodQuantity] bigint NULL,
        [BeverageIncidentCount] int NULL,
        [FoodIncidentCount] int NULL,
        [BeverageAndFoodIncidentCount] int NULL,
        [ComboCount] int NULL,
        [TransactionCount] int NULL,
        [InsertedDateTime] datetime NULL,
        [FileId] int NULL
    )
    WITH (
        DISTRIBUTION = HASH([FactSalesID]),
        CLUSTERED COLUMNSTORE INDEX
    )
    """
    cursor.execute(create_sql)
    print("  Table created with 42-column schema")

def load_range(cursor, range_name, files):
    """Load ALL files in a range using single COPY INTO with wildcard (much faster!)"""
    
    rows_before = 0
    try:
        cursor.execute(f'SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{TARGET_TABLE}]')
        rows_before = cursor.fetchone()[0]
    except:
        pass
    
    errors = []
    
    # Use wildcard to load ALL files in range at once (much faster than individual files)
    # Pattern: staging/FactSales_range00_*.csv
    blob_pattern = f'https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/staging/FactSales_{range_name}_*.csv'
    
    copy_sql = f"""
    COPY INTO [{TARGET_SCHEMA}].[{TARGET_TABLE}]
    FROM '{blob_pattern}'
    WITH (
        FILE_TYPE = 'CSV',
        FIELDTERMINATOR = '|',
        FIRSTROW = 1,
        CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '{STORAGE_KEY}')
    )
    """
    
    try:
        cursor.execute(copy_sql)
    except Exception as e:
        errors.append(f"{range_name}: {str(e)[:200]}")
    
    # Get rows loaded
    cursor.execute(f'SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{TARGET_TABLE}]')
    rows_after = cursor.fetchone()[0]
    rows_loaded = rows_after - rows_before
    
    return rows_loaded, errors

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 70)
    print("  FactSales Loader - Blob to Synapse")
    print("=" * 70)
    print(f"Target: {TARGET_SERVER}/{TARGET_DATABASE}")
    print(f"Table:  [{TARGET_SCHEMA}].[{TARGET_TABLE}]")
    print(f"Storage: {STORAGE_ACCOUNT}")
    print("=" * 70)
    
    # Load watermark
    watermark = load_watermark()
    ranges_loaded = set(watermark.get("ranges_loaded", []))
    
    # Get ranges from blob
    print("\nScanning blob storage...")
    ranges, total_files = get_ranges_from_blobs()
    total_ranges = len(ranges)
    
    print(f"Found {total_ranges} ranges with {total_files:,} files")
    
    if not ranges:
        print("\n[ERROR] No CSV files found!")
        return
    
    # Filter already loaded
    ranges_to_load = {k: v for k, v in ranges.items() if k not in ranges_loaded}
    
    if ranges_loaded:
        print(f"\nRESUMING: {len(ranges_loaded)} ranges already loaded")
        print(f"Remaining: {len(ranges_to_load)} ranges")
    
    if not ranges_to_load:
        print("\nAll ranges already loaded!")
        return
    
    # Connect
    print("\nConnecting to target Synapse...")
    conn = get_target_connection()
    cursor = conn.cursor()
    print("Connected!")
    
    # Check/create table
    print("\nChecking target table...")
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}' AND TABLE_NAME = '{TARGET_TABLE}'
    """)
    table_exists = cursor.fetchone()[0] > 0
    
    if table_exists and not ranges_loaded:
        # Fresh start - recreate table
        print("  Fresh start - recreating table...")
        create_target_table(cursor)
    elif table_exists:
        cursor.execute(f'SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{TARGET_TABLE}]')
        count = cursor.fetchone()[0]
        print(f"  Table exists with {count:,} rows (resuming)")
    else:
        print("  Creating table...")
        create_target_table(cursor)
    
    # Track progress
    start_time = time.time()
    if not watermark.get("start_time"):
        watermark["start_time"] = datetime.now().isoformat()
        save_watermark(watermark)
    
    ranges_completed = len(ranges_loaded)
    last_milestone = int((ranges_completed / total_ranges) * 100) // 25 * 25 if total_ranges > 0 else 0
    last_hourly = time.time()
    total_rows_loaded = watermark.get("total_rows", 0)
    all_errors = []
    
    # Send start notification
    send_slack_notification(
        f"*FactSales Load Started*\n"
        f"• Target: {TARGET_DATABASE}\n"
        f"• Ranges: {len(ranges_to_load)}/{total_ranges}\n"
        f"• Files: {total_files:,}",
        ":rocket:"
    )
    
    print("\n" + "=" * 70)
    print("  LOADING DATA")
    print("=" * 70)
    
    for range_name, files in ranges_to_load.items():
        ranges_completed += 1
        progress = (ranges_completed / total_ranges) * 100
        
        print(f"\n[{ranges_completed}/{total_ranges}] Loading {range_name} ({len(files)} files)...")
        
        range_start = time.time()
        rows_loaded, errors = load_range(cursor, range_name, files)
        range_time = time.time() - range_start
        
        total_rows_loaded += rows_loaded
        all_errors.extend(errors)
        
        if errors:
            print(f"  Loaded {rows_loaded:,} rows in {format_time(range_time)} | {len(errors)} errors")
        else:
            print(f"  Loaded {rows_loaded:,} rows in {format_time(range_time)} | Progress: {progress:.1f}%")
        
        # Update watermark
        ranges_loaded.add(range_name)
        watermark["ranges_loaded"] = list(ranges_loaded)
        watermark["total_rows"] = total_rows_loaded
        save_watermark(watermark)
        
        # Milestone notifications (25%, 50%, 75%, 100%)
        current_milestone = int(progress // 25) * 25
        if current_milestone > last_milestone and current_milestone in [25, 50, 75, 100]:
            last_milestone = current_milestone
            
            cursor.execute(f'SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{TARGET_TABLE}]')
            current_total = cursor.fetchone()[0]
            
            send_slack_notification(
                f"*FactSales Load - {current_milestone}% Milestone*\n"
                f"• Ranges: {ranges_completed}/{total_ranges}\n"
                f"• Rows in table: {current_total:,}\n"
                f"• Errors: {len(all_errors)}",
                ":chart_with_upwards_trend:" if current_milestone < 100 else ":white_check_mark:"
            )
        
        # Hourly notifications
        if time.time() - last_hourly >= 3600:
            last_hourly = time.time()
            elapsed = time.time() - start_time
            
            cursor.execute(f'SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{TARGET_TABLE}]')
            current_total = cursor.fetchone()[0]
            
            # Calculate ETA
            if ranges_completed > 0:
                eta_seconds = (elapsed / ranges_completed) * (total_ranges - ranges_completed)
            else:
                eta_seconds = 0
            
            send_slack_notification(
                f"*:clock1: Hourly Update: FactSales Load*\n"
                f"• Progress: {progress:.1f}%\n"
                f"• Ranges: {ranges_completed}/{total_ranges}\n"
                f"• Rows: {current_total:,}\n"
                f"• ETA: {format_time(eta_seconds)}",
                ""
            )
    
    # Final stats
    total_time = time.time() - start_time
    
    cursor.execute(f'SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{TARGET_TABLE}]')
    final_count = cursor.fetchone()[0]
    
    print("\n" + "=" * 70)
    print("  LOAD COMPLETE!")
    print("=" * 70)
    print(f"  Total rows: {final_count:,}")
    print(f"  Total time: {format_time(total_time)}")
    print(f"  Errors: {len(all_errors)}")
    print("=" * 70)
    
    send_slack_notification(
        f"*:tada: FactSales Load Complete!*\n"
        f"• Total rows: {final_count:,}\n"
        f"• Time: {format_time(total_time)}\n"
        f"• Errors: {len(all_errors)}",
        ""
    )
    
    # Show errors
    if all_errors:
        print("\nErrors encountered:")
        for err in all_errors[:10]:
            print(f"  - {err}")
        if len(all_errors) > 10:
            print(f"  ... and {len(all_errors) - 10} more")
    
    # Cleanup
    cursor.close()
    conn.close()
    
    # Clear watermark on success
    if not all_errors:
        if os.path.exists(WATERMARK_FILE):
            os.remove(WATERMARK_FILE)
            print("\nWatermark cleared (load complete)")
    
    print("\n" + "=" * 70)
    print("  NEXT STEPS:")
    print("=" * 70)
    print("  1. Verify row counts match source (~1.86 billion)")
    print("  2. Run DELTA SYNC for rows added during export:")
    print("     SELECT * FROM dwh.FactSales WHERE FactSalesID > 8840227578")
    print("=" * 70)

if __name__ == "__main__":
    main()
