"""
Load FactTender from blob storage to target Synapse (DEV)
=========================================================
Using wildcard COPY INTO per range (same strategy as FactSales)
- Loads all chunks for each range in one command
- No memory spillage on DW200c
- Estimated time: ~40 minutes
"""
import os
import sys
import json
import time
import struct
from datetime import datetime
from pathlib import Path

import pyodbc
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions
from azure.storage.blob import BlobServiceClient

# Battery monitoring
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ======================== CONFIGURATION ========================
TABLE_NAME = 'FactTender'
SCHEMA = 'stg_dwh'

# Target Synapse (DEV)
TARGET_SERVER = 'synapse-fbnova-dev.sql.azuresynapse.net'
TARGET_DATABASE = 'sqlpoolfbnovadev'

# Azure Storage
CONTAINER_NAME = 'synapsedata'
STORAGE_PREFIX = f'staging/{TABLE_NAME}'

# Progress
PROGRESS_DIR = Path('progress')
WATERMARK_FILE = PROGRESS_DIR / f'{TABLE_NAME}_load_watermark.json'

# Slack (same as FactSales)
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')  # Set in .env file

# Number of ranges (must match export - updated for larger ranges)
NUM_RANGES = 10  # Ranges 0-9 (was 19, now consolidated)

# Milestones (25%, 50%, 75%, 90%)
MILESTONES = [25, 50, 75, 90]
milestones_sent = set()

# ======================== TABLE SCHEMA (14 columns) ========================
CREATE_TABLE_SQL = f"""
CREATE TABLE [{SCHEMA}].[{TABLE_NAME}] (
    [FactTenderID] bigint NOT NULL,
    [DimRestaurantID] int NULL,
    [DimCurrencyID] int NULL,
    [TransNum] int NULL,
    [DimTransTypeID] int NULL,
    [DimTransDateID] int NULL,
    [DimTransTimeID] int NULL,
    [DimBusinessDateID] int NULL,
    [DimOrderMethodID] int NULL,
    [DimOrderTypeID] int NULL,
    [DimTenderTypeID] int NULL,
    [TenderAmount] decimal(19,6) NULL,
    [InsertedDateTime] datetime NULL,
    [FileId] int NULL
)
WITH (
    DISTRIBUTION = HASH([FactTenderID]),
    CLUSTERED COLUMNSTORE INDEX
)
"""

# Load env
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

# ======================== HELPERS ========================
def format_time(seconds):
    """Format seconds into human readable string"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds//60)}m {int(seconds%60)}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"

def get_battery_status():
    """Get battery percentage and charging status"""
    if not HAS_PSUTIL:
        return None
    
    try:
        battery = psutil.sensors_battery()
        if battery is None:
            return None
        
        percent = battery.percent
        plugged = battery.power_plugged
        secs_left = battery.secsleft
        
        # Check if secs_left is valid (not unknown/unlimited)
        valid_time = (secs_left is not None and 
                      secs_left > 0 and 
                      secs_left < 1000000)  # Less than ~277 hours
        
        if percent <= 20:
            status = f"🪫 {percent:.0f}% ⚠️ LOW"
        else:
            status = f"🔋 {percent:.0f}%"
        
        if plugged:
            status += " ⚡Charging"
            if valid_time:
                status += f" ({format_time(secs_left)} to full)"
        else:
            status += " 🔌On Battery"
            if valid_time:
                status += f" ({format_time(secs_left)} left)"
                # Estimate time to 20%
                if percent > 20:
                    time_to_20 = secs_left * (percent - 20) / percent
                    status += f" | {format_time(time_to_20)} to 20%"
        
        return status
    except:
        return None

def send_slack(message):
    """Send Slack notification"""
    if not SLACK_WEBHOOK_URL:
        print(f"[Slack] {message}")
        return
    try:
        import urllib.request
        data = json.dumps({"text": message}).encode('utf-8')
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[Slack Error] {e}")

def get_storage_account_info():
    """Extract storage account name and key from connection string"""
    conn_str = os.environ.get('STORAGE_CONNECTION_STRING', '')
    parts = dict(p.split('=', 1) for p in conn_str.split(';') if '=' in p)
    return parts.get('AccountName'), parts.get('AccountKey')

# Global credential - authenticate once, reuse for all connections
_cached_credential = None
_cached_token = None
_token_expiry = None

def get_azure_token():
    """Get Azure AD token, reusing cached token if still valid"""
    global _cached_credential, _cached_token, _token_expiry
    
    # Check if we have a valid cached token (with 5 min buffer)
    if _cached_token and _token_expiry:
        from datetime import timezone
        now = datetime.now(timezone.utc)
        if now.timestamp() < _token_expiry - 300:  # 5 min buffer
            return _cached_token
    
    # Need fresh token - use cached credential if available
    if _cached_credential is None:
        print("\n" + "=" * 60)
        print("🔐 AUTHENTICATION REQUIRED - BROWSER POPUP INCOMING!")
        print("=" * 60)
        print("👀 WATCH FOR BROWSER POPUP - Complete login within 10 minutes")
        print("=" * 60 + "\n")
        # Beep to alert user
        print('\a')  # Terminal bell
        # Enable persistent token cache for silent refresh
        cache_options = TokenCachePersistenceOptions(allow_unencrypted_storage=True)
        _cached_credential = InteractiveBrowserCredential(
            cache_persistence_options=cache_options,
            timeout=600  # 10 minutes instead of 5
        )
    
    token = _cached_credential.get_token('https://database.windows.net/.default')
    _cached_token = token.token
    _token_expiry = token.expires_on
    
    return _cached_token

def get_target_connection():
    """Connect to target Synapse using Azure AD token (reuses auth)"""
    print("Connecting to target (DEV)...")
    
    token = get_azure_token()
    token_bytes = token.encode('UTF-16-LE')
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={TARGET_SERVER};DATABASE={TARGET_DATABASE};'
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    print("[OK] Connected to target")
    return conn

def count_range_chunks(range_idx):
    """Count chunks for a range in blob storage"""
    try:
        conn_str = os.environ.get('STORAGE_CONNECTION_STRING', '')
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = blob_service.get_container_client(CONTAINER_NAME)
        
        prefix = f'{STORAGE_PREFIX}_range{range_idx:02d}_'
        count = sum(1 for _ in container.list_blobs(name_starts_with=prefix))
        return count
    except Exception as e:
        print(f"Error counting chunks: {e}")
        return 0

def load_watermark():
    """Load progress"""
    if WATERMARK_FILE.exists():
        with open(WATERMARK_FILE) as f:
            return json.load(f)
    return {
        'status': 'not_started',
        'ranges_loaded': [],
        'total_rows': 0,
        'start_time': None
    }

def save_watermark(data):
    """Save progress"""
    with open(WATERMARK_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# ======================== MAIN ========================
def main():
    print("=" * 70)
    print(f"FB NOVA - {TABLE_NAME} LOAD TO TARGET")
    print("=" * 70)
    print(f"Target: {TARGET_SERVER}/{TARGET_DATABASE}")
    print(f"Schema: {SCHEMA}")
    print("=" * 70)
    
    # Load watermark
    watermark = load_watermark()
    print(f"\nStatus: {watermark['status']}")
    
    if watermark['ranges_loaded']:
        print(f"Ranges loaded: {len(watermark['ranges_loaded'])}/{NUM_RANGES}")
    
    # Connect to target
    conn = get_target_connection()
    conn.autocommit = True  # Required for DDL in Synapse
    cursor = conn.cursor()
    
    # Check/create schema
    print(f"\nChecking schema [{SCHEMA}]...")
    try:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{SCHEMA}')
            BEGIN
                EXEC('CREATE SCHEMA [{SCHEMA}]')
            END
        """)
        print(f"  [OK] Schema ready")
    except Exception as e:
        print(f"  [WARN] Schema check: {e}")
    
    # Check if table exists
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{TABLE_NAME}'
    """)
    table_exists = cursor.fetchone()[0] > 0
    
    if table_exists and not watermark['ranges_loaded']:
        # Fresh start - drop and recreate
        print(f"\nDropping existing table [{SCHEMA}].[{TABLE_NAME}]...")
        cursor.execute(f"DROP TABLE [{SCHEMA}].[{TABLE_NAME}]")
        table_exists = False
    
    if not table_exists:
        print(f"\nCreating table [{SCHEMA}].[{TABLE_NAME}]...")
        cursor.execute(CREATE_TABLE_SQL)
        print(f"  [OK] Table created")
    
    # Get storage credentials
    storage_account, storage_key = get_storage_account_info()
    print(f"\nStorage account: {storage_account}")
    
    # Count available ranges
    print(f"\nCounting chunks per range...")
    range_chunks = {}
    total_chunks = 0
    for r in range(NUM_RANGES):
        count = count_range_chunks(r)
        if count > 0:
            range_chunks[r] = count
            total_chunks += count
            print(f"  Range {r:02d}: {count} chunks")
    
    print(f"\nTotal: {total_chunks} chunks across {len(range_chunks)} ranges")
    
    if not range_chunks:
        print("\n[ERROR] No chunks found! Run export first.")
        return
    
    # Start loading
    if watermark['start_time'] is None:
        watermark['start_time'] = datetime.now().isoformat()
    watermark['status'] = 'loading'
    save_watermark(watermark)
    
    ranges_to_load = [r for r in range_chunks.keys() if r not in watermark['ranges_loaded']]
    
    if not ranges_to_load:
        print("\n[OK] All ranges already loaded!")
        return
    
    total_ranges_to_load = len(ranges_to_load)
    send_slack(
        f"🚀 *{TABLE_NAME} Load Started*\n"
        f"• Ranges: {total_ranges_to_load}\n"
        f"• Chunks: {total_chunks}\n"
        f"• Est. time: ~40 min"
    )
    
    start_time = time.time()
    last_hourly = start_time
    errors = 0
    global milestones_sent
    
    for i, range_idx in enumerate(sorted(ranges_to_load)):
        print(f"\n{'='*50}")
        print(f"LOADING RANGE {range_idx} ({i+1}/{total_ranges_to_load}) - {range_chunks[range_idx]} chunks")
        print(f"{'='*50}")
        
        # Use wildcard to load all chunks in range at once
        blob_pattern = f'{STORAGE_PREFIX}_range{range_idx:02d}_*.csv'
        blob_url = f'https://{storage_account}.blob.core.windows.net/{CONTAINER_NAME}/{blob_pattern}'
        
        copy_sql = f"""
        COPY INTO [{SCHEMA}].[{TABLE_NAME}]
        FROM '{blob_url}'
        WITH (
            FILE_TYPE = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '{storage_key}'),
            MAXERRORS = 0
        )
        """
        
        range_start = time.time()
        try:
            print(f"  Executing COPY INTO (wildcard)...")
            cursor.execute(copy_sql)
            # No commit needed - autocommit is on
            
            range_elapsed = time.time() - range_start
            print(f"  [OK] Range {range_idx} loaded in {format_time(range_elapsed)}")
            
            watermark['ranges_loaded'].append(range_idx)
            save_watermark(watermark)
            
            # Calculate progress
            ranges_done = len(watermark['ranges_loaded'])
            pct_done = ranges_done / total_ranges_to_load * 100
            elapsed_total = time.time() - start_time
            
            # Estimate remaining time based on average time per range
            avg_time_per_range = elapsed_total / ranges_done if ranges_done > 0 else 0
            ranges_remaining = total_ranges_to_load - ranges_done
            eta_seconds = avg_time_per_range * ranges_remaining
            
            # Milestone notifications (25%, 50%, 75%, 90%)
            for m in MILESTONES:
                if pct_done >= m and m not in milestones_sent:
                    milestones_sent.add(m)
                    battery = get_battery_status()
                    msg = (
                        f"📊 *{TABLE_NAME} Load {m}% Complete*\n"
                        f"• Ranges: {ranges_done}/{total_ranges_to_load}\n"
                        f"• Elapsed: {format_time(elapsed_total)}\n"
                        f"• ETA: {format_time(eta_seconds)}"
                    )
                    if battery:
                        msg += f"\n• {battery}"
                    send_slack(msg)
            
            # Hourly notification
            if time.time() - last_hourly > 3600:
                battery = get_battery_status()
                msg = (
                    f"⏰ *{TABLE_NAME} Load Hourly Update*\n"
                    f"• Progress: {pct_done:.1f}% ({ranges_done}/{total_ranges_to_load} ranges)\n"
                    f"• Elapsed: {format_time(elapsed_total)}\n"
                    f"• ETA: {format_time(eta_seconds)}"
                )
                if battery:
                    msg += f"\n• {battery}"
                send_slack(msg)
                last_hourly = time.time()
            
        except Exception as e:
            print(f"  [ERROR] Range {range_idx}: {e}")
            errors += 1
            elapsed_total = time.time() - start_time
            send_slack(
                f"❌ *{TABLE_NAME} Load Error*\n"
                f"• Range {range_idx}\n"
                f"• Elapsed: {format_time(elapsed_total)}\n"
                f"• Error: {str(e)[:200]}"
            )
    
    # Get final row count
    cursor.execute(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
    final_rows = cursor.fetchone()[0]
    
    elapsed_total = time.time() - start_time
    
    watermark['status'] = 'complete'
    watermark['total_rows'] = final_rows
    save_watermark(watermark)
    
    print(f"\n{'='*70}")
    print(f"LOAD COMPLETE!")
    print(f"{'='*70}")
    print(f"  Total rows: {final_rows:,}")
    print(f"  Time: {elapsed_total/60:.1f} minutes")
    print(f"  Errors: {errors}")
    print(f"{'='*70}")
    
    send_slack(
        f"🎉 *{TABLE_NAME} Load Complete!*\n"
        f"• Total rows: {final_rows:,}\n"
        f"• Time: {format_time(elapsed_total)}\n"
        f"• Errors: {errors}"
    )
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()

