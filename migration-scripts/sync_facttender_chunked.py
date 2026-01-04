"""
FB NOVA - FactTender CHUNKED STREAMING Export
==============================================
Based on successful FactSales strategy:
- Split into ID ranges based on data distribution
- Stream each range with fresh connection
- Watermark after each range for resume
- Slack notifications for progress
"""
import os
import sys
import json
import time
import struct
import gc
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
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
PK_COLUMN = 'FactTenderID'
CHUNK_SIZE = 250_000  # rows per chunk file
SCHEMA = 'dwh'

# Source (PROD)
SOURCE_SERVER = 'az-zan-sws-prod-01.sql.azuresynapse.net'
SOURCE_DATABASE = 'FB_DW'

# Azure Storage
CONTAINER_NAME = 'synapsedata'
STORAGE_PREFIX = f'staging/{TABLE_NAME}'

# Progress folder
PROGRESS_DIR = Path('progress')
PROGRESS_DIR.mkdir(exist_ok=True)
WATERMARK_FILE = PROGRESS_DIR / f'{TABLE_NAME}_watermark.json'

# Slack webhook (same as FactSales)
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')  # Set in .env file

# Milestones (25%, 50%, 75%, 90%)
MILESTONES = [25, 50, 75, 90]
milestones_sent = set()

# ======================== OPTIMIZED RANGES ========================
# UPDATED: Larger ranges (~150-200M rows each) for fewer connections
# Indices 0-5 kept identical to preserve watermark compatibility
OPTIMIZED_RANGES = [
    # Ranges 0-5: KEEP SAME (already completed or in progress)
    (0, 100_000_000),            # Range 0 - DONE (~98M rows)
    (100_000_000, 200_000_000),  # Range 1 - DONE (~66M rows)
    (200_000_000, 300_000_000),  # Range 2 - DONE (~46M rows)
    (300_000_000, 400_000_000),  # Range 3 - DONE (~37M rows)
    (400_000_000, 550_000_000),  # Range 4 - DONE (~37M rows)
    (550_000_000, 700_000_000),  # Range 5 - IN PROGRESS (~58M rows)
    
    # NEW LARGER RANGES (combining old ranges 6-18 into 4 big ranges)
    # Range 6: IDs 700M - 1.2B (~150M rows, ~50 min at 50K/s)
    (700_000_000, 1_200_000_000),
    
    # Range 7: IDs 1.2B - 2.0B (~200M rows, ~67 min at 50K/s)
    (1_200_000_000, 2_000_000_000),
    
    # Range 8: IDs 2.0B - 2.7B (~140M rows, ~47 min at 50K/s)
    (2_000_000_000, 2_700_000_000),
    
    # Range 9: IDs 2.7B - 3.75B (~75M rows, ~25 min at 50K/s)
    (2_700_000_000, 3_750_000_000),
]

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

# ======================== SLACK NOTIFICATIONS ========================
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
                    # secs_left is time to 0%, so scale to time to 20%
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

# ======================== STORAGE ========================
def get_blob_service():
    """Get blob service client"""
    conn_str = os.environ.get('STORAGE_CONNECTION_STRING', '')
    return BlobServiceClient.from_connection_string(conn_str)

def upload_chunk(df, range_idx, chunk_idx):
    """Upload chunk to blob storage"""
    blob_service = get_blob_service()
    container = blob_service.get_container_client(CONTAINER_NAME)
    
    blob_name = f'{STORAGE_PREFIX}_range{range_idx:02d}_chunk_{chunk_idx:05d}.csv'
    
    csv_data = df.to_csv(index=False, header=(chunk_idx == 0))
    container.upload_blob(name=blob_name, data=csv_data, overwrite=True)
    
    return blob_name

def count_existing_chunks(range_idx):
    """Count existing chunks for a range"""
    try:
        blob_service = get_blob_service()
        container = blob_service.get_container_client(CONTAINER_NAME)
        prefix = f'{STORAGE_PREFIX}_range{range_idx:02d}_'
        
        count = 0
        for blob in container.list_blobs(name_starts_with=prefix):
            count += 1
        return count
    except:
        return 0

def delete_range_chunks(range_idx):
    """Delete all chunks for a range (for re-export)"""
    try:
        blob_service = get_blob_service()
        container = blob_service.get_container_client(CONTAINER_NAME)
        prefix = f'{STORAGE_PREFIX}_range{range_idx:02d}_'
        
        deleted = 0
        for blob in container.list_blobs(name_starts_with=prefix):
            container.delete_blob(blob.name)
            deleted += 1
        return deleted
    except Exception as e:
        print(f"  [WARN] Could not delete chunks: {e}")
        return 0

# Target (DEV) - for pre-authentication
TARGET_SERVER = 'synapse-fbnova-dev.sql.azuresynapse.net'
TARGET_DATABASE = 'sqlpoolfbnovadev'

# ======================== DATABASE ========================
# Global credential - authenticate once, reuse for all connections
_cached_credential = None
_cached_token = None
_token_expiry = None

def get_azure_token(force_refresh=False):
    """Get Azure AD token, reusing cached token if still valid"""
    global _cached_credential, _cached_token, _token_expiry
    
    # Check if we have a valid cached token (with 5 min buffer)
    if not force_refresh and _cached_token and _token_expiry:
        from datetime import timezone
        now = datetime.now(timezone.utc)
        if now.timestamp() < _token_expiry - 300:  # 5 min buffer
            return _cached_token
    
    # Need fresh token - use cached credential if available
    if _cached_credential is None:
        print("  🔐 Authenticating with Azure AD (with persistent cache)...")
        # Enable persistent token cache for silent refresh
        cache_options = TokenCachePersistenceOptions(allow_unencrypted_storage=True)
        _cached_credential = InteractiveBrowserCredential(cache_persistence_options=cache_options)
    
    token = _cached_credential.get_token('https://database.windows.net/.default')
    _cached_token = token.token
    _token_expiry = token.expires_on
    
    return _cached_token

def pre_authenticate_both_servers():
    """Pre-authenticate to BOTH source and target servers at startup"""
    print("\n" + "=" * 70)
    print("PRE-AUTHENTICATION (Both servers - login once!)")
    print("=" * 70)
    
    # Get token (will prompt for browser login)
    print("\n📍 Step 1: Authenticating to Azure AD...")
    print("   (This single login works for BOTH source and target!)")
    token = get_azure_token()
    
    # Test connection to SOURCE
    print("\n📍 Step 2: Testing SOURCE connection (PROD)...")
    token_bytes = token.encode('UTF-16-LE')
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SOURCE_SERVER};DATABASE={SOURCE_DATABASE};'
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    conn.close()
    print("   ✅ SOURCE (PROD) - OK")
    
    # Test connection to TARGET
    print("\n📍 Step 3: Testing TARGET connection (DEV)...")
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={TARGET_SERVER};DATABASE={TARGET_DATABASE};'
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    conn.close()
    print("   ✅ TARGET (DEV) - OK")
    
    print("\n" + "=" * 70)
    print("✅ AUTHENTICATION COMPLETE - No more logins needed!")
    print("   You can leave now - export + load will run unattended.")
    print("=" * 70 + "\n")
    
    return True

def get_source_connection():
    """Create connection to source using Azure AD token (reuses auth)"""
    print("  Connecting to source (PROD)...")
    
    token = get_azure_token()
    token_bytes = token.encode('UTF-16-LE')
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SOURCE_SERVER};DATABASE={SOURCE_DATABASE};'
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    print("  [OK] Connected to source")
    return conn

def get_target_connection():
    """Create connection to target using Azure AD token (reuses auth)"""
    print("  Connecting to target (DEV)...")
    
    token = get_azure_token()
    token_bytes = token.encode('UTF-16-LE')
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={TARGET_SERVER};DATABASE={TARGET_DATABASE};'
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    print("  [OK] Connected to target")
    return conn

def get_table_stats(conn):
    """Get table statistics"""
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]')
    total_rows = cursor.fetchone()[0]
    
    cursor.execute(f'SELECT MIN([{PK_COLUMN}]), MAX([{PK_COLUMN}]) FROM [{SCHEMA}].[{TABLE_NAME}]')
    min_id, max_id = cursor.fetchone()
    
    cursor.close()
    return total_rows, min_id, max_id

def get_range_row_count(conn, start_id, end_id):
    """Get row count for a range"""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]
        WHERE [{PK_COLUMN}] >= {start_id} AND [{PK_COLUMN}] < {end_id}
    """)
    count = cursor.fetchone()[0]
    cursor.close()
    return count

# ======================== WATERMARK ========================
def load_watermark():
    """Load progress from watermark file"""
    if WATERMARK_FILE.exists():
        with open(WATERMARK_FILE) as f:
            return json.load(f)
    return {
        'status': 'not_started',
        'ranges_completed': [],
        'total_rows_exported': 0,
        'start_time': None,
        'snapshot_max_id': None
    }

def save_watermark(data):
    """Save progress to watermark file"""
    with open(WATERMARK_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# ======================== EXPORT ========================
def export_range(conn, range_idx, start_id, end_id, watermark):
    """Export a single ID range using streaming"""
    print(f"\n  ==================================================")
    print(f"  RANGE {range_idx}: IDs {start_id:,} to {end_id:,}")
    print(f"  ==================================================")
    
    # Get row count for this range
    rows_in_range = get_range_row_count(conn, start_id, end_id)
    print(f"  Rows in range: {rows_in_range:,}")
    
    if rows_in_range == 0:
        print(f"  [SKIP] Empty range")
        return 0, 0
    
    # Delete any existing chunks for this range (ensure clean export)
    deleted = delete_range_chunks(range_idx)
    if deleted > 0:
        print(f"  Deleted {deleted} existing chunks")
    
    # Build query - no ORDER BY for speed
    query = f"""
        SELECT * FROM [{SCHEMA}].[{TABLE_NAME}]
        WHERE [{PK_COLUMN}] >= {start_id} AND [{PK_COLUMN}] < {end_id}
    """
    
    # Stream export
    chunk_idx = 0
    total_rows = 0
    start_time = time.time()
    last_update = start_time
    
    expected_chunks = (rows_in_range // CHUNK_SIZE) + 1
    
    for chunk in pd.read_sql(query, conn, chunksize=CHUNK_SIZE):
        upload_chunk(chunk, range_idx, chunk_idx)
        
        chunk_idx += 1
        total_rows += len(chunk)
        
        # Progress update every 30 seconds
        now = time.time()
        if now - last_update > 30:
            elapsed = now - start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            pct = (total_rows / rows_in_range * 100) if rows_in_range > 0 else 0
            eta = (rows_in_range - total_rows) / rate if rate > 0 else 0
            
            print(f"  Range {range_idx}: {total_rows:,} rows | {chunk_idx}/{expected_chunks} chunks | {rate/1000:.1f}K rows/s")
            last_update = now
        
        # Memory cleanup
        del chunk
        gc.collect()
    
    elapsed = time.time() - start_time
    rate = total_rows / elapsed if elapsed > 0 else 0
    
    print(f"  [OK] Range {range_idx} complete: {total_rows:,} rows in {elapsed/60:.1f} min ({rate/1000:.1f}K rows/s)")
    
    return total_rows, chunk_idx

# ======================== MAIN ========================
def main():
    print("=" * 70)
    print(f"FB NOVA - {TABLE_NAME} CHUNKED STREAMING (Watermark-based)")
    print("=" * 70)
    print(f"  Strategy: Split into ID ranges to avoid session timeouts")
    print(f"  Each range gets a fresh connection")
    print(f"  Watermark saved after each range for resume capability")
    print("=" * 70)
    
    # PRE-AUTHENTICATE TO BOTH SERVERS AT STARTUP
    # This way user can leave and export+load runs unattended
    pre_authenticate_both_servers()
    
    # Load watermark
    watermark = load_watermark()
    print(f"\nWatermark file: {WATERMARK_FILE}")
    print(f"Status: {watermark['status']}")
    
    if watermark['ranges_completed']:
        print(f"Ranges completed: {watermark['ranges_completed']}")
        print(f"Rows exported: {watermark['total_rows_exported']:,}")
    
    # Get initial connection for stats
    conn = get_source_connection()
    total_rows, min_id, max_id = get_table_stats(conn)
    
    print(f"\n  Min ID: {min_id:,}")
    print(f"  Max ID: {max_id:,}")
    print(f"  Total rows: {total_rows:,}")
    
    # Record snapshot max ID for delta sync later
    if watermark['snapshot_max_id'] is None:
        watermark['snapshot_max_id'] = max_id
        print(f"\n  ⚠️  LIVE TABLE DETECTED!")
        print(f"  Recording snapshot watermark: {max_id:,}")
        print(f"  After full migration, run delta sync for IDs > {max_id:,}")
    
    conn.close()
    
    # Show ranges
    print(f"\nUsing OPTIMIZED RANGES based on actual data distribution!")
    print(f"Total ranges: {len(OPTIMIZED_RANGES)} (data-driven)")
    
    # Start time
    if watermark['start_time'] is None:
        watermark['start_time'] = datetime.now().isoformat()
    
    watermark['status'] = 'in_progress'
    save_watermark(watermark)
    
    # Send start notification
    ranges_done = len(watermark['ranges_completed'])
    ranges_total = len(OPTIMIZED_RANGES)
    if ranges_done == 0:
        send_slack(f"🚀 *{TABLE_NAME} Export Started*\n• Total rows: {total_rows:,}\n• Ranges: {ranges_total}\n• Est. time: ~28 hours")
    else:
        send_slack(f"🔄 *{TABLE_NAME} Export Resumed*\n• Ranges done: {ranges_done}/{ranges_total}\n• Rows exported: {watermark['total_rows_exported']:,}")
    
    # Track for hourly notifications
    last_hourly = time.time()
    export_start = time.time()
    global milestones_sent
    
    # Process each range
    for range_idx, (start_id, end_id) in enumerate(OPTIMIZED_RANGES):
        # Skip completed ranges
        if range_idx in watermark['ranges_completed']:
            print(f"\n[SKIP] Range {range_idx} already completed")
            continue
        
        print(f"\n{'='*70}")
        print(f"PROCESSING RANGE {range_idx + 1}/{ranges_total}")
        print(f"{'='*70}")
        
        # Create fresh connection for each range
        print(f"\n  Creating fresh connection for range {range_idx}...")
        conn = get_source_connection()
        
        range_start_time = time.time()
        
        try:
            rows, chunks = export_range(conn, range_idx, start_id, end_id, watermark)
            
            # Update watermark
            watermark['ranges_completed'].append(range_idx)
            watermark['total_rows_exported'] += rows
            save_watermark(watermark)
            
            # Calculate progress stats
            ranges_done = len(watermark['ranges_completed'])
            pct_done = ranges_done / ranges_total * 100
            elapsed_total = time.time() - export_start
            rate = watermark['total_rows_exported'] / elapsed_total if elapsed_total > 0 else 0
            
            # Estimate remaining time
            rows_remaining = total_rows - watermark['total_rows_exported']
            eta_seconds = rows_remaining / rate if rate > 0 else 0
            
            # Milestone notifications (25%, 50%, 75%, 90%)
            for m in MILESTONES:
                if pct_done >= m and m not in milestones_sent:
                    milestones_sent.add(m)
                    battery = get_battery_status()
                    msg = (
                        f"📊 *{TABLE_NAME} Export {m}% Complete*\n"
                        f"• Ranges: {ranges_done}/{ranges_total}\n"
                        f"• Rows: {watermark['total_rows_exported']:,} / {total_rows:,}\n"
                        f"• Speed: {rate/1000:.1f}K rows/s\n"
                        f"• Elapsed: {format_time(elapsed_total)}\n"
                        f"• ETA: {format_time(eta_seconds)}"
                    )
                    if battery:
                        msg += f"\n• {battery}"
                    send_slack(msg)
            
            # Hourly notification with detailed stats
            if time.time() - last_hourly > 3600:
                battery = get_battery_status()
                msg = (
                    f"⏰ *{TABLE_NAME} Export Hourly Update*\n"
                    f"• Overall: {pct_done:.1f}% ({ranges_done}/{ranges_total} ranges)\n"
                    f"• Rows: {watermark['total_rows_exported']:,} / {total_rows:,}\n"
                    f"• Speed: {rate/1000:.1f}K rows/s\n"
                    f"• Elapsed: {format_time(elapsed_total)}\n"
                    f"• ETA: {format_time(eta_seconds)}"
                )
                if battery:
                    msg += f"\n• {battery}"
                send_slack(msg)
                last_hourly = time.time()
            
        except Exception as e:
            print(f"  [ERROR] Range {range_idx} failed: {e}")
            elapsed_total = time.time() - export_start
            send_slack(f"❌ *{TABLE_NAME} Export Error*\n• Range {range_idx} failed\n• Elapsed: {format_time(elapsed_total)}\n• Error: {str(e)[:200]}")
            raise
        
        finally:
            conn.close()
        
        # Memory cleanup between ranges
        gc.collect()
    
    # Export complete
    elapsed_total = time.time() - export_start
    watermark['status'] = 'export_complete'
    save_watermark(watermark)
    
    print(f"\n{'='*70}")
    print(f"EXPORT COMPLETE!")
    print(f"{'='*70}")
    print(f"  Total rows: {watermark['total_rows_exported']:,}")
    print(f"  Total time: {elapsed_total/3600:.1f} hours")
    print(f"  Avg speed: {watermark['total_rows_exported']/elapsed_total/1000:.1f}K rows/s")
    print(f"{'='*70}")
    
    avg_speed = watermark['total_rows_exported'] / elapsed_total if elapsed_total > 0 else 0
    send_slack(
        f"✅ *{TABLE_NAME} Export Complete!*\n"
        f"• Rows: {watermark['total_rows_exported']:,}\n"
        f"• Time: {format_time(elapsed_total)}\n"
        f"• Avg speed: {avg_speed/1000:.1f}K rows/s\n"
        f"• Auto-starting load..."
    )
    
    print(f"\n{'='*70}")
    print(f"DELTA SYNC INFO (for rows added during migration):")
    print(f"{'='*70}")
    print(f"   Snapshot max ID: {watermark['snapshot_max_id']:,}")
    print(f"   Run this query to get new rows:")
    print(f"   SELECT * FROM {SCHEMA}.{TABLE_NAME} WHERE {PK_COLUMN} > {watermark['snapshot_max_id']}")
    
    # Auto-start load - import and call directly to share authentication
    print(f"\n{'='*70}")
    print(f"AUTO-STARTING LOAD TO TARGET...")
    print(f"{'='*70}")
    print(f"  Using pre-authenticated credentials (no new login needed!)")
    
    try:
        # Import load script and inject our cached credential
        import load_facttender
        
        # Share our cached credential with the load script
        load_facttender._cached_credential = _cached_credential
        load_facttender._cached_token = _cached_token
        load_facttender._token_expiry = _token_expiry
        
        print(f"\n✅ Credentials shared - starting load...")
        load_facttender.main()
        print("\n✅ Load completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Load failed: {e}")
        print("Run manually: python load_facttender.py")

if __name__ == '__main__':
    main()

