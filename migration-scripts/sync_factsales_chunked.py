"""
FB Nova - FactSales CHUNKED STREAMING with Watermark
=====================================================
Splits the 1.8B row export into ID-range chunks to avoid session timeouts.

How it works:
1. Query MIN/MAX FactSalesID to determine full range
2. Split into chunks (e.g., 200M rows each = ~4-6 hours per chunk)
3. Stream each chunk with a fresh connection
4. Save watermark after each chunk completes
5. Resume from last watermark if interrupted

Benefits:
- Each chunk completes within session timeout limits
- Fresh connection for each chunk (no 48-hour connection)
- Watermark-based resume capability
- Still uses streaming (no ORDER BY = fast!)
"""

import pyodbc
import pandas as pd
import numpy as np
import os
import json
import sys
import time
import struct
import warnings
import gc
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from azure.identity import InteractiveBrowserCredential

warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# =============================================================================
# CONFIGURATION
# =============================================================================

if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()

# Servers
SOURCE_SERVER = "az-zan-sws-prod-01.sql.azuresynapse.net"
SOURCE_DATABASE = "FB_DW"
TARGET_SERVER = "synapse-fbnova-dev.sql.azuresynapse.net"
TARGET_DATABASE = "sqlpoolfbnovadev"

# Storage
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
STORAGE_ACCOUNT = "stsynfbnovadev"
CONTAINER_NAME = "synapsedata"
STORAGE_KEY = os.getenv("STORAGE_KEY")

# Chunking configuration
ROWS_PER_CHUNK = 250000          # CSV chunk size (250K rows per file)
# OPTIMIZED RANGES based on actual data distribution!
# Instead of fixed 200M ID ranges, we use data-driven ranges
# that skip empty areas and split large chunks
USE_OPTIMIZED_RANGES = True

# Fallback fixed range size (if USE_OPTIMIZED_RANGES = False)
ID_RANGE_SIZE = 200_000_000

# Data-driven ranges based on actual distribution analysis:
# - Skips empty ranges (1,2,3 had no data)
# - Splits large ranges (100M+ rows) for safety
# - Targets ~50M rows per range for balanced workload
OPTIMIZED_RANGES = [
    # Range ID, Start ID, End ID, Approx Rows
    (0,  0,            200_000_000,   51_600_000),   # 51.6M
    # Ranges 1,2,3 SKIPPED - no data in 200M-800M!
    (1,  800_000_000,  1_200_000_000, 44_893_170),   # 8.1M + 36.8M combined
    (2,  1_200_000_000, 1_400_000_000, 60_351_119),  # 60.4M
    (3,  1_400_000_000, 1_600_000_000, 75_558_747),  # 75.6M - bit large but OK
    (4,  1_600_000_000, 1_800_000_000, 83_179_407),  # 83.2M
    (5,  1_800_000_000, 2_000_000_000, 78_828_965),  # 78.8M
    (6,  2_000_000_000, 2_200_000_000, 76_531_042),  # 76.5M
    (7,  2_200_000_000, 2_400_000_000, 70_545_302),  # 70.5M
    (8,  2_400_000_000, 2_600_000_000, 65_063_627),  # 65.1M
    (9,  2_600_000_000, 2_800_000_000, 61_246_643),  # 61.2M
    (10, 2_800_000_000, 3_000_000_000, 58_346_226),  # 58.3M
    (11, 3_000_000_000, 3_200_000_000, 58_908_593),  # 58.9M
    (12, 3_200_000_000, 3_400_000_000, 58_153_172),  # 58.2M
    (13, 3_400_000_000, 3_600_000_000, 57_677_298),  # 57.7M
    (14, 3_600_000_000, 3_800_000_000, 55_164_139),  # 55.2M
    (15, 3_800_000_000, 4_000_000_000, 54_459_454),  # 54.5M
    (16, 4_000_000_000, 4_200_000_000, 43_061_967),  # 43.1M
    (17, 4_200_000_000, 4_400_000_000, 36_146_139),  # 36.1M
    (18, 4_400_000_000, 4_600_000_000, 34_814_721),  # 34.8M
    (19, 4_600_000_000, 4_800_000_000, 31_364_600),  # 31.4M
    (20, 4_800_000_000, 5_000_000_000, 28_957_368),  # 29.0M
    (21, 5_000_000_000, 5_400_000_000, 39_798_516),  # 22.5M + 17.3M combined
    (22, 5_400_000_000, 5_800_000_000, 28_022_876),  # 16.0M + 12.0M combined
    (23, 5_800_000_000, 6_400_000_000, 30_389_449),  # Small ranges combined
    (24, 6_400_000_000, 7_200_000_000, 9_874_273),   # Very small ranges combined
    (25, 7_200_000_000, 7_400_000_000, 74_122_307),  # 74.1M - back to big data!
    (26, 7_400_000_000, 7_500_000_000, 50_030_442),  # Split 100M range in half
    (27, 7_500_000_000, 7_600_000_000, 50_030_442),  # Split 100M range in half
    (28, 7_600_000_000, 7_700_000_000, 49_826_957),  # Split 99.7M range
    (29, 7_700_000_000, 7_800_000_000, 49_826_957),  # Split 99.7M range
    (30, 7_800_000_000, 7_900_000_000, 51_681_193),  # Split 103M range
    (31, 7_900_000_000, 8_000_000_000, 51_681_193),  # Split 103M range
    (32, 8_000_000_000, 8_100_000_000, 43_744_502),  # Split 87.5M range
    (33, 8_100_000_000, 8_200_000_000, 43_744_502),  # Split 87.5M range
    (34, 8_200_000_000, 8_400_000_000, 61_419_191),  # 61.4M
    (35, 8_400_000_000, 8_600_000_000, 25_164_493),  # 25.2M
    (36, 8_600_000_000, 8_850_000_000, 11_285_161),  # 10.9M + 0.4M combined
]
MAX_RETRIES = 3
RETRY_DELAY = 60

# Progress/Watermark file
WATERMARK_FILE = "progress/FactSales_watermark.json"

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  # Set in .env file

# Caching
_cached_credential = None
_cached_token_struct = None

# =============================================================================
# SLACK NOTIFICATIONS
# =============================================================================

def send_slack(message, emoji=":hourglass:"):
    """Send Slack notification"""
    import requests
    try:
        payload = {"text": f"{emoji} {message}"}
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    except:
        pass

# Tracking for notifications
_milestones_sent = set()  # Track 25%, 50%, 75%, 100%
_last_hourly_notification = 0  # Track last hour notified

def send_milestone_notification(pct, rows_done, total_rows, speed, eta_seconds, start_time):
    """Send milestone notifications at 25%, 50%, 75%, 100%"""
    global _milestones_sent
    
    milestones = [25, 50, 75, 100]
    for milestone in milestones:
        if pct >= milestone and milestone not in _milestones_sent:
            _milestones_sent.add(milestone)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            elapsed_str = format_eta(elapsed)
            eta_str = format_eta(eta_seconds)
            speed_str = f"{speed/1000:.1f}K" if speed >= 1000 else f"{speed:.0f}"
            
            emoji = ":trophy:" if milestone == 100 else ":chart_with_upwards_trend:"
            
            send_slack(
                f"*{milestone}% Milestone Reached!*\n"
                f"{make_progress_bar(pct)} {pct:.1f}%\n"
                f"• Rows: {rows_done:,} / {total_rows:,}\n"
                f"• Speed: {speed_str} rows/s\n"
                f"• Elapsed: {elapsed_str}\n"
                f"• ETA: {eta_str}",
                emoji
            )
            break  # Only send one milestone at a time

def send_hourly_notification(pct, rows_done, total_rows, speed, eta_seconds, start_time):
    """Send hourly progress updates"""
    global _last_hourly_notification
    
    elapsed = (datetime.now() - start_time).total_seconds()
    current_hour = int(elapsed / 3600)
    
    if current_hour > _last_hourly_notification:
        _last_hourly_notification = current_hour
        
        elapsed_str = format_eta(elapsed)
        eta_str = format_eta(eta_seconds)
        speed_str = f"{speed/1000:.1f}K" if speed >= 1000 else f"{speed:.0f}"
        
        send_slack(
            f"*:alarm_clock: Hourly Update: FactSales Progress*\n"
            f"{make_progress_bar(pct)} {pct:.1f}%\n"
            f"• Phase: EXPORT\n"
            f"• Rows: {rows_done:,} / {total_rows:,}\n"
            f"• Speed: {speed_str} rows/s\n"
            f"• Elapsed: {elapsed_str}\n"
            f"• ETA: {eta_str}",
            ":alarm_clock:"
        )

def make_progress_bar(pct, width=20):
    filled = int(width * min(pct, 100) / 100)
    return "[" + "█" * filled + "░" * (width - filled) + "]"

def format_eta(seconds):
    if seconds <= 0:
        return "done"
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 24:
        days = hours // 24
        hours = hours % 24
        return f"{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# =============================================================================
# AZURE AUTHENTICATION
# =============================================================================

def get_azure_token():
    """Get Azure AD token for SQL authentication"""
    global _cached_credential, _cached_token_struct
    
    if _cached_credential is None:
        _cached_credential = InteractiveBrowserCredential()
    
    token = _cached_credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("UTF-16-LE")
    _cached_token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    return _cached_token_struct

def connect_source():
    """Connect to source Synapse"""
    print("  Connecting to source (PROD)...")
    token_struct = get_azure_token()
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SOURCE_SERVER};"
        f"DATABASE={SOURCE_DATABASE};"
    )
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    conn.timeout = 0  # No timeout
    print("  [OK] Connected to source")
    return conn

# =============================================================================
# BLOB STORAGE
# =============================================================================

def get_blob_client():
    """Get blob service client"""
    return BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

def upload_chunk(chunk_filename, range_id, chunk_num):
    """Upload chunk to blob storage"""
    blob_service = get_blob_client()
    blob_name = f"staging/FactSales_range{range_id:02d}_chunk_{chunk_num:05d}.csv"
    blob_client = blob_service.get_blob_client(CONTAINER_NAME, blob_name)
    
    with open(chunk_filename, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

def count_range_chunks(range_id):
    """Count how many chunks exist for a range"""
    try:
        blob_service = get_blob_client()
        container = blob_service.get_container_client(CONTAINER_NAME)
        prefix = f"staging/FactSales_range{range_id:02d}_chunk_"
        return len(list(container.list_blobs(name_starts_with=prefix)))
    except:
        return 0

# =============================================================================
# WATERMARK MANAGEMENT
# =============================================================================

def load_watermark():
    """Load watermark from file"""
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, 'r') as f:
            return json.load(f)
    return {
        "status": "not_started",
        "min_id": None,
        "max_id": None,
        "current_watermark": None,
        "ranges_completed": [],
        "total_rows_exported": 0,
        "started_at": None
    }

def save_watermark(watermark):
    """Save watermark to file"""
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    watermark["last_updated"] = datetime.now().isoformat()
    with open(WATERMARK_FILE, 'w') as f:
        json.dump(watermark, f, indent=2)

# =============================================================================
# DATA EXPORT
# =============================================================================

def fix_columns(df):
    """Fix problematic values in dataframe"""
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        elif df[col].dtype == 'object':
            df[col] = df[col].replace(['nan', 'None', 'NULL', 'null'], np.nan)
    return df

def export_range(conn, range_id, start_id, end_id, watermark):
    """
    Export a single ID range using streaming.
    Fresh connection, no ORDER BY needed within range.
    """
    print(f"\n  {'='*50}")
    print(f"  RANGE {range_id}: IDs {start_id:,} to {end_id:,}")
    print(f"  {'='*50}")
    
    # First, check if this range has any data (quick count)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COUNT(*) FROM [dwh].[FactSales]
        WHERE [FactSalesID] >= {start_id} AND [FactSalesID] < {end_id}
    """)
    range_row_count = cursor.fetchone()[0]
    cursor.close()
    
    if range_row_count == 0:
        print(f"  [SKIP] Range {range_id} is EMPTY - no data in this ID range")
        return 0, 0  # Return 0 rows, 0 chunks
    
    print(f"  Rows in range: {range_row_count:,}")
    
    # Query for this range (no ORDER BY = fast streaming!)
    query = f"""
        SELECT * FROM [dwh].[FactSales]
        WHERE [FactSalesID] >= {start_id} AND [FactSalesID] < {end_id}
    """
    
    chunk_num = 0
    rows_in_range = 0
    range_start_time = datetime.now()
    
    try:
        for chunk in pd.read_sql(query, conn, chunksize=ROWS_PER_CHUNK):
            chunk_num += 1
            
            # Process chunk
            chunk = fix_columns(chunk)
            chunk_len = len(chunk)
            
            # Save to CSV
            chunk_filename = f"FactSales_temp_chunk.csv"
            chunk.to_csv(chunk_filename, index=False, sep='|', encoding='utf-8',
                        na_rep='', float_format='%.10f', header=False)
            
            # Upload to blob
            upload_chunk(chunk_filename, range_id, chunk_num)
            os.remove(chunk_filename)
            
            rows_in_range += chunk_len
            
            # Calculate progress
            elapsed = (datetime.now() - range_start_time).total_seconds()
            speed = rows_in_range / elapsed if elapsed > 0 else 0
            speed_str = f"{speed/1000:.1f}K" if speed >= 1000 else f"{speed:.0f}"
            
            # Progress display
            sys.stdout.write(f"\r  Range {range_id}: {rows_in_range:,} rows | {chunk_num} chunks | {speed_str} rows/s    ")
            sys.stdout.flush()
            
            # Memory cleanup
            del chunk
            if chunk_num % 50 == 0:
                gc.collect()
            
            # Check if this was the last chunk
            if chunk_len < ROWS_PER_CHUNK:
                break
                
    except Exception as e:
        print(f"\n  [ERROR] Range {range_id} failed: {e}")
        raise
    
    elapsed = (datetime.now() - range_start_time).total_seconds()
    speed = rows_in_range / elapsed if elapsed > 0 else 0
    
    print(f"\n  [OK] Range {range_id} complete: {rows_in_range:,} rows in {elapsed/60:.1f} min ({speed/1000:.1f}K rows/s)")
    
    return rows_in_range, chunk_num

def main():
    print("=" * 70)
    print("FB NOVA - FactSales CHUNKED STREAMING (Watermark-based)")
    print("=" * 70)
    print("  Strategy: Split into ID ranges to avoid session timeouts")
    print("  Each range gets a fresh connection (~4-6 hours per range)")
    print("  Watermark saved after each range for resume capability")
    print("=" * 70)
    
    # Load watermark
    watermark = load_watermark()
    print(f"\nWatermark file: {WATERMARK_FILE}")
    print(f"Status: {watermark['status']}")
    
    # Connect to get ID range (if not already known)
    if watermark['min_id'] is None or watermark['max_id'] is None:
        print("\nGetting FactSalesID range...")
        conn = connect_source()
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(FactSalesID), MAX(FactSalesID), COUNT(*) FROM [dwh].[FactSales]")
        min_id, max_id, total_rows = cursor.fetchone()
        cursor.close()
        conn.close()
        
        watermark['min_id'] = min_id
        watermark['max_id'] = max_id
        watermark['total_rows'] = total_rows
        watermark['status'] = 'initialized'
        watermark['snapshot_max_id'] = max_id  # Record for delta sync later!
        watermark['snapshot_time'] = datetime.now().isoformat()
        save_watermark(watermark)
        
        print(f"\n  ⚠️  LIVE TABLE DETECTED!")
        print(f"  Recording snapshot watermark: {max_id:,}")
        print(f"  After full migration, run delta sync for IDs > {max_id:,}")
        
        print(f"  Min ID: {min_id:,}")
        print(f"  Max ID: {max_id:,}")
        print(f"  Total rows: {total_rows:,}")
    else:
        min_id = watermark['min_id']
        max_id = watermark['max_id']
        total_rows = watermark.get('total_rows', 1_850_000_000)
        print(f"\nResuming from watermark:")
        print(f"  Min ID: {min_id:,}")
        print(f"  Max ID: {max_id:,}")
        print(f"  Ranges completed: {watermark['ranges_completed']}")
    
    # Calculate ranges
    if USE_OPTIMIZED_RANGES:
        # Use data-driven optimized ranges
        ranges = [(r[0], r[1], r[2]) for r in OPTIMIZED_RANGES]
        print(f"\nUsing OPTIMIZED RANGES based on actual data distribution!")
        print(f"Total ranges: {len(ranges)} (data-driven, skips empty areas)")
        print(f"Benefits: No empty ranges, balanced workload (~50M rows each)")
        
        # Calculate estimated time based on actual row counts
        total_est_rows = sum(r[3] for r in OPTIMIZED_RANGES)
        avg_rows = total_est_rows / len(ranges)
        est_time_per_range = avg_rows / 8000 / 3600  # at 8K rows/sec
        print(f"Avg rows per range: {avg_rows/1_000_000:.1f}M")
        print(f"Est. time per range: {est_time_per_range:.1f} hours")
        print(f"Total estimated time: {len(ranges) * est_time_per_range:.0f} hours ({len(ranges) * est_time_per_range / 24:.1f} days)")
    else:
        # Use fixed-size ranges (fallback)
        ranges = []
        current_start = min_id
        range_id = 0
        while current_start < max_id:
            range_end = min(current_start + ID_RANGE_SIZE, max_id + 1)
            ranges.append((range_id, current_start, range_end))
            current_start = range_end
            range_id += 1
        
        print(f"\nTotal ranges: {len(ranges)} (each ~{ID_RANGE_SIZE/1_000_000:.0f}M IDs)")
        print(f"Estimated time per range: 4-6 hours")
        print(f"Total estimated time: {len(ranges) * 5} hours ({len(ranges) * 5 / 24:.1f} days)")
    
    # Record start time
    if watermark.get('started_at') is None:
        watermark['started_at'] = datetime.now().isoformat()
        watermark['status'] = 'exporting'
        save_watermark(watermark)
    
    overall_start = datetime.fromisoformat(watermark['started_at'])
    
    # Send start notification
    send_slack(
        f"*FactSales Chunked Export Starting*\n"
        f"• Total rows: {total_rows:,}\n"
        f"• Ranges: {len(ranges)}\n"
        f"• Already completed: {len(watermark['ranges_completed'])} ranges",
        ":rocket:"
    )
    
    # Process each range
    total_exported = watermark.get('total_rows_exported', 0)
    
    for range_id, start_id, end_id in ranges:
        # Skip already completed ranges
        if range_id in watermark['ranges_completed']:
            print(f"\n  [SKIP] Range {range_id} already completed")
            continue
        
        print(f"\n{'='*70}")
        print(f"PROCESSING RANGE {range_id + 1}/{len(ranges)}")
        print(f"{'='*70}")
        
        # Fresh connection for each range!
        retries = 0
        while retries < MAX_RETRIES:
            try:
                print(f"\n  Creating fresh connection for range {range_id}...")
                conn = connect_source()
                
                rows, chunks = export_range(conn, range_id, start_id, end_id, watermark)
                
                conn.close()
                
                # Update watermark
                total_exported += rows
                watermark['ranges_completed'].append(range_id)
                watermark['current_watermark'] = end_id
                watermark['total_rows_exported'] = total_exported
                save_watermark(watermark)
                
                # Calculate overall progress
                overall_pct = (total_exported / total_rows) * 100
                elapsed_total = (datetime.now() - overall_start).total_seconds()
                overall_speed = total_exported / elapsed_total if elapsed_total > 0 else 0
                remaining_rows = total_rows - total_exported
                eta_seconds = remaining_rows / overall_speed if overall_speed > 0 else 0
                
                # Send range completion notification (quieter - not every range)
                if range_id % 5 == 0 or overall_pct >= 90:  # Every 5th range or near end
                    send_slack(
                        f"*Range {range_id + 1}/{len(ranges)} Complete*\n"
                        f"{make_progress_bar(overall_pct)} {overall_pct:.1f}%\n"
                        f"• Rows exported: {total_exported:,}/{total_rows:,}\n"
                        f"• Speed: {overall_speed/1000:.1f}K rows/s\n"
                        f"• ETA: {format_eta(eta_seconds)}",
                        ":white_check_mark:"
                    )
                
                # Check for milestone notifications (25%, 50%, 75%, 100%)
                send_milestone_notification(overall_pct, total_exported, total_rows, 
                                           overall_speed, eta_seconds, overall_start)
                
                # Check for hourly notifications
                send_hourly_notification(overall_pct, total_exported, total_rows,
                                        overall_speed, eta_seconds, overall_start)
                
                break  # Success, move to next range
                
            except Exception as e:
                retries += 1
                print(f"\n  [ERROR] Range {range_id} attempt {retries} failed: {e}")
                
                if retries < MAX_RETRIES:
                    print(f"  Waiting {RETRY_DELAY}s before retry...")
                    send_slack(
                        f"*Range {range_id} Failed - Retrying*\n"
                        f"• Error: {str(e)[:100]}\n"
                        f"• Attempt: {retries}/{MAX_RETRIES}",
                        ":warning:"
                    )
                    time.sleep(RETRY_DELAY)
                else:
                    send_slack(
                        f"*Range {range_id} Failed - Max Retries*\n"
                        f"• Error: {str(e)[:100]}\n"
                        f"• Run script again to resume from this range",
                        ":x:"
                    )
                    watermark['status'] = 'error'
                    watermark['error'] = str(e)[:500]
                    save_watermark(watermark)
                    raise
    
    # All ranges complete!
    watermark['status'] = 'export_complete'
    watermark['export_completed_at'] = datetime.now().isoformat()
    save_watermark(watermark)
    
    elapsed_total = (datetime.now() - overall_start).total_seconds()
    final_speed = total_exported / elapsed_total if elapsed_total > 0 else 0
    
    print("\n" + "=" * 70)
    print("EXPORT COMPLETE!")
    print("=" * 70)
    print(f"  Total rows: {total_exported:,}")
    print(f"  Total time: {elapsed_total/3600:.1f} hours")
    print(f"  Avg speed: {final_speed/1000:.1f}K rows/s")
    print("=" * 70)
    
    # Get snapshot info for delta sync reminder
    snapshot_max_id = watermark.get('snapshot_max_id', max_id)
    
    send_slack(
        f"*:tada: FactSales Export COMPLETE!*\n"
        f"• Total rows: {total_exported:,}\n"
        f"• Time: {elapsed_total/3600:.1f} hours\n"
        f"• Avg speed: {final_speed/1000:.1f}K rows/s\n"
        f"• Snapshot max ID: {snapshot_max_id:,}\n"
        f"• Next: Run COPY INTO, then DELTA SYNC for new rows",
        ":white_check_mark:"
    )
    
    print("\n" + "=" * 70)
    print("NEXT STEPS:")
    print("=" * 70)
    print(f"1. Load data to target: python load_to_target.py")
    print(f"")
    print(f"2. DELTA SYNC (for rows added during migration):")
    print(f"   Snapshot max ID: {snapshot_max_id:,}")
    print(f"   Run this query to get new rows:")
    print(f"   SELECT * FROM dwh.FactSales WHERE FactSalesID > {snapshot_max_id}")
    print("=" * 70)

if __name__ == "__main__":
    main()

