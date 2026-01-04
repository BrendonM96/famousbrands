"""
Move all 3 tables to stg_mig for consistency
- FactSales: stg_dwh -> stg_mig (1.86B rows)
- FactTender: stg_dwh -> stg_mig (875M rows)
- FactSalesSummary: already in stg_mig, drop stg_dwh copy
"""
import pyodbc
import struct
import time
from datetime import datetime
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions

print('=' * 70)
print('MOVE ALL TABLES TO stg_mig')
print('=' * 70)

# Auth with persistent cache
print('\nAuthenticating...')
cache_options = TokenCachePersistenceOptions(allow_unencrypted_storage=True)
credential = InteractiveBrowserCredential(cache_persistence_options=cache_options, timeout=600)
token = credential.get_token('https://database.windows.net/.default')

token_bytes = token.token.encode('UTF-16-LE')
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

conn_str = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=synapse-fbnova-dev.sql.azuresynapse.net;DATABASE=sqlpoolfbnovadev;'
conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
conn.autocommit = True
cursor = conn.cursor()

print('[OK] Connected to synapse-fbnova-dev.sql.azuresynapse.net/sqlpoolfbnovadev')

def table_exists(schema, table):
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    """)
    return cursor.fetchone()[0] > 0

def get_row_count(schema, table):
    cursor.execute(f'SELECT COUNT(*) FROM [{schema}].[{table}]')
    return cursor.fetchone()[0]

def move_table(table_name, source_schema, target_schema, dist_column):
    """Move table using CTAS"""
    print(f'\n{"="*70}')
    print(f'Moving {table_name}: {source_schema} -> {target_schema}')
    print('='*70)
    
    # Check source
    if not table_exists(source_schema, table_name):
        print(f'  [SKIP] Source [{source_schema}].[{table_name}] does not exist')
        
        # Check if already in target
        if table_exists(target_schema, table_name):
            count = get_row_count(target_schema, table_name)
            print(f'  [OK] Already exists in [{target_schema}].[{table_name}]: {count:,} rows')
        return True
    
    source_count = get_row_count(source_schema, table_name)
    print(f'  Source [{source_schema}].[{table_name}]: {source_count:,} rows')
    
    # Check target
    if table_exists(target_schema, table_name):
        target_count = get_row_count(target_schema, table_name)
        print(f'  Target [{target_schema}].[{table_name}] exists: {target_count:,} rows')
        
        if target_count == source_count:
            print(f'  [OK] Same row count - already moved!')
            print(f'  Dropping source [{source_schema}].[{table_name}]...')
            cursor.execute(f'DROP TABLE [{source_schema}].[{table_name}]')
            print(f'  [OK] Dropped source')
            return True
        else:
            print(f'  Dropping existing target (different row count)...')
            cursor.execute(f'DROP TABLE [{target_schema}].[{table_name}]')
    
    # CTAS to move data
    print(f'  Creating [{target_schema}].[{table_name}] via CTAS...')
    print(f'  Moving {source_count:,} rows...')
    start_time = time.time()
    
    cursor.execute(f"""
        CREATE TABLE [{target_schema}].[{table_name}]
        WITH (
            DISTRIBUTION = HASH([{dist_column}]),
            CLUSTERED COLUMNSTORE INDEX
        )
        AS SELECT * FROM [{source_schema}].[{table_name}]
    """)
    
    elapsed = time.time() - start_time
    mins = elapsed / 60
    print(f'  [OK] Created in {mins:.1f} minutes ({source_count/elapsed:,.0f} rows/sec)')
    
    # Verify
    new_count = get_row_count(target_schema, table_name)
    print(f'  Verifying: {new_count:,} rows')
    
    if new_count == source_count:
        print(f'  [OK] Verified! Dropping source...')
        cursor.execute(f'DROP TABLE [{source_schema}].[{table_name}]')
        print(f'  [OK] Source dropped')
        return True
    else:
        print(f'  [ERROR] Row count mismatch! Source kept.')
        return False

# ============================================================
# MAIN
# ============================================================
total_start = time.time()

# 1. FactSalesSummary - drop stg_dwh copy (already in stg_mig)
print('\n' + '='*70)
print('Step 1: FactSalesSummary (drop stg_dwh copy)')
print('='*70)

if table_exists('stg_mig', 'FactSalesSummary'):
    mig_count = get_row_count('stg_mig', 'FactSalesSummary')
    print(f'  [stg_mig].[FactSalesSummary]: {mig_count:,} rows (KEEP)')
    
    if table_exists('stg_dwh', 'FactSalesSummary'):
        dwh_count = get_row_count('stg_dwh', 'FactSalesSummary')
        print(f'  [stg_dwh].[FactSalesSummary]: {dwh_count:,} rows (DROP)')
        cursor.execute('DROP TABLE [stg_dwh].[FactSalesSummary]')
        print('  [OK] Dropped stg_dwh copy')
    else:
        print('  [OK] stg_dwh copy already gone')
else:
    print('  [WARN] FactSalesSummary not in stg_mig!')

# 2. FactTender - move from stg_dwh to stg_mig
move_table('FactTender', 'stg_dwh', 'stg_mig', 'FactTenderID')

# 3. FactSales - move from stg_dwh to stg_mig (LARGEST - ~45 min)
move_table('FactSales', 'stg_dwh', 'stg_mig', 'FactSalesID')

# ============================================================
# Summary
# ============================================================
total_elapsed = time.time() - total_start
print('\n' + '='*70)
print('COMPLETE!')
print('='*70)
print(f'Total time: {total_elapsed/60:.1f} minutes')
print()
print('Final state in stg_mig:')

for table in ['FactSales', 'FactTender', 'FactSalesSummary']:
    if table_exists('stg_mig', table):
        count = get_row_count('stg_mig', table)
        print(f'  [{table}]: {count:,} rows')
    else:
        print(f'  [{table}]: NOT FOUND')

conn.close()
print('\nDone!')


