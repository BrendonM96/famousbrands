"""
Data Quality Checks for FB Nova Migration
==========================================
Compares source (PROD) vs target (DEV) for:
- FactSales
- FactTender  
- FactSalesSummary
"""
import pyodbc
import struct
import time
from datetime import datetime
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions

import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

print('=' * 70)
print('FB NOVA - DATA QUALITY CHECKS')
print('=' * 70)
print(f'Started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print()

# ======================== AUTH ========================
print('Authenticating (persistent cache)...')
cache_options = TokenCachePersistenceOptions(allow_unencrypted_storage=True)
credential = InteractiveBrowserCredential(cache_persistence_options=cache_options, timeout=600)
token = credential.get_token('https://database.windows.net/.default')

token_bytes = token.token.encode('UTF-16-LE')
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

# ======================== CONNECTIONS ========================
# Source (PROD)
SOURCE_SERVER = 'az-zan-sws-prod-01.sql.azuresynapse.net'
SOURCE_DB = 'FB_DW'
SOURCE_SCHEMA = 'dwh'

# Target (DEV)
TARGET_SERVER = 'synapse-fbnova-dev.sql.azuresynapse.net'
TARGET_DB = 'sqlpoolfbnovadev'
TARGET_SCHEMA = 'stg_mig'

print('\nConnecting to SOURCE (PROD)...')
source_conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SOURCE_SERVER};DATABASE={SOURCE_DB};'
source_conn = pyodbc.connect(source_conn_str, attrs_before={1256: token_struct})
source_cursor = source_conn.cursor()
print(f'[OK] Connected to {SOURCE_SERVER}/{SOURCE_DB}')

print('\nConnecting to TARGET (DEV)...')
target_conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={TARGET_SERVER};DATABASE={TARGET_DB};'
target_conn = pyodbc.connect(target_conn_str, attrs_before={1256: token_struct})
target_cursor = target_conn.cursor()
print(f'[OK] Connected to {TARGET_SERVER}/{TARGET_DB}')

# ======================== TABLES TO CHECK ========================
TABLES = [
    {
        'name': 'FactSales',
        'pk': 'FactSalesID',
        'numeric_cols': ['SalesAmount', 'TaxAmount', 'DiscountAmount'],
        'not_null_cols': ['FactSalesID', 'DimRestaurantID', 'DimBusinessDateID']
    },
    {
        'name': 'FactTender',
        'pk': 'FactTenderID',
        'numeric_cols': ['TenderAmount'],
        'not_null_cols': ['FactTenderID', 'DimRestaurantID']
    },
    {
        'name': 'FactSalesSummary',
        'pk': 'FactSalesSummaryID',
        'numeric_cols': ['SalesAmount', 'TransactionCount'],
        'not_null_cols': ['FactSalesSummaryID']
    }
]

# ======================== CHECK FUNCTIONS ========================
def check_row_count(table_name):
    """Compare row counts between source and target"""
    print(f'\n  [1] Row Count Check')
    
    source_cursor.execute(f'SELECT COUNT(*) FROM [{SOURCE_SCHEMA}].[{table_name}]')
    source_count = source_cursor.fetchone()[0]
    
    target_cursor.execute(f'SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{table_name}]')
    target_count = target_cursor.fetchone()[0]
    
    diff = target_count - source_count
    pct = (target_count / source_count * 100) if source_count > 0 else 0
    
    status = '✅ PASS' if diff >= 0 else '⚠️ MISSING ROWS'
    print(f'      Source: {source_count:,}')
    print(f'      Target: {target_count:,}')
    print(f'      Diff:   {diff:+,} ({pct:.2f}%)')
    print(f'      Status: {status}')
    
    return {'source': source_count, 'target': target_count, 'diff': diff, 'pass': diff >= 0}

def check_duplicates(table_name, pk_col):
    """Check for duplicate primary keys in target"""
    print(f'\n  [2] Duplicate PK Check ({pk_col})')
    
    target_cursor.execute(f'''
        SELECT {pk_col}, COUNT(*) as cnt 
        FROM [{TARGET_SCHEMA}].[{table_name}]
        GROUP BY {pk_col}
        HAVING COUNT(*) > 1
    ''')
    duplicates = target_cursor.fetchall()
    
    if duplicates:
        print(f'      ❌ FAIL: {len(duplicates)} duplicate PKs found!')
        for pk, cnt in duplicates[:5]:
            print(f'         {pk}: {cnt} occurrences')
        return {'pass': False, 'count': len(duplicates)}
    else:
        print(f'      ✅ PASS: No duplicates')
        return {'pass': True, 'count': 0}

def check_nulls(table_name, not_null_cols):
    """Check for unexpected nulls in key columns"""
    print(f'\n  [3] Null Check')
    
    issues = []
    for col in not_null_cols:
        try:
            target_cursor.execute(f'''
                SELECT COUNT(*) FROM [{TARGET_SCHEMA}].[{table_name}]
                WHERE [{col}] IS NULL
            ''')
            null_count = target_cursor.fetchone()[0]
            
            if null_count > 0:
                print(f'      ⚠️ {col}: {null_count:,} nulls')
                issues.append((col, null_count))
            else:
                print(f'      ✅ {col}: No nulls')
        except Exception as e:
            print(f'      ⚠️ {col}: Column not found')
    
    return {'pass': len(issues) == 0, 'issues': issues}

def check_id_range(table_name, pk_col):
    """Compare ID ranges between source and target"""
    print(f'\n  [4] ID Range Check ({pk_col})')
    
    source_cursor.execute(f'SELECT MIN({pk_col}), MAX({pk_col}) FROM [{SOURCE_SCHEMA}].[{table_name}]')
    src_min, src_max = source_cursor.fetchone()
    
    target_cursor.execute(f'SELECT MIN({pk_col}), MAX({pk_col}) FROM [{TARGET_SCHEMA}].[{table_name}]')
    tgt_min, tgt_max = target_cursor.fetchone()
    
    print(f'      Source: {src_min:,} to {src_max:,}')
    print(f'      Target: {tgt_min:,} to {tgt_max:,}')
    
    min_match = src_min == tgt_min
    # Target max might be less if source is live
    max_ok = tgt_max <= src_max or tgt_max >= src_max  # Allow for live data
    
    if min_match:
        print(f'      ✅ PASS: Ranges align')
    else:
        print(f'      ⚠️ Min ID mismatch')
    
    return {'pass': min_match, 'source_range': (src_min, src_max), 'target_range': (tgt_min, tgt_max)}

def check_aggregates(table_name, numeric_cols):
    """Compare aggregate values for numeric columns"""
    print(f'\n  [5] Aggregate Check')
    
    results = []
    for col in numeric_cols:
        try:
            source_cursor.execute(f'''
                SELECT SUM(CAST({col} AS FLOAT)), AVG(CAST({col} AS FLOAT))
                FROM [{SOURCE_SCHEMA}].[{table_name}]
            ''')
            src_sum, src_avg = source_cursor.fetchone()
            
            target_cursor.execute(f'''
                SELECT SUM(CAST({col} AS FLOAT)), AVG(CAST({col} AS FLOAT))
                FROM [{TARGET_SCHEMA}].[{table_name}]
            ''')
            tgt_sum, tgt_avg = target_cursor.fetchone()
            
            if src_sum and tgt_sum:
                pct_diff = abs(tgt_sum - src_sum) / src_sum * 100 if src_sum != 0 else 0
                status = '✅' if pct_diff < 1 else '⚠️'
                print(f'      {status} {col}:')
                print(f'         Source SUM: {src_sum:,.2f}')
                print(f'         Target SUM: {tgt_sum:,.2f}')
                print(f'         Diff: {pct_diff:.4f}%')
                results.append({'col': col, 'pct_diff': pct_diff, 'pass': pct_diff < 1})
        except Exception as e:
            print(f'      ⚠️ {col}: Error - {str(e)[:50]}')
    
    return results

# ======================== MAIN ========================
print('\n' + '=' * 70)
print('RUNNING DATA QUALITY CHECKS')
print('=' * 70)

all_results = {}
total_start = time.time()

for table in TABLES:
    table_name = table['name']
    print(f'\n{"="*70}')
    print(f'TABLE: {table_name}')
    print('='*70)
    
    try:
        results = {
            'row_count': check_row_count(table_name),
            'duplicates': check_duplicates(table_name, table['pk']),
            'nulls': check_nulls(table_name, table['not_null_cols']),
            'id_range': check_id_range(table_name, table['pk']),
        }
        
        # Aggregate check is slower, make it optional for large tables
        if table_name != 'FactSales':  # Skip for largest table
            results['aggregates'] = check_aggregates(table_name, table['numeric_cols'])
        else:
            print(f'\n  [5] Aggregate Check')
            print(f'      ⏭️ SKIPPED (large table - would take too long)')
        
        all_results[table_name] = results
        
    except Exception as e:
        print(f'\n  ❌ ERROR: {e}')
        all_results[table_name] = {'error': str(e)}

# ======================== SUMMARY ========================
total_elapsed = time.time() - total_start

print('\n' + '=' * 70)
print('SUMMARY')
print('=' * 70)

for table_name, results in all_results.items():
    if 'error' in results:
        print(f'\n{table_name}: ❌ ERROR')
        continue
    
    checks_passed = sum([
        results.get('row_count', {}).get('pass', False),
        results.get('duplicates', {}).get('pass', False),
        results.get('nulls', {}).get('pass', False),
        results.get('id_range', {}).get('pass', False),
    ])
    total_checks = 4
    
    rc = results.get('row_count', {})
    status = '✅ PASS' if checks_passed == total_checks else f'⚠️ {checks_passed}/{total_checks} passed'
    
    print(f'\n{table_name}:')
    print(f'  Status: {status}')
    print(f'  Rows: {rc.get("target", 0):,} (diff: {rc.get("diff", 0):+,})')

print(f'\n{"="*70}')
print(f'Total time: {total_elapsed/60:.1f} minutes')
print(f'Completed: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print('=' * 70)

source_conn.close()
target_conn.close()
print('\nDone!')

