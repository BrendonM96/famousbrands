"""Quick check for tables in target Synapse"""
import pyodbc
import struct
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions

print('Connecting to target Synapse (DEV)...')
print('=' * 50)

# Auth with persistent cache
cache_options = TokenCachePersistenceOptions(allow_unencrypted_storage=True)
credential = InteractiveBrowserCredential(cache_persistence_options=cache_options, timeout=600)
token = credential.get_token('https://database.windows.net/.default')

token_bytes = token.token.encode('UTF-16-LE')
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

conn_str = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=synapse-fbnova-dev.sql.azuresynapse.net;DATABASE=sqlpoolfbnovadev;'
conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
cursor = conn.cursor()

print('[OK] Connected to synapse-fbnova-dev.sql.azuresynapse.net/sqlpoolfbnovadev')
print()

# Check tables
tables = ['FactSales', 'FactSalesSummary', 'FactTender']
print('Checking tables in stg_dwh schema:')
print('-' * 50)

for table in tables:
    cursor.execute(f"""
        SELECT 
            CASE WHEN EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'stg_dwh' AND TABLE_NAME = '{table}'
            ) THEN 1 ELSE 0 END AS exists_flag
    """)
    exists = cursor.fetchone()[0]
    
    if exists:
        cursor.execute(f'SELECT COUNT(*) FROM stg_dwh.{table}')
        count = cursor.fetchone()[0]
        print(f'  {table}: EXISTS - {count:,} rows')
    else:
        print(f'  {table}: NOT FOUND')

conn.close()
print()
print('Done!')


