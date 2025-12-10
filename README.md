# FB Nova - Synapse Data Sync Tool

A high-performance Python utility for synchronizing data between Azure Synapse Analytics environments using the CSV → Blob Storage → COPY INTO pipeline approach.

**✅ SADD Aligned** - This tool is aligned with the Famous Brands Solution Architecture Design Document (SADD).

## Overview

This tool copies data from a **source** Synapse database (Production) to a **target** Synapse database (Dev/Test) using the fastest bulk loading method available in Azure Synapse: the `COPY INTO` command.

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Source Synapse │ ───► │  Azure Blob     │ ───► │  Target Synapse │
│  (Production)   │ CSV  │  Storage        │ COPY │  (Dev)          │
└─────────────────┘      └─────────────────┘ INTO └─────────────────┘
```

## How It Works

### Pipeline Steps

1. **Read Config** – Loads `config file.xlsx` to determine which tables to sync (only tables marked as enabled)

2. **Connect** – Establishes connections to both source and target Synapse databases using Azure AD Interactive authentication

3. **For Each Table:**
   - **Export to CSV** – Reads data from source in chunks (500K rows at a time) and writes to CSV files with pipe (`|`) delimiter
   - **Upload to Blob** – Each chunk is uploaded to Azure Blob Storage immediately, then deleted locally to save disk space
   - **Create/Truncate Table** – Creates the target table if it doesn't exist (schema inferred from source), or truncates if it does
   - **COPY INTO** – Executes `COPY INTO` command to bulk load all chunks from blob storage into the target table
   - **Cleanup** – Deletes temporary blob files after successful load

4. **Summary** – Reports total tables processed, rows synced, time taken, and any failures

### Data Processing Features

- **Chunked Processing** – Large tables are processed in 500K row chunks to avoid memory issues
- **Large Table Sampling** – Tables >10M rows can optionally be sampled (configurable via `LARGE_TABLES` list)
- **Data Type Fixing** – Automatically handles:
  - Infinity values → NULL
  - Float columns that are actually integers → Int64
  - Float precision rounding (10 decimal places)
  - String NULL variants ('nan', 'None', 'NULL') → actual NULL

## Configuration

### Environment Variables (`.env` file)

Create a `.env` file in the project root:

```env
# Azure Storage credentials
STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=stsynfbnovadev;AccountKey=...;EndpointSuffix=core.windows.net
STORAGE_KEY=your_storage_account_key_here

# Your Azure AD username
USERNAME=your.email@company.com
```

### Config File (`config file.xlsx`)

An Excel file with the following columns:

| Column | Description |
|--------|-------------|
| `Source_Schema` | Schema name in source database |
| `Source_table_name` | Table name in source database |
| `Target_Schema` | Schema name in target database |
| `Target_Table_Name` | Table name in target database |
| `Enabled` | Set to `Y` to sync this table, any other value to skip |

### Script Configuration

Edit these constants in `sync_data_copy.py` as needed:

```python
# Chunk size for processing
CHUNK_SIZE = 500000  # 500K rows per chunk

# Large table handling
LARGE_TABLE_THRESHOLD = 10000000  # 10 million rows
SAMPLE_SIZE = 1000000             # Sample size for large tables

# Tables to sample instead of full load (empty = full load all)
LARGE_TABLES = [
    # 'FactCustomerSales',
    # 'FactSales',
]
```

## Prerequisites

### Required Software

1. **Python 3.8+**
2. **ODBC Driver 17 for SQL Server** – [Download here](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
3. **Azure AD account** with access to both source and target databases

### Python Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
- `pyodbc>=4.0.39` – Database connectivity
- `pandas` – Data manipulation
- `numpy` – Numerical operations
- `azure-storage-blob` – Azure Blob Storage SDK

### Azure Resources

- **Azure Blob Storage Account** – With a container named `synapsedata`
- **Storage Account Key** – For authentication in COPY INTO command
- **Synapse Permissions** – Read access to source, read/write to target

## Usage

### Running the Sync

```bash
python sync_data_copy.py
```

### Authentication

The script uses Azure AD Interactive authentication. When you run it, a browser window will open twice:
1. First for the **source** connection
2. Second for the **target** connection

### Output

The script provides detailed progress output:

```
============================================================
FB NOVA - DATA SYNC (COPY method)
Started: 2025-12-09 10:30:00
============================================================

STEP 1: READ CONFIG
Reading config file...
  ✓ Found 15 enabled tables

STEP 2: CONNECT
  Connecting to source: az-zan-sws-prod-01.sql.azuresynapse.net...
  ✓ Source connected
  Connecting to target: synapse-fbnova-dev.sql.azuresynapse.net...
  ✓ Target connected

STEP 3: PROCESS TABLES

[1/15] dbo.DimCustomer
  Exporting [dbo].[DimCustomer] to CSV...
    Rows: 125,000
    ✓ Exported and uploaded 1 chunks
  Table exists, truncating...
  COPY INTO [staging].[DimCustomer] from 1 chunks...
    ✓ COPY completed (1 chunks)
  Time: 8.3s

...

============================================================
SUMMARY
============================================================
Tables: 15 (Success: 15, Failed: 0)
Total rows: 2,450,000
Total time: 145.2s

Completed: 2025-12-09 10:32:25
```

## Architecture Details

### Why CSV + COPY INTO?

The `COPY INTO` command is the fastest way to load data into Azure Synapse Analytics because:
- It bypasses row-by-row insert overhead
- It reads directly from blob storage (parallel I/O)
- It's optimized for distributed architecture

### File Format

- **Delimiter**: Pipe (`|`) character
- **Encoding**: UTF-8
- **NULL handling**: Empty string = NULL
- **Header**: First chunk only (FIRSTROW=2 in COPY command)

### Blob Storage Structure

```
synapsedata/
└── staging/
    ├── DimCustomer_chunk_0001.csv
    ├── DimCustomer_chunk_0002.csv
    ├── FactSales_chunk_0001.csv
    └── ...
```

Files are automatically deleted after successful load.

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `STORAGE_CONNECTION_STRING not set` | Ensure `.env` file exists and contains the connection string |
| Authentication popup doesn't appear | Check if browser is blocking popups |
| `COPY INTO` fails with data type error | Check for infinity/NaN values in source data |
| Out of memory | Reduce `CHUNK_SIZE` in the script |
| Timeout on large tables | Add table to `LARGE_TABLES` list to use sampling |

### Logs

All operations are logged to stdout. Redirect to a file if needed:

```bash
python sync_data_copy.py > sync_log.txt 2>&1
```

## Server Configuration

| Environment | Server | Database |
|-------------|--------|----------|
| Source (Prod) | `az-zan-sws-prod-01.sql.azuresynapse.net` | `FB_DW` |
| Target (Dev) | `synapse-fbnova-dev.sql.azuresynapse.net` | `sqlpoolfbnovadev` |

## Deployment

### Azure Container Instances (Recommended for Production)

For scheduled/automated runs, deploy as a container:

```bash
# PowerShell
.\deploy.ps1

# Bash
./deploy.sh
```

See deployment files:
- `Dockerfile` - Container image definition
- `deploy.ps1` / `deploy.sh` - Deployment scripts
- `logic-app-schedule.json` - Scheduling ARM template
- `env.example` - Environment variable template

### Automated Authentication

For production deployments, use Service Principal authentication instead of interactive:

```env
AZURE_CLIENT_ID=your-service-principal-id
AZURE_CLIENT_SECRET=your-service-principal-secret
```

Use `sync_data_copy_automated.py` for production runs.

## SADD Alignment

This tool implements key patterns from the Solution Architecture Design Document (SADD):

### Metadata Columns (per SADD ETL Pipeline Considerations)

| Column | SADD Requirement | Purpose |
|--------|-----------------|---------|
| `_batch_id` | Batch_Id | Unique batch identifier |
| `_loaded_at` | FDW_Timestamp | Data landing timestamp |
| `_source_system_id` | Source_system_Id | Source system identifier |

### Key Logging Dimensions (per SADD)

The `meta.nova_load_stats` table captures:
- Pipeline name, job name, BatchID
- Start time, end time, duration
- Trigger type (manual, scheduled, event-based)
- Row counts: read, inserted, rejected
- Source and target table names
- Status, Error Message

### Data Quality Controls (per SADD)

- **Source vs FDW Validation**: Row count comparison after each load
- **Load Ready Validation**: Data validation before final load
- **30-day Trend Analysis**: Historical metrics tracking
- **Alerting**: Webhook notifications on failures

### Configuration (per SADD)

```env
# Source System Identification (per SADD)
SOURCE_SYSTEM_ID=1
SOURCE_SYSTEM_NAME=FB_DW_PROD

# Pipeline Configuration (per SADD)
PIPELINE_NAME=PL_NOVA_SYNC
TRIGGER_TYPE=scheduled

# Data Quality (per SADD)
ENABLE_DATA_QUALITY_CHECKS=true

# Alerting (per SADD)
ALERT_WEBHOOK_URL=https://outlook.office.com/webhook/...
```

## Table Analysis

See `TABLE_ANALYSIS.md` for:
- Complete table inventory with row counts
- Recommended sync phases
- Large table handling strategies
- Performance estimates
- SADD alignment details

## Related Documentation

- Schema reference: `famous-brands-labs/src/database/schema/`
- Report inventory: `famous-brands-labs/src/reports/all_reports.md`
- Nova sync docs: `famous-brands-labs/.docs/general/nova-sync-tool.md`

## License

Internal use only – FB Nova project.

