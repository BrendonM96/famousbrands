# FB Nova - Migration Scripts

Scripts for migrating large fact tables from Azure Synapse PROD to DEV.

## Migration Summary

| Table | Source Rows | Target Rows | Status |
|-------|-------------|-------------|--------|
| **FactSales** | 1.86B | 1.86B | ✅ Complete |
| **FactTender** | 875M | 875M | ✅ Complete |
| **FactSalesSummary** | 517M | 517M | ✅ Complete |

**Total: 3.25 billion rows migrated**

## Scripts

### Export Scripts (Source → Blob Storage)
| Script | Description |
|--------|-------------|
| `sync_factsales_chunked.py` | Export FactSales (1.86B rows) in optimized ID ranges |
| `sync_facttender_chunked.py` | Export FactTender (875M rows) in optimized ID ranges |

### Load Scripts (Blob Storage → Target)
| Script | Description |
|--------|-------------|
| `load_to_target.py` | Load FactSales using COPY INTO |
| `load_facttender.py` | Load FactTender using COPY INTO |

### Utility Scripts
| Script | Description |
|--------|-------------|
| `data_quality_check.py` | **Data quality validation** - compares source vs target |
| `move_to_stg_mig.py` | Move tables to stg_mig schema |
| `check_tables.py` | Verify table existence and row counts |

## Features

- **Chunked Export**: Optimized ID-based ranges to prevent timeouts
- **Resumable**: Watermark files track progress for resume capability  
- **Slack Notifications**: Hourly updates + milestone alerts (25%, 50%, 75%, 100%)
- **Azure AD Auth**: Uses InteractiveBrowserCredential with persistent token cache
- **COPY INTO**: Efficient bulk loading from blob storage

## Data Quality Results

All tables passed quality checks (ran 2026-01-03):

| Check | FactSales | FactTender | FactSalesSummary |
|-------|-----------|------------|------------------|
| **No Duplicates** | ✅ | ✅ | ✅ |
| **No Nulls** | ✅ | ✅ | ✅ |
| **ID Range Aligned** | ✅ | ✅ | ✅ |
| **Aggregate Match** | Skipped | ✅ 0.0049% | ✅ |

> Row count differences (~0.1%) are expected due to ongoing PROD inserts during/after migration.

## Usage

### Run Data Quality Checks

```powershell
cd migration-scripts
pip install -r requirements.txt
python data_quality_check.py
```

### Run Migration

```powershell
# 1. Export to blob storage
python sync_factsales_chunked.py
python sync_facttender_chunked.py

# 2. Load to target (auto-runs after export)
python load_to_target.py
python load_facttender.py

# 3. Consolidate tables
python move_to_stg_mig.py

# 4. Verify
python data_quality_check.py
```

## Configuration

Required environment variables (`.env`):
```
STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

## Server Configuration

| Environment | Server | Database |
|-------------|--------|----------|
| Source (PROD) | az-zan-sws-prod-01.sql.azuresynapse.net | FB_DW |
| Target (DEV) | synapse-fbnova-dev.sql.azuresynapse.net | sqlpoolfbnovadev |

## Author

Brendon Mapinda - January 2026

