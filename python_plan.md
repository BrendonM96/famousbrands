# Python Plan - FB Nova Data Sync

## Overview
Python script to replicate data from old Synapse (prod) to new Synapse (dev).

---

## Environments

| | Source (Prod) | Target (Dev) |
|--|---------------|--------------|
| Server | `az-zan-sws-prod-01.sql.azuresynapse.net` | `synapse-fbnova-dev.sql.azuresynapse.net` |
| Database | `FB_DW` | `sqldb-fbnova-dev` |
| Schema | `dwh` | `stg_dwh` (from config) |
| Pool | Dedicated (FB_DW) | Serverless (Built-in) |

---

## Config File: `config file.xlsx`

| Column | Description |
|--------|-------------|
| Source_Schema | `dwh` |
| Source_table_name | Table name in source |
| Target_Schema | `stg_dwh` |
| Target_Table_Name | Table name in target |
| Enabled | `Y` = process, `N` = skip |

**Current state:** 30 dimension tables enabled, all full loads to `stg_dwh`

---

## Execution Steps

### Step 1: Connect
- Connect to **source** (az-zan-sws-prod-01 / FB_DW)
- Connect to **target** (synapse-fbnova-dev / sqldb-fbnova-dev)
- Authentication: Azure AD Interactive

### Step 2: Read Config
- Load `config file.xlsx`
- Filter to `Enabled = 'Y'` only
- Result: List of 30 tables to process

### Step 3: For Each Table...

#### 3a. Create Target Table (if not exists)
- Check if `[Target_Schema].[Target_Table_Name]` exists
- If not, create it with same structure as source
- Method: Copy column definitions from source table

#### 3b. Truncate Target Table
- Empty the target table before loading
- SQL: `TRUNCATE TABLE [stg_dwh].[TableName]`

#### 3c. Copy Data
- Read all rows from source: `SELECT * FROM [dwh].[TableName]`
- Insert into target: `INSERT INTO [stg_dwh].[TableName]`
- Use batching for performance

#### 3d. Report Result
- Print: table name, rows copied, time taken, success/fail

### Step 4: Summary
- Print total tables processed
- Print total rows copied
- Print any failures

---

## Current Scope

**IN SCOPE:**
- 30 enabled dimension tables
- Full load only (copy entire table)
- Source schema: `dwh`
- Target schema: `stg_dwh`

**NOT IN SCOPE (yet):**
- Fact tables (too large)
- Delta loads
- Merge to integration layer
- Logging to status table

---

## Next Action

**BUILD THE SCRIPT** following Steps 1-4 above.
