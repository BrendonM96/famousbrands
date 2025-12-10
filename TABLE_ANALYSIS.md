# FB Nova - Table Cross-Reference Analysis

Generated from config file.xlsx and famous-brands-labs schema extraction.

## Summary

| Category | Count |
|----------|-------|
| Total Tables in Config | 57 |
| Dimension Tables | 36 |
| Fact Tables | 21 |
| Currently Enabled | 0 (all set to 'n') |

## Loading Strategy

| Load Type | Description | Tables |
|-----------|-------------|--------|
| **FULL** | Truncate and reload entire table | Dimension tables, small facts |
| **DELTA** | Load only new/changed records | Large fact tables with date columns |

### Delta Loading Approach

For fact tables, the sync uses **date-based delta loading**:

1. **First Run**: Load last 7 days (configurable via `DELTA_LOOKBACK_DAYS`)
2. **Subsequent Runs**: Load from last watermark to current time
3. **Watermark Tracking**: `meta.nova_watermark` table stores last sync point

```
First Delta Run:
├── Check watermark → None found
├── Calculate: NOW - 7 days = start_date
├── Query: SELECT * FROM FactSales WHERE TransactionDate >= start_date
├── Load data with _loaded_at and _batch_id columns
└── Update watermark with end timestamp

Subsequent Delta Runs:
├── Check watermark → Found: 2025-12-09 14:30:00
├── Query: SELECT * FROM FactSales WHERE TransactionDate >= '2025-12-09 14:30:00'
├── Load new data (appends to existing)
└── Update watermark with new end timestamp
```

---

## Table Categories by Size

### 🟢 Small Tables (< 1,000 rows) - Safe for Full Load

| Table | Rows | Daily Change | Notes |
|-------|------|--------------|-------|
| DimAssumptions | 185 | 0 | Static dimension |
| DimBasketSize | 99 | 0 | Static dimension |
| DimBenchmarks | 185 | 0 | Static dimension |
| DimBrand | 47 | 0 | Core dimension |
| DimBusinessPartnerCategory | 64 | 0 | Static dimension |
| DimCampaignCategory | 40 | 0 | Static dimension |
| DimClosingReason | 19 | 0 | Static dimension |
| DimCurrency | 39 | 0 | Static dimension |
| DimDCFMaster | 186 | 0 | Static dimension |
| DimDCFReason | 5 | 0 | Static dimension |
| DimDeliveryStatus | 4 | 0 | Static dimension |
| DimInterCompany | 2 | 0 | Static dimension |
| DimInterSite | 2 | 0 | Static dimension |
| DimLineUser | 197 | 0 | User dimension |
| DimOrderMethod | 6 | 0 | Static dimension |
| DimOrderType | 18 | 0 | Static dimension |
| DimPlan | 54 | 0 | Static dimension |
| DimPlanner | 189 | 0 | User dimension |
| DimRestaurantType | 72 | 0 | Static dimension |
| DimReturnReason | 44 | 0 | Static dimension |
| DimSalesOrderStatus | 2 | 0 | Static dimension |
| DimSite | 147 | 0 | Location dimension |
| DimTenderType | 8 | 0 | Static dimension |
| DimTransLineType | 5 | 0 | Static dimension |
| DimTransType | 7 | 0 | Static dimension |
| TimeCalculation | 23 | 0 | Utility table |

**Recommendation**: Enable all for immediate sync. Estimated time: < 1 minute total.

---

### 🟡 Medium Tables (1,000 - 100,000 rows) - Full Load OK

| Table | Rows | Daily Change | Notes |
|-------|------|--------------|-------|
| DimDate | 29,221 | 16 | Date dimension |
| DimDCFInput | 15,513 | 9 | DCF data |
| DimItem | 16,606 | 9 | Product dimension |
| DimMenuItem | 145,955 | 80 | Menu items |
| DimTime | 86,400 | 47 | Time dimension |

**Recommendation**: Enable all for full load. Estimated time: 1-2 minutes total.

---

### 🟠 Large Tables (100K - 1M rows) - Full Load with Monitoring

| Table | Rows | Daily Change | Notes |
|-------|------|--------------|-------|
| DimItemSiteStatus | 171,632 | 94 | Item status |
| DimOrderNumbers | 504,304 | 276 | Order tracking |
| FactSalesvsFcst_Week | 940,566 | 515 | Weekly forecast |

**Recommendation**: Enable for full load. Estimated time: 5-10 minutes total.

---

### 🔴 Very Large Tables (1M - 10M rows) - Consider Delta Load

| Table | Rows | Daily Change | Notes |
|-------|------|--------------|-------|
| DimSupplyChainBrand | 1,794 | 1 | Supply chain |
| DimSupplyChainCustomer | 16,764 | 9 | Supply chain |
| DimVendor | 11,254 | 6 | Vendor dimension |
| DimStoreProduct | 1,929,267 | 1,057 | ⚠️ Large, daily changes |
| FactPurchaseOrderLines | 1,542,678 | 845 | ⚠️ Growing fact |
| FactStockonHand | 1,808,826 | 991 | ⚠️ Daily snapshot |
| FactStockCoverSalesForecast | 2,863,089 | 1,569 | ⚠️ Forecast data |
| FactWeeklyDeclaration | 3,286,111 | 1 | Weekly data |

**Recommendation**: 
- Enable smaller ones (< 100K) for full load
- Consider sampling or delta for DimStoreProduct and Fact tables

---

### ⛔ Massive Tables (> 10M rows) - Requires Strategy

| Table | Rows | Daily Change | Strategy |
|-------|------|--------------|----------|
| FactDCF | 15,138 | 8 | ✅ Small, full load OK |
| FactCustomerSales | 36,924,608 | 20,233 | ⚠️ Sample or incremental |
| FactCustomerSalesHistory | 17,732,351 | 9,716 | ⚠️ Sample or incremental |
| FactSalesThirdPartySummary | 22,875,169 | 12,534 | ⚠️ Sample or incremental |
| FactSalesSummary | 495,638,344 | 271,583 | 🛑 Too large for full load |
| FactTender | 836,576,451 | 458,398 | 🛑 Too large for full load |
| FactSales | 1,770,406,798 | 970,086 | 🛑 1.7 BILLION rows |

**Recommendations**:
1. **FactDCF**: Small enough for full load
2. **FactCustomerSales/History**: Use `LARGE_TABLES` sampling (1M rows)
3. **FactSalesThirdPartySummary**: Use sampling
4. **FactSalesSummary/FactTender/FactSales**: 
   - Option A: Skip entirely (use ADF for these)
   - Option B: Use extreme sampling (100K rows)
   - Option C: Partition by date range

---

## Fact Tables with Placeholder Rows

These tables show "1" row in config, indicating they may be empty or placeholders:

| Table | Config Rows | Notes |
|-------|-------------|-------|
| FactAppointment | 1 | May be empty |
| FactBudget | 1 | May be empty |
| FactCampaignScore | 1 | May be empty |
| FactForecastCalc | 1 | May be empty |
| FactLeasesConsolidated | 1 | May be empty |
| FactMonthlyDeclaration | 1 | May be empty |
| FactRestaurantStrategicCategory | 1 | May be empty |
| FactRoyalty | 1 | May be empty |

**Recommendation**: Verify these tables before enabling. They might be empty or have very few rows.

---

## Schema Validation

### Tables in Config vs Source Schema

All 57 tables in the config file exist in the `dwh` schema of the source database (FB_DW).

| Status | Count |
|--------|-------|
| ✅ Found in source | 57 |
| ❌ Missing from source | 0 |
| ⚠️ Extra in source | ~50+ (views, utility tables) |

---

## Recommended Sync Phases

### Phase 1: Quick Win (< 5 minutes)
Enable these 26 small dimension tables:

```
DimAssumptions, DimBasketSize, DimBenchmarks, DimBrand, 
DimBusinessPartnerCategory, DimCampaignCategory, DimClosingReason, 
DimCurrency, DimDCFMaster, DimDCFReason, DimDeliveryStatus, 
DimInterCompany, DimInterSite, DimLineUser, DimOrderMethod, 
DimOrderType, DimPlan, DimPlanner, DimRestaurantType, 
DimReturnReason, DimSalesOrderStatus, DimSite, DimTenderType, 
DimTransLineType, DimTransType, TimeCalculation
```

### Phase 2: Core Dimensions (10-15 minutes)
Enable medium dimension tables:

```
DimDate, DimDCFInput, DimItem, DimMenuItem, DimTime
```

### Phase 3: Growing Tables (30-60 minutes)
Enable with monitoring:

```
DimItemSiteStatus, DimOrderNumbers, DimStoreProduct, 
DimSupplyChainBrand, DimSupplyChainCustomer, DimVendor
```

### Phase 4: Smaller Fact Tables (1-2 hours)
Enable:

```
FactDCF, FactPurchaseOrderLines, FactSalesvsFcst_Week, 
FactStockonHand, FactStockCoverSalesForecast, FactWeeklyDeclaration
```

### Phase 5: Large Fact Tables (Separate Strategy)
These require special handling:

```
FactCustomerSales, FactCustomerSalesHistory, 
FactSalesThirdPartySummary, FactSalesSummary, 
FactTender, FactSales
```

**Options:**
1. Use Azure Data Factory with partitioned copy
2. Use sampling mode in sync script
3. Copy incrementally by date range

---

## Config File Updates Required

To enable Phase 1, update `config file.xlsx`:
1. Open the file
2. Change `Enabled` column from `n` to `Y` for Phase 1 tables
3. Save and run sync

---

## Related Views in Source

The source database also has views in the `dwh` schema that may be useful:

| View | Purpose |
|------|---------|
| vDimRestaurant | Restaurant dimension view |
| vDimFranchiseAgreement | Franchise data |
| vFactSales | Sales fact view |
| vFactSalesSummary | Aggregated sales |
| vFactTender | Tender transactions |
| dwv_DimDate | Date dimension view |
| dwv_DimTime | Time dimension view |

**Note**: Views are not currently in the config file. Consider adding if needed for reporting.

---

## Delta Loading Configuration

### Config File Columns Used for Delta

| Column | Purpose | Example |
|--------|---------|---------|
| `Date_Column_Name` | Column for delta filtering | `TransactionDate` |
| `PK_Column_Name` | Primary key for tracking | `FactSalesID` |

### Tables with Delta Column Configured

Based on your config file, these tables have date columns defined:

| Table | Date Column | Can Use Delta |
|-------|-------------|---------------|
| DimDate | DimDateID | ✅ Yes (but small, use FULL) |
| DimDCFInput | DimDCFInputID | ✅ Yes (but small) |
| Other Dims | Various | Use FULL load |

### Large Fact Tables - Delta Required

| Table | Recommended Date Column | Strategy |
|-------|------------------------|----------|
| FactSales | TransactionDate | Delta by date |
| FactTender | TransactionDate | Delta by date |
| FactSalesSummary | SummaryDate | Delta by date |
| FactCustomerSales | TransactionDate | Delta by date |

---

## Metadata Columns Added

The delta sync script adds these columns to target tables:

| Column | Type | Purpose |
|--------|------|---------|
| `_loaded_at` | DATETIME2 | When the row was loaded |
| `_batch_id` | VARCHAR(50) | Unique batch identifier |

Example batch ID: `nova_20251210_143022_a1b2c3d4`

---

## Load Statistics Tracking

The sync creates tables similar to your existing `load_stats`:

### meta.nova_load_stats

| Column | Description |
|--------|-------------|
| run_id | Unique batch identifier |
| run_started | Start timestamp |
| run_ended | End timestamp |
| source_table | Source table name |
| target_table | Target table name |
| load_type | FULL or DELTA |
| delta_column | Column used for delta |
| delta_start_value | Delta range start |
| delta_end_value | Delta range end |
| rows_exported | Rows read from source |
| rows_loaded | Rows inserted to target |
| duration_ms | Time taken |
| success | 1 = success, 0 = failed |
| error_message | Error details if failed |

### meta.nova_watermark

| Column | Description |
|--------|-------------|
| table_name | Table identifier |
| last_delta_value | Last sync timestamp |
| last_max_id | Max ID synced |
| last_row_count | Rows in last sync |
| last_batch_id | Batch ID of last sync |
| last_success_at | Last successful sync time |

---

## Comparison with Your Existing Load Stats

Your colleague's `load_stats.csv` tracks `stg_mig` → `int_mig` loads.
The Nova `nova_load_stats` tracks `dwh` → `stg_mig` loads.

```
┌─────────────────────────────────────────────────────────────┐
│                    Complete Pipeline                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Production           Staging (Dev)         Integration      │
│  ┌─────────┐          ┌─────────┐          ┌─────────┐      │
│  │   dwh   │  ──────► │ stg_mig │  ──────► │ int_mig │      │
│  │ (FB_DW) │  Nova    │         │  Your    │         │      │
│  └─────────┘  Sync    └─────────┘  ETL     └─────────┘      │
│                 │                    │                       │
│                 ▼                    ▼                       │
│         nova_load_stats        load_stats                   │
│         nova_watermark         (existing)                   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

