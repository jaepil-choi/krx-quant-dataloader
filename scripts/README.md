# Production Scripts

Scripts for building and managing the KRX Parquet database.

## Quick Start

### 1. Build DB for Recent Data (Recommended for First Run)

```bash
# Last 30 days (fast, good for testing)
poetry run python scripts/build_db.py --days 30 --db ./data/krx_db

# Last 90 days
poetry run python scripts/build_db.py --days 90 --db ./data/krx_db

# Last 252 days (1 trading year)
poetry run python scripts/build_db.py --days 365 --db ./data/krx_db
```

### 2. Build DB for Specific Date Range

```bash
# Full year 2024
poetry run python scripts/build_db.py \
    --start 20240101 \
    --end 20241231 \
    --db ./data/krx_db

# Specific month (August 2024)
poetry run python scripts/build_db.py \
    --start 20240801 \
    --end 20240831 \
    --db ./data/krx_db

# Last 3 years
poetry run python scripts/build_db.py \
    --start 20220101 \
    --end 20241231 \
    --db ./data/krx_db
```

### 3. Build DB for Specific Market

```bash
# KOSPI only (STK)
poetry run python scripts/build_db.py \
    --days 90 \
    --db ./data/krx_db \
    --market STK

# KOSDAQ only (KSQ)
poetry run python scripts/build_db.py \
    --days 90 \
    --db ./data/krx_db \
    --market KSQ

# All markets (default)
poetry run python scripts/build_db.py \
    --days 90 \
    --db ./data/krx_db \
    --market ALL
```

## Script Details

### `build_db.py` - Main DB Builder

**Features:**
- ✅ **Resume-safe**: Can restart from any date; already-ingested dates are overwritten (idempotent)
- ✅ **Automatic holiday handling**: Skips non-trading days automatically
- ✅ **Market-wide ingestion**: Fetches all stocks (~3000) per trading day
- ✅ **Post-hoc adjustment factors**: Computes corporate action adjustments after ingestion
- ✅ **Progress reporting**: Shows ingestion status per day

**Options:**

```
Date Range (choose one):
  --days NUM          Number of recent days to fetch (including today)
  --start YYYYMMDD    Start date (requires --end)
  --end YYYYMMDD      End date (requires --start)

Database:
  --db PATH           Root path for Parquet database (required)

Market (optional):
  --market {ALL,STK,KSQ,KNX}
                      Market ID. Default: ALL
                      ALL = all markets
                      STK = KOSPI only
                      KSQ = KOSDAQ only
                      KNX = KONEX only

Price Adjustment (optional):
  --adjusted          Request adjusted prices from KRX (adjStkPrc=2)
                      Default: False (raw prices, compute factors ourselves)

Factor Computation (optional):
  --skip-factors      Skip adjustment factor computation
                      Default: False (compute factors)

Configuration (optional):
  --config PATH       Path to config YAML
                      Default: tests/fixtures/test_config.yaml
```

**Output Structure:**

```
./data/krx_db/
├── snapshots/
│   ├── TRD_DD=20240820/
│   │   └── data.parquet          # ~105 KB per file (compressed)
│   ├── TRD_DD=20240821/
│   │   └── data.parquet
│   └── ...
└── adj_factors/
    ├── TRD_DD=20240820/
    │   └── data.parquet          # ~10-50 KB per file
    ├── TRD_DD=20240821/
    │   └── data.parquet
    └── ...
```

**Example Output:**

```
======================================================================
PHASE 1: Ingest daily snapshots (resume-safe)
======================================================================

======================================================================
INGESTION SUMMARY
======================================================================
Total days attempted:  90
Trading days:          62
Holidays/non-trading:  28
Errors:                0

Total rows ingested:   175,340
Avg stocks per day:    2,828

First 5 trading days:
  20240820: 2,828 stocks
  20240821: 2,829 stocks
  20240822: 2,831 stocks
  ...

Hive partitions created: 62
  First: TRD_DD=20240820
  Last:  TRD_DD=20241120
  Total size: 6.54 MB

======================================================================
PHASE 2: Compute adjustment factors (post-hoc)
======================================================================

Read 175,340 snapshot rows
Computed and persisted 175,340 adjustment factors
Factor partitions: 62
Factor DB size: 1.23 MB

======================================================================
BUILD COMPLETE
======================================================================
Database root: C:\Users\...\data\krx_db

Total DB size: 7.77 MB

✓ Database ready for queries
```

## Production Recommendations

### Initial Build

For production, we recommend:

1. **Start with recent data** (last 90 days) to validate the system:
   ```bash
   poetry run python scripts/build_db.py --days 90 --db ./data/krx_db
   ```

2. **Backfill historical data** once validated:
   ```bash
   # Year by year to manage memory
   poetry run python scripts/build_db.py --start 20220101 --end 20221231 --db ./data/krx_db
   poetry run python scripts/build_db.py --start 20230101 --end 20231231 --db ./data/krx_db
   poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db
   ```

3. **Daily updates** (run via cron/scheduler):
   ```bash
   # Fetch last 5 days (includes holidays, ensures no gaps)
   poetry run python scripts/build_db.py --days 5 --db ./data/krx_db
   ```

### Performance Expectations

- **Ingestion speed**: ~3-5 seconds per trading day (network dependent)
- **DB size**: ~100-120 KB per trading day (compressed)
- **90 days**: ~7-8 MB, ~2 minutes
- **1 year (252 trading days)**: ~30 MB, ~10-15 minutes
- **3 years**: ~90 MB, ~30-45 minutes

### Scheduling Daily Updates

**Linux/Mac (cron):**
```bash
# Run at 7 PM every day (after KRX market close at 3:30 PM KST)
0 19 * * * cd /path/to/krx-quant-dataloader && poetry run python scripts/build_db.py --days 5 --db ./data/krx_db >> /var/log/krx_update.log 2>&1
```

**Windows (Task Scheduler):**
```powershell
# Create scheduled task
schtasks /create /tn "KRX DB Update" /tr "C:\path\to\krx-quant-dataloader\.venv\Scripts\python.exe C:\path\to\krx-quant-dataloader\scripts\build_db.py --days 5 --db C:\data\krx_db" /sc daily /st 19:00
```

## Resume Safety

The ingestion is **resume-safe**:

- If the script fails or is interrupted, simply rerun with the same parameters
- Already-ingested dates are overwritten (idempotent)
- No duplicate data
- No need to clean up before rerun

Example:
```bash
# Initial run (interrupted on day 50)
poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db
# ^C (Ctrl+C to interrupt)

# Resume (will overwrite days 1-50, continue from day 51)
poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db
```

## Troubleshooting

### "Config file not found"

Ensure you have the test config file:
```bash
ls tests/fixtures/test_config.yaml
```

Or specify your own config:
```bash
poetry run python scripts/build_db.py --days 30 --db ./data/krx_db --config ./my_config.yaml
```

### "No data for certain dates"

This is normal! KRX does not trade on:
- Weekends (Saturday, Sunday)
- Public holidays
- Special non-trading days

The script automatically handles this and reports them as "Holidays/non-trading".

### "Errors on specific dates"

If errors occur on specific dates:
1. Check your network connection
2. Verify KRX API is accessible
3. Try re-running (resume-safe)
4. Check the error logs for specific error messages

### "Database size larger than expected"

Expected sizes:
- ~100-120 KB per trading day (snapshots)
- ~10-50 KB per trading day (adj_factors)
- 1 year ≈ 30-35 MB total

If significantly larger:
- Check if you're fetching adjusted prices (--adjusted flag)
- Verify compression is working (check file extensions: .parquet)
- Ensure zstd compression is available (should be in pyarrow)

