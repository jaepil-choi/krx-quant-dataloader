# Quick Start Guide - KRX Quant Data Loader

## 🚀 Production Usage - Build Your Database

### Step 1: Quick Test (5 days)

Test the system with recent data:

```bash
poetry run python scripts/build_db.py --days 5 --db ./data/krx_db
```

**Expected output:**
```
Trading days:          3-5
Total rows ingested:   8,000-15,000
Total DB size:         0.3-0.5 MB
Time:                  ~15-30 seconds
```

### Step 2: Build Recent History (90 days)

Get 3 months of data for backtesting:

```bash
poetry run python scripts/build_db.py --days 90 --db ./data/krx_db
```

**Expected output:**
```
Trading days:          60-65
Total rows ingested:   170,000-185,000
Total DB size:         7-8 MB
Time:                  ~2-3 minutes
```

### Step 3: Build Full Year (252 trading days)

```bash
poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db
```

**Expected output:**
```
Trading days:          ~250
Total rows ingested:   700,000-750,000
Total DB size:         30-35 MB
Time:                  ~10-15 minutes
```

### Step 4: Backfill Historical Data (Multi-Year)

For multiple years, run year by year:

```bash
# 2022
poetry run python scripts/build_db.py --start 20220101 --end 20221231 --db ./data/krx_db

# 2023
poetry run python scripts/build_db.py --start 20230101 --end 20231231 --db ./data/krx_db

# 2024
poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db
```

**3-year database:**
```
Trading days:          ~750
Total rows ingested:   ~2,100,000
Total DB size:         90-100 MB
Time:                  ~30-45 minutes
```

---

## 📂 What Gets Created

After running the script, your database structure:

```
./data/krx_db/
├── snapshots/                    # Daily market-wide data
│   ├── TRD_DD=20241101/
│   │   └── data.parquet          # ~105 KB (2,800 stocks)
│   ├── TRD_DD=20241104/
│   │   └── data.parquet
│   └── ...
└── adj_factors/                  # Adjustment factors
    ├── TRD_DD=20241101/
    │   └── data.parquet          # ~10-50 KB
    ├── TRD_DD=20241104/
    │   └── data.parquet
    └── ...
```

**Each snapshot contains:**
- ~2,800-2,900 stocks (all KRX markets: KOSPI, KOSDAQ, KONEX)
- 15 fields: prices, volumes, market info
- Hive-partitioned by date for fast queries
- Sorted by symbol for row-group pruning

---

## 🔄 Daily Updates (Production Workflow)

### Manual Update

Run daily after market close (KRX closes at 3:30 PM KST):

```bash
# Fetch last 5 days (ensures no gaps from holidays)
poetry run python scripts/build_db.py --days 5 --db ./data/krx_db
```

### Automated Update (Recommended)

**Linux/Mac (cron):**

```bash
# Edit crontab
crontab -e

# Add this line (runs at 7 PM every day)
0 19 * * * cd /path/to/krx-quant-dataloader && poetry run python scripts/build_db.py --days 5 --db /path/to/data/krx_db >> /var/log/krx_update.log 2>&1
```

**Windows (Task Scheduler):**

```powershell
# PowerShell (run as Administrator)
schtasks /create /tn "KRX DB Daily Update" `
    /tr "C:\path\to\.venv\Scripts\python.exe C:\path\to\scripts\build_db.py --days 5 --db C:\data\krx_db" `
    /sc daily /st 19:00
```

---

## 🎯 Common Use Cases

### 1. Backtest Strategy (Need 2 years of data)

```bash
poetry run python scripts/build_db.py --start 20230101 --end 20241231 --db ./data/krx_db
```

### 2. Paper Trading (Need recent 30 days)

```bash
poetry run python scripts/build_db.py --days 30 --db ./data/krx_db
```

### 3. KOSPI Only (Large-cap stocks)

```bash
poetry run python scripts/build_db.py --days 90 --db ./data/krx_db --market STK
```

### 4. Research Project (Need full history)

```bash
# Build 5 years incrementally
for year in 2020 2021 2022 2023 2024; do
    poetry run python scripts/build_db.py \
        --start ${year}0101 \
        --end ${year}1231 \
        --db ./data/krx_db
done
```

### 5. Resume After Interruption

If the script is interrupted (Ctrl+C, network failure, etc.):

```bash
# Just rerun with same parameters (resume-safe, idempotent)
poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db
```

Already-ingested dates are overwritten; no cleanup needed.

---

## 📊 Performance Benchmarks

| Data Range | Trading Days | Total Rows | DB Size | Time |
|------------|--------------|------------|---------|------|
| 5 days | 3-5 | 8K-15K | 0.3-0.5 MB | 15-30 sec |
| 30 days | 20-22 | 56K-62K | 2-3 MB | 1-2 min |
| 90 days | 60-65 | 170K-185K | 7-8 MB | 2-3 min |
| 1 year | 250-252 | 700K-750K | 30-35 MB | 10-15 min |
| 3 years | 750+ | 2.1M+ | 90-100 MB | 30-45 min |

*Benchmarks on typical internet connection; may vary based on network speed and KRX API load.*

---

## ⚠️ Important Notes

### Holidays & Non-Trading Days

The script automatically handles:
- Weekends (Saturday, Sunday)
- Public holidays
- Special non-trading days

These show as "Holidays/non-trading: N" in the summary.

### Resume Safety

**The ingestion is resume-safe:**
- ✅ Can restart from any date
- ✅ Already-ingested dates are overwritten (idempotent)
- ✅ No duplicate data
- ✅ No need to clean up before rerun

### Storage Location

Choose your DB location wisely:
- **Development**: `./data/krx_db` (project directory)
- **Production**: `/var/lib/krx_db` (Linux) or `C:\Data\krx_db` (Windows)
- **Backup**: Regularly back up the entire DB directory

### Rate Limiting

The script respects KRX API rate limits automatically. If you encounter rate limit errors:
- Wait a few minutes and rerun (resume-safe)
- The script will continue from where it left off

---

## 🔧 Advanced Options

### Skip Adjustment Factor Computation

If you only need raw snapshots:

```bash
poetry run python scripts/build_db.py --days 90 --db ./data/krx_db --skip-factors
```

### Request Adjusted Prices from KRX

*Note: We compute our own adjustment factors; this is usually not needed.*

```bash
poetry run python scripts/build_db.py --days 90 --db ./data/krx_db --adjusted
```

### Use Custom Config

```bash
poetry run python scripts/build_db.py --days 90 --db ./data/krx_db --config ./my_config.yaml
```

---

## 🐛 Troubleshooting

### "No data for certain dates"

**Normal behavior!** KRX does not trade on weekends and holidays.

### "Errors on specific dates"

1. Check network connection
2. Verify KRX API is accessible
3. Rerun the script (resume-safe)

### "Database size too large"

Expected: ~100-120 KB per trading day

If larger, check if you're using `--adjusted` flag (fetches pre-adjusted prices from KRX).

### "ImportError: DLL load failed" (Windows)

Always use `poetry run`:

```bash
# ✅ Correct
poetry run python scripts/build_db.py ...

# ❌ Wrong (uses system Python, not venv)
python scripts/build_db.py ...
```

---

## 📚 Next Steps

Once your database is built:

1. **Query the data** (coming soon: `storage/query.py`)
2. **Build universes** (coming soon: `pipelines/universe_builder.py`)
3. **Use DataLoader API** (coming soon: `apis/dataloader.py`)

For more details, see:
- `scripts/README.md` - Detailed script documentation
- `docs/vibe_coding/architecture.md` - System architecture
- `docs/vibe_coding/prd.md` - Product requirements

---

**You're ready to build your KRX database!** 🚀

Start with:
```bash
poetry run python scripts/build_db.py --days 30 --db ./data/krx_db
```

