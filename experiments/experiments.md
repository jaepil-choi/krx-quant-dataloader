# Experiment Log

## Experiment: Samsung Stock Split 2018 (exp_samsung_stock_split_2018.py)

**Date:** 2025-10-03  
**Status:** ✅ Completed (identified critical issues)  
**Objective:** Validate end-to-end pipeline with real corporate action (Samsung 50:1 split on 2018-05-04)

### Results

#### ✅ What Worked
1. **API Fetch & Storage:** Successfully fetched 23,115 rows across 10 trading days with rate limiting
2. **Parquet Storage:** Hive partitioning working correctly
3. **Query Layer:** Symbol filtering, column pruning, partition pruning all functional
4. **Adjustment Factor Computation:** Correctly detected split with factor = 0.02 (1/50)
5. **Corporate Action Detection:** Clear discontinuity in raw prices (₩2,650,000 → ₩51,900)

#### ❌ Critical Issues Discovered

### Issue #1: Non-Existent Columns in Schema

**Problem:**
```
Raw Price Data - Samsung Electronics]
  TRD_DD ISU_ABBRV  OPNPRC  HGPRC  LWPRC  TDD_CLSPRC  BAS_PRC
20180425      삼성전자     NaN    NaN    NaN     2520000  2523000
```

Columns `OPNPRC`, `HGPRC`, `LWPRC` show as `NaN` because they **don't exist** in the `stock.all_change_rates` endpoint.

**Root Cause:**
- **Config** (`tests/fixtures/test_config.yaml` lines 169-178): Endpoint only returns:
  - ISU_SRT_CD, ISU_ABBRV, BAS_PRC, TDD_CLSPRC, CMPPREVDD_PRC, FLUC_RT, ACC_TRDVOL, ACC_TRDVAL, FLUC_TP
- **Schema** (`src/krx_quant_dataloader/storage/schema.py` lines 44-46): Incorrectly includes:
  ```python
  ('OPNPRC', pa.int64()),      # Open price - DOES NOT EXIST
  ('HGPRC', pa.int64()),       # High price - DOES NOT EXIST
  ('LWPRC', pa.int64()),       # Low price - DOES NOT EXIST
  ```
- **Preprocessing** (`src/krx_quant_dataloader/transforms/preprocessing.py` lines 60-62): Tries to parse:
  ```python
  shaped["OPNPRC"] = parse_int_krx(row.get("OPNPRC"))   # Returns None
  shaped["HGPRC"] = parse_int_krx(row.get("HGPRC"))     # Returns None
  shaped["LWPRC"] = parse_int_krx(row.get("LWPRC"))     # Returns None
  ```

**Impact:**
- Storage wastes space storing `NULL` columns
- Queries return confusing `NaN` values
- Users may think data is incomplete when it's actually correct

**Fix Required:**
1. Remove `OPNPRC`, `HGPRC`, `LWPRC` from `SNAPSHOTS_SCHEMA` in `storage/schema.py`
2. Remove parsing logic from `preprocess_change_rates_row()` in `transforms/preprocessing.py`
3. If OHLC (Open/High/Low/Close) data is needed, use a different endpoint (e.g., `stock.individual_history` or `MDCSTAT01501`)

---

### Issue #2: Adjustment Factor Application Logic

**Problem:**
Current adjustment factor computation is correct (`0.02` for 50:1 split), but the **application logic is wrong**.

**Desired Behavior:**
```python
# Simple per-row multiplication
adj_close_t = raw_close_t * adj_factor_t
```

**Current Behavior:**
```python
# Complex cumulative product (backward from latest date)
CUM_ADJ = ADJ_FACTOR_NUM[::-1].cumprod()[::-1]
adj_close_t = raw_close_t * CUM_ADJ_t
```

**Why Current Logic is Wrong:**

The experiment output shows:
```
    Date  Raw Close  Daily Factor  Cumulative Factor  Adjusted Close
20180425    2520000          1.00               0.02           50400  ← WRONG
20180426    2607000          1.00               0.02           52140  ← WRONG
20180427    2650000          1.00               0.02           53000  ← WRONG
20180430    2650000          1.00               0.02           53000  ← WRONG
20180502    2650000          1.00               0.02           53000  ← WRONG
20180503    2650000          1.00               0.02           53000  ← WRONG
20180504      51900          0.02               0.02            1038  ← WRONG
20180508      52600          1.00               1.00           52600  ← Correct
20180509      50900          1.00               1.00           50900  ← Correct
20180510      51600          1.00               1.00           51600  ← Correct
```

**Expected Behavior:**
```
    Date  Raw Close  Daily Factor  Adjusted Close
20180425    2520000          1.00        2520000  ← Keep historical prices as-is
20180426    2607000          1.00        2607000
20180427    2650000          1.00        2650000
20180430    2650000          1.00        2650000
20180502    2650000          1.00        2650000
20180503    2650000          1.00        2650000
20180504      51900          0.02           1038  ← Adjusted for split
20180508      52600          1.00          52600  ← Post-split, no adjustment
20180509      50900          1.00          50900
20180510      51600          1.00          51600
```

Wait... that's also wrong. Let me reconsider.

**The Real Problem:**

The adjustment factor formula is:
```
adj_factor_t = BAS_PRC_t / TDD_CLSPRC_{t-1}
```

For Samsung on 2018-05-04:
```
adj_factor = 53,000 / 2,650,000 = 0.02
```

This factor represents **how much today's base price changed relative to yesterday's close**. For a 50:1 split, the base price should be 1/50th of the previous close.

**Two Adjustment Approaches:**

1. **Forward Adjustment (multiply historical prices by split ratio):**
   - Historical prices × 0.02 = comparable to current scale
   - Pre-split: ₩2,650,000 × 0.02 = ₩53,000
   - This is what most backtesting platforms do

2. **Backward Adjustment (divide current prices by split ratio):**
   - Current prices ÷ 0.02 = comparable to historical scale
   - Post-split: ₩52,600 ÷ 0.02 = ₩2,630,000

**Current Implementation Does Neither Correctly:**
- The cumulative product approach tries to do forward adjustment but applies factors incorrectly
- The per-day factor approach (`adj_close_t = raw_close_t * adj_factor_t`) doesn't work because:
  - Days **before** split have `adj_factor = 1.0` (no change)
  - Split day has `adj_factor = 0.02` (50:1 split signal)
  - Days **after** split have `adj_factor = 1.0` (no change)

**What We Need:**

The adjustment factor should NOT be applied per-day. Instead:

1. **Store adjustment factors as "event markers"** (what we're doing now is correct ✅)
2. **Compute cumulative adjustment multiplier on-the-fly** when user requests adjusted prices

**Correct Forward Adjustment Logic:**

For each date, compute `product_of_all_future_factors` (cumulative product from that date to latest):

```
Example (Samsung 2018 split):
  TRD_DD  |  adj_factor  |  product_of_all_future_factors  |  Adjusted Close
----------|--------------|----------------------------------|------------------
20180425  |     1.00     |  0.02 = 1 × 1 × 1 × 1 × 1 × 0.02 |  2,520,000 × 0.02 = 50,400
20180426  |     1.00     |  0.02 = 1 × 1 × 1 × 1 × 0.02     |  2,607,000 × 0.02 = 52,140
20180427  |     1.00     |  0.02 = 1 × 1 × 1 × 0.02         |  2,650,000 × 0.02 = 53,000
20180430  |     1.00     |  0.02 = 1 × 1 × 0.02             |  2,650,000 × 0.02 = 53,000
20180502  |     1.00     |  0.02 = 1 × 0.02                 |  2,650,000 × 0.02 = 53,000
20180503  |     1.00     |  0.02 = 0.02                     |  2,650,000 × 0.02 = 53,000
20180504  |     0.02     |  1.00 = 1 × 1 × 1                |     51,900 × 1.00 = 51,900
20180508  |     1.00     |  1.00 = 1 × 1                    |     52,600 × 1.00 = 52,600
20180509  |     1.00     |  1.00 = 1                        |     50,900 × 1.00 = 50,900
20180510  |     1.00     |  1.00                            |     51,600 × 1.00 = 51,600
```

**Algorithm:**
```python
# Per-symbol, in chronological order
factors = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.02, 1.0, 1.0, 1.0]
cum_adj = []

# Compute cumulative product from RIGHT to LEFT (future → past)
cum = 1.0
for factor in reversed(factors):
    cum *= factor
    cum_adj.insert(0, cum)

# Result: [0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 1.0, 1.0, 1.0, 1.0]
```

**Why This Works:**
- Historical prices (pre-split) are scaled down to match current prices
- Prices become continuous: ~₩53,000 before and after split
- Time series analysis (returns, volatility, etc.) remains valid

**Fix Required:**

**Option B: Compute on-the-fly in high-level API (RECOMMENDED)**

**Where to implement:**
- High-level `DataLoader.get_data(adjusted=True)` API
- Compute cumulative multiplier in-memory (temporary)
- Apply to raw prices before returning DataFrame
- No persistent storage of cumulative factors (recomputed each query)

**Advantages:**
- ✅ Keeps storage simple (only raw prices + daily event markers)
- ✅ Flexible (can add backward adjustment later if needed)
- ✅ Efficient (computation is fast: O(n) per symbol)
- ✅ No cache invalidation issues
- ✅ User-facing API hides complexity

**Implementation Strategy:**
1. User calls `DataLoader.get_data(symbols, start_date, end_date, adjusted=True)`
2. Query raw prices from `snapshots` table
3. Query adjustment factors from `adj_factors` table
4. Per-symbol:
   - Sort factors chronologically
   - Compute cumulative product (reverse iteration)
   - Multiply raw prices by cumulative multipliers
5. Return adjusted DataFrame

**Storage:**
- Raw prices: Parquet (persistent)
- Adjustment factors (event markers): Parquet (persistent)
- Cumulative multipliers: In-memory (temporary, computed per query)
- Adjusted prices: In-memory (temporary, computed per query)

On exit: Nothing to clean up (all temporary data in memory)

---

### Action Items

**Before Production:**
1. ✅ Remove non-existent OHLC columns from schema and preprocessing
2. ✅ Fix adjustment factor application logic (cumulative multiplier)
3. ✅ Add test case for adjusted price continuity validation
4. ✅ Document adjustment methodology (forward vs backward)

**Status:** ✅ Complete - Production code committed with 22 passing tests.

---

## Experiment 2: Cumulative Adjustments Pipeline Validation

**Date**: 2025-10-04  
**Status**: ✅ Complete  
**Script**: `experiments/exp_cumulative_adjustments.py`

### Objective

Validate the cumulative adjustments pipeline (Stage 5) end-to-end:
- Compute cumulative adjustments from adjustment factors
- Write to ephemeral cache (`data/temp/`)
- Read back and validate
- Apply to prices and verify continuity

### Critical Discovery: Split Day Exclusion Rule

**Problem**: Initial implementation included split day's factor in its own cumulative multiplier.

**Impact**: 
- Split day (2018-05-04) had cum_adj = 0.02
- Adjusted price = 51,900 × 0.02 = 1,038
- **98.04% discontinuity** with previous day (53,000)

**Root Cause**:
```python
# WRONG: Multiply THEN store
cum_product *= adj_factor
cum_multipliers.insert(0, float(cum_product))
```

**Correct Algorithm**:
```python
# CORRECT: Store THEN multiply
cum_multipliers.insert(0, float(cum_product))  # Store current value
cum_product *= adj_factor                      # Affects earlier dates only
```

**Why This Works**:
- Date T's `adj_factor` describes the transition FROM T-1 TO T
- Date T's **close price** is ALREADY in post-adjustment scale
- Therefore, date T's cum_adj should = product of **future** factors only
- Including T's own factor would double-adjust the close price

**Result After Fix**:
- Pre-split (2018-05-03): 2,650,000 × 0.02 = 53,000
- Split day (2018-05-04): 51,900 × 1.0 = 51,900
- **Price change: 2.08%** (continuous!) ✅

### Test Coverage

**Unit Tests** (`test_cumulative_adjustments_unit.py`):
- 15 tests with synthetic and real data
- Edge cases: None factors, extreme splits (100:1), multiple symbols
- Date ordering validation, precision checks

**Live Smoke Tests** (`test_cumulative_adjustments_live_smoke.py`):
- 7 end-to-end tests with real Samsung data
- Validates: compute → write → read → price application
- Confirms price continuity (2.08% change across split)

**Results**: ✅ 22/22 tests passing

### Key Learnings

1. **Split Day Exclusion is Critical**
   - Date T's cum_adj must exclude T's own adj_factor
   - Otherwise, close prices are double-adjusted

2. **Precision Maintained**
   - `Decimal` for computation (arbitrary precision)
   - `float64` for storage (sufficient for 1e-6)
   - No precision loss in round-trip

3. **Schema Design for Hive Partitioning**
   - Partition keys (like `TRD_DD`) should NOT be in data columns
   - Writer strips partition keys before PyArrow conversion
   - Query layer injects partition keys when reading

4. **Ephemeral Cache Works**
   - Write to `data/temp/cumulative_adjustments/`
   - Hive-partitioned by `TRD_DD`
   - Read-back matches computed values exactly

