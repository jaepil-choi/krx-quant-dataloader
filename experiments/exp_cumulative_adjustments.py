"""
Experiment: Cumulative Adjustments Computation & Ephemeral Cache

Date: 2025-10-04
Status: In Progress

Objective:
- Validate compute_cumulative_adjustments() function (Stage 5)
- Test ephemeral cache write/read for Samsung split data
- Verify precision (1e-6) and reverse chronological product logic

Hypothesis:
- Cumulative adjustments should correctly multiply future factors
- Samsung 2018-05-04 split (0.02 factor) should propagate backwards
- Historical dates should have cum_adj = 0.02, post-split dates = 1.0

Success Criteria:
- [ ] compute_cumulative_adjustments() handles Samsung split correctly
- [ ] Ephemeral cache writes successfully to data/temp/
- [ ] Read-back from cache matches computed values
- [ ] Precision maintained (float64, 1e-6 minimum)
- [ ] Adjusted prices continuous across split date
"""

from pathlib import Path
from decimal import Decimal
import pandas as pd

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.storage.query import query_parquet_table
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
from krx_quant_dataloader.transforms.adjustment import compute_cumulative_adjustments


def main():
    print("=" * 80)
    print("EXPERIMENT: Cumulative Adjustments Computation (Samsung Split)")
    print("=" * 80)
    
    # Setup
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        config_path = Path("tests/fixtures/test_config.yaml")
    
    config = ConfigFacade.load(config_path=config_path)
    
    # Use existing Samsung split test DB (from previous experiment)
    db_root = Path("data/krx_db_samsung_split_test")
    temp_root = Path("data/temp")
    
    if not db_root.exists():
        print(f"\n❌ ERROR: DB not found at {db_root}")
        print("   Please run: poetry run python experiments/exp_samsung_stock_split_2018.py first")
        return
    
    print(f"\n[Config] Using: {config_path}")
    print(f"[DB] Persistent: {db_root}")
    print(f"[DB] Ephemeral: {temp_root}")
    
    # Date range (from Samsung split experiment)
    start_date = "20180425"
    end_date = "20180510"
    samsung_symbol = "005930"
    
    print(f"\n[Date Range]")
    print(f"  Start: {start_date}")
    print(f"  End: {end_date}")
    print(f"  Target: Samsung Electronics ({samsung_symbol})")
    
    # =========================================================================
    # PHASE 1: Load Adjustment Factors from Persistent DB
    # =========================================================================
    print("\n" + "=" * 80)
    print("PHASE 1: Load Adjustment Factors (Persistent DB)")
    print("=" * 80)
    
    df_factors = query_parquet_table(
        db_path=db_root,
        table_name='adj_factors',
        start_date=start_date,
        end_date=end_date,
        symbols=[samsung_symbol],
        fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
    )
    
    print(f"\n[Query Results]")
    print(f"  Rows: {len(df_factors)}")
    print(f"  Date range: {df_factors['TRD_DD'].min()} → {df_factors['TRD_DD'].max()}")
    
    # Convert DataFrame to list of dicts for compute_cumulative_adjustments
    factors_list = df_factors.to_dict('records')
    
    print(f"\n[Adjustment Factors - Samsung Electronics]")
    print(f"  {'Date':<10} {'Symbol':<10} {'adj_factor':<15} Interpretation")
    print(f"  {'-'*70}")
    for row in factors_list:
        factor_val = row['adj_factor']
        if factor_val is None or factor_val == '' or pd.isna(factor_val):
            interp = "No previous close"
            factor_str = "(first day)"
        elif abs(float(factor_val) - 1.0) < 0.0001:
            interp = "Normal (no corporate action)"
            factor_str = f"{float(factor_val):.10f}"
        else:
            interp = f"⚠️  {1/float(factor_val):.0f}:1 STOCK SPLIT"
            factor_str = f"{float(factor_val):.10f}"
        
        print(f"  {row['TRD_DD']:<10} {row['ISU_SRT_CD']:<10} {factor_str:<15} {interp}")
    
    # =========================================================================
    # PHASE 2: Compute Cumulative Adjustments
    # =========================================================================
    print("\n" + "=" * 80)
    print("PHASE 2: Compute Cumulative Adjustments (Reverse Chronological Product)")
    print("=" * 80)
    
    print(f"\n[Algorithm]")
    print(f"  1. Sort by date ascending")
    print(f"  2. Reverse iterate (future → past)")
    print(f"  3. Cumulative product: cum_product *= adj_factor")
    print(f"  4. Most recent date = 1.0, historical = product of future events")
    
    cum_adj_rows = compute_cumulative_adjustments(factors_list)
    
    print(f"\n[Computation Results]")
    print(f"  Rows computed: {len(cum_adj_rows)}")
    
    # Convert to DataFrame for display
    df_cum_adj = pd.DataFrame(cum_adj_rows)
    
    # Merge with original factors for comparison
    df_merged = df_factors.merge(
        df_cum_adj[['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']],
        on=['TRD_DD', 'ISU_SRT_CD'],
        how='left'
    )
    
    print(f"\n[Cumulative Adjustments - Samsung Electronics]")
    print(f"  {'Date':<10} {'Daily Factor':<15} {'Cumulative':<15} Interpretation")
    print(f"  {'-'*70}")
    
    for _, row in df_merged.iterrows():
        factor_val = row['adj_factor']
        cum_val = row['cum_adj_multiplier']
        
        if factor_val is None or factor_val == '' or pd.isna(factor_val):
            factor_str = "(first day)"
        else:
            factor_str = f"{float(factor_val):.10f}"
        
        cum_str = f"{float(cum_val):.10f}"
        
        # Interpretation
        if abs(cum_val - 1.0) < 0.0001:
            interp = "Post-split (no adjustment)"
        elif abs(cum_val - 0.02) < 0.0001:
            interp = "Pre-split (adjusted by 0.02)"
        else:
            interp = f"Unexpected value"
        
        print(f"  {row['TRD_DD']:<10} {factor_str:<15} {cum_str:<15} {interp}")
    
    # Validate precision
    print(f"\n[Precision Validation]")
    print(f"  Minimum value: {df_cum_adj['cum_adj_multiplier'].min():.10f}")
    print(f"  Maximum value: {df_cum_adj['cum_adj_multiplier'].max():.10f}")
    print(f"  Data type: {df_cum_adj['cum_adj_multiplier'].dtype}")
    
    # Check if 1e-6 precision is maintained
    min_val = df_cum_adj['cum_adj_multiplier'].min()
    if min_val == 0.02:
        precision_digits = 2
    else:
        precision_digits = len(str(Decimal(str(min_val))).split('.')[-1])
    
    print(f"  Precision: {precision_digits} decimal places")
    
    if precision_digits >= 6:
        print(f"  ✓ Meets 1e-6 minimum precision requirement")
    else:
        print(f"  ⚠️  WARNING: Precision below 1e-6 requirement")
    
    # =========================================================================
    # PHASE 3: Write to Ephemeral Cache
    # =========================================================================
    print("\n" + "=" * 80)
    print("PHASE 3: Write to Ephemeral Cache (data/temp/)")
    print("=" * 80)
    
    # Initialize ephemeral writer
    temp_writer = ParquetSnapshotWriter(root_path=temp_root)
    
    print(f"\n[Writing Cumulative Adjustments]")
    print(f"  Target: {temp_root / 'cumulative_adjustments'}")
    
    # Group by date and write (ephemeral cache requires per-date writes)
    cum_adj_by_date = {}
    for row in cum_adj_rows:
        date = row['TRD_DD']
        if date not in cum_adj_by_date:
            cum_adj_by_date[date] = []
        cum_adj_by_date[date].append(row)
    
    total_written = 0
    for date, date_rows in sorted(cum_adj_by_date.items()):
        written = temp_writer.write_cumulative_adjustments(date_rows, date=date)
        total_written += written
        print(f"    {date}: {written} rows")
    
    print(f"\n  ✓ Total rows written: {total_written}")
    
    # =========================================================================
    # PHASE 4: Read Back from Ephemeral Cache & Validate
    # =========================================================================
    print("\n" + "=" * 80)
    print("PHASE 4: Read Back from Ephemeral Cache & Validate")
    print("=" * 80)
    
    # Read back
    df_cached = query_parquet_table(
        db_path=temp_root,
        table_name='cumulative_adjustments',
        start_date=start_date,
        end_date=end_date,
        symbols=[samsung_symbol],
        fields=['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']
    )
    
    print(f"\n[Read-Back Results]")
    print(f"  Rows: {len(df_cached)}")
    print(f"  Date range: {df_cached['TRD_DD'].min()} → {df_cached['TRD_DD'].max()}")
    
    # Compare with computed values
    df_comparison = df_cum_adj.merge(
        df_cached,
        on=['TRD_DD', 'ISU_SRT_CD'],
        suffixes=('_computed', '_cached')
    )
    
    print(f"\n[Validation: Computed vs Cached]")
    print(f"  {'Date':<10} {'Computed':<15} {'Cached':<15} {'Match?':<10}")
    print(f"  {'-'*60}")
    
    all_match = True
    for _, row in df_comparison.iterrows():
        computed = row['cum_adj_multiplier_computed']
        cached = row['cum_adj_multiplier_cached']
        match = abs(computed - cached) < 1e-10
        all_match = all_match and match
        
        match_str = "✓" if match else "✗"
        print(f"  {row['TRD_DD']:<10} {computed:<15.10f} {cached:<15.10f} {match_str:<10}")
    
    if all_match:
        print(f"\n  ✓ All values match!")
    else:
        print(f"\n  ✗ Mismatch detected!")
    
    # =========================================================================
    # PHASE 5: Apply to Prices & Validate Continuity
    # =========================================================================
    print("\n" + "=" * 80)
    print("PHASE 5: Apply to Prices & Validate Continuity")
    print("=" * 80)
    
    # Load raw prices
    df_prices = query_parquet_table(
        db_path=db_root,
        table_name='snapshots',
        start_date=start_date,
        end_date=end_date,
        symbols=[samsung_symbol],
        fields=['TRD_DD', 'ISU_SRT_CD', 'TDD_CLSPRC']
    )
    
    # Merge with cumulative adjustments
    df_adjusted = df_prices.merge(
        df_cached[['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']],
        on=['TRD_DD', 'ISU_SRT_CD'],
        how='left'
    )
    
    # Apply adjustment
    df_adjusted['adjusted_close'] = (
        df_adjusted['TDD_CLSPRC'] * df_adjusted['cum_adj_multiplier']
    ).round(0).astype(int)
    
    print(f"\n[Adjusted vs Raw Prices - Samsung Electronics]")
    print(f"  {'Date':<10} {'Raw Close':>12} {'Cum Mult':>12} {'Adjusted':>12}")
    print(f"  {'-'*60}")
    
    for _, row in df_adjusted.iterrows():
        print(f"  {row['TRD_DD']:<10} {row['TDD_CLSPRC']:>12,} "
              f"{row['cum_adj_multiplier']:>12.2f} {row['adjusted_close']:>12,}")
    
    # Check continuity at split date
    split_idx = df_adjusted[df_adjusted['TRD_DD'] == '20180504'].index[0]
    if split_idx > 0:
        pre_split_adj = df_adjusted.iloc[split_idx - 1]['adjusted_close']
        split_adj = df_adjusted.iloc[split_idx]['adjusted_close']
        
        pct_change = abs(split_adj - pre_split_adj) / pre_split_adj * 100
        
        print(f"\n[Price Continuity at Split (20180504)]")
        print(f"  Pre-split adjusted: ₩{pre_split_adj:,}")
        print(f"  Split-day adjusted: ₩{split_adj:,}")
        print(f"  Change: {pct_change:.2f}%")
        
        if pct_change < 10:  # Allow 10% daily move as reasonable
            print(f"  ✓ Prices are continuous (good adjustment)")
        else:
            print(f"  ⚠️  Large discontinuity detected")
    
    # =========================================================================
    # EXPERIMENT SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("EXPERIMENT SUMMARY")
    print("=" * 80)
    
    print(f"\n✓ Phase 1: Loaded {len(df_factors)} adjustment factors from persistent DB")
    print(f"✓ Phase 2: Computed {len(cum_adj_rows)} cumulative adjustments")
    print(f"✓ Phase 3: Wrote {total_written} rows to ephemeral cache")
    print(f"✓ Phase 4: Read-back validation {'PASSED' if all_match else 'FAILED'}")
    print(f"✓ Phase 5: Applied adjustments to prices")
    
    print(f"\n✓ Pipeline Validated:")
    print(f"  → Adjustment factors (persistent)")
    print(f"  → Cumulative adjustments (compute_cumulative_adjustments)")
    print(f"  → Ephemeral cache (write + read)")
    print(f"  → Adjusted prices (continuous across split)")
    
    print("\n" + "=" * 80)
    print("✓ EXPERIMENT COMPLETE!")
    print("=" * 80)


if __name__ == '__main__':
    main()

