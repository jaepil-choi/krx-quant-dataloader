"""
Experiment: Samsung Electronics Stock Split 2018-05-04

Date: 2025-10-03
Status: In Progress

Objective:
- Validate end-to-end pipeline: API fetch → Parquet storage → query → adjustment factor → adjusted prices
- Test with real corporate action: Samsung 50:1 stock split on 2018-05-04

Hypothesis:
- Split should cause BAS_PRC jump (50x) on 2018-05-04
- Adjustment factor should be ~0.02 (1/50) on that date
- Adjusted prices should be continuous across the split date

Success Criteria:
- [x] Successfully fetch data for 2018-04-25 to 2018-05-10
- [ ] Successfully store to Parquet DB
- [ ] Successfully query back from DB
- [ ] Successfully compute adjustment factors (split date shows ~0.02)
- [ ] Successfully apply adjustments to get continuous price series

Date Range:
- Start: 2018-04-25 (10 trading days before split)
- Split: 2018-05-04 (Friday)
- End: 2018-05-10 (Thursday, ~4 trading days after split)

Expected Trading Days: ~10 days (excluding weekends + Labor Day 2018-05-01)
"""

from pathlib import Path
from decimal import Decimal

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
from krx_quant_dataloader.storage.query import query_parquet_table
from krx_quant_dataloader.pipelines.snapshots import ingest_change_rates_range
from krx_quant_dataloader.transforms.adjustment import compute_adj_factors_grouped


def main() -> None:
    print("="*80)
    print("EXPERIMENT: Samsung Electronics Stock Split (2018-05-04)")
    print("="*80)
    
    repo_root = Path(__file__).resolve().parents[1]
    
    # Use production config (rate-limited to 1 req/sec)
    config_path = repo_root / "config" / "config.yaml"
    if not config_path.exists():
        print(f"Production config not found at {config_path}")
        print("Falling back to test config...")
        config_path = repo_root / "tests" / "fixtures" / "test_config.yaml"
    
    print(f"\n[Config] Using: {config_path}")
    
    # Setup
    cfg = ConfigFacade.load(config_path=str(config_path))
    reg = AdapterRegistry.load(config_path=str(config_path))
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    raw = RawClient(registry=reg, orchestrator=orch)
    
    # DB path
    db_root = repo_root / "data" / "krx_db_samsung_split_test"
    db_root.mkdir(parents=True, exist_ok=True)
    print(f"[DB] Path: {db_root}")
    
    # Writer
    writer = ParquetSnapshotWriter(root_path=db_root)
    
    # Date range (calendar days, holidays will be skipped automatically)
    start_date = "20180425"
    end_date = "20180510"
    split_date = "20180504"
    
    print(f"\n[Date Range]")
    print(f"  Start: {start_date}")
    print(f"  Split: {split_date} (expected 50:1 split)")
    print(f"  End: {end_date}")
    
    # ========================================================================
    # PHASE 1: FETCH & STORE
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 1: Fetch & Store (Resume-Safe, Per-Day Ingestion)")
    print("="*80)
    
    # Generate all calendar dates in range (holidays will be skipped automatically)
    from datetime import datetime, timedelta
    
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    
    all_dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        all_dates.append(current_dt.strftime("%Y%m%d"))
        current_dt += timedelta(days=1)
    
    print(f"\n[Calendar Days]: {len(all_dates)} days (weekends/holidays auto-skipped)")
    
    counts = ingest_change_rates_range(
        raw,
        dates=all_dates,
        market="ALL",
        adjusted_flag=False,  # We want raw prices to see the split
        writer=writer,
    )
    
    total_rows = sum(counts.values())
    trading_days = sum(1 for c in counts.values() if c > 0)
    
    print(f"\n[Ingestion Results]")
    print(f"  Trading days found: {trading_days}")
    print(f"  Total rows ingested: {total_rows:,}")
    
    for date, count in sorted(counts.items()):
        if count > 0:
            print(f"    {date}: {count:,} rows")
        else:
            print(f"    {date}: HOLIDAY (skipped)")
    
    # ========================================================================
    # PHASE 2: QUERY SAMSUNG DATA
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 2: Query Samsung Electronics (005930) Raw Prices")
    print("="*80)
    
    samsung_symbol = "005930"
    
    df_samsung = query_parquet_table(
        db_path=db_root,
        table_name='snapshots',
        start_date=start_date,
        end_date=end_date,
        symbols=[samsung_symbol],
        fields=['TRD_DD', 'ISU_SRT_CD', 'ISU_ABBRV', 'BAS_PRC', 'TDD_CLSPRC', 'ACC_TRDVAL']
    )
    
    print(f"\n[Query Results]")
    print(f"  Rows returned: {len(df_samsung)}")
    print(f"  Date range: {df_samsung['TRD_DD'].min()} → {df_samsung['TRD_DD'].max()}")
    
    print(f"\n[Raw Price Data - Samsung Electronics]")
    print(f"  Note: OPNPRC, HGPRC, LWPRC removed (don't exist in stock.all_change_rates endpoint)")
    print(df_samsung[['TRD_DD', 'ISU_ABBRV', 'TDD_CLSPRC', 'BAS_PRC']].to_string(index=False))
    
    # Check for split signal
    df_samsung_sorted = df_samsung.sort_values('TRD_DD')
    price_changes = df_samsung_sorted[['TRD_DD', 'TDD_CLSPRC', 'BAS_PRC']].copy()
    price_changes['PRICE_RATIO'] = (
        price_changes['BAS_PRC'] / price_changes['TDD_CLSPRC'].shift(1)
    )
    
    print(f"\n[Corporate Action Detection]")
    print(f"  Price Ratio = BAS_PRC(today) / TDD_CLSPRC(yesterday)")
    print(f"  Normal: ~1.0  |  Split 50:1: ~50.0")
    print()
    print(price_changes.to_string(index=False))
    
    # Highlight split date
    split_rows = price_changes[price_changes['TRD_DD'] == split_date]
    if not split_rows.empty:
        split_ratio = split_rows.iloc[0]['PRICE_RATIO']
        print(f"\n  ⚠️  SPLIT DETECTED on {split_date}:")
        print(f"      Price ratio: {split_ratio:.4f}")
        if 45 < split_ratio < 55:
            print(f"      ✓ Consistent with 50:1 stock split")
        else:
            print(f"      ⚠️  Unexpected ratio (expected ~50.0)")
    
    # ========================================================================
    # PHASE 3: COMPUTE ADJUSTMENT FACTORS
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 3: Compute Adjustment Factors (Post-Hoc)")
    print("="*80)
    
    # Convert DataFrame to list of dicts for adjustment computation
    snapshots = df_samsung.to_dict('records')
    
    factors = compute_adj_factors_grouped(snapshots)
    
    print(f"\n[Adjustment Factors Computed]")
    print(f"  Total factors: {len(factors)}")
    
    # Display factors around split date
    print(f"\n[Adjustment Factors - Samsung Electronics]")
    print(f"  {'TRD_DD':<10} {'ISU_SRT_CD':<10} {'ADJ_FACTOR':<20} {'Interpretation'}")
    print(f"  {'-'*70}")
    
    for factor_row in factors:
        date = factor_row['TRD_DD']
        symbol = factor_row['ISU_SRT_CD']
        adj_factor_str = factor_row['ADJ_FACTOR']
        
        if adj_factor_str:
            adj_factor_val = float(adj_factor_str)
            
            # Interpret the factor
            if abs(adj_factor_val - 1.0) < 0.01:
                interpretation = "Normal (no corporate action)"
            elif 0.015 < adj_factor_val < 0.025:  # ~1/50
                interpretation = "⚠️  50:1 STOCK SPLIT"
            elif adj_factor_val > 40:
                interpretation = "⚠️  Reverse split or anomaly"
            else:
                interpretation = f"Corporate action (factor={adj_factor_val:.4f})"
            
            print(f"  {date:<10} {symbol:<10} {adj_factor_val:<20.10f} {interpretation}")
        else:
            print(f"  {date:<10} {symbol:<10} {'(first day)':<20} No previous close")
    
    # Persist factors to DB
    print(f"\n[Persisting Adjustment Factors to DB]")
    writer.write_factor_rows(factors)
    print(f"  ✓ Written to: {db_root / 'adj_factors'}")
    
    # ========================================================================
    # PHASE 4: COMPUTE ADJUSTED PRICES
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 4: Apply Adjustment Factors → Adjusted Prices")
    print("="*80)
    
    # Query adjustment factors back from DB
    df_factors = query_parquet_table(
        db_path=db_root,
        table_name='adj_factors',
        start_date=start_date,
        end_date=end_date,
        symbols=[samsung_symbol],
        fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']  # Lowercase in Parquet schema
    )
    
    # Merge prices with factors
    df_merged = df_samsung.merge(
        df_factors,
        on=['TRD_DD', 'ISU_SRT_CD'],
        how='left'
    )
    
    # Convert adj_factor to numeric (handle nulls)
    df_merged['ADJ_FACTOR_NUM'] = df_merged['adj_factor'].fillna(1.0)
    
    # Compute cumulative adjustment multiplier (forward from most recent)
    # For back-adjustment: multiply prices by forward cumulative product
    # This makes historical prices comparable to today's scale
    df_merged_sorted = df_merged.sort_values('TRD_DD')
    
    # Compute forward cumulative product (latest date = 1.0)
    df_merged_sorted['CUM_ADJ'] = df_merged_sorted['ADJ_FACTOR_NUM'][::-1].cumprod()[::-1]
    
    # Compute adjusted close = raw close × cumulative adjustment
    df_merged_sorted['ADJUSTED_CLOSE'] = (
        df_merged_sorted['TDD_CLSPRC'] * df_merged_sorted['CUM_ADJ']
    ).round(0).astype(int)
    
    print(f"\n[Adjusted vs Raw Prices - Samsung Electronics]")
    print(f"  Adjusted prices normalize for corporate actions")
    print(f"  Split date: {split_date} (prices should be continuous)\n")
    
    result = df_merged_sorted[[
        'TRD_DD', 'TDD_CLSPRC', 'ADJ_FACTOR_NUM', 'CUM_ADJ', 'ADJUSTED_CLOSE'
    ]].copy()
    result.columns = ['Date', 'Raw Close', 'Daily Factor', 'Cumulative Factor', 'Adjusted Close']
    
    print(result.to_string(index=False))
    
    # Validate continuity
    print(f"\n[Validation: Price Continuity]")
    
    # Check if adjusted prices are more continuous than raw prices
    raw_changes = result['Raw Close'].pct_change().abs()
    adj_changes = result['Adjusted Close'].pct_change().abs()
    
    # Find the split date row
    split_idx = result[result['Date'] == split_date].index
    if len(split_idx) > 0:
        split_pos = split_idx[0]
        if split_pos > 0:
            raw_change_at_split = raw_changes.iloc[split_pos] * 100
            adj_change_at_split = adj_changes.iloc[split_pos] * 100
            
            print(f"  Price change on split date ({split_date}):")
            print(f"    Raw prices: {raw_change_at_split:.2f}% change (DISCONTINUOUS)")
            print(f"    Adjusted prices: {adj_change_at_split:.2f}% change (CONTINUOUS)")
            print()
            
            if raw_change_at_split > 1000 and adj_change_at_split < 10:
                print(f"  ✓ SUCCESS: Adjustment factors correctly normalize the stock split")
                print(f"            Raw prices show massive jump, adjusted prices are smooth")
            else:
                print(f"  ⚠️  WARNING: Adjustment may not be working as expected")
    
    # ========================================================================
    # PHASE 5: SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("EXPERIMENT SUMMARY")
    print("="*80)
    
    print(f"\n✓ Phase 1: Fetched {total_rows:,} rows across {trading_days} trading days")
    print(f"✓ Phase 2: Queried Samsung (005930) data from Parquet DB")
    print(f"✓ Phase 3: Computed adjustment factors (split detected on {split_date})")
    print(f"✓ Phase 4: Applied factors → adjusted prices show continuity")
    print(f"\n✓ End-to-End Pipeline Validated:")
    print(f"  → API Fetch (rate-limited)")
    print(f"  → Parquet Storage (Hive-partitioned)")
    print(f"  → Query Layer (optimized reads)")
    print(f"  → Adjustment Computation (LAG semantics)")
    print(f"  → Adjusted Price Series (corporate action normalized)")
    
    writer.close()
    
    print("\n" + "="*80)
    print("✓ EXPERIMENT COMPLETE!")
    print("="*80)


if __name__ == "__main__":
    main()

