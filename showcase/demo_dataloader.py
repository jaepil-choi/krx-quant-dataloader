"""
DataLoader Showcase: Full Pipeline Demonstration

Simple demonstration of DataLoader with automatic 3-stage pipeline.

Usage:
    poetry run python showcase/demo_dataloader.py

This will:
1. Initialize DataLoader (auto-builds DB if missing)
2. Query various fields with universe filtering
3. Compare adjusted vs raw prices (Samsung 50:1 split on 2018-05-04)
4. Demonstrate wide-format output

Date range: 2018-04-25 to 2018-05-10 (includes Samsung split)
"""

from krx_quant_dataloader.apis.dataloader import DataLoader


def main():
    """Demonstrate DataLoader with Samsung split date range."""
    print("\n" + "="*70)
    print("DATALOADER SHOWCASE")
    print("="*70)
    print("\nDate range: 2018-04-25 to 2018-05-10")
    print("Includes: Samsung Electronics 50:1 stock split on 2018-05-04")
    
    # ==================================================================
    # INITIALIZE DATALOADER (Simple - handles everything automatically)
    # ==================================================================
    print("\n" + "="*70)
    print("1. Initialize DataLoader")
    print("="*70)
    print("\nSimple initialization - DataLoader handles everything:")
    
    loader = DataLoader(
        start_date='20180425',
        end_date='20180510'
    )
    
    print("\n[OK] DataLoader ready!")
    
    # ==================================================================
    # QUERY 1: All symbols, raw prices
    # ==================================================================
    print("\n" + "="*70)
    print("2. Query All Symbols (Raw Prices)")
    print("="*70)
    
    df_all = loader.get_data('close', adjusted=False)
    
    print(f"\nResult: {df_all.shape[0]} dates × {df_all.shape[1]} symbols")
    print(f"\nSample (first 5 symbols, first 5 dates):")
    print(df_all.iloc[:5, :5])
    
    # ==================================================================
    # QUERY 2: Top 100 universe
    # ==================================================================
    print("\n" + "="*70)
    print("3. Query with Universe Filter (univ100)")
    print("="*70)
    
    df_univ100 = loader.get_data('close', universe='univ100', adjusted=False)
    
    print(f"\nResult: {df_univ100.shape[0]} dates × {df_univ100.shape[1]} symbols")
    print(f"Universe size ≤ 100: {df_univ100.shape[1] <= 100}")
    print(f"\nSample (first 10 symbols, first 5 dates):")
    print(df_univ100.iloc[:5, :10])
    
    # ==================================================================
    # QUERY 3: Explicit symbols
    # ==================================================================
    print("\n" + "="*70)
    print("4. Query Specific Stocks")
    print("="*70)
    
    stocks = ['005930', '000660', '035720']  # Samsung, SK Hynix, Kakao
    df_stocks = loader.get_data('close', universe=stocks, adjusted=False)
    
    print(f"\nStocks: {stocks}")
    print(f"\nRaw prices:")
    print(df_stocks)
    
    # ==================================================================
    # QUERY 4: Samsung - Adjusted vs Raw (THE SPLIT!)
    # ==================================================================
    print("\n" + "="*70)
    print("5. Samsung Split Demonstration (CRITICAL TEST)")
    print("="*70)
    print("\nSamsung Electronics 50:1 split on 2018-05-04")
    print("- Before split (2018-05-03): ~2,500,000 won")
    print("- After split (2018-05-04):  ~50,000 won (÷50)")
    
    # Raw prices
    samsung_raw = loader.get_data('close', universe=['005930'], adjusted=False)
    
    # Adjusted prices
    samsung_adj = loader.get_data('close', universe=['005930'], adjusted=True)
    
    print(f"\n--- RAW PRICES (as-is from KRX) ---")
    print(samsung_raw)
    
    print(f"\n--- ADJUSTED PRICES (normalized to most recent scale) ---")
    print(samsung_adj)
    
    # Calculate the adjustment
    if '005930' in samsung_raw.columns and len(samsung_raw) > 0:
        split_date_idx = samsung_raw.index.get_loc('20180504') if '20180504' in samsung_raw.index else None
        if split_date_idx and split_date_idx > 0:
            before_split_raw = samsung_raw.iloc[split_date_idx - 1, 0]
            after_split_raw = samsung_raw.iloc[split_date_idx, 0]
            before_split_adj = samsung_adj.iloc[split_date_idx - 1, 0]
            after_split_adj = samsung_adj.iloc[split_date_idx, 0]
            
            print(f"\n--- SPLIT ANALYSIS ---")
            print(f"Day before split (2018-05-03):")
            print(f"  Raw:      KRW {before_split_raw:,}")
            print(f"  Adjusted: KRW {before_split_adj:,}")
            print(f"\nDay of split (2018-05-04):")
            print(f"  Raw:      KRW {after_split_raw:,}")
            print(f"  Adjusted: KRW {after_split_adj:,}")
            print(f"\nSplit ratio detected: {before_split_raw / after_split_raw:.1f}:1")
            print(f"Adjustment factor: {before_split_adj / before_split_raw:.6f}")
    
    # ==================================================================
    # QUERY 5: Different field (volume)
    # ==================================================================
    print("\n" + "="*70)
    print("6. Query Different Field (Trading Volume)")
    print("="*70)
    
    volume = loader.get_data('volume', universe=['005930'])
    
    print(f"\nSamsung trading volume:")
    print(volume)
    
    # ==================================================================
    # SUMMARY
    # ==================================================================
    print("\n" + "="*70)
    print("SHOWCASE COMPLETE")
    print("="*70)
    
    print(f"\n[OK] All features demonstrated:")
    print(f"  1. Simple initialization (no complex setup)")
    print(f"  2. Auto-ingestion if DB missing")
    print(f"  3. Universe filtering (univ100)")
    print(f"  4. Explicit symbol lists")
    print(f"  5. Adjusted vs raw prices (50:1 split detected!)")
    print(f"  6. Multiple fields (close, volume)")
    print(f"  7. Wide-format output (dates × symbols)")
    
    print(f"\nTypical usage:")
    print(f"  loader = DataLoader(start_date='20180101', end_date='20181231')")
    print(f"  prices = loader.get_data('close', universe='univ100', adjusted=True)")
    print(f"  returns = prices.pct_change()")
    
    print("\n" + "="*70 + "\n")


if __name__ == '__main__':
    main()
