#!/usr/bin/env python3
"""
HMM Macro Regime v2 — Data Pipeline.

Runs all HMM data fetchers, optionally retrains the model.

Usage:
    python scripts/update_hmm_data.py              # sync data only
    python scripts/update_hmm_data.py --retrain     # sync + retrain model
"""

import argparse
import logging
import time
from datetime import datetime, timezone, timedelta

PKT = timezone(timedelta(hours=5))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("hmm_update")


def main():
    parser = argparse.ArgumentParser(description="HMM v2 data pipeline")
    parser.add_argument("--retrain", action="store_true", help="Retrain model after data sync")
    args = parser.parse_args()

    start = time.time()
    now = datetime.now(PKT).strftime("%Y-%m-%d %H:%M PKT")
    print(f"\n{'='*60}")
    print(f"  HMM MACRO REGIME v2 — DATA UPDATE")
    print(f"  {now}")
    print(f"{'='*60}\n")

    # Step 1: Sync all HMM data sources
    print("Step 1: Syncing data sources...")
    from pakfindata.sources.hmm_data_fetchers import sync_all_hmm_data
    results = sync_all_hmm_data()
    for source, rows in results.items():
        status = f"{rows:,} rows" if rows > 0 else "no new data"
        print(f"  {source}: {status}")

    total_rows = sum(results.values())
    print(f"\n  Total: {total_rows:,} rows synced")

    # Step 2: Optionally retrain
    if args.retrain:
        print("\nStep 2: Retraining HMM v2 model...")
        try:
            from pakfindata.engine.macro_regime_hmm_v2 import train_and_save_v2
            result = train_and_save_v2()
            if "error" in result:
                print(f"  ERROR: {result['error']}")
            else:
                print(f"  Model trained on {result['months_trained']} months")
                print(f"  Current regime: {result['current_regime']}")
                print(f"  Saved to: {result['model_path']}")
                m = result.get("backtest", {})
                if m:
                    print(f"  Strategy: {m.get('strategy_return', 0):+.1%} | Sharpe: {m.get('strategy_sharpe', 0):.2f} | DD: {m.get('strategy_max_dd', 0):.1%}")
        except Exception as e:
            print(f"  Retrain failed: {e}")
    else:
        print("\nStep 2: Skipping retrain (use --retrain to enable)")

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"  DONE in {elapsed:.1f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
