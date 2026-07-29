"""
backtest_report.py
===================
REAL historical validation tool for the DDY.AI cores. This REPLACES
train_model.py, which only ever generated random numbers dressed up as
"AI training" (random.random() > 0.3 etc.) — it never actually predicted
anything.

This script runs the *exact same* strategy formulas that ddy_server.py uses
live (composite_technical_signal, ml_formula_signal, smc_strategy_signal,
sma_crossover_signal, mtf_structure_signal, price_action_pattern_signal)
against real historical M5 candles, walk-forward (no lookahead), and prints
an honest accuracy report. No randomness, no "training" claim — it's a
backtest, and it says so.

HOW TO GET CANDLES OUT OF MT5:
  In MT5: View -> Strategy Tester -> pick EURUSD, M5 -> run any EA/period ->
  History tab -> right-click -> "Export data" (or use Tools -> History
  Center -> EURUSD -> M5 -> Export). You need a CSV with a header row:

      time,open,high,low,close,volume

  `time` can be a unix timestamp (seconds) or an ISO date string
  (e.g. 2026-07-29T10:45:00). MT5's own CSV export usually needs a light
  reformat (date+time in separate columns) — if yours does, just merge them
  into one `time` column before running this.

USAGE:
    pip3 install flask websockets      # ddy_server.py's only real deps
    python3 backtest_report.py EURUSD_M5.csv
"""
import csv
import json
import sys
from datetime import datetime

import ddy_server as core  # reuse the EXACT formulas the live dashboard runs

STRATEGIES = [
    ("CORE_01 Composite Technical", core.composite_technical_signal),
    ("CORE_02 ML Formula", core.ml_formula_signal),
    ("CORE_03 SMC", core.smc_strategy_signal),
    ("CORE_04 SMA Crossover", core.sma_crossover_signal),
    # mtf_context defaults to None here -> same M5-only fallback the live
    # server uses whenever M15/H1 haven't been cached yet, so this stays an
    # honest, lookahead-free comparison across all six strategies.
    ("CORE_05 MTF Structure", core.mtf_structure_signal),
    ("CORE_06 Price Action", core.price_action_pattern_signal),
]


def load_candles(csv_path):
    candles = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_t = row["time"].strip()
            try:
                t_val = int(float(raw_t))
            except ValueError:
                t_val = int(datetime.fromisoformat(raw_t).timestamp())
            candles.append({
                "time": t_val,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0) or 0),
            })
    candles.sort(key=lambda c: c["time"])
    return candles


def run_report(csv_path):
    candles = load_candles(csv_path)
    print(f"Loaded {len(candles)} candles from {csv_path}")

    min_needed = core.MIN_CANDLES + 2
    if len(candles) < min_needed:
        print(f"Need at least {min_needed} candles for a meaningful backtest "
              f"(only {core.MIN_CANDLES} to even generate a first signal).")
        return

    report = {}
    print(f"\n{'STRATEGY':32s} {'SIGNALS':>8s} {'CORRECT':>8s} {'ACCURACY':>9s}")
    print("-" * 62)
    for name, fn in STRATEGIES:
        results, total, correct, accuracy = core.walk_forward_backtest(
            candles, fn, "EURUSD", "5 MIN"
        )
        report[name] = {"total": total, "correct": correct, "accuracy": accuracy}
        print(f"{name:32s} {total:8d} {correct:8d} {accuracy:8d}%")
    print("-" * 62)

    print(
        "\nNOTE:\n"
        "- Only directional calls are scored (NEUTRAL signals are excluded).\n"
        "- Small totals (under ~200) are not statistically meaningful — the\n"
        "  accuracy% can swing wildly by chance alone. Feed this script a\n"
        "  few thousand M5 candles (weeks of data) before trusting a number.\n"
        "- ~50% accuracy on a large sample means no real directional edge —\n"
        "  that's the expected result for most simple TA formulas on raw\n"
        "  5-minute forex data, not a bug.\n"
        "- This script does NOT account for spread/slippage/commission. A\n"
        "  formula that's 'right' 52% of the time can still lose money after\n"
        "  costs on a 5-minute timeframe."
    )

    out_path = csv_path.rsplit(".", 1)[0] + "_backtest_report.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nFull report written to {out_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 backtest_report.py path/to/candles.csv")
        sys.exit(1)
    run_report(sys.argv[1])
