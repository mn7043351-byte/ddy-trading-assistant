"""
DDY.AI Bridge Server
====================
Sits between the MT5 EA (AGY_Complete_Bridge.mq5) and the DDY dashboard.

  MT5 Terminal  --POST (every N sec)-->  Flask  /data   (port 5050)
                                            |
                                            |  broadcasts over asyncio
                                            v
  Dashboard    <--WebSocket------------  ws://localhost:8765

What each poll from the EA drives:
  - Live bid/ask for every symbol            -> "ticks"    (ticker tape)
  - OHLCV candles for every symbol/tf         -> "candles"  (chart + client-side EMA/RSI)
  - Account + open positions                  -> "account" / "positions"

  On EURUSD M5 candles:
    - CORE_01 "technical" signal: 5 indicators voting      -> "core1"
    - CORE_02 "ML formula" signal: weighted-sum + sigmoid  -> "core2"
    - CORE_04 "SMA crossover" signal: SMA10/SMA30          -> "core4"
    - OSC / MA data tables                                 -> "osc_data" / "ma_data"
    - Walk-forward backtest and live accuracy for each core -> "history_c1" / "history_c2" / "history_c4"

  On EURUSD M15 candles:
    - Composite technical signal (scanner)                  -> "scanner"
    - Backtest / live accuracy                              -> "history_scan"

  On BTCUSD M15 candles:
    - Composite technical signal                            -> "core3"
    - Backtest / live accuracy                              -> "history_c3"

IMPORTANT: only ONE instance of this server may run at a time (it binds ports
5050 and 8765). Before starting, kill anything already listening on those ports:
    lsof -i :5050
    lsof -i :8765
    kill -9 <PID>

Run it:
    pip3 install flask websockets
    python3 ddy_server.py

Then in MT5, make sure http://127.0.0.1:5050 is whitelisted under
Tools -> Options -> Expert Advisors -> "Allow WebRequest for listed URL",
and consider raising the EA's CandlesToSend input to 1000 for a deeper backtest.
"""

import asyncio
import json
import math
import threading
from datetime import datetime

from flask import Flask, request, jsonify
import websockets

# ----------------------------------------------------------------------------
# Shared state
# ----------------------------------------------------------------------------
connected_clients = set()   # active dashboard websocket connections
main_loop = None            # the asyncio loop the websocket server runs on

latest_account = {}
latest_positions = []
pending_command = {"action": "none"}   # queued order for the EA to pick up on its next poll

# Cache of the most recent candles per (symbol, tf) — the EA sends M1/M5/M15/H1
# every poll, so we stash each one here as it arrives. CORE_05 uses this to look
# at M15/H1 structure at the same time as an M5 signal, without needing another
# round trip. NOTE: this cache only ever holds the *current live* snapshot, so
# during the walk-forward backtest (which replays historical M5 windows) the
# HTF confirmation step is intentionally skipped — see mtf_structure_signal().
LATEST_CANDLES = {}   # key: (symbol, tf) -> candles list

app = Flask(__name__)


# ----------------------------------------------------------------------------
# Reusable indicator math (plain python, no numpy/ta-lib needed)
# ----------------------------------------------------------------------------
def sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def stddev(values, period):
    if len(values) < period:
        return None
    window = values[-period:]
    m = sum(window) / period
    return (sum((v - m) ** 2 for v in window) / period) ** 0.5


def ema_series(values, period):
    """Return an EMA value for every index >= period-1 (None before that)."""
    out = [None] * len(values)
    if len(values) < period:
        return out
    k = 2 / (period + 1)
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    ema_val = seed
    for i in range(period, len(values)):
        ema_val = values[i] * k + ema_val * (1 - k)
        out[i] = ema_val
    return out


def ema_last(values, period):
    series = ema_series(values, period)
    return series[-1] if series else None


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def macd(values, fast=12, slow=26, signal=9):
    if len(values) < slow + signal:
        return None, None
    fast_series = ema_series(values, fast)
    slow_series = ema_series(values, slow)
    macd_line = [
        (f - s) if (f is not None and s is not None) else None
        for f, s in zip(fast_series, slow_series)
    ]
    clean = [v for v in macd_line if v is not None]
    if len(clean) < signal:
        return None, None
    signal_series = ema_series(clean, signal)
    return clean[-1], signal_series[-1]


def stochastic(highs, lows, closes, period=9):
    if len(closes) < period:
        return None
    hh = max(highs[-period:])
    ll = min(lows[-period:])
    if hh == ll:
        return 50.0
    return (closes[-1] - ll) / (hh - ll) * 100


def cci(highs, lows, closes, period=14):
    if len(closes) < period:
        return None
    typical = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)][-period:]
    m = sum(typical) / period
    mean_dev = sum(abs(t - m) for t in typical) / period
    if mean_dev == 0:
        return 0.0
    return (typical[-1] - m) / (0.015 * mean_dev)


def sigmoid(x):
    try:
        return 1 / (1 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def get_pivots(highs, lows, period=4):
    """Find recent swing highs and swing lows."""
    swing_highs = []
    swing_lows = []
    n = len(highs)
    for i in range(period, n - period):
        is_sh = True
        is_sl = True
        for j in range(i - period, i + period + 1):
            if i == j: continue
            if highs[j] >= highs[i]: is_sh = False
            if lows[j] <= lows[i]: is_sl = False
        if is_sh: swing_highs.append((i, highs[i]))
        if is_sl: swing_lows.append((i, lows[i]))
    return swing_highs, swing_lows


def analyze_market_structure(candles):
    if len(candles) < 20:
        return {"trend": "AWAITING DATA", "support": [], "resistance": []}
        
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    sh, sl = get_pivots(highs, lows, period=5)
    
    structure = "CHOPPY (MIXED)"
    if len(sh) >= 2 and len(sl) >= 2:
        last_sh, prev_sh = sh[-1][1], sh[-2][1]
        last_sl, prev_sl = sl[-1][1], sl[-2][1]
        
        if last_sh > prev_sh and last_sl > prev_sl:
            structure = "BULLISH (HH+HL)"
        elif last_sh < prev_sh and last_sl < prev_sl:
            structure = "BEARISH (LH+LL)"
        elif last_sh < prev_sh and last_sl > prev_sl:
            structure = "CONSOLIDATION"
        elif last_sh > prev_sh and last_sl < prev_sl:
            structure = "EXPANSION"
            
    # Send the most recent 3 support/resistance levels
    support_levels = [round(s[1], 5) for s in sl[-3:]]
    resistance_levels = [round(r[1], 5) for r in sh[-3:]]
    
    return {
        "trend": structure,
        "support": support_levels,
        "resistance": resistance_levels
    }


MIN_CANDLES = 30  # minimum history before any strategy speaks up
MAX_BACKTEST_POINTS = 1000  # use up to 1000 candles for walk‑forward backtest


# ----------------------------------------------------------------------------
# CORE_01 — "technical analysis": 5 indicators combined by simple voting
#   1. EMA9/EMA21 cross
#   2. RSI(14) overbought/oversold
#   3. MACD(12,26,9) histogram sign
#   4. Stochastic(9) overbought/oversold
#   5. Bollinger(20,2) band position
# ----------------------------------------------------------------------------
def composite_technical_signal(candles, pair, timeframe_label):
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    if len(closes) < MIN_CANDLES:
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": "NEUTRAL", "confidence": 0,
            "price": closes[-1] if closes else 0,
            "logic": f"Collecting {timeframe_label} history ({len(closes)}/{MIN_CANDLES} candles)..."
        }

    price = closes[-1]
    votes = []          # each vote is +1 (bullish), -1 (bearish) or 0 (neutral)
    notes = []

    # 1. EMA9/EMA21 cross
    fast = ema_series(closes, 9)
    slow = ema_series(closes, 21)
    if fast[-1] is not None and slow[-1] is not None:
        if fast[-1] > slow[-1]:
            votes.append(1); notes.append("EMA9>EMA21")
        else:
            votes.append(-1); notes.append("EMA9<EMA21")

    # 2. RSI
    rsi_now = rsi(closes, 14)
    if rsi_now is not None:
        if rsi_now < 35:
            votes.append(1); notes.append(f"RSI {rsi_now:.0f} oversold")
        elif rsi_now > 65:
            votes.append(-1); notes.append(f"RSI {rsi_now:.0f} overbought")
        else:
            votes.append(0)

    # 3. MACD histogram
    macd_now, macd_sig = macd(closes)
    if macd_now is not None:
        if macd_now > macd_sig:
            votes.append(1); notes.append("MACD>signal")
        else:
            votes.append(-1); notes.append("MACD<signal")

    # 4. Stochastic
    stoch_now = stochastic(highs, lows, closes, 9)
    if stoch_now is not None:
        if stoch_now < 20:
            votes.append(1); notes.append(f"Stoch {stoch_now:.0f} oversold")
        elif stoch_now > 80:
            votes.append(-1); notes.append(f"Stoch {stoch_now:.0f} overbought")
        else:
            votes.append(0)

    # 5. Bollinger position
    mid = sma(closes, 20)
    sd = stddev(closes, 20)
    if mid is not None and sd:
        upper, lower = mid + 2 * sd, mid - 2 * sd
        if price <= lower:
            votes.append(1); notes.append("at lower Bollinger band")
        elif price >= upper:
            votes.append(-1); notes.append("at upper Bollinger band")
        else:
            votes.append(0)

    score = sum(votes)
    max_score = len(votes) if votes else 1

    if score >= 2:
        direction = "BUY"
    elif score <= -2:
        direction = "SELL"
    else:
        direction = "NEUTRAL"

    confidence = round(50 + abs(score) / max_score * 45) if direction != "NEUTRAL" else round(50 + abs(score) * 3)
    confidence = min(confidence, 95)

    logic = f"{len([v for v in votes if v != 0])}/{len(votes)} indicators agree ({', '.join(notes) if notes else 'mixed signals'})."

    return {
        "pair": pair, "timeframe": timeframe_label,
        "direction": direction, "confidence": confidence,
        "price": price, "logic": logic
    }


# ----------------------------------------------------------------------------
# CORE_02 — "ML formula": normalized features -> weighted sum -> sigmoid.
# Not a trained model, but a genuine mathematical scoring formula distinct
# from CORE_01's indicator-vote approach, so the two engines can disagree.
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# CORE_03 — SMC (Smart Money Concepts)
# ----------------------------------------------------------------------------
def smc_strategy_signal(candles, pair, timeframe_label):
    closes = [c["close"] for c in candles]
    
    if len(closes) < MIN_CANDLES:
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": "NEUTRAL", "confidence": 0,
            "price": closes[-1] if closes else 0,
            "logic": f"Collecting {timeframe_label} history ({len(closes)}/{MIN_CANDLES} candles)..."
        }
        
    price = closes[-1]
    struct = analyze_market_structure(candles)
    trend = struct["trend"]
    support = struct["support"]
    resistance = struct["resistance"]
    
    direction = "NEUTRAL"
    confidence = 50
    logic = "SMC: Chop or no clear liquidity setup."
    
    if trend == "BULLISH (HH+HL)":
        if support and price <= support[-1] * 1.0005:
            direction = "BUY"
            confidence = 85
            logic = f"SMC: Bullish MS. Price tapping into discount OB / Support at {support[-1]}."
        else:
            direction = "BUY"
            confidence = 65
            logic = "SMC: Bullish MS. Waiting for pullback to discount."
    elif trend == "BEARISH (LH+LL)":
        if resistance and price >= resistance[-1] * 0.9995:
            direction = "SELL"
            confidence = 85
            logic = f"SMC: Bearish MS. Price tapping premium OB / Resistance at {resistance[-1]}."
        else:
            direction = "SELL"
            confidence = 65
            logic = "SMC: Bearish MS. Waiting for pullback to premium."
            
    return {
        "pair": pair,
        "timeframe": timeframe_label,
        "direction": direction,
        "confidence": confidence,
        "price": price,
        "logic": logic
    }

def ml_formula_signal(candles, pair, timeframe_label):
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    if len(closes) < MIN_CANDLES:
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": "NEUTRAL", "confidence": 0,
            "price": closes[-1] if closes else 0,
            "logic": f"Collecting {timeframe_label} history ({len(closes)}/{MIN_CANDLES} candles)..."
        }

    price = closes[-1]

    rsi_now = rsi(closes, 14) or 50.0
    macd_now, macd_sig = macd(closes)
    macd_hist = (macd_now - macd_sig) if (macd_now is not None and macd_sig is not None) else 0.0
    stoch_now = stochastic(highs, lows, closes, 9) or 50.0
    sma20 = sma(closes, 20) or price
    sd20 = stddev(closes, 20) or 1e-9
    zscore = (price - sma20) / sd20 if sd20 else 0.0
    momentum = (price - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0.0

    # Normalize each feature roughly onto a [-1, 1] scale
    f_rsi = (rsi_now - 50) / 50
    f_macd = max(-1, min(1, macd_hist * 5000))
    f_stoch = (stoch_now - 50) / 50
    f_z = max(-1, min(1, zscore / 2))
    f_mom = max(-1, min(1, momentum * 20))

    # Fixed weights (hand-tuned, not "trained" -- this is a formula, not an ML model)
    weights = {"rsi": 0.9, "macd": 1.3, "stoch": 0.6, "zscore": 1.1, "momentum": 1.4}
    z = (
        weights["rsi"] * f_rsi
        + weights["macd"] * f_macd
        + weights["stoch"] * f_stoch
        + weights["zscore"] * f_z
        + weights["momentum"] * f_mom
    )
    prob_up = sigmoid(z)

    if prob_up > 0.60:
        direction = "BUY"
    elif prob_up < 0.40:
        direction = "SELL"
    else:
        direction = "NEUTRAL"

    confidence = round(min(95, abs(prob_up - 0.5) * 190 + 50)) if direction != "NEUTRAL" else round(abs(prob_up - 0.5) * 100 + 40)

    logic = (
        f"Weighted formula P(up)={prob_up * 100:.1f}% "
        f"(RSI z={f_rsi:+.2f}, MACD z={f_macd:+.2f}, Stoch z={f_stoch:+.2f}, "
        f"price-z={f_z:+.2f}, momentum={f_mom:+.2f})."
    )

    return {
        "pair": pair, "timeframe": timeframe_label,
        "direction": direction, "confidence": confidence,
        "price": price, "logic": logic
    }


# ----------------------------------------------------------------------------
# CORE_04 — SMA Crossover (SMA10 / SMA30)
# A simple, easy‑to‑understand strategy for comparison.
# ----------------------------------------------------------------------------
def sma_crossover_signal(candles, pair, timeframe_label):
    closes = [c["close"] for c in candles]

    if len(closes) < MIN_CANDLES:
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": "NEUTRAL", "confidence": 0,
            "price": closes[-1] if closes else 0,
            "logic": f"Collecting {timeframe_label} history ({len(closes)}/{MIN_CANDLES} candles)..."
        }

    price = closes[-1]
    sma10 = sma(closes, 10)
    sma30 = sma(closes, 30)

    if sma10 is None or sma30 is None:
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": "NEUTRAL", "confidence": 0,
            "price": price,
            "logic": "Insufficient data for SMA10/SMA30."
        }

    # We need previous values for cross detection
    if len(closes) < 31:
        # Not enough to detect cross, just compare current
        if sma10 > sma30:
            direction = "BUY"
            confidence = 60
            logic = f"SMA10 ({sma10:.5f}) > SMA30 ({sma30:.5f}) → uptrend."
        elif sma10 < sma30:
            direction = "SELL"
            confidence = 60
            logic = f"SMA10 ({sma10:.5f}) < SMA30 ({sma30:.5f}) → downtrend."
        else:
            direction = "NEUTRAL"
            confidence = 50
            logic = "SMA10 and SMA30 are equal."
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": direction, "confidence": confidence,
            "price": price, "logic": logic
        }

    # Compute SMA for previous candle
    prev_sma10 = sum(closes[-11:-1]) / 10 if len(closes) >= 11 else sma10
    prev_sma30 = sum(closes[-31:-1]) / 30 if len(closes) >= 31 else sma30

    crossed_up = prev_sma10 <= prev_sma30 and sma10 > sma30
    crossed_down = prev_sma10 >= prev_sma30 and sma10 < sma30

    if crossed_up:
        direction = "BUY"
        confidence = 70
        logic = f"SMA10 crossed above SMA30. Bullish momentum."
    elif crossed_down:
        direction = "SELL"
        confidence = 70
        logic = f"SMA10 crossed below SMA30. Bearish momentum."
    elif sma10 > sma30:
        direction = "BUY"
        confidence = 55
        logic = f"SMA10 ({sma10:.5f}) > SMA30 ({sma30:.5f}) – uptrend intact."
    elif sma10 < sma30:
        direction = "SELL"
        confidence = 55
        logic = f"SMA10 ({sma10:.5f}) < SMA30 ({sma30:.5f}) – downtrend intact."
    else:
        direction = "NEUTRAL"
        confidence = 50
        logic = "SMA10 and SMA30 converged."

    return {
        "pair": pair, "timeframe": timeframe_label,
        "direction": direction, "confidence": confidence,
        "price": price, "logic": logic
    }


# ----------------------------------------------------------------------------
# CORE_05 — Multi-Timeframe Structure Confluence
# Same idea as the TradingView "Market Structure Dashboard": M5 alone is noisy,
# so weight M5/M15/H1 structure + EMA9 trend together (higher TF = more weight)
# and only fire a signal when the timeframes actually agree. This is meant to
# trade LESS often than core1-4, but with a real reason for each trade.
# ----------------------------------------------------------------------------
def _tf_bias(candles):
    """Return (+1/-1/0) combining market-structure + EMA9/21 trend for one TF."""
    if not candles or len(candles) < MIN_CANDLES:
        return 0, "no data"
    closes = [c["close"] for c in candles]
    struct = analyze_market_structure(candles)["trend"]
    struct_dir = 1 if "BULLISH" in struct else -1 if "BEARISH" in struct else 0

    fast = ema_series(closes, 9)
    slow = ema_series(closes, 21)
    ema_dir = 0
    if fast[-1] is not None and slow[-1] is not None:
        ema_dir = 1 if fast[-1] > slow[-1] else -1

    combined = struct_dir + ema_dir  # -2..+2
    bias = 1 if combined >= 1 else -1 if combined <= -1 else 0
    return bias, struct


def mtf_structure_signal(candles, pair, timeframe_label, mtf_context=None):
    """
    mtf_context: {"PERIOD_M15": [...candles], "PERIOD_H1": [...candles]} — only
    passed in by the live pipeline (receive_data). Left as None during the
    walk-forward backtest, so the backtest only ever scores the M5 component
    (no lookahead from "future" H1 data).
    """
    closes = [c["close"] for c in candles]
    if len(closes) < MIN_CANDLES:
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": "NEUTRAL", "confidence": 0,
            "price": closes[-1] if closes else 0,
            "logic": f"Collecting {timeframe_label} history ({len(closes)}/{MIN_CANDLES} candles)..."
        }

    price = closes[-1]
    m5_bias, m5_struct = _tf_bias(candles)

    weighted_score = m5_bias * 1   # M5 weight = 1
    max_score = 1
    tf_notes = [f"M5={m5_struct.split(' ')[0]}"]

    if mtf_context:
        m15_candles = mtf_context.get("PERIOD_M15")
        h1_candles = mtf_context.get("PERIOD_H1")
        if m15_candles:
            m15_bias, m15_struct = _tf_bias(m15_candles)
            weighted_score += m15_bias * 2   # M15 weight = 2
            max_score += 2
            tf_notes.append(f"M15={m15_struct.split(' ')[0]}")
        if h1_candles:
            h1_bias, h1_struct = _tf_bias(h1_candles)
            weighted_score += h1_bias * 3    # H1 weight = 3
            max_score += 3
            tf_notes.append(f"H1={h1_struct.split(' ')[0]}")

    # Require the majority of *available* weight to agree — this is what makes
    # it a confluence filter instead of just another single-TF vote.
    if max_score >= 3 and weighted_score >= math.ceil(max_score * 0.6):
        direction = "BUY"
    elif max_score >= 3 and weighted_score <= -math.ceil(max_score * 0.6):
        direction = "SELL"
    elif max_score < 3:
        # Not enough HTF data cached yet (e.g. backtest, or just started) —
        # fall back to M5-only, but cap confidence since it's a weaker signal.
        direction = "BUY" if m5_bias == 1 else "SELL" if m5_bias == -1 else "NEUTRAL"
    else:
        direction = "NEUTRAL"

    if direction == "NEUTRAL":
        confidence = 50
    else:
        agree_ratio = abs(weighted_score) / max_score if max_score else 0
        confidence = round(50 + agree_ratio * 40)
        if max_score < 3:
            confidence = min(confidence, 60)  # M5-only fallback, less trust
        confidence = min(confidence, 95)

    logic = f"MTF confluence [{', '.join(tf_notes)}] weighted {weighted_score:+d}/{max_score}."
    return {
        "pair": pair, "timeframe": timeframe_label,
        "direction": direction, "confidence": confidence,
        "price": price, "logic": logic
    }


# ----------------------------------------------------------------------------
# CORE_06 — Price Action Patterns (pin bar / engulfing / inside-outside bar)
# Adapted from the classic ChrisMoody "CM_Price-Action-Bars" logic the user
# supplied: reversal candles that reject a swing high/low, plus engulfing
# bars, scored on the M5 chart.
# ----------------------------------------------------------------------------
def price_action_pattern_signal(candles, pair, timeframe_label,
                                 pct_pin=0.66, lookback=6):
    if len(candles) < max(MIN_CANDLES, lookback + 2):
        return {
            "pair": pair, "timeframe": timeframe_label,
            "direction": "NEUTRAL", "confidence": 0,
            "price": candles[-1]["close"] if candles else 0,
            "logic": f"Collecting {timeframe_label} history ({len(candles)}/{MIN_CANDLES} candles)..."
        }

    c = candles[-1]
    prev = candles[-2]
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = max(h - l, 1e-9)
    price = cl

    window_lows = [x["low"] for x in candles[-(lookback + 1):-1]]
    window_highs = [x["high"] for x in candles[-(lookback + 1):-1]]

    direction = "NEUTRAL"
    confidence = 50
    logic = "No clean price-action pattern on the last candle."

    # Bullish pin bar: closes/opens in the top (1-pct_pin) of the range,
    # and the low undercuts the recent swing low (liquidity grab + rejection)
    if o >= h - rng * (1 - pct_pin) and cl >= h - rng * (1 - pct_pin) and window_lows and l <= min(window_lows):
        direction = "BUY"
        confidence = 78
        logic = f"Bullish pin bar: swept {lookback}-bar low ({min(window_lows):.5f}) and rejected upward."

    # Bearish pin bar
    elif o <= l + rng * (1 - pct_pin) and cl <= l + rng * (1 - pct_pin) and window_highs and h >= max(window_highs):
        direction = "SELL"
        confidence = 78
        logic = f"Bearish pin bar: swept {lookback}-bar high ({max(window_highs):.5f}) and rejected downward."

    # Bullish engulfing: current green candle's body engulfs prior red body
    elif cl > o and prev["close"] < prev["open"] and cl >= prev["open"] and o <= prev["close"]:
        direction = "BUY"
        confidence = 68
        logic = "Bullish engulfing bar over prior candle."

    # Bearish engulfing
    elif cl < o and prev["close"] > prev["open"] and cl <= prev["open"] and o >= prev["close"]:
        direction = "SELL"
        confidence = 68
        logic = "Bearish engulfing bar over prior candle."

    return {
        "pair": pair, "timeframe": timeframe_label,
        "direction": direction, "confidence": confidence,
        "price": price, "logic": logic
    }


# ----------------------------------------------------------------------------
# OSC DATA / MA DATA tables — built from the same EURUSD M5 candles
# ----------------------------------------------------------------------------
def build_osc_table(candles):
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    rsi_now = rsi(closes, 14)
    macd_now, macd_signal = macd(closes)
    stoch_now = stochastic(highs, lows, closes, 9)
    cci_now = cci(highs, lows, closes, 14)

    rows = []
    if rsi_now is not None:
        rows.append({"name": "RSI(14)", "value": round(rsi_now),
                     "action": "Sell" if rsi_now > 70 else "Buy" if rsi_now < 30 else "Neutral"})
    if macd_now is not None and macd_signal is not None:
        rows.append({"name": "MACD(12,26)", "value": round(macd_now, 5),
                     "action": "Buy" if macd_now > macd_signal else "Sell"})
    if stoch_now is not None:
        rows.append({"name": "STOCH(9,6)", "value": round(stoch_now),
                     "action": "Sell" if stoch_now > 80 else "Buy" if stoch_now < 20 else "Neutral"})
    if cci_now is not None:
        rows.append({"name": "CCI(14)", "value": round(cci_now),
                     "action": "Sell" if cci_now > 100 else "Buy" if cci_now < -100 else "Neutral"})
    return rows


def build_ma_table(candles):
    closes = [c["close"] for c in candles]
    price = closes[-1]
    sma20, ema50, sma200 = sma(closes, 20), ema_last(closes, 50), sma(closes, 200)

    rows = []
    if sma20 is not None:
        rows.append({"name": "SMA(20)", "value": round(sma20, 5), "action": "Buy" if price > sma20 else "Sell"})
    if ema50 is not None:
        rows.append({"name": "EMA(50)", "value": round(ema50, 5), "action": "Buy" if price > ema50 else "Sell"})
    if sma200 is not None:
        rows.append({"name": "SMA(200)", "value": round(sma200, 5), "action": "Buy" if price > sma200 else "Sell"})
    return rows


# ----------------------------------------------------------------------------
# Walk-forward "Strategy Tester": for each historical point, generate a
# signal using ONLY the data available up to that point (no lookahead), then
# check whether the very next candle actually moved the way the signal said.
# This is exactly "take N candles, predict the next candle" for every point
# in the supplied history.
# ----------------------------------------------------------------------------
def walk_forward_backtest(candles, strategy_fn, pair, timeframe_label, max_points=MAX_BACKTEST_POINTS):
    results = []
    n = len(candles)
    start = max(MIN_CANDLES, n - max_points)
    for i in range(start, n - 1):
        window = candles[: i + 1]
        signal = strategy_fn(window, pair, timeframe_label)
        direction = signal["direction"]
        if direction == "NEUTRAL":
            continue  # no directional claim made, nothing to score
        actual_up = candles[i + 1]["close"] > candles[i]["close"]
        correct = (direction == "BUY" and actual_up) or (direction == "SELL" and not actual_up)
        results.append({"time": candles[i]["time"], "accuracy": 100 if correct else 0})
    total = len(results)
    correct_count = len([r for r in results if r["accuracy"] == 100])
    accuracy = round(correct_count / total * 100) if total else 0
    return results, total, correct_count, accuracy


# Tracks, per (core_name, pair, tf): whether the one-time bulk backtest has
# run yet, plus the "pending" signal awaiting confirmation from the next
# candle close (for ongoing live accuracy tracking).
BACKTEST_DONE = {}     # key -> bool
PENDING_SIGNAL = {}    # key -> {"time":..., "direction":..., "ref_close":...}
LAST_SCANNER_TIME = None


def track_key(core, pair, tf):
    return f"{core}:{pair}:{tf}"


def maybe_seed_backtest(core, strategy_fn, candles, pair, timeframe_label):
    key = track_key(core, pair, timeframe_label)
    if BACKTEST_DONE.get(key) or len(candles) < MIN_CANDLES + 2:
        return None
    results, total, correct_count, accuracy = walk_forward_backtest(candles, strategy_fn, pair, timeframe_label)
    BACKTEST_DONE[key] = True
    log(f"{core.upper()} strategy tester: {total} candles evaluated on {pair} {timeframe_label}, {accuracy}% directional accuracy.")
    return {"results": results, "total": total, "correct": correct_count, "accuracy": accuracy}


def update_live_accuracy(core, strategy_fn, candles, pair, timeframe_label):
    """
    Called every poll. Once a new candle CLOSES, checks the signal that was
    pending from before it closed, scores it, and starts tracking a new one.
    Returns a single {"time":.., "accuracy":..} entry, or None.
    """
    if len(candles) < MIN_CANDLES + 2:
        return None

    closed = candles[:-1]              # drop the still-forming last candle
    latest_closed_time = closed[-1]["time"]
    key = track_key(core, pair, timeframe_label)
    pending = PENDING_SIGNAL.get(key)

    entry = None
    if pending and pending["time"] != latest_closed_time and pending["direction"] != "NEUTRAL":
        actual_up = closed[-1]["close"] > pending["ref_close"]
        correct = (pending["direction"] == "BUY" and actual_up) or (pending["direction"] == "SELL" and not actual_up)
        entry = {"time": latest_closed_time, "accuracy": 100 if correct else 0}

    if not pending or pending["time"] != latest_closed_time:
        sig = strategy_fn(closed, pair, timeframe_label)
        PENDING_SIGNAL[key] = {
            "time": latest_closed_time,
            "direction": sig["direction"],
            "ref_close": closed[-1]["close"]
        }

    return entry


# ----------------------------------------------------------------------------
# Broadcasting helpers (Flask thread -> asyncio websocket loop)
# ----------------------------------------------------------------------------
async def _broadcast(message: dict):
    if not connected_clients:
        return
    data = json.dumps(message)
    dead = set()
    for ws in connected_clients:
        try:
            await ws.send(data)
        except Exception:
            dead.add(ws)
    connected_clients.difference_update(dead)


def broadcast(message: dict):
    if main_loop is None:
        return
    asyncio.run_coroutine_threadsafe(_broadcast(message), main_loop)


def log(msg: str):
    stamp = datetime.now().strftime("%H:%M:%S")
    broadcast({"type": "log", "msg": f"[{stamp}] {msg}"})


# ----------------------------------------------------------------------------
# Flask route: the MT5 EA posts its full snapshot here every TimerSeconds
# ----------------------------------------------------------------------------
@app.route("/data", methods=["POST"])
def receive_data():
    global latest_account, latest_positions, pending_command, LAST_SCANNER_TIME

    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({"action": "none"})

    if not isinstance(payload, list):
        return jsonify({"action": "none"})

    ticks = []

    for entry in payload:
        symbol = entry.get("symbol")
        bid = entry.get("bid")
        ask = entry.get("ask")
        if symbol and bid and ask:
            ticks.append({"symbol": symbol, "bid": bid, "ask": ask})

        if "account" in entry:
            latest_account = entry["account"]
        if "positions" in entry:
            latest_positions = entry["positions"]

        for tf_block in entry.get("timeframes", []):
            tf = tf_block.get("tf")
            candles = tf_block.get("candles", [])
            if not candles:
                continue

            # Raw candles straight through -> chart (and client-side EMA/RSI) draw from this
            broadcast({"type": "candles", "symbol": symbol, "tf": tf, "candles": candles})

            # Cache latest candles per (symbol, tf) for CORE_05's MTF confluence
            LATEST_CANDLES[(symbol, tf)] = candles

            # Broadcast advanced technicals & Market Structure per TF
            struct = analyze_market_structure(candles)
            broadcast({"type": "market_structure", "symbol": symbol, "tf": tf, "data": struct})

            # ---- EURUSD M5: CORE_01 + CORE_02 + CORE_03 + CORE_04 + indicator tables + backtests ----
            if symbol == "EURUSD" and tf == "PERIOD_M5":
                core1_sig = composite_technical_signal(candles, "EURUSD", "5 MIN")
                core2_sig = ml_formula_signal(candles, "EURUSD", "5 MIN")
                core3_sig = smc_strategy_signal(candles, "EURUSD", "5 MIN")
                core4_sig = sma_crossover_signal(candles, "EURUSD", "5 MIN")
                mtf_ctx = {
                    "PERIOD_M15": LATEST_CANDLES.get(("EURUSD", "PERIOD_M15")),
                    "PERIOD_H1": LATEST_CANDLES.get(("EURUSD", "PERIOD_H1")),
                }
                core5_sig = mtf_structure_signal(candles, "EURUSD", "5 MIN", mtf_context=mtf_ctx)
                core6_sig = price_action_pattern_signal(candles, "EURUSD", "5 MIN")
                broadcast({"type": "core1", "data": core1_sig})
                broadcast({"type": "core2", "data": core2_sig})
                broadcast({"type": "core3", "data": core3_sig})
                broadcast({"type": "core4", "data": core4_sig})
                broadcast({"type": "core5", "data": core5_sig})
                broadcast({"type": "core6", "data": core6_sig})

                if len(candles) >= 21:
                    broadcast({"type": "osc_data", "data": build_osc_table(candles)})
                    broadcast({"type": "ma_data", "data": build_ma_table(candles)})

                # Backtest & live accuracy for each core
                for core, fn in [("core1", composite_technical_signal),
                                 ("core2", ml_formula_signal),
                                 ("core3", smc_strategy_signal),
                                 ("core4", sma_crossover_signal),
                                 ("core5", mtf_structure_signal),
                                 ("core6", price_action_pattern_signal)]:
                    seed = maybe_seed_backtest(core, fn, candles, "EURUSD", "5 MIN")
                    if seed:
                        broadcast({"type": f"{core}_backtest", "data": seed})
                    else:
                        live = update_live_accuracy(core, fn, candles, "EURUSD", "5 MIN")
                        if live:
                            broadcast({"type": f"history_{core}", "data": live})

            # ---- EURUSD M15: scanner feed ----
            if symbol == "EURUSD" and tf == "PERIOD_M15":
                newest_time = candles[-1]["time"]
                scan_sig = composite_technical_signal(candles, "EURUSD", "15 MIN")
                broadcast({
                    "type": "scanner",
                    "data": {
                        "id": newest_time,
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "pair": "EURUSD",
                        "timeframe": "15 MIN",
                        "dir": scan_sig["direction"],
                        "price": scan_sig["price"]
                    }
                })

                seed_scan = maybe_seed_backtest("scan", composite_technical_signal, candles, "EURUSD", "15 MIN")
                if seed_scan:
                    broadcast({"type": "scan_backtest", "data": seed_scan})
                else:
                    live_scan = update_live_accuracy("scan", composite_technical_signal, candles, "EURUSD", "15 MIN")
                    if live_scan:
                        broadcast({"type": "history_scan", "data": live_scan})

    if ticks:
        broadcast({"type": "ticks", "data": ticks})
    if latest_account:
        broadcast({"type": "account", "data": latest_account})
    broadcast({"type": "positions", "data": latest_positions})

    cmd = pending_command
    pending_command = {"action": "none"}
    return jsonify(cmd)


@app.route("/order", methods=["POST"])
def queue_order():
    global pending_command
    pending_command = request.get_json(force=True)
    log(f"Order queued for MT5: {pending_command}")
    return jsonify({"status": "queued", "command": pending_command})


# ----------------------------------------------------------------------------
# WebSocket server: the dashboard (index.html) connects here
# ----------------------------------------------------------------------------
async def ws_handler(websocket):
    connected_clients.add(websocket)
    log("Dashboard client connected.")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                msg_type = data.get("type")
                if msg_type in ["training_log", "training_stats"]:
                    # Broadcast AI training updates to all dashboard clients
                    for client in list(connected_clients):
                        if client != websocket:
                            try:
                                await client.send(message)
                            except Exception:
                                pass
            except Exception:
                pass
    except Exception:
        pass
    finally:
        connected_clients.discard(websocket)


async def start_ws_server():
    global main_loop
    main_loop = asyncio.get_running_loop()
    async with websockets.serve(ws_handler, "0.0.0.0", 8765):
        await asyncio.Future()


def run_ws_server():
    asyncio.run(start_ws_server())


def _port_already_in_use(port: int) -> bool:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


if __name__ == "__main__":
    for port in (5050, 8765):
        if _port_already_in_use(port):
            print("!" * 64)
            print(f" Port {port} is already in use by another process.")
            print(f" Find it with:   lsof -i :{port}")
            print(" Kill it with:   kill -9 <PID>")
            print(" Then re-run this script.")
            print("!" * 64)
            raise SystemExit(1)

    ws_thread = threading.Thread(target=run_ws_server, daemon=True)
    ws_thread.start()

    print("=" * 64)
    print(" DDY.AI Bridge Server")
    print(" MT5 EA should POST to:   http://127.0.0.1:5050/data")
    print(" Dashboard connects to:   ws://127.0.0.1:8765")
    print(" Tip: raise the EA's CandlesToSend input to 1000 for a deeper")
    print(" Strategy Tester backtest on first connect.")
    print("=" * 64)

    app.run(host="0.0.0.0", port=5050, debug=False, use_reloader=False)
