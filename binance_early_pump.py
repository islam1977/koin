#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Binance Early-Pump Scanner
--------------------------
Educational market scanner. It DOES NOT place orders.

Strategy:
- Binance Spot USDT symbols
- 5m candles
- Volume expansion vs historical baseline
- Volume acceleration
- Price momentum (but avoid already-exploded coins)
- Breakout of recent highs
- Candle quality / close location
- Liquidity filter using 24h quote volume
- Composite score (0..100)
- Cooldown to reduce repeated alerts
- CSV logging
- Optional Telegram alerts
- Signal outcome tracking (win-rate / accuracy over time)
- Trailing-stop paper-trading simulation (never places real orders)
- All summaries can be pushed to Telegram automatically

Install:
    pip install -r requirements.txt

Run:
    python binance_early_pump.py

Optional Telegram:
    Set environment variables:
      TELEGRAM_BOT_TOKEN
      TELEGRAM_CHAT_ID

The scanner uses public Binance market data only; API keys are not required.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import math
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------- CONFIG ----------------

BASE_URL = "https://api.binance.com"

INTERVAL = "5m"
KLINES_LIMIT = 180

SCAN_EVERY_SECONDS = 300       # 5 minutes
WORKERS = 12

# Universe filters
MIN_24H_QUOTE_VOLUME = 2_000_000
MAX_24H_GAIN_PCT = 25.0
MIN_PRICE = 0.00000001

# Early-move filters
MIN_15M_GAIN_PCT = -2.0
MAX_15M_GAIN_PCT = 12.0
MIN_1H_GAIN_PCT = -5.0
MAX_1H_GAIN_PCT = 20.0

# Volume
VOLUME_RATIO_GOOD = 2.0
VOLUME_RATIO_STRONG = 4.0
ACCELERATION_GOOD = 1.5
ACCELERATION_STRONG = 2.5

# Breakout
BREAKOUT_LOOKBACK = 48       # 4 hours on 5m candles
NEAR_BREAKOUT_PCT = 1.5

# Scoring
ALERT_SCORE = 65
STRONG_ALERT_SCORE = 80
TOP_N = 15

# Safety
HTTP_TIMEOUT = 12
COOLDOWN_SECONDS = 45 * 60

LOG_FILE = "scanner_alerts.csv"

# ---- Cross-run state (needed because GitHub Actions starts a fresh VM
#      every time — nothing survives in memory between scans there) ----
LAST_ALERT_FILE = "last_alert.json"
SCAN_COUNT_FILE = "scan_count.json"

# ---- Outcome tracking (new) ----
# More checkpoints = finer-grained picture of how fast a signal decays or
# keeps running. 5m catches instant fakeouts, 240m catches the bigger swings.
OUTCOME_CHECKPOINTS_MIN = [5, 15, 30, 60, 120, 240]
OUTCOME_MAX_AGE_MIN = 300                  # stop tracking a signal after this long
OUTCOME_STATE_FILE = "pending_outcomes.json"
OUTCOME_LOG_FILE = "signal_outcomes.csv"
PRINT_SUMMARY_EVERY_N_SCANS = 3            # print/send accuracy stats every N scans

# ---- Trailing-stop simulation (paper trading only — never places real orders) ----
SIM_INITIAL_STOP_PCT = 4.0        # hard stop below entry, active before trailing kicks in
SIM_ACTIVATION_PCT = 3.0          # once price is this % above entry, switch to trailing mode
SIM_TRAILING_PCT = 3.0            # once trailing, exit if price falls this % below the peak
SIM_MAX_HOLD_MIN = OUTCOME_MAX_AGE_MIN   # force a time-exit if neither stop is ever hit
SIM_LOG_FILE = "trailing_stop_sim.csv"

# Push the periodic accuracy/sim summaries to Telegram too, not just the console.
SEND_SUMMARY_TO_TELEGRAM = True

# ----------------------------------------

session = requests.Session()
session.headers.update({
    "User-Agent": "BinanceEarlyPumpScanner/1.0",
    "Accept": "application/json",
})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclass
class Signal:
    symbol: str
    price: float
    gain_5m: float
    gain_15m: float
    gain_1h: float
    gain_24h: float
    quote_volume_24h: float
    volume_ratio_1h: float
    volume_ratio_15m: float
    volume_acceleration: float
    breakout_pct: float
    close_location: float
    score: int
    setup: str


def get_json(path: str, params: dict | None = None):
    r = session.get(BASE_URL + path, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_usdt_symbols():
    data = get_json("/api/v3/exchangeInfo")
    symbols = []

    for s in data["symbols"]:
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("isSpotTradingAllowed") is False:
            continue

        symbol = s["symbol"]

        # Exclude obvious leveraged-token naming patterns.
        if any(x in symbol for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue

        symbols.append(symbol)

    return symbols


def get_24h_tickers():
    data = get_json("/api/v3/ticker/24hr")
    out = {}

    for x in data:
        symbol = x["symbol"]
        if not symbol.endswith("USDT"):
            continue

        try:
            out[symbol] = {
                "price": float(x["lastPrice"]),
                "change": float(x["priceChangePercent"]),
                "quote_volume": float(x["quoteVolume"]),
            }
        except (KeyError, ValueError):
            continue

    return out


def get_klines(symbol: str):
    return get_json(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": INTERVAL,
            "limit": KLINES_LIMIT,
        },
    )


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a / b - 1.0) * 100.0


def safe_mean(values):
    values = [x for x in values if math.isfinite(x)]
    return sum(values) / len(values) if values else 0.0


def analyze(symbol: str, ticker: dict) -> Signal | None:
    try:
        klines = get_klines(symbol)
    except Exception as e:
        logging.debug("Kline error %s: %s", symbol, e)
        return None

    if len(klines) < 80:
        return None

    # Ignore the currently forming candle.
    k = klines[:-1]

    closes = [float(x[4]) for x in k]
    highs = [float(x[2]) for x in k]
    lows = [float(x[3]) for x in k]
    volumes = [float(x[5]) for x in k]
    quote_volumes = [float(x[7]) for x in k]

    price = closes[-1]

    if price < MIN_PRICE:
        return None

    # Momentum
    gain_5m = pct(closes[-1], closes[-2])
    gain_15m = pct(closes[-1], closes[-4])
    gain_1h = pct(closes[-1], closes[-13])

    gain_24h = ticker["change"]
    qv24 = ticker["quote_volume"]

    # Recent volume in dollars/quote asset.
    vol_15m = sum(quote_volumes[-3:])
    baseline_15m = safe_mean(
        [sum(quote_volumes[i-3:i]) for i in range(23, len(quote_volumes)-3, 3)]
    )
    volume_ratio_15m = vol_15m / baseline_15m if baseline_15m else 0

    vol_1h = sum(quote_volumes[-12:])
    baseline_1h = safe_mean(
        [sum(quote_volumes[i-12:i]) for i in range(48, len(quote_volumes)-12, 12)]
    )
    volume_ratio_1h = vol_1h / baseline_1h if baseline_1h else 0

    # Acceleration: current 15m block vs previous 15m block,
    # normalized by the historical 15m baseline.
    prev_15m = sum(quote_volumes[-6:-3])
    acceleration = vol_15m / prev_15m if prev_15m else 0

    # Breakout: compare current price with prior 4h high,
    # excluding the current candle.
    prior_high = max(highs[-(BREAKOUT_LOOKBACK + 1):-1])
    breakout_pct = pct(price, prior_high)

    # Candle quality: close position in the latest completed candle.
    last_high = highs[-1]
    last_low = lows[-1]
    rng = last_high - last_low
    close_location = (price - last_low) / rng if rng > 0 else 0.5

    # Hard filters: we want early movement, not a coin already +50%.
    if qv24 < MIN_24H_QUOTE_VOLUME:
        return None
    if gain_24h > MAX_24H_GAIN_PCT:
        return None
    if gain_15m < MIN_15M_GAIN_PCT or gain_15m > MAX_15M_GAIN_PCT:
        return None
    if gain_1h < MIN_1H_GAIN_PCT or gain_1h > MAX_1H_GAIN_PCT:
        return None

    score = 0

    # 1) Volume expansion: max 30
    if volume_ratio_1h >= VOLUME_RATIO_STRONG:
        score += 30
    elif volume_ratio_1h >= VOLUME_RATIO_GOOD:
        score += 20
    elif volume_ratio_1h >= 1.5:
        score += 10

    # 2) Short-term volume acceleration: max 20
    if acceleration >= ACCELERATION_STRONG:
        score += 20
    elif acceleration >= ACCELERATION_GOOD:
        score += 12
    elif acceleration >= 1.2:
        score += 6

    # 3) Price momentum: max 15
    if 2 <= gain_15m <= 10:
        score += 15
    elif 0 <= gain_15m < 2:
        score += 7
    elif 10 < gain_15m <= 12:
        score += 5

    # 4) Breakout: max 20
    if breakout_pct >= 0:
        score += 20
    elif breakout_pct >= -NEAR_BREAKOUT_PCT:
        score += 10

    # 5) Candle quality: max 10
    if close_location >= 0.75:
        score += 10
    elif close_location >= 0.60:
        score += 5

    # 6) 24h movement restraint: max 5
    if 0 <= gain_24h <= 12:
        score += 5
    elif gain_24h <= 20:
        score += 2

    # Setup classification
    if breakout_pct >= 0 and volume_ratio_1h >= 4 and acceleration >= 2:
        setup = "BREAKOUT + VOLUME ACCELERATION"
    elif volume_ratio_1h >= 4 and acceleration >= 2:
        setup = "VOLUME EXPANSION"
    elif breakout_pct >= 0:
        setup = "BREAKOUT"
    else:
        setup = "EARLY MOMENTUM"

    return Signal(
        symbol=symbol,
        price=price,
        gain_5m=gain_5m,
        gain_15m=gain_15m,
        gain_1h=gain_1h,
        gain_24h=gain_24h,
        quote_volume_24h=qv24,
        volume_ratio_1h=volume_ratio_1h,
        volume_ratio_15m=volume_ratio_15m,
        volume_acceleration=acceleration,
        breakout_pct=breakout_pct,
        close_location=close_location,
        score=score,
        setup=setup,
    )


def telegram_send(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    try:
        session.post(
            url,
            json={"chat_id": chat_id, "text": text},
            timeout=HTTP_TIMEOUT,
        ).raise_for_status()
    except Exception as e:
        logging.warning("Telegram error: %s", e)


def save_csv(signal: Signal):
    exists = Path(LOG_FILE).exists()

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=asdict(signal).keys())

        if not exists:
            writer.writeheader()

        writer.writerow(asdict(signal))


def format_signal(s: Signal) -> str:
    return (
        f"🚨 {s.setup}\n"
        f"{s.symbol} | SCORE {s.score}/100\n"
        f"Price: {s.price:.12g}\n"
        f"5m: {s.gain_5m:+.2f}% | 15m: {s.gain_15m:+.2f}% | "
        f"1h: {s.gain_1h:+.2f}% | 24h: {s.gain_24h:+.2f}%\n"
        f"Volume 1h: {s.volume_ratio_1h:.2f}x | "
        f"Volume 15m: {s.volume_ratio_15m:.2f}x | "
        f"Acceleration: {s.volume_acceleration:.2f}x\n"
        f"Breakout: {s.breakout_pct:+.2f}% | "
        f"Close location: {s.close_location:.0%}\n"
        f"24h quote volume: ${s.quote_volume_24h:,.0f}"
    )


# ==================== CROSS-RUN STATE (new) ====================
# GitHub Actions runs each scan on a brand-new, throwaway virtual machine.
# Without this, the cooldown (last_alert) and the scan counter would reset
# to zero on every single run. The workflow commits these small JSON files
# back into the repo after each run so the next run can pick up where the
# last one left off.

def load_last_alert() -> dict:
    if not Path(LAST_ALERT_FILE).exists():
        return {}
    try:
        with open(LAST_ALERT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning("Could not read %s: %s", LAST_ALERT_FILE, e)
        return {}


def save_last_alert(last_alert: dict):
    try:
        with open(LAST_ALERT_FILE, "w", encoding="utf-8") as f:
            json.dump(last_alert, f)
    except Exception as e:
        logging.warning("Could not write %s: %s", LAST_ALERT_FILE, e)


def load_scan_count() -> int:
    if not Path(SCAN_COUNT_FILE).exists():
        return 0
    try:
        with open(SCAN_COUNT_FILE, "r", encoding="utf-8") as f:
            return int(json.load(f).get("count", 0))
    except Exception:
        return 0


def save_scan_count(n: int):
    try:
        with open(SCAN_COUNT_FILE, "w", encoding="utf-8") as f:
            json.dump({"count": n}, f)
    except Exception as e:
        logging.warning("Could not write %s: %s", SCAN_COUNT_FILE, e)

# ==================================================================


# ==================== OUTCOME TRACKING (new) ====================

def load_pending() -> dict:
    """Load the in-flight signals we're still waiting to score."""
    if not Path(OUTCOME_STATE_FILE).exists():
        return {}
    try:
        with open(OUTCOME_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning("Could not read %s: %s", OUTCOME_STATE_FILE, e)
        return {}


def save_pending(pending: dict):
    try:
        with open(OUTCOME_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(pending, f)
    except Exception as e:
        logging.warning("Could not write %s: %s", OUTCOME_STATE_FILE, e)


def register_signal(signal: Signal, pending: dict):
    """Start tracking a freshly-alerted signal so we can grade it later."""
    key = f"{signal.symbol}|{datetime.now(timezone.utc).isoformat()}"
    pending[key] = {
        "symbol": signal.symbol,
        "score": signal.score,
        "setup": signal.setup,
        "entry_price": signal.price,
        "entry_time": datetime.now(timezone.utc).isoformat(),
        "checked_minutes": [],
        # Trailing-stop simulation state:
        "peak_price": signal.price,
        "activated": False,
        "sim_closed": False,
    }


def log_outcome(symbol, entry_time, score, setup, entry_price,
                 checkpoint_min, current_price, change_pct):
    exists = Path(OUTCOME_LOG_FILE).exists()

    with open(OUTCOME_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow([
                "symbol", "entry_time", "score", "setup", "entry_price",
                "checkpoint_min", "price_at_checkpoint", "change_pct",
            ])
        writer.writerow([
            symbol, entry_time, score, setup, entry_price,
            checkpoint_min, current_price, f"{change_pct:.2f}",
        ])


def check_outcomes(tickers: dict, pending: dict):
    """
    For every signal we're tracking, see if enough time has passed to
    grade it at one of OUTCOME_CHECKPOINTS_MIN, and log the result.
    Signals older than OUTCOME_MAX_AGE_MIN are dropped from tracking.
    """
    now = datetime.now(timezone.utc)
    to_delete = []

    for key, entry in pending.items():
        try:
            entry_time = datetime.fromisoformat(entry["entry_time"])
        except Exception:
            to_delete.append(key)
            continue

        age_min = (now - entry_time).total_seconds() / 60.0
        symbol = entry["symbol"]

        current_price = tickers.get(symbol, {}).get("price")

        if current_price is not None:
            for cp in OUTCOME_CHECKPOINTS_MIN:
                if age_min >= cp and cp not in entry["checked_minutes"]:
                    change_pct = pct(current_price, entry["entry_price"])
                    log_outcome(
                        symbol, entry["entry_time"], entry["score"],
                        entry.get("setup", ""), entry["entry_price"],
                        cp, current_price, change_pct,
                    )
                    entry["checked_minutes"].append(cp)

        if age_min >= OUTCOME_MAX_AGE_MIN:
            to_delete.append(key)

    for key in to_delete:
        pending.pop(key, None)


def build_accuracy_summary_text() -> str | None:
    """Build the win-rate / average-return table as plain text, or None if empty."""
    if not Path(OUTCOME_LOG_FILE).exists():
        return None

    stats = {}

    with open(OUTCOME_LOG_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cp = int(row["checkpoint_min"])
                change = float(row["change_pct"])
            except (KeyError, ValueError):
                continue
            stats.setdefault(cp, []).append(change)

    if not stats:
        return None

    lines = ["📊 SIGNAL ACCURACY SUMMARY (all-time)"]
    for cp in sorted(stats):
        changes = stats[cp]
        n = len(changes)
        wins = sum(1 for c in changes if c > 0)
        win_rate = (wins / n * 100) if n else 0.0
        avg = safe_mean(changes)
        best = max(changes) if changes else 0.0
        worst = min(changes) if changes else 0.0
        lines.append(
            f"+{cp:>3}m | n={n:<4} | win={win_rate:5.1f}% | "
            f"avg={avg:+6.2f}% | best={best:+6.2f}% | worst={worst:+6.2f}%"
        )

    return "\n".join(lines)


def print_accuracy_summary():
    text = build_accuracy_summary_text()
    if text:
        print("\n" + "-" * 72)
        print(text)
        print("-" * 72)

    if SEND_SUMMARY_TO_TELEGRAM and text:
        telegram_send(text)

# ==================================================================


# ==================== TRAILING-STOP SIMULATION (new) ====================
#
# This does NOT place real orders. It answers a different question than
# check_outcomes(): "if I had actually traded this signal with a stop-loss
# strategy, would I have made money, and how long would I have held it?"
#
# Rule simulated:
#   1) Hard stop at -SIM_INITIAL_STOP_PCT% from entry (protects against an
#      immediate reversal / fakeout).
#   2) Once price rises SIM_ACTIVATION_PCT% above entry, switch to a
#      trailing stop that sits SIM_TRAILING_PCT% below the highest price
#      seen since entry (locks in gains as the move continues).
#   3) If neither stop is hit within SIM_MAX_HOLD_MIN minutes, force-close
#      at whatever the price is then (time exit).

def log_sim_close(entry: dict, exit_price: float, exit_reason: str, minutes_held: float):
    exists = Path(SIM_LOG_FILE).exists()
    entry_price = entry["entry_price"]
    return_pct = pct(exit_price, entry_price)
    max_gain_pct = pct(entry["peak_price"], entry_price)

    with open(SIM_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow([
                "symbol", "entry_time", "score", "setup", "entry_price",
                "exit_price", "exit_reason", "return_pct", "max_gain_pct",
                "minutes_held",
            ])
        writer.writerow([
            entry["symbol"], entry["entry_time"], entry["score"], entry.get("setup", ""),
            entry_price, exit_price, exit_reason, f"{return_pct:.2f}",
            f"{max_gain_pct:.2f}", f"{minutes_held:.0f}",
        ])

    return return_pct


def simulate_trailing_stop(tickers: dict, pending: dict):
    now = datetime.now(timezone.utc)

    for entry in pending.values():
        if entry.get("sim_closed"):
            continue

        symbol = entry["symbol"]
        current_price = tickers.get(symbol, {}).get("price")
        if current_price is None:
            continue

        entry_time = datetime.fromisoformat(entry["entry_time"])
        age_min = (now - entry_time).total_seconds() / 60.0
        entry_price = entry["entry_price"]

        entry["peak_price"] = max(entry["peak_price"], current_price)

        exit_reason = None

        if not entry["activated"]:
            hard_stop = entry_price * (1 - SIM_INITIAL_STOP_PCT / 100)
            activation_price = entry_price * (1 + SIM_ACTIVATION_PCT / 100)

            if current_price <= hard_stop:
                exit_reason = "initial_stop"
            elif current_price >= activation_price:
                entry["activated"] = True

        if exit_reason is None and entry["activated"]:
            trailing_stop = entry["peak_price"] * (1 - SIM_TRAILING_PCT / 100)
            if current_price <= trailing_stop:
                exit_reason = "trailing_stop"

        if exit_reason is None and age_min >= SIM_MAX_HOLD_MIN:
            exit_reason = "time_exit"

        if exit_reason:
            return_pct = log_sim_close(entry, current_price, exit_reason, age_min)
            entry["sim_closed"] = True

            msg = (
                f"🧪 SIM TRADE CLOSED — {symbol}\n"
                f"Reason: {exit_reason}\n"
                f"Entry: {entry_price:.8g} → Exit: {current_price:.8g}\n"
                f"Result: {return_pct:+.2f}% after {age_min:.0f} min "
                f"(peak was {pct(entry['peak_price'], entry_price):+.2f}%)"
            )
            print("\n" + msg)
            telegram_send(msg)


def build_sim_summary_text() -> str | None:
    if not Path(SIM_LOG_FILE).exists():
        return None

    rows = []
    with open(SIM_LOG_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                row["return_pct"] = float(row["return_pct"])
                row["minutes_held"] = float(row["minutes_held"])
            except (KeyError, ValueError):
                continue
            rows.append(row)

    if not rows:
        return None

    returns = [r["return_pct"] for r in rows]
    n = len(returns)
    wins = sum(1 for r in returns if r > 0)
    win_rate = wins / n * 100
    avg_return = safe_mean(returns)
    avg_hold = safe_mean([r["minutes_held"] for r in rows])

    by_reason = {}
    for r in rows:
        by_reason.setdefault(r["exit_reason"], 0)
        by_reason[r["exit_reason"]] += 1

    lines = [
        "🧪 TRAILING-STOP SIM SUMMARY (all-time)",
        f"n={n} | win rate={win_rate:.1f}% | avg return={avg_return:+.2f}% | "
        f"avg hold={avg_hold:.0f} min",
        "Exit reasons: " + ", ".join(f"{k}={v}" for k, v in by_reason.items()),
    ]
    return "\n".join(lines)


def print_sim_summary():
    text = build_sim_summary_text()
    if text:
        print("\n" + "-" * 72)
        print(text)
        print("-" * 72)

    if SEND_SUMMARY_TO_TELEGRAM and text:
        telegram_send(text)

# ==========================================================================


def run_scan(symbols, tickers, last_alert, pending):
    # First, grade any signals from earlier scans whose checkpoints are due,
    # and update the trailing-stop simulation for every open "position".
    check_outcomes(tickers, pending)
    simulate_trailing_stop(tickers, pending)

    candidates = []

    # Pre-filter using 24h ticker to reduce API calls.
    universe = [
        s for s in symbols
        if s in tickers
        and tickers[s]["quote_volume"] >= MIN_24H_QUOTE_VOLUME
        and tickers[s]["change"] <= MAX_24H_GAIN_PCT
    ]

    logging.info("Scanning %d symbols...", len(universe))

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(analyze, s, tickers[s]): s
            for s in universe
        }

        for fut in as_completed(futures):
            try:
                signal = fut.result()
                if signal and signal.score >= ALERT_SCORE:
                    candidates.append(signal)
            except Exception as e:
                logging.debug("Worker error: %s", e)

    candidates.sort(
        key=lambda x: (
            x.score,
            x.volume_ratio_1h,
            x.volume_acceleration,
            x.breakout_pct,
        ),
        reverse=True,
    )

    now = time.time()

    for s in candidates[:TOP_N]:
        last = last_alert.get(s.symbol, 0)

        # Always display top candidates, but Telegram/CSV only after cooldown.
        print("\n" + "=" * 72)
        print(format_signal(s))

        if now - last >= COOLDOWN_SECONDS:
            save_csv(s)
            telegram_send(format_signal(s))
            last_alert[s.symbol] = now
            register_signal(s, pending)   # start grading this signal

    save_pending(pending)

    if not candidates:
        logging.info("No signals >= %d right now.", ALERT_SCORE)
    else:
        logging.info(
            "Found %d candidates. Top: %s (score %d)",
            len(candidates),
            candidates[0].symbol,
            candidates[0].score,
        )


def parse_args():
    p = argparse.ArgumentParser(description="Binance Early-Pump Scanner")
    p.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one scan cycle and exit (used by GitHub Actions / cron). "
             "Without this flag the script loops forever like before, for local/Colab use.",
    )
    return p.parse_args()


def perform_scan_cycle(symbols, last_alert, pending, scan_count: int) -> int:
    """One full cycle: fetch tickers, scan, grade outcomes, maybe print summaries."""
    tickers = get_24h_tickers()
    run_scan(symbols, tickers, last_alert, pending)

    scan_count += 1
    if scan_count % PRINT_SUMMARY_EVERY_N_SCANS == 0:
        print_accuracy_summary()
        print_sim_summary()

    # Persist everything a fresh process/VM would otherwise lose.
    save_last_alert(last_alert)
    save_scan_count(scan_count)

    return scan_count


def main():
    args = parse_args()

    print("""
============================================================
 Binance Early-Pump Scanner
 Spot / USDT / 5m
 No automatic trading
============================================================
""")

    try:
        symbols = get_usdt_symbols()
        logging.info("Loaded %d Binance USDT Spot symbols.", len(symbols))
    except Exception as e:
        logging.error("Cannot load Binance symbols: %s", e)
        sys.exit(1)

    last_alert = load_last_alert()
    pending = load_pending()
    scan_count = load_scan_count()

    if args.once:
        # GitHub Actions / cron mode: one scan, then exit so the runner can shut down.
        try:
            perform_scan_cycle(symbols, last_alert, pending, scan_count)
        except Exception as e:
            logging.exception("Scan failed: %s", e)
            sys.exit(1)
        logging.info("Single scan complete — exiting (GitHub Actions mode).")
        return

    # Local / Colab mode: loop forever like before.
    while True:
        started = time.time()

        try:
            scan_count = perform_scan_cycle(symbols, last_alert, pending, scan_count)
        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            logging.exception("Scan failed: %s", e)

        elapsed = time.time() - started
        sleep_for = max(5, SCAN_EVERY_SECONDS - elapsed)

        logging.info("Next scan in %.0f seconds.", sleep_for)

        try:
            time.sleep(sleep_for)
        except KeyboardInterrupt:
            print("\nStopped.")
            break


if __name__ == "__main__":
    main()
