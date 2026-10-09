# ============================================================
# BINANCE EARLY-PUMP SCANNER V3.2+DIAGNOSTICS — VPS / COLAB / TELEGRAM
# ============================================================
# نسخة محسّنة بناءً على تحليل نتائج فعلية (2026-10-06):
# - إصلاح فشل CRCLBUSDT (score=100 بدون انفجار)
# - رفض wash trading وevent-driven volume spikes
# - إضافة Trend Continuity + Green Streak
# - Entry / TP1 / TP2 / SL واقعية مبنية على ATR
# - إصلاح تتبع النتائج (حفظ عند كل نقطة)
# - تنظيف دوري للحالة
# - PERSISTENT + REVERSAL lanes
# - Shariah list auto-update
# ------------------------------------------------------------
# V3.2 (2026-10-09) — إصلاحات حرجة:
# 1) maybe_final_signal: إضافة candidates قبل التحقق من الأهلية
# 2) final_candidate_eligible: استقلال PERSISTENT/REVERSAL
# 3) PERSISTENT thresholds: vol_1h = 1.5, persistence = 2.0
# ============================================================

import requests, time, math, csv, os, json, sys, logging, re, html
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scanner.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("scanner")

# ---------- Binance ----------
BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]
STATE_FILE = "scanner_state.json"
INTERVAL = "5m"
KLINES_LIMIT = 180

# ---------- Scan ----------
SCAN_EVERY_SECONDS = 300
MIN_24H_QUOTE_VOLUME = 3_000_000
MAX_24H_GAIN_PCT = 35.0
MIN_15M_GAIN = -2.0
MAX_15M_GAIN = 10.0
MIN_1H_GAIN = -12.0
MAX_1H_GAIN = 18.0

# ---------- Wash Trading Detector ----------
WASH_MAX_PRICE_MOVE_FOR_BIG_VOL = 0.5
WASH_ACCEL_THRESHOLD = 5.0
WASH_VOL_1H_THRESHOLD = 6.0
WASH_VOL_15M_HARD_CAP = 18.0

# ---------- FINAL gate ----------
FINAL_MIN_15M_MOVE = 1.25
FINAL_MIN_5M_MOVE = 0.25
FINAL_MIN_VOLUME_15M = 4.0
FINAL_MIN_VOLUME_1H = 2.0
FINAL_MIN_ACCELERATION = 1.2
FINAL_MAX_ACCELERATION = 4.5
FINAL_MIN_24H_VOLUME = 3_000_000
FINAL_MAX_24H_GAIN = 12.0
FINAL_MAX_RSI = 78.0
FINAL_MIN_BUY_PRESSURE = 0.55
FINAL_MIN_GREEN_STREAK = 2
FINAL_MIN_TREND_CONTINUITY = 3

# ---------- EXPLOSIVE EARLY ----------
EXPLOSIVE_MIN_5M_MOVE = 1.0
EXPLOSIVE_MIN_15M_MOVE = 0.8
EXPLOSIVE_MIN_VOLUME_15M = 4.0
EXPLOSIVE_MAX_VOLUME_15M = 12.0
EXPLOSIVE_MIN_VOLUME_1H = 2.0
EXPLOSIVE_MIN_ACCELERATION = 1.5
EXPLOSIVE_MAX_ACCELERATION = 4.0
EXPLOSIVE_MIN_BUY_PRESSURE = 0.58
EXPLOSIVE_MIN_24H_VOLUME = 3_000_000
EXPLOSIVE_MAX_24H_GAIN = 15.0
EXPLOSIVE_MAX_1H_GAIN = 8.0
EXPLOSIVE_MAX_RSI = 76.0
EXPLOSIVE_MAX_UPPER_WICK = 0.40
EXPLOSIVE_MIN_BREAKOUT = -1.50
EXPLOSIVE_MIN_GREEN_STREAK = 3
EXPLOSIVE_MIN_TREND_CONTINUITY = 4

# ---------- Persistent lane ----------
PERSISTENT_FINAL_SCORE = 70
PERSISTENT_MIN_5M_MOVE = 0.70
PERSISTENT_MIN_15M_MOVE = 1.25
PERSISTENT_MIN_VOLUME_15M = 2.50
PERSISTENT_MIN_VOLUME_1H = 1.50          # ← V3.2: كان 2.00
PERSISTENT_MIN_PERSISTENCE = 2.00        # ← V3.2: كان 2.50
PERSISTENT_MIN_BUY_PRESSURE = 0.54
PERSISTENT_MAX_1H_GAIN = 18.0
PERSISTENT_MAX_24H_GAIN = 15.0
PERSISTENT_MAX_RSI = 80.0
PERSISTENT_MAX_UPPER_WICK = 0.45
PERSISTENT_MIN_BREAKOUT = -1.25
PERSISTENT_MIN_GREEN_STREAK = 2
PERSISTENT_MIN_TREND_CONTINUITY = 3

# ---------- Reversal lane ----------
REVERSAL_FINAL_SCORE = 72
REVERSAL_MIN_5M_MOVE = 0.50
REVERSAL_MIN_15M_MOVE = 1.25
REVERSAL_MIN_VOLUME_15M = 3.00
REVERSAL_MIN_VOLUME_1H = 1.70
REVERSAL_MIN_BUY_PRESSURE = 0.54
REVERSAL_MIN_24H_VOLUME = 3_000_000
REVERSAL_MAX_24H_GAIN = 12.0
REVERSAL_MAX_RSI = 78.0
REVERSAL_MAX_UPPER_WICK = 0.45
REVERSAL_MIN_BREAKOUT = -1.50
REVERSAL_MIN_GREEN_STREAK = 2
REVERSAL_MIN_TREND_CONTINUITY = 3

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI",
    "USDE", "USD1", "RLUSD", "USDD", "EUR", "EURI", "USTC",
}
MEGA_CAP_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE"}

# ---------- Risk model ----------
RISK_MIN_SL_PCT = 3.5
RISK_MAX_SL_PCT = 12.0
RISK_ATR_MULT_SL = 1.5
RISK_RR_TP1 = 1.5
RISK_RR_TP2 = 3.0
RISK_HIGH_ATR_THRESHOLD = 6.0
RISK_HIGH_ATR_TP1_RR = 1.2
RISK_HIGH_RSI_THRESHOLD = 76.0
RISK_HIGH_RSI_TP1_RR = 1.0
RISK_LOW_LIQ_TP1_RR = 1.0

# ---------- Scores ----------
WATCH_SCORE = 65
FINAL_SCORE = 80
EXCEPTIONAL_SCORE = 88
CONFIRMATIONS_REQUIRED = 2
CANDIDATE_EXPIRY_MINUTES = 45
FINAL_COOLDOWN_HOURS = 12
MIN_CANDIDATE_SCORE = 70   # ← V3.2: score أدنى لإضافة الرمز لقائمة المرشحين

# ---------- Cross-Exchange ----------
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
CROSS_CONFIRM_AGREE_PCT = 1.0
CROSS_CONFIRM_DISAGREE_PCT = -1.0
SUPPRESS_ON_DIVERGENCE = False
CROSS_CACHE_TTL_SECONDS = 300

# ---------- Tracking ----------
TRACK_MINUTES = [5, 15, 30, 60, 240, 1440]
TRACK_FILE = "early_pump_signal_results.csv"
EARLY_WATCH_LOG_FILE = "early_watch_log.csv"
SCAN_DIAGNOSTICS_FILE = "early_scan_diagnostics.csv"
PREPUMP_MIN_SCORE = 55
PREPUMP_MAX_BREAKOUT = -6.0
PREPUMP_MIN_4H_GAIN = 0.5
PREPUMP_MIN_VOLUME_1H = 1.5
PREPUMP_MIN_VOLUME_15M = 1.2
OKX_LEAD_MIN_1H_GAIN = 2.0
OKX_LEAD_MAX_BINANCE_1H_GAIN = 1.0
OKX_LEAD_COOLDOWN_HOURS = 4
OKX_LEAD_MAX_CHECKS_PER_CYCLE = 10
EARLY_WATCH_COOLDOWN_HOURS = 2

# ---------- CSV log retention ----------
LOG_RETENTION_DAYS = 4
LOG_CLEANUP_INTERVAL_HOURS = 1

# ---------- Shariah list auto-update ----------
SHARIAH_CACHE_FILE = "shariah_compliant_bases.json"
SHARIAH_UPDATE_HOURS = 24
SHARIAH_SOURCE_URL = "https://cryptoummah.com/halal-crypto-list"
SHARIAH_SOURCE_API = "https://gethalalcrypto.com/api/datasets/halal-coins?status=halal"

# ---------- Telegram ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    log.warning("TELEGRAM_BOT_TOKEN أو TELEGRAM_CHAT_ID غير موجودين — سيعمل بدون إرسال.")

session = requests.Session()
session.headers.update({"User-Agent": "Binance-Early-Pump-Scanner-V3.2/1.0"})

# ---------- Global state ----------
candidates = {}
final_cooldown = {}
active_tracks = {}
early_watch_logged = {}
cross_cache = {}
okx_lead_logged = {}
shariah_excluded_this_startup = []


# ============================================================
# HTTP
# ============================================================

def get_json(path, params=None, retries=3):
    last_err = None
    for base in BASE_URLS:
        for attempt in range(retries):
            try:
                r = session.get(base + path, params=params, timeout=15)
                if r.status_code == 451:
                    last_err = requests.HTTPError(f"451 from {base}", response=r)
                    break
                if r.status_code in (418, 429):
                    wait = 2 ** attempt
                    log.warning(f"Rate limit {r.status_code} من {base} — انتظار {wait}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.HTTPError:
                raise
            except requests.RequestException as e:
                last_err = e
                time.sleep(1 + attempt)
                continue
    raise last_err if last_err else RuntimeError("no base url worked")


def telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ============================================================
# Shariah list
# ============================================================

STABLECOIN_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "BFUSD",
    "USDE", "PYUSD", "EUR", "GBP", "AEUR", "USD1", "WBETH",
}
SHARIAH_COMPLIANT_BASES = {
    "BTC", "BNB", "ADA", "ETH", "XRP", "XLM", "ALGO", "AVAX",
    "LTC", "DOT", "MATIC", "XTZ", "SOL", "LINK", "ETC", "UNI",
    "ATOM", "FIL", "HNT", "ICP", "XMR", "NEAR", "THETA", "TON",
    "TRX", "SUI", "CTSI", "STRK", "W", "OGN", "MET",
}
SHARIAH_USER_APPROVED_BASES = {"W", "CTSI", "STRK", "OGN", "MET"}
SHARIAH_FILTER_ENABLED = True


def _extract_symbols_from_halalcrypto(payload):
    rows = payload
    if isinstance(payload, dict):
        for key in ("data", "results", "assets", "coins", "rows"):
            if isinstance(payload.get(key), list):
                rows = payload[key]
                break
    if not isinstance(rows, list):
        return set()
    out = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", row.get("verdict", ""))).strip().lower()
        if status and "halal" not in status:
            continue
        sym = row.get("symbol") or row.get("ticker") or row.get("base")
        if isinstance(sym, str):
            sym = sym.strip().upper()
            if sym and sym.isascii() and 1 <= len(sym) <= 20:
                out.add(sym)
    return out


def _extract_symbols_from_crypto_ummmah(text):
    out = set()
    clean = html.unescape(re.sub(r"<[^>]+>", " ", text))
    clean = re.sub(r"\s+", " ", clean)
    pat = re.compile(r"\b([A-Z][A-Z0-9]{1,14})\s*\|\s*[0-9]+(?:\.[0-9]+)?\s*\|\s*HALAL\b")
    for m in pat.finditer(clean):
        out.add(m.group(1).upper())
    pat2 = re.compile(r"\b([A-Z][A-Z0-9]{1,14})\s+Same category:.*?\|\s*Halal\b", re.S)
    for m in pat2.finditer(clean):
        out.add(m.group(1).upper())
    return out


def load_user_shariah_overrides():
    filename = "shariah_user_overrides.json"
    if not os.path.exists(filename):
        return set(SHARIAH_USER_APPROVED_BASES)
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        vals = data.get("approved_symbols", []) if isinstance(data, dict) else []
        return {str(x).strip().upper() for x in vals if isinstance(x, str) and x.strip()}
    except Exception as e:
        log.warning(f"Shariah overrides load failed: {e}")
        return set(SHARIAH_USER_APPROVED_BASES)


def load_shariah_cache():
    if not os.path.exists(SHARIAH_CACHE_FILE):
        return set()
    try:
        with open(SHARIAH_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(str(x).upper() for x in data.get("symbols", []) if isinstance(x, str))
    except Exception as e:
        log.warning(f"Shariah cache load failed: {e}")
        return set()


def save_shariah_cache(symbols, source):
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "symbols": sorted(symbols),
    }
    with open(SHARIAH_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def update_shariah_list(force=False):
    global SHARIAH_COMPLIANT_BASES
    cached = load_shariah_cache()
    now = datetime.now(timezone.utc)
    should_update = force or not os.path.exists(SHARIAH_CACHE_FILE)
    if not should_update:
        try:
            with open(SHARIAH_CACHE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            ts = pd.to_datetime(d.get("updated_at_utc"), utc=True, errors="coerce")
            should_update = pd.isna(ts) or (now - ts.to_pydatetime()).total_seconds() >= SHARIAH_UPDATE_HOURS * 3600
        except Exception:
            should_update = True

    discovered = set()
    source = "cache/fallback"
    if should_update:
        try:
            r = session.get(SHARIAH_SOURCE_API, timeout=20)
            r.raise_for_status()
            discovered = _extract_symbols_from_halalcrypto(r.json())
            if discovered:
                source = SHARIAH_SOURCE_API
        except Exception as e:
            log.warning(f"Shariah API update failed: {e}")

        if not discovered:
            try:
                r = session.get(SHARIAH_SOURCE_URL, timeout=20)
                r.raise_for_status()
                discovered = _extract_symbols_from_crypto_ummmah(r.text)
                if discovered:
                    source = SHARIAH_SOURCE_URL
            except Exception as e:
                log.warning(f"Shariah web update failed: {e}")

        if discovered:
            save_shariah_cache(discovered, source)
            cached = discovered
            log.info(f"Shariah list updated: {len(discovered)} halal symbols from {source}")
        else:
            log.warning("Shariah update unavailable — using last cached/fallback list")

    if cached:
        SHARIAH_COMPLIANT_BASES = set(cached)
    SHARIAH_COMPLIANT_BASES.update(load_user_shariah_overrides())
    log.info(f"Shariah scan universe: {len(SHARIAH_COMPLIANT_BASES)} symbols (including user-approved overrides)")


def get_symbols():
    global shariah_excluded_this_startup
    shariah_excluded_this_startup = []
    data = get_json("/api/v3/exchangeInfo")
    out = []
    for s in data["symbols"]:
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("isSpotTradingAllowed") is False:
            continue
        sym = s["symbol"]
        if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        base = sym[:-4]
        if base in STABLECOIN_BASES or base in MEGA_CAP_BASES:
            continue
        if SHARIAH_FILTER_ENABLED and base not in SHARIAH_COMPLIANT_BASES:
            shariah_excluded_this_startup.append(sym)
            continue
        out.append(sym)
    return out


def get_tickers():
    data = get_json("/api/v3/ticker/24hr")
    out = {}
    for x in data:
        sym = x["symbol"]
        if not sym.endswith("USDT"):
            continue
        try:
            out[sym] = {
                "price": float(x["lastPrice"]),
                "change": float(x["priceChangePercent"]),
                "quote_volume": float(x["quoteVolume"]),
            }
        except Exception:
            pass
    return out


# ============================================================
# Math
# ============================================================

def pct(a, b):
    if b == 0:
        return 0.0
    return (a / b - 1) * 100.0


def mean(v):
    v = [x for x in v if math.isfinite(x)]
    return sum(v) / len(v) if v else 0.0


def ema(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return 100.0 - (100.0 / (1.0 + rs))


def atr_pct(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return (atr / closes[-1] * 100.0) if closes[-1] else None


def pct_slope(values):
    if len(values) < 2 or values[0] == 0:
        return 0.0
    return (values[-1] / values[0] - 1.0) * 100.0


# ============================================================
# Trend continuity + wash detector
# ============================================================

def trend_continuity_score(closes_5m, volumes_5m):
    if len(closes_5m) < 6:
        return 0, 0

    last_closes = closes_5m[-6:]
    last_vols = volumes_5m[-6:]

    changes = [pct(last_closes[i], last_closes[i - 1]) for i in range(1, len(last_closes))]
    green_streak = 0
    for c in reversed(changes):
        if c > 0:
            green_streak += 1
        else:
            break

    green_count = sum(1 for c in changes if c > 0)
    higher_lows = sum(
        1 for i in range(2, len(last_closes))
        if last_closes[i] > last_closes[i - 2]
    )

    vol_increasing = 0
    for i in range(1, len(last_vols)):
        if last_vols[i] >= last_vols[i - 1] * 0.85:
            vol_increasing += 1

    score = 0
    if green_count >= 4:
        score += 3
    elif green_count >= 3:
        score += 2
    if green_streak >= 3:
        score += 2
    if higher_lows >= 3:
        score += 2
    if vol_increasing >= 4:
        score += 1

    return min(score, 8), green_streak


def is_likely_wash_trading(x):
    v15 = x.get("volume_15m", 0)
    v1h = x.get("volume_1h", 0)
    accel = x.get("acceleration", 0)
    g5 = x.get("5m", 0)
    g15 = x.get("15m", 0)
    g1h = x.get("1h", 0)

    if v15 > WASH_VOL_15M_HARD_CAP:
        return True, "vol_15m شاذ (>18x)"

    if v15 > 10 and abs(g15) < WASH_MAX_PRICE_MOVE_FOR_BIG_VOL:
        return True, f"vol_15m={v15:.1f}x مع 15m={g15:+.2f}%"

    if accel > WASH_ACCEL_THRESHOLD and abs(g15) < 1.0:
        return True, f"accel={accel:.1f}x مع 15m={g15:+.2f}%"

    if v1h > WASH_VOL_1H_THRESHOLD and abs(g1h) < 1.0:
        return True, f"vol_1h={v1h:.1f}x مع 1h={g1h:+.2f}%"

    return False, ""


# ============================================================
# Risk model
# ============================================================

def compute_risk_levels(entry, atr_val, rsi_val, liquidity):
    if not entry or entry <= 0:
        return None
    atr_use = atr_val if (atr_val and atr_val > 0) else 4.0

    sl_pct_raw = atr_use * RISK_ATR_MULT_SL
    sl_pct = max(RISK_MIN_SL_PCT, min(sl_pct_raw, RISK_MAX_SL_PCT))

    tp1_rr = RISK_RR_TP1
    notes = []
    if atr_use > RISK_HIGH_ATR_THRESHOLD:
        tp1_rr = min(tp1_rr, RISK_HIGH_ATR_TP1_RR)
        notes.append(f"ATR مرتفع ({atr_use:.1f}%) → TP1 أقرب")
    if rsi_val is not None and rsi_val > RISK_HIGH_RSI_THRESHOLD:
        tp1_rr = min(tp1_rr, RISK_HIGH_RSI_TP1_RR)
        notes.append(f"RSI مرتفع ({rsi_val:.0f}) → TP1 أقرب")
    if liquidity == "LOW":
        tp1_rr = min(tp1_rr, RISK_LOW_LIQ_TP1_RR)
        notes.append("سيولة منخفضة → TP1 أقرب")

    tp2_rr = max(RISK_RR_TP2, tp1_rr + 0.5)

    sl_price = entry * (1 - sl_pct / 100.0)
    risk_per_unit = entry - sl_price
    tp1_price = entry + risk_per_unit * tp1_rr
    tp2_price = entry + risk_per_unit * tp2_rr

    return {
        "entry": entry,
        "sl": sl_price,
        "sl_pct": sl_pct,
        "tp1": tp1_price,
        "tp1_pct": pct(tp1_price, entry),
        "tp1_rr": tp1_rr,
        "tp2": tp2_price,
        "tp2_pct": pct(tp2_price, entry),
        "tp2_rr": tp2_rr,
        "atr_used": atr_use,
        "notes": notes,
    }


# ============================================================
# Analyze
# ============================================================

def analyze(symbol, ticker):
    try:
        klines = get_json(
            "/api/v3/klines",
            {"symbol": symbol, "interval": INTERVAL, "limit": KLINES_LIMIT},
        )
    except Exception as e:
        log.debug(f"analyze klines failed for {symbol}: {e}")
        return {"symbol": symbol, "_rejected": "KLINES_FETCH_ERROR"}

    if len(klines) < 80:
        return {"symbol": symbol, "_rejected": "INSUFFICIENT_KLINES"}

    k = klines[:-1]

    opens = [float(x[1]) for x in k]
    closes = [float(x[4]) for x in k]
    highs = [float(x[2]) for x in k]
    lows = [float(x[3]) for x in k]
    quote_volumes = [float(x[7]) for x in k]
    taker_buy = [float(x[10]) for x in k]

    price = closes[-1]
    if price <= 0:
        return {"symbol": symbol, "_rejected": "INVALID_PRICE"}

    gain_5m = pct(closes[-1], closes[-2])
    gain_15m = pct(closes[-1], closes[-4])
    gain_1h = pct(closes[-1], closes[-13])
    gain_4h = pct(closes[-1], closes[-49]) if len(closes) >= 49 else 0.0
    gain_24h = ticker["change"]
    qv24 = ticker["quote_volume"]

    if qv24 < MIN_24H_QUOTE_VOLUME:
        return {"symbol": symbol, "_rejected": "LOW_24H_QUOTE_VOLUME"}
    if gain_24h > MAX_24H_GAIN_PCT:
        return {"symbol": symbol, "_rejected": "24H_GAIN_ABOVE_MAX"}
    if gain_15m < MIN_15M_GAIN or gain_15m > MAX_15M_GAIN:
        return {"symbol": symbol, "_rejected": "15M_GAIN_OUT_OF_RANGE"}
    if gain_1h < MIN_1H_GAIN or gain_1h > MAX_1H_GAIN:
        return {"symbol": symbol, "_rejected": "1H_GAIN_OUT_OF_RANGE"}

    # Volume
    current_15m = sum(quote_volumes[-3:])
    historical_15m = [sum(quote_volumes[i - 3:i]) for i in range(23, len(quote_volumes) - 3, 3)]
    baseline_15m = mean(historical_15m)
    volume_ratio_15m = current_15m / baseline_15m if baseline_15m else 0

    current_1h = sum(quote_volumes[-12:])
    historical_1h = [sum(quote_volumes[i - 12:i]) for i in range(48, len(quote_volumes) - 12, 12)]
    baseline_1h = mean(historical_1h)
    volume_ratio_1h = current_1h / baseline_1h if baseline_1h else 0

    previous_15m = sum(quote_volumes[-6:-3])
    prev_prev_15m = sum(quote_volumes[-9:-6])
    acceleration = current_15m / previous_15m if previous_15m else 0
    prior_acceleration = previous_15m / prev_prev_15m if prev_prev_15m else 0.0

    # Breakout
    lookback = 48
    previous_high = max(highs[-(lookback + 1):-1])
    breakout_pct = pct(price, previous_high)

    # Candle
    candle_range = highs[-1] - lows[-1]
    close_location = (price - lows[-1]) / candle_range if candle_range else 0.5
    body_pct = pct(closes[-1], opens[-1]) if opens[-1] else 0.0
    upper_wick = highs[-1] - max(opens[-1], closes[-1])
    upper_wick_ratio = upper_wick / candle_range if candle_range else 0.0

    # Indicators
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    atr14_pct = atr_pct(highs, lows, closes, 14)

    ema9_slope_15m = pct_slope(closes[-4:]) if len(closes) >= 4 else 0.0
    ema21_slope_1h = pct_slope(closes[-13:]) if len(closes) >= 13 else 0.0

    trend_bullish = ema9 is not None and ema21 is not None and price > ema9 > ema21
    trend_strong = trend_bullish and ema50 is not None and price > ema50

    # Buy pressure
    current_buy = sum(taker_buy[-3:])
    current_total = sum(quote_volumes[-3:])
    buy_pressure_15m = current_buy / current_total if current_total else 0.5

    prev_buy = sum(taker_buy[-6:-3])
    prev_total = sum(quote_volumes[-6:-3])
    prev_buy_pressure = prev_buy / prev_total if prev_total else 0.5
    buy_pressure_delta = buy_pressure_15m - prev_buy_pressure

    # Trend continuity
    trend_cont, green_streak = trend_continuity_score(closes, quote_volumes)

    # Wash trading
    is_wash, wash_reason = is_likely_wash_trading({
        "volume_15m": volume_ratio_15m,
        "volume_1h": volume_ratio_1h,
        "acceleration": acceleration,
        "5m": gain_5m,
        "15m": gain_15m,
        "1h": gain_1h,
    })

    volume_persistence = min(volume_ratio_15m, volume_ratio_1h)
    recent_high_1h = max(highs[-13:-1])
    distance_from_1h_high = pct(price, recent_high_1h) if recent_high_1h else 0.0
    efficiency = abs(gain_15m) / max(volume_ratio_15m, 1.0)

    # ---------- SCORE ----------
    score = 0

    # Volume
    if volume_ratio_1h >= 6 and gain_15m >= 1.5:
        score += 16
    elif volume_ratio_1h >= 6 and gain_15m >= 0.5:
        score += 8
    elif volume_ratio_1h >= 4 and gain_15m >= 1.0:
        score += 12
    elif volume_ratio_1h >= 4:
        score += 5
    elif volume_ratio_1h >= 2.5:
        score += 7
    elif volume_ratio_1h >= 1.7:
        score += 4

    if volume_ratio_15m >= 10 and gain_15m >= 1.5:
        score += 10
    elif volume_ratio_15m >= 6 and gain_15m >= 1.0:
        score += 8
    elif volume_ratio_15m >= 3:
        score += 5
    elif volume_ratio_15m >= 2:
        score += 3

    if volume_persistence >= 3:
        score += 6
    elif volume_persistence >= 2:
        score += 4
    elif volume_persistence >= 1.5:
        score += 2

    # Acceleration
    if 1.3 <= acceleration <= 3.0 and prior_acceleration >= 1.2:
        score += 6
    elif 1.3 <= acceleration <= 3.0:
        score += 4
    elif acceleration > 4.5:
        score -= 5

    # Earlyness
    if 0.5 <= gain_15m <= 4:
        score += 12
    elif 0 <= gain_15m < 0.5:
        score += 8
    elif 4 < gain_15m <= 6:
        score += 6
    elif 6 < gain_15m <= 10:
        score += 1

    if 0.5 <= gain_1h <= 5:
        score += 8
    elif 0 <= gain_1h < 0.5:
        score += 5
    elif 5 < gain_1h <= 8:
        score += 4
    elif 8 < gain_1h <= 12:
        score += 1

    # Efficiency
    if 0.05 <= efficiency <= 1.25:
        score += 6
    elif efficiency <= 2:
        score += 2
    elif efficiency > 3:
        score -= 5

    # Trend
    if trend_strong:
        score += 7
    elif trend_bullish:
        score += 4
    elif ema9 is not None and ema21 is not None and ema9 < ema21:
        score -= 3

    if ema9_slope_15m > 0.3 and ema21_slope_1h > 0.5:
        score += 4
    elif ema9_slope_15m < -0.5 or ema21_slope_1h < -1.0:
        score -= 4

    # Buy pressure
    if buy_pressure_15m >= 0.62 and buy_pressure_delta >= 0.02:
        score += 7
    elif buy_pressure_15m >= 0.56:
        score += 4
    elif buy_pressure_15m < 0.45:
        score -= 6

    score += trend_cont

    # Breakout
    if -1.0 <= breakout_pct <= 1.5:
        score += 5
    elif 1.5 < breakout_pct <= 3:
        score += 2
    elif breakout_pct > 4:
        score -= 4
    elif breakout_pct < -3:
        score -= 2

    # Exhaustion
    if upper_wick_ratio > 0.35 and body_pct < 0.5:
        score -= 5
    elif close_location >= 0.75 and body_pct > 0:
        score += 3

    # RSI
    if rsi14 is not None:
        if 50 <= rsi14 <= 68:
            score += 4
        elif 68 < rsi14 <= 76:
            score += 2
        elif rsi14 > 82:
            score -= 8
        elif rsi14 < 42:
            score -= 4

    # ATR
    if atr14_pct is not None:
        if 0.4 <= atr14_pct <= 5:
            score += 2
        elif atr14_pct > 8:
            score -= 3

    # 24h
    if 0 <= gain_24h <= 8:
        score += 4
    elif gain_24h < 0:
        score += 2
    elif gain_24h <= 14:
        score += 1
    elif gain_24h > 20:
        score -= 3

    # Liquidity
    if qv24 >= 20_000_000:
        liquidity = "HIGH"; score += 4
    elif qv24 >= 10_000_000:
        liquidity = "GOOD"; score += 3
    elif qv24 >= 5_000_000:
        liquidity = "MEDIUM"; score += 1
    else:
        liquidity = "LOW"

    # Late entry penalty
    if gain_15m > 7 or gain_1h > 10:
        score -= 8
    if gain_15m > 8.5 and gain_1h > 12:
        score -= 10

    # Wash trading penalty
    if is_wash:
        score -= 25

    score = max(0, min(int(round(score)), 100))

    # ---------- SETUP ----------
    if is_wash:
        setup = "SUSPECT WASH"
    elif (volume_ratio_15m >= 5 and volume_ratio_1h >= 2 and 1.3 <= acceleration <= 4.0
          and 0.5 <= gain_15m <= 6 and gain_1h <= 8
          and buy_pressure_15m >= 0.54 and trend_cont >= 3
          and (rsi14 is None or rsi14 < 78)):
        setup = "SMART EARLY PUMP"
    elif (breakout_pct >= -1 and volume_ratio_1h >= 2
          and 1.3 <= acceleration <= 4.0 and gain_15m <= 6):
        setup = "BREAKOUT + VOLUME ACCELERATION"
    elif volume_ratio_1h >= 3 and volume_persistence >= 2:
        setup = "VOLUME EXPANSION"
    elif breakout_pct >= 0:
        setup = "BREAKOUT"
    else:
        setup = "EARLY MOMENTUM"

    # Explosive/Persistent/Reversal
    explosive_probe = {
        "symbol": symbol, "score": score, "5m": gain_5m, "15m": gain_15m, "1h": gain_1h, "4h": gain_4h,
        "24h": gain_24h, "volume_15m": volume_ratio_15m,
        "volume_1h": volume_ratio_1h, "acceleration": acceleration,
        "buy_pressure": buy_pressure_15m, "upper_wick_ratio": upper_wick_ratio,
        "breakout": breakout_pct, "rsi14": rsi14, "quote_volume": qv24,
        "green_streak": green_streak, "trend_continuity": trend_cont,
        "is_wash": is_wash, "volume_persistence": volume_persistence,
    }
    if explosive_early_gate(explosive_probe):
        setup = "EXPLOSIVE EARLY"
    elif persistent_explosion_gate(explosive_probe):
        setup = "PERSISTENT EXPLOSION"
    elif reversal_explosion_gate(explosive_probe):
        setup = "REVERSAL EXPLOSION"
    elif prepump_compressed_gate(explosive_probe):
        setup = "PRE-PUMP COMPRESSED"

    price_confirmation = (
        (gain_15m >= 1.0 and gain_5m >= 0.3)
        or (gain_1h >= 2.0 and gain_5m >= 0.2)
        or (breakout_pct >= 0.0 and close_location >= 0.90
            and volume_ratio_1h >= 4.0 and gain_15m >= 0.6)
    )
    continuation_quality = (
        buy_pressure_15m >= 0.52
        and (rsi14 is None or rsi14 < 80)
        and upper_wick_ratio < 0.50
        and not (gain_15m > 8 and gain_1h > 12)
        and not is_wash
    )

    risk = compute_risk_levels(price, atr14_pct, rsi14, liquidity)

    return {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "5m": gain_5m, "15m": gain_15m, "1h": gain_1h, "4h": gain_4h, "24h": gain_24h,
        "volume_1h": volume_ratio_1h,
        "volume_15m": volume_ratio_15m,
        "acceleration": acceleration,
        "prior_acceleration": prior_acceleration,
        "breakout": breakout_pct,
        "close_location": close_location,
        "score": score,
        "setup": setup,
        "quote_volume": qv24,
        "liquidity": liquidity,
        "ema9": ema9, "ema21": ema21, "ema50": ema50,
        "rsi14": rsi14,
        "atr_pct": atr14_pct,
        "buy_pressure": buy_pressure_15m,
        "buy_pressure_delta": buy_pressure_delta,
        "body_pct": body_pct,
        "upper_wick_ratio": upper_wick_ratio,
        "trend": "STRONG_UP" if trend_strong else ("UP" if trend_bullish else "NEUTRAL"),
        "distance_from_1h_high": distance_from_1h_high,
        "volume_persistence": volume_persistence,
        "efficiency": efficiency,
        "price_confirmation": price_confirmation,
        "continuation_quality": continuation_quality,
        "trend_continuity": trend_cont,
        "green_streak": green_streak,
        "is_wash": is_wash,
        "wash_reason": wash_reason,
        "risk": risk,
    }


# ============================================================
# Gates
# ============================================================

def persistent_explosion_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES or x.get("is_wash"):
        return False
    rsi = x.get("rsi14")
    return bool(
        x.get("5m", -999) >= PERSISTENT_MIN_5M_MOVE
        and x.get("15m", -999) >= PERSISTENT_MIN_15M_MOVE
        and x.get("volume_15m", 0) >= PERSISTENT_MIN_VOLUME_15M
        and x.get("volume_1h", 0) >= PERSISTENT_MIN_VOLUME_1H
        and x.get("volume_persistence", 0) >= PERSISTENT_MIN_PERSISTENCE
        and x.get("buy_pressure", 0) >= PERSISTENT_MIN_BUY_PRESSURE
        and x.get("upper_wick_ratio", 1.0) < PERSISTENT_MAX_UPPER_WICK
        and x.get("breakout", -999) >= PERSISTENT_MIN_BREAKOUT
        and rsi is not None and rsi < PERSISTENT_MAX_RSI
        and x.get("1h", 999) <= PERSISTENT_MAX_1H_GAIN
        and x.get("24h", 999) <= PERSISTENT_MAX_24H_GAIN
        and x.get("green_streak", 0) >= PERSISTENT_MIN_GREEN_STREAK
        and x.get("trend_continuity", 0) >= PERSISTENT_MIN_TREND_CONTINUITY
        and x.get("quote_volume", 0) >= MIN_24H_QUOTE_VOLUME
    )


def reversal_explosion_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES or x.get("is_wash"):
        return False
    rsi = x.get("rsi14")
    return bool(
        x.get("5m", -999) >= REVERSAL_MIN_5M_MOVE
        and x.get("15m", -999) >= REVERSAL_MIN_15M_MOVE
        and x.get("1h", 999) <= 1.0
        and x.get("volume_15m", 0) >= REVERSAL_MIN_VOLUME_15M
        and x.get("volume_1h", 0) >= REVERSAL_MIN_VOLUME_1H
        and x.get("buy_pressure", 0) >= REVERSAL_MIN_BUY_PRESSURE
        and x.get("upper_wick_ratio", 1.0) < REVERSAL_MAX_UPPER_WICK
        and x.get("breakout", -999) >= REVERSAL_MIN_BREAKOUT
        and rsi is not None and rsi < REVERSAL_MAX_RSI
        and x.get("24h", 999) <= REVERSAL_MAX_24H_GAIN
        and x.get("green_streak", 0) >= REVERSAL_MIN_GREEN_STREAK
        and x.get("trend_continuity", 0) >= REVERSAL_MIN_TREND_CONTINUITY
        and x.get("quote_volume", 0) >= REVERSAL_MIN_24H_VOLUME
    )


def explosive_early_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES:
        return False
    if x.get("is_wash"):
        return False

    v15 = x.get("volume_15m", 0)
    accel = x.get("acceleration", 0)
    rsi = x.get("rsi14")

    if v15 > EXPLOSIVE_MAX_VOLUME_15M:
        return False
    if accel > EXPLOSIVE_MAX_ACCELERATION:
        return False
    if rsi is None or rsi >= EXPLOSIVE_MAX_RSI:
        return False

    price_impulse = (
        x.get("5m", -999) >= EXPLOSIVE_MIN_5M_MOVE
        and x.get("15m", -999) >= EXPLOSIVE_MIN_15M_MOVE
    )
    acceleration_ok = (
        v15 >= EXPLOSIVE_MIN_VOLUME_15M
        and x.get("volume_1h", 0) >= EXPLOSIVE_MIN_VOLUME_1H
        and accel >= EXPLOSIVE_MIN_ACCELERATION
    )
    structure_ok = (
        x.get("buy_pressure", 0) >= EXPLOSIVE_MIN_BUY_PRESSURE
        and x.get("upper_wick_ratio", 1.0) < EXPLOSIVE_MAX_UPPER_WICK
        and x.get("breakout", -999) >= EXPLOSIVE_MIN_BREAKOUT
    )
    early_ok = (
        x.get("24h", 999) <= EXPLOSIVE_MAX_24H_GAIN
        and x.get("1h", 999) <= EXPLOSIVE_MAX_1H_GAIN
        and x.get("15m", 999) <= 4.0
    )
    trend_ok = (
        x.get("green_streak", 0) >= EXPLOSIVE_MIN_GREEN_STREAK
        and x.get("trend_continuity", 0) >= EXPLOSIVE_MIN_TREND_CONTINUITY
    )
    liquidity_ok = x.get("quote_volume", 0) >= EXPLOSIVE_MIN_24H_VOLUME

    return bool(price_impulse and acceleration_ok and structure_ok
                and early_ok and trend_ok and liquidity_ok)


def prepump_compressed_gate(x):
    """Watch-only hypothesis: compressed below recent high with early positive recovery."""
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    return bool(
        base not in STABLE_BASES
        and not x.get("is_wash")
        and x.get("score", 0) >= PREPUMP_MIN_SCORE
        and x.get("breakout", 0) <= PREPUMP_MAX_BREAKOUT
        and x.get("4h", -999) >= PREPUMP_MIN_4H_GAIN
        and x.get("volume_1h", 0) >= PREPUMP_MIN_VOLUME_1H
        and x.get("volume_15m", 0) >= PREPUMP_MIN_VOLUME_15M
        and x.get("buy_pressure", 0) >= 0.52
        and (x.get("rsi14") is not None and x.get("rsi14") < 76)
        and x.get("quote_volume", 0) >= MIN_24H_QUOTE_VOLUME
    )


def strict_pump_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES:
        return False
    if x.get("is_wash"):
        return False

    v15 = x.get("volume_15m", 0)
    accel = x.get("acceleration", 0)
    rsi = x.get("rsi14")

    if v15 > WASH_VOL_15M_HARD_CAP:
        return False
    if accel > FINAL_MAX_ACCELERATION:
        return False
    if rsi is None or rsi >= FINAL_MAX_RSI:
        return False
    if x.get("buy_pressure", 0) < FINAL_MIN_BUY_PRESSURE:
        return False
    if x.get("green_streak", 0) < FINAL_MIN_GREEN_STREAK:
        return False
    if x.get("trend_continuity", 0) < FINAL_MIN_TREND_CONTINUITY:
        return False

    price_ok = (
        x.get("15m", -999) >= FINAL_MIN_15M_MOVE
        and (x.get("5m", -999) >= FINAL_MIN_5M_MOVE
             or x.get("breakout", -999) >= 0.0)
    )
    volume_ok = (
        x.get("volume_1h", 0) >= FINAL_MIN_VOLUME_1H
        and x.get("volume_15m", 0) >= FINAL_MIN_VOLUME_15M
        and accel >= FINAL_MIN_ACCELERATION
    )
    structure_ok = (
        x.get("upper_wick_ratio", 1.0) < 0.45
        and x.get("breakout", -999) >= -0.75
    )
    not_late = (
        x.get("24h", 999) <= FINAL_MAX_24H_GAIN
        and x.get("15m", 999) <= 6.0
        and x.get("1h", 999) <= 10.0
    )
    liquidity_ok = x.get("quote_volume", 0) >= FINAL_MIN_24H_VOLUME

    return bool(price_ok and volume_ok and structure_ok and not_late and liquidity_ok)


def has_price_confirmation(x):
    """المسار العام (strict/explosive) فقط. PERSISTENT و REVERSAL مستقلان."""
    if not x.get("price_confirmation") or not x.get("continuation_quality"):
        return False
    return bool(strict_pump_gate(x) or explosive_early_gate(x))


def final_candidate_eligible(x):
    """
    V3.2: فصل المسارات.
    - المسار العام (strict/explosive): يحتاج price_confirmation + continuation_quality.
    - PERSISTENT: مستقل، يحتاج continuation_quality + persistent_explosion_gate.
    - REVERSAL: مستقل، يحتاج continuation_quality + reversal_explosion_gate.
    """
    # 1) المسار العام
    if x.get("score", 0) >= FINAL_SCORE and has_price_confirmation(x):
        return True

    # 2) PERSISTENT مستقل
    if (x.get("score", 0) >= PERSISTENT_FINAL_SCORE
        and x.get("continuation_quality")
        and persistent_explosion_gate(x)):
        return True

    # 3) REVERSAL مستقل
    if (x.get("score", 0) >= REVERSAL_FINAL_SCORE
        and x.get("continuation_quality")
        and reversal_explosion_gate(x)):
        return True

    return False


# ============================================================
# Cross-exchange
# ============================================================

def okx_inst_id(sym):
    base = sym[:-4] if sym.endswith("USDT") else sym
    return f"{base}-USDT"


def get_bybit_klines_15m(symbol, limit=6):
    try:
        r = session.get(
            f"{BYBIT_BASE}/v5/market/kline",
            params={"category": "spot", "symbol": symbol, "interval": "15", "limit": limit},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            log.debug(f"Bybit returned retCode={data.get('retCode')} for {symbol}: {data.get('retMsg')}")
            return None
        rows = data.get("result", {}).get("list", []) or None
        if not rows:
            log.debug(f"Bybit returned no spot candles for {symbol}")
        return rows
    except Exception as e:
        log.debug(f"Bybit request failed for {symbol}: {type(e).__name__}: {e}")
        return None


def get_okx_klines_15m(inst_id, limit=6):
    try:
        r = session.get(
            f"{OKX_BASE}/api/v5/market/candles",
            params={"instId": inst_id, "bar": "15m", "limit": limit},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "0":
            return None
        return data.get("data", []) or None
    except Exception:
        return None


def klines_1h_change_pct(rows, close_index):
    if not rows:
        return None
    back = min(4, len(rows) - 1)
    if back <= 0:
        return None
    try:
        latest = float(rows[0][close_index])
        older = float(rows[back][close_index])
        if older == 0:
            return None
        return (latest / older - 1.0) * 100.0
    except (ValueError, IndexError):
        return None


def cross_exchange_confirmation(symbol):
    now = time.time()
    cached = cross_cache.get(symbol)
    if cached and (now - cached[0]) < CROSS_CACHE_TTL_SECONDS:
        return cached[1], cached[2]

    notes = []
    agree = 0
    disagree = 0
    listed = False

    bybit = get_bybit_klines_15m(symbol)
    if bybit:
        listed = True
        chg = klines_1h_change_pct(bybit, close_index=4)
        if chg is not None:
            notes.append(f"Bybit 1h: {chg:+.2f}%")
            if chg >= CROSS_CONFIRM_AGREE_PCT:
                agree += 1
            elif chg <= CROSS_CONFIRM_DISAGREE_PCT:
                disagree += 1

    okx = get_okx_klines_15m(okx_inst_id(symbol))
    if okx:
        listed = True
        chg = klines_1h_change_pct(okx, close_index=4)
        if chg is not None:
            notes.append(f"OKX 1h: {chg:+.2f}%")
            if chg >= CROSS_CONFIRM_AGREE_PCT:
                agree += 1
            elif chg <= CROSS_CONFIRM_DISAGREE_PCT:
                disagree += 1

    if not listed:
        label = "BINANCE-EXCLUSIVE"
    elif agree > 0:
        label = "CONFIRMED"
    elif disagree > 0:
        label = "DIVERGENCE"
    else:
        label = "NEUTRAL"

    detail = " | ".join(notes) if notes else "لا توجد بيانات"
    cross_cache[symbol] = (now, label, detail)
    return label, detail


def check_okx_lead_watch(x, now, now_iso):
    """Send a WATCH-ONLY alert when OKX leads while Binance's 1h move is still small."""
    sym = x.get("symbol", "")
    if not sym or x.get("is_wash") or x.get("quote_volume", 0) < MIN_24H_QUOTE_VOLUME:
        return None
    last_sent = float(okx_lead_logged.get(sym, 0) or 0)
    if now - last_sent < OKX_LEAD_COOLDOWN_HOURS * 3600:
        return None
    rows = get_okx_klines_15m(okx_inst_id(sym), limit=6)
    okx_1h = klines_1h_change_pct(rows, close_index=4)
    if okx_1h is None:
        return {"symbol": sym, "stage": "OKX_LEAD_CHECK", "reason": "OKX_DATA_UNAVAILABLE"}
    binance_1h = x.get("1h", 999)
    if okx_1h >= OKX_LEAD_MIN_1H_GAIN and binance_1h <= OKX_LEAD_MAX_BINANCE_1H_GAIN:
        okx_lead_logged[sym] = now
        msg = (
            "👀 OKX-LEAD WATCH (تنبيه مراقبة فقط، ليس دخولًا)\n"
            f"COIN: {sym}\nOKX 1h: {okx_1h:+.2f}%\n"
            f"Binance 1h: {binance_1h:+.2f}%\n"
            f"Binance 15m: {x.get('15m', 0):+.2f}% | Score: {x.get('score', 0)}\n"
            "الفرضية تحتاج تحقق تاريخي؛ لا تعتبر إشارة شراء مؤكدة."
        )
        telegram_send(msg)
        log.info(msg)
        return {"symbol": sym, "stage": "OKX_LEAD_WATCH", "reason": "OKX_UP_BINANCE_LAGGING",
                "okx_1h": okx_1h, "binance_1h": binance_1h, "logged_time_utc": now_iso}
    return {"symbol": sym, "stage": "OKX_LEAD_CHECK", "reason": "LEAD_CONDITIONS_NOT_MET",
            "okx_1h": okx_1h, "binance_1h": binance_1h, "logged_time_utc": now_iso}


CROSS_LABEL_EMOJI = {
    "CONFIRMED": "✅",
    "DIVERGENCE": "⚠️",
    "NEUTRAL": "➖",
    "BINANCE-EXCLUSIVE": "🔒",
}


# ============================================================
# Formatting
# ============================================================

def safe_fmt(val, fmt=".2f", default=0):
    return format(val if val is not None else default, fmt)


def format_final(x, confirmations, cross_label=None, cross_detail=None):
    r = x.get("risk") or {}
    risk_section = ""
    if r:
        notes_txt = ("\n".join(f"  • {n}" for n in r.get("notes", []))
                     if r.get("notes") else "  • لا تعديلات")
        risk_section = f"""
━━━ 🎯 خطة التداول (R/R) ━━━
Entry:      {r['entry']:.10g}
Stop Loss:  {r['sl']:.10g}  ({-r['sl_pct']:+.2f}%)
TP1:        {r['tp1']:.10g}  ({r['tp1_pct']:+.2f}%)  R/R={r['tp1_rr']:.2f}
TP2:        {r['tp2']:.10g}  ({r['tp2_pct']:+.2f}%)  R/R={r['tp2_rr']:.2f}
ATR14 used: {r['atr_used']:.2f}%
تعديلات:
{notes_txt}

⚠️ بعد TP1 → انقل SL إلى Entry (Break-even)
⚠️ لا تخاطر بأكثر من 1-2% من رأس مالك في الصفقة
"""

    cross_section = ""
    if cross_label:
        emoji = CROSS_LABEL_EMOJI.get(cross_label, "")
        cross_section = f"\nCross-exchange: {emoji} {cross_label}\n{cross_detail}\n"

    wash_warn = ""
    if x.get("is_wash"):
        wash_warn = f"\n⚠️ تحذير: النمط يشبه Wash Trading — {x.get('wash_reason','')}\n"

    return f"""🔥 FINAL EARLY-PUMP CANDIDATE

⚠️ إشارة تحليلية — ليست ضمانًا ولا أمر شراء.

COIN: {x['symbol']}
SETUP: {x['setup']}
EARLY SCORE: {x['score']}/100

Price: {x['price']:.10g}

5m:  {x['5m']:+.2f}%
15m: {x['15m']:+.2f}%
1h:  {x['1h']:+.2f}%
24h: {x['24h']:+.2f}%

Volume 1h: {x['volume_1h']:.2f}x
Volume 15m: {x['volume_15m']:.2f}x
Acceleration: {x['acceleration']:.2f}x
Volume persistence: {x.get('volume_persistence', 0):.2f}x

Trend: {x.get('trend','N/A')}
Trend Continuity: {x.get('trend_continuity',0)}/8  (Green streak: {x.get('green_streak',0)})
EMA9/EMA21: {safe_fmt(x.get('ema9'), '.8g')} / {safe_fmt(x.get('ema21'), '.8g')}
RSI14: {safe_fmt(x.get('rsi14'), '.1f')}
ATR14: {safe_fmt(x.get('atr_pct'), '.2f')}%
Buy pressure: {x.get('buy_pressure',0):.2f}
Buy pressure Δ: {x.get('buy_pressure_delta',0):+.3f}
Candle body: {x.get('body_pct',0):+.2f}%
Upper wick: {x.get('upper_wick_ratio',0)*100:.1f}%

Breakout: {x['breakout']:+.2f}%
Distance from 1h high: {x.get('distance_from_1h_high',0):+.2f}%
Liquidity: {x['liquidity']}
24h Volume: ${x['quote_volume']:,.0f}
Confirmation scans: {confirmations}
{cross_section}{wash_warn}{risk_section}
📌 لماذا ظهرت؟
حجم غير طبيعي + استمرار حجم + حركة سعر متواصلة + اتجاه/ضغط شراء
+ فلتر ضد الـexhaustion وضد الـwash trading.

🚫 لا يوجد Target ثابت. سيتم قياس MFE/MAE فعليًا بعد الإشارة.
📊 تتبع النتيجة: 5m / 15m / 30m / 1h / 4h / 24h
"""


# ============================================================
# CSV retention
# ============================================================

def cleanup_old_csv_logs(force=False):
    """Remove rows older than four days; preserve rows with invalid timestamps."""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOG_RETENTION_DAYS)
    files = [
        (EARLY_WATCH_LOG_FILE, ("logged_time_utc", "timestamp_utc", "timestamp", "time_utc", "time")),
        (TRACK_FILE, ("final_time_utc", "logged_time_utc", "timestamp_utc", "timestamp", "time_utc", "time")),
        (SCAN_DIAGNOSTICS_FILE, ("logged_time_utc", "timestamp_utc", "timestamp", "time_utc", "time")),
    ]
    for filename, possible_columns in files:
        if not os.path.isfile(filename):
            continue
        tmp_name = filename + ".cleanup.tmp"
        try:
            with open(filename, "r", newline="", encoding="utf-8-sig") as src:
                reader = csv.reader(src)
                header = next(reader, None)
                if not header:
                    continue
                time_col = next((c for c in possible_columns if c in header), None)
                if time_col is None:
                    log.warning(f"CSV cleanup skipped {filename}: no timestamp column; columns={header}")
                    continue
                time_idx = header.index(time_col)
                removed = kept = invalid_dates = 0
                with open(tmp_name, "w", newline="", encoding="utf-8") as dst:
                    writer = csv.writer(dst)
                    writer.writerow(header)
                    for row in reader:
                        if not row:
                            continue
                        raw_ts = row[time_idx].strip() if len(row) > time_idx else ""
                        try:
                            ts = pd.to_datetime(raw_ts, utc=True, errors="raise").to_pydatetime()
                            if ts < cutoff:
                                removed += 1
                                continue
                        except Exception:
                            invalid_dates += 1
                        writer.writerow(row)
                        kept += 1
            if removed:
                os.replace(tmp_name, filename)
                log.info(f"CSV cleanup: removed {removed} rows older than {LOG_RETENTION_DAYS} days from {filename}; kept {kept}")
            else:
                os.remove(tmp_name)
                log.info(f"CSV cleanup checked {filename}: no old rows removed (retention={LOG_RETENTION_DAYS} days)")
            if invalid_dates:
                log.warning(f"CSV cleanup preserved {invalid_dates} rows with invalid timestamps in {filename}")
        except Exception as e:
            try:
                if os.path.exists(tmp_name): os.remove(tmp_name)
            except OSError:
                pass
            log.exception(f"CSV cleanup failed for {filename}: {e}")


def append_scan_diagnostics(rows):
    if not rows:
        return
    columns = [
        "logged_time_utc", "symbol", "stage", "reason", "score", "setup",
        "gain_5m", "gain_15m", "gain_1h", "gain_4h", "gain_24h",
        "volume_15m", "volume_1h", "acceleration", "buy_pressure",
        "breakout", "trend_continuity", "green_streak", "is_wash", "quote_volume",
        "candidate_count", "final_eligible", "okx_1h", "binance_1h",
    ]
    exists = os.path.exists(SCAN_DIAGNOSTICS_FILE) and os.path.getsize(SCAN_DIAGNOSTICS_FILE) > 0
    with open(SCAN_DIAGNOSTICS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerows({key: row.get(key, "") for key in columns} for row in rows)


def diagnostic_row(now_iso, symbol, stage, reason, result=None, **extra):
    row = {"logged_time_utc": now_iso, "symbol": symbol, "stage": stage, "reason": reason}
    if result:
        row.update({
            "score": result.get("score", ""), "setup": result.get("setup", ""),
            "gain_5m": result.get("5m", ""), "gain_15m": result.get("15m", ""),
            "gain_1h": result.get("1h", ""), "gain_4h": result.get("4h", ""),
            "gain_24h": result.get("24h", ""), "volume_15m": result.get("volume_15m", ""),
            "volume_1h": result.get("volume_1h", ""), "acceleration": result.get("acceleration", ""),
            "buy_pressure": result.get("buy_pressure", ""), "breakout": result.get("breakout", ""),
            "trend_continuity": result.get("trend_continuity", ""),
            "green_streak": result.get("green_streak", ""),
            "is_wash": int(bool(result.get("is_wash", False))),
            "quote_volume": result.get("quote_volume", ""),
        })
    row.update(extra)
    return row


# ============================================================
# Tracking
# ============================================================

def init_results_file():
    columns = [
        "final_time_utc", "symbol", "setup", "score",
        "entry_price", "sl", "sl_pct", "tp1", "tp1_pct", "tp2", "tp2_pct",
        "atr_used", "quote_volume", "liquidity",
        "gain_5m_at_alert", "gain_15m_at_alert",
        "gain_1h_at_alert", "gain_24h_at_alert",
        "peak_price", "lowest_price", "mfe_pct", "mae_pct",
        "result_5m", "result_15m", "result_30m",
        "result_1h", "result_4h", "result_24h", "status",
    ]
    if not os.path.exists(TRACK_FILE):
        pd.DataFrame(columns=columns).to_csv(TRACK_FILE, index=False)


def save_track(track):
    """Upsert one row per signal identity instead of appending every scan cycle."""
    row = {
        "final_time_utc": track["final_time_utc"], "symbol": track["symbol"],
        "setup": track["setup"], "score": track["score"],
        "entry_price": track["entry_price"], "sl": track.get("sl"), "sl_pct": track.get("sl_pct"),
        "tp1": track.get("tp1"), "tp1_pct": track.get("tp1_pct"), "tp2": track.get("tp2"),
        "tp2_pct": track.get("tp2_pct"), "atr_used": track.get("atr_used"),
        "quote_volume": track["quote_volume"], "liquidity": track["liquidity"],
        "gain_5m_at_alert": track["gain_5m_at_alert"], "gain_15m_at_alert": track["gain_15m_at_alert"],
        "gain_1h_at_alert": track["gain_1h_at_alert"], "gain_24h_at_alert": track["gain_24h_at_alert"],
        "peak_price": track["peak_price"], "lowest_price": track["lowest_price"],
        "mfe_pct": track["mfe_pct"], "mae_pct": track["mae_pct"],
        "result_5m": track["results"].get(5), "result_15m": track["results"].get(15),
        "result_30m": track["results"].get(30), "result_1h": track["results"].get(60),
        "result_4h": track["results"].get(240), "result_24h": track["results"].get(1440),
        "status": track["status"],
    }
    if os.path.exists(TRACK_FILE) and os.path.getsize(TRACK_FILE) > 0:
        try:
            df = pd.read_csv(TRACK_FILE)
            for col in row:
                if col not in df.columns:
                    df[col] = pd.NA
            # Remove duplicate records left by the previous repeated-append bug.
            if {"final_time_utc", "symbol"}.issubset(df.columns):
                df = df.drop_duplicates(subset=["final_time_utc", "symbol"], keep="last")
                mask = (df["final_time_utc"].astype(str) == str(row["final_time_utc"])) & (df["symbol"].astype(str) == str(row["symbol"]))
                if mask.any():
                    idx = df.index[mask][-1]
                    for col, value in row.items(): df.at[idx, col] = value
                else:
                    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            else:
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            df.to_csv(TRACK_FILE, index=False)
            return
        except Exception as e:
            log.warning(f"Track upsert fallback to append for {track.get('symbol')}: {e}")
    pd.DataFrame([row]).to_csv(TRACK_FILE, mode="a", header=not os.path.exists(TRACK_FILE), index=False)


def update_tracking(tickers):
    now = time.time()
    MAX_AGE_MIN = 1500
    for symbol, track in list(active_tracks.items()):
        price = tickers.get(symbol, {}).get("price")
        if price is None or price <= 0:
            if (now - track["start_ts"]) / 60 > MAX_AGE_MIN:
                track["status"] = "COMPLETED"
                save_track(track)
                del active_tracks[symbol]
            continue

        entry = track["entry_price"]
        track["peak_price"] = max(track["peak_price"], price)
        track["lowest_price"] = min(track["lowest_price"], price)
        track["mfe_pct"] = pct(track["peak_price"], entry)
        track["mae_pct"] = pct(track["lowest_price"], entry)

        elapsed_min = (now - track["start_ts"]) / 60
        saved = False
        for target in TRACK_MINUTES:
            if target not in track["results"] and elapsed_min >= target:
                track["results"][target] = pct(price, entry)
                saved = True

        if saved:
            track["status"] = "TRACKING"
            save_track(track)

        if elapsed_min >= 1440:
            track["status"] = "COMPLETED"
            save_track(track)
            del active_tracks[symbol]


def log_early_watch(x, now):
    sym = x["symbol"]
    last = early_watch_logged.get(sym)
    if last is not None and now - last < EARLY_WATCH_COOLDOWN_HOURS * 3600:
        return
    early_watch_logged[sym] = now
    exists = os.path.exists(EARLY_WATCH_LOG_FILE)
    with open(EARLY_WATCH_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow([
                "logged_time_utc", "symbol", "setup", "score", "price",
                "gain_15m", "gain_1h", "gain_24h",
                "volume_1h", "volume_15m", "acceleration",
                "breakout", "liquidity", "quote_volume",
                "trend_continuity", "green_streak", "is_wash",
            ])
        w.writerow([
            datetime.now(timezone.utc).isoformat(), sym, x["setup"], x["score"], x["price"],
            x["15m"], x["1h"], x["24h"],
            x["volume_1h"], x["volume_15m"], x["acceleration"],
            x["breakout"], x["liquidity"], x["quote_volume"],
            x.get("trend_continuity", 0), x.get("green_streak", 0),
            int(x.get("is_wash", False)),
        ])


def start_final_tracking(x):
    r = x.get("risk") or {}
    active_tracks[x["symbol"]] = {
        "final_time_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": x["symbol"],
        "setup": x["setup"],
        "score": x["score"],
        "entry_price": x["price"],
        "sl": r.get("sl"),
        "sl_pct": r.get("sl_pct"),
        "tp1": r.get("tp1"),
        "tp1_pct": r.get("tp1_pct"),
        "tp2": r.get("tp2"),
        "tp2_pct": r.get("tp2_pct"),
        "atr_used": r.get("atr_used"),
        "quote_volume": x["quote_volume"],
        "liquidity": x["liquidity"],
        "gain_5m_at_alert": x["5m"],
        "gain_15m_at_alert": x["15m"],
        "gain_1h_at_alert": x["1h"],
        "gain_24h_at_alert": x["24h"],
        "peak_price": x["price"],
        "lowest_price": x["price"],
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "results": {},
        "start_ts": time.time(),
        "status": "TRACKING",
    }


# ============================================================
# Final signal — V3.2 REWRITTEN
# ============================================================

def maybe_final_signal(x, now):
    """
    V3.2: الإصلاح الحرج.
    - نضيف candidates بمجرد score >= MIN_CANDIDATE_SCORE (70).
    - لا نطلب final_candidate_eligible في الدورة الأولى.
    - التأكيد النهائي يحتاج count >= CONFIRMATIONS_REQUIRED + eligible.
    """
    sym = x["symbol"]

    # cooldown
    if sym in final_cooldown:
        if now - final_cooldown[sym] < FINAL_COOLDOWN_HOURS * 3600:
            return False

    # لا تُضاف إلا لو score معقول
    if x["score"] < MIN_CANDIDATE_SCORE:
        return False

    c = candidates.get(sym)
    if not c or (now - c["last_ts"]) > CANDIDATE_EXPIRY_MINUTES * 60:
        # أول ظهور (أو انتهت صلاحية المرشح القديم)
        candidates[sym] = {
            "count": 1,
            "first_ts": now,
            "last_ts": now,
            "best": x,
            "first": x,
        }
        return False

    # تحديث المرشح
    c["count"] += 1
    c["last_ts"] = now
    if x["score"] > c["best"]["score"]:
        c["best"] = x

    # هل نحن جاهزون للتأكيد النهائي؟
    if c["count"] < CONFIRMATIONS_REQUIRED:
        return False

    if not final_candidate_eligible(x):
        return False

    # تأكيد إضافي: السعر لم ينخفض، والحجم لم يضعف
    first = c["first"]
    second_confirmed = (
        x.get("continuation_quality", False)
        and x["5m"] > -0.3
        and x["15m"] >= first["15m"] - 1.0
        and x["volume_1h"] >= first["volume_1h"] * 0.70
    )

    if not second_confirmed:
        return False

    # اختيار أفضل مرشح
    final_x = x if x["score"] >= c["best"]["score"] else c["best"]

    cross_label, cross_detail = cross_exchange_confirmation(sym)

    if SUPPRESS_ON_DIVERGENCE and cross_label == "DIVERGENCE":
        log.info(f"[Cross] {sym}: DIVERGENCE — تم منع FINAL. {cross_detail}")
        final_cooldown[sym] = now
        candidates.pop(sym, None)
        return False

    final_cooldown[sym] = now
    candidates.pop(sym, None)

    msg = format_final(final_x, c["count"], cross_label, cross_detail)
    telegram_send(msg)
    log.info(f"\n{'!'*70}\n{msg}\n{'!'*70}")

    start_final_tracking(final_x)
    return True


def cleanup_expired_candidates(now):
    for sym, c in list(candidates.items()):
        if now - c["last_ts"] > CANDIDATE_EXPIRY_MINUTES * 60:
            del candidates[sym]


# ============================================================
# State
# ============================================================

def save_state():
    state = {
        "candidates": candidates,
        "final_cooldown": final_cooldown,
        "active_tracks": active_tracks,
        "early_watch_logged": early_watch_logged,
        "okx_lead_logged": okx_lead_logged,
        "shariah_excluded_logged": shariah_excluded_logged,
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, default=str)
    except Exception as e:
        log.error(f"save_state failed: {e}")


def load_state():
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        log.error(f"load_state failed: {e}")
        return

    for sym, cand in state.get("candidates", {}).items():
        if isinstance(cand, dict) and "first" in cand and "best" in cand:
            candidates[sym] = cand
    final_cooldown.update(state.get("final_cooldown", {}))
    early_watch_logged.update(state.get("early_watch_logged", {}))
    okx_lead_logged.update({k: float(v) for k, v in state.get("okx_lead_logged", {}).items()})
    shariah_excluded_logged.update({k: float(v) for k, v in state.get("shariah_excluded_logged", {}).items()})

    for sym, tr in state.get("active_tracks", {}).items():
        tr["results"] = {int(k): v for k, v in tr.get("results", {}).items()}
        active_tracks[sym] = tr


# ============================================================
# Cycle
# ============================================================

def run_cycle(symbols):
    cycle_start = time.time()
    tickers = get_tickers()
    update_tracking(tickers)
    now = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    diagnostics = []

    # Log symbols excluded by the Shariah universe filter so absence is explainable.
    for excluded_symbol in shariah_excluded_this_startup:
        last_logged = float(shariah_excluded_logged.get(excluded_symbol, 0) or 0)
        if now - last_logged >= 24 * 3600:
            diagnostics.append(diagnostic_row(now_iso, excluded_symbol, "SHARIAH_FILTER", "NOT_IN_CURRENT_COMPLIANT_LIST"))
            shariah_excluded_logged[excluded_symbol] = now
    shariah_excluded_this_startup.clear()

    universe = []
    for symbol in symbols:
        ticker = tickers.get(symbol)
        if ticker is None:
            diagnostics.append(diagnostic_row(now_iso, symbol, "TICKER_DATA", "SYMBOL_MISSING_FROM_TICKERS"))
        elif ticker["quote_volume"] < MIN_24H_QUOTE_VOLUME:
            diagnostics.append(diagnostic_row(now_iso, symbol, "UNIVERSE_FILTER", "LOW_24H_QUOTE_VOLUME", quote_volume=ticker["quote_volume"], gain_24h=ticker["change"]))
        elif ticker["change"] > MAX_24H_GAIN_PCT:
            diagnostics.append(diagnostic_row(now_iso, symbol, "UNIVERSE_FILTER", "24H_GAIN_ABOVE_MAX", quote_volume=ticker["quote_volume"], gain_24h=ticker["change"]))
        else:
            universe.append(symbol)

    results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(analyze, symbol, tickers[symbol]): symbol for symbol in universe}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                result = fut.result()
                if not result:
                    diagnostics.append(diagnostic_row(now_iso, symbol, "ANALYZE", "NO_RESULT"))
                elif result.get("_rejected"):
                    diagnostics.append(diagnostic_row(now_iso, symbol, "ANALYZE_FILTER", result["_rejected"]))
                elif result["score"] < WATCH_SCORE and result.get("setup") != "PRE-PUMP COMPRESSED":
                    diagnostics.append(diagnostic_row(now_iso, symbol, "SCORE_FILTER", f"SCORE_BELOW_WATCH_{WATCH_SCORE}", result))
                else:
                    results.append(result)
                    diagnostics.append(diagnostic_row(now_iso, symbol, "WATCH_CANDIDATE", "SCORE_PASSED_WATCH" if result["score"] >= WATCH_SCORE else "PREPUMP_COMPRESSED_WATCH_ONLY", result))
            except Exception as e:
                log.debug(f"analyze error for {symbol}: {type(e).__name__}: {e}")
                diagnostics.append(diagnostic_row(now_iso, symbol, "ANALYZE_ERROR", type(e).__name__))

    results.sort(key=lambda x: (x["score"], x["volume_1h"]), reverse=True)
    log.info("=" * 70)
    log.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if not results:
        log.info(f"No watch candidates >= {WATCH_SCORE} and no compressed watch candidates")
    else:
        log.info(f"Watch candidates: {len(results)}")
        for x in results[:10]:
            log.info(
                f"{x['symbol']:12} Score={x['score']:3} Setup={x['setup']} "
                f"15m={x['15m']:+6.2f}% 1h={x['1h']:+6.2f}% 4h={x.get('4h', 0):+6.2f}% "
                f"Vol1h={x['volume_1h']:5.2f}x Vol15m={x['volume_15m']:5.2f}x "
                f"Accel={x['acceleration']:4.2f}x TrendC={x.get('trend_continuity',0)}/8 "
                f"Green={x.get('green_streak',0)} Wash={'YES' if x.get('is_wash') else 'no'} Liq={x['liquidity']}"
            )
            log_early_watch(x, now)
            if x["score"] >= MIN_CANDIDATE_SCORE and x["setup"] != "PRE-PUMP COMPRESSED":
                eligible = final_candidate_eligible(x)
                previous_count = candidates.get(x["symbol"], {}).get("count", 0)
                sent = maybe_final_signal(x, now)
                if sent:
                    reason = "FINAL_SENT"
                elif previous_count + 1 < CONFIRMATIONS_REQUIRED:
                    reason = "WAITING_FOR_CONFIRMATIONS"
                elif not eligible:
                    reason = "FINAL_GATE_FAILED"
                else:
                    reason = "CONFIRMATION_QUALITY_OR_COOLDOWN"
                diagnostics.append(diagnostic_row(now_iso, x["symbol"], "FINAL_DECISION", reason, x,
                                                  candidate_count=candidates.get(x["symbol"], {}).get("count", 0),
                                                  final_eligible=eligible))

    # OKX lead experiment is a separate WATCH-only alert and never makes FINAL eligible.
    for x in results[:OKX_LEAD_MAX_CHECKS_PER_CYCLE]:
        row = check_okx_lead_watch(x, now, now_iso)
        if row:
            diagnostics.append(diagnostic_row(now_iso, row.get("symbol", x["symbol"]), row.get("stage", "OKX_LEAD_CHECK"), row.get("reason", "UNKNOWN"), x,
                                              okx_1h=row.get("okx_1h", ""), binance_1h=row.get("binance_1h", "")))

    append_scan_diagnostics(diagnostics)
    cleanup_expired_candidates(now)
    cleanup_old_csv_logs(force=True)
    log.info(f"Active tracks: {len(active_tracks)} | Pending: {len(candidates)}")
    return max(10, SCAN_EVERY_SECONDS - (time.time() - cycle_start))


# ============================================================
# Main
# ============================================================

ONCE = "--once" in sys.argv

init_results_file()
load_state()
update_shariah_list(force=False)
cleanup_old_csv_logs(force=True)
last_log_cleanup_ts = time.time()

log.info("تحميل قائمة Binance...")
try:
    symbols = get_symbols()
except Exception as e:
    log.error(f"Cannot load symbols: {e}")
    sys.exit(1)

log.info(f"تم تحميل {len(symbols)} زوج USDT.")
log.info("Scanner V3.2 started" + (" (single run)." if ONCE else "."))

if ONCE:
    try:
        run_cycle(symbols)
    except Exception as e:
        log.exception(f"Scanner error: {e}")
        save_state()
        sys.exit(1)
    save_state()
    sys.exit(0)

while True:
    try:
        sleep_time = run_cycle(symbols)
        save_state()
        if time.time() - last_log_cleanup_ts >= LOG_CLEANUP_INTERVAL_HOURS * 3600:
            cleanup_old_csv_logs(force=True)
            update_shariah_list(force=False)
            last_log_cleanup_ts = time.time()
        log.info(f"Next scan in {sleep_time:.0f}s...")
        time.sleep(sleep_time)
    except KeyboardInterrupt:
        log.info("Scanner stopped.")
        save_state()
        break
    except Exception as e:
        log.exception(f"Scanner error: {e}")
        time.sleep(30)