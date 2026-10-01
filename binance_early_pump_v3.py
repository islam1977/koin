# ============================================================
# BINANCE EARLY-PUMP SCANNER V3 — GOOGLE COLAB / TELEGRAM
# ============================================================
#
# الهدف:
# - البحث عن العملات التي قد تكون في بداية حركة قوية.
# - فلتر سيولة أفضل.
# - لا يرسل Telegram في كل دورة.
# - يبني "مرشحًا" داخليًا ثم يرسل رسالة FINAL واحدة فقط
#   عندما تتأكد الإشارة.
# - يتابع نتيجة الإشارة بعد 5m / 15m / 30m / 1h / 4h / 24h.
# - يسجل النتائج في CSV.
# - لا ينفذ صفقات حقيقية.
#
# ملاحظة:
# هذا ليس ضمانًا لتوقع ارتفاع 100% أو 200%.
# الإشارة هي نظام قياس واحتمال، وليست توصية استثمارية مؤكدة.
# ============================================================


import requests, time, math, csv, os
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://api.binance.com"
INTERVAL = "5m"
KLINES_LIMIT = 180

# ---------- إعدادات الفحص ----------

SCAN_EVERY_SECONDS = 300

# السيولة
MIN_24H_QUOTE_VOLUME = 3_000_000  # keep lower liquidity watch

# لا نريد مطاردة عملة ارتفعت بالفعل بقوة.
MAX_24H_GAIN_PCT = 35.0

# الحركة قصيرة الأجل المقبولة
MIN_15M_GAIN = -2.0
MAX_15M_GAIN = 10.0

MIN_1H_GAIN = -5.0
MAX_1H_GAIN = 18.0

# الدرجات
WATCH_SCORE = 65
FINAL_SCORE = 85
EXCEPTIONAL_SCORE = 90

# تأكيد الإشارة:
# يجب أن تظهر الإشارة القوية في دورتين متتاليتين
# أو تصل إلى EXCEPTIONAL_SCORE في دورة واحدة.
CONFIRMATIONS_REQUIRED = 1
CANDIDATE_EXPIRY_MINUTES = 20

# بعد إرسال FINAL لا نرسل نفس العملة مرة أخرى خلال هذه المدة.
FINAL_COOLDOWN_HOURS = 12

# ---------- تتبع النتيجة ----------

TRACK_MINUTES = [5, 15, 30, 60, 240, 1440]

# ملف النتائج
TRACK_FILE = "early_pump_signal_results.csv"

# ---------- Telegram ----------

import os

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

session = requests.Session()
session.headers.update({"User-Agent": "Binance-Early-Pump-Scanner-V2/1.0"})


def get_json(path, params=None):
    r = session.get(BASE_URL + path, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        r = session.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=15
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram error:", e)
        return False


def get_symbols():
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

        if any(x in symbol for x in
               ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue

        symbols.append(symbol)

    return symbols


def get_tickers():
    data = get_json("/api/v3/ticker/24hr")
    result = {}

    for x in data:
        symbol = x["symbol"]

        if not symbol.endswith("USDT"):
            continue

        try:
            result[symbol] = {
                "price": float(x["lastPrice"]),
                "change": float(x["priceChangePercent"]),
                "quote_volume": float(x["quoteVolume"])
            }
        except Exception:
            pass

    return result


def pct(a, b):
    if b == 0:
        return 0.0
    return (a / b - 1) * 100.0


def mean(values):
    values = [x for x in values if math.isfinite(x)]
    return sum(values) / len(values) if values else 0.0


def analyze(symbol, ticker):
    try:
        klines = get_json(
            "/api/v3/klines",
            {"symbol": symbol, "interval": INTERVAL, "limit": KLINES_LIMIT}
        )
    except Exception:
        return None

    if len(klines) < 80:
        return None

    # الشمعة الحالية غير المكتملة لا تدخل في الحساب.
    k = klines[:-1]

    closes = [float(x[4]) for x in k]
    highs = [float(x[2]) for x in k]
    lows = [float(x[3]) for x in k]
    quote_volumes = [float(x[7]) for x in k]

    price = closes[-1]

    gain_5m = pct(closes[-1], closes[-2])
    gain_15m = pct(closes[-1], closes[-4])
    gain_1h = pct(closes[-1], closes[-13])
    gain_24h = ticker["change"]
    qv24 = ticker["quote_volume"]

    if qv24 < MIN_24H_QUOTE_VOLUME:
        return None

    if gain_24h > MAX_24H_GAIN_PCT:
        return None

    if gain_15m < MIN_15M_GAIN or gain_15m > MAX_15M_GAIN:
        return None

    if gain_1h < MIN_1H_GAIN or gain_1h > MAX_1H_GAIN:
        return None

    # ---------- Volume 15m ----------
    current_15m = sum(quote_volumes[-3:])
    historical_15m = []

    for i in range(23, len(quote_volumes) - 3, 3):
        historical_15m.append(sum(quote_volumes[i-3:i]))

    baseline_15m = mean(historical_15m)
    volume_ratio_15m = current_15m / baseline_15m if baseline_15m else 0

    # ---------- Volume 1h ----------
    current_1h = sum(quote_volumes[-12:])
    historical_1h = []

    for i in range(48, len(quote_volumes) - 12, 12):
        historical_1h.append(sum(quote_volumes[i-12:i]))

    baseline_1h = mean(historical_1h)
    volume_ratio_1h = current_1h / baseline_1h if baseline_1h else 0

    # ---------- Acceleration ----------
    previous_15m = sum(quote_volumes[-6:-3])
    acceleration = current_15m / previous_15m if previous_15m else 0

    # ---------- Breakout ----------
    lookback = 48
    previous_high = max(highs[-(lookback+1):-1])
    breakout_pct = pct(price, previous_high)

    # ---------- Candle quality ----------
    candle_high = highs[-1]
    candle_low = lows[-1]
    candle_range = candle_high - candle_low

    if candle_range:
        close_location = (price - candle_low) / candle_range
    else:
        close_location = 0.5

    # ---------- Score ----------
    score = 0

    if volume_ratio_1h >= 4:
        score += 30
    elif volume_ratio_1h >= 2:
        score += 20
    elif volume_ratio_1h >= 1.5:
        score += 10

    if acceleration >= 2.5:
        score += 20
    elif acceleration >= 1.5:
        score += 12
    elif acceleration >= 1.2:
        score += 6

    if 2 <= gain_15m <= 8:
        score += 15
    elif 0 <= gain_15m < 2:
        score += 7
    elif 8 < gain_15m <= 10:
        score += 5

    if breakout_pct >= 0:
        score += 20
    elif breakout_pct >= -1.5:
        score += 10

    if close_location >= 0.75:
        score += 10
    elif close_location >= 0.60:
        score += 5

    # ما زلنا في المنطقة المبكرة
    if 0 <= gain_24h <= 8:
        score += 5
    elif gain_24h <= 14:
        score += 3
    elif gain_24h <= 18:
        score += 1

    # ---------- Liquidity score ----------
    if qv24 >= 20_000_000:
        liquidity = "HIGH"
        liquidity_points = 5
    elif qv24 >= 10_000_000:
        liquidity = "GOOD"
        liquidity_points = 4
    elif qv24 >= 5_000_000:
        liquidity = "MEDIUM"
        liquidity_points = 2
    else:
        liquidity = "LOW"
        liquidity_points = 0

    score += liquidity_points

    if (
        breakout_pct >= 0
        and volume_ratio_1h >= 2
        and acceleration >= 1.5
    ):
        setup = "BREAKOUT + VOLUME ACCELERATION"
    elif volume_ratio_1h >= 3 and acceleration >= 1.5:
        setup = "VOLUME EXPANSION"
    elif breakout_pct >= 0:
        setup = "BREAKOUT"
    else:
        setup = "EARLY MOMENTUM"

    return {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "5m": gain_5m,
        "15m": gain_15m,
        "1h": gain_1h,
        "24h": gain_24h,
        "volume_1h": volume_ratio_1h,
        "volume_15m": volume_ratio_15m,
        "acceleration": acceleration,
        "breakout": breakout_pct,
        "close_location": close_location,
        "score": score,
        "setup": setup,
        "quote_volume": qv24,
        "liquidity": liquidity
    }


# ---------- Candidate state ----------
candidates = {}
final_cooldown = {}

# ---------- Active tracking ----------
active_tracks = {}


def init_results_file():
    columns = [
        "final_time_utc", "symbol", "setup", "score",
        "entry_price", "quote_volume", "liquidity",
        "gain_5m_at_alert", "gain_15m_at_alert",
        "gain_1h_at_alert", "gain_24h_at_alert",
        "peak_price", "lowest_price",
        "mfe_pct", "mae_pct",
        "result_5m", "result_15m", "result_30m",
        "result_1h", "result_4h", "result_24h",
        "status"
    ]

    if not os.path.exists(TRACK_FILE):
        pd.DataFrame(columns=columns).to_csv(
            TRACK_FILE, index=False
        )


def save_track(track):
    row = {
        "final_time_utc": track["final_time_utc"],
        "symbol": track["symbol"],
        "setup": track["setup"],
        "score": track["score"],
        "entry_price": track["entry_price"],
        "quote_volume": track["quote_volume"],
        "liquidity": track["liquidity"],
        "gain_5m_at_alert": track["gain_5m_at_alert"],
        "gain_15m_at_alert": track["gain_15m_at_alert"],
        "gain_1h_at_alert": track["gain_1h_at_alert"],
        "gain_24h_at_alert": track["gain_24h_at_alert"],
        "peak_price": track["peak_price"],
        "lowest_price": track["lowest_price"],
        "mfe_pct": track["mfe_pct"],
        "mae_pct": track["mae_pct"],
        "result_5m": track["results"].get(5),
        "result_15m": track["results"].get(15),
        "result_30m": track["results"].get(30),
        "result_1h": track["results"].get(60),
        "result_4h": track["results"].get(240),
        "result_24h": track["results"].get(1440),
        "status": track["status"]
    }

    pd.DataFrame([row]).to_csv(
        TRACK_FILE,
        mode="a",
        header=not os.path.exists(TRACK_FILE),
        index=False
    )


def update_tracking(tickers):
    now = time.time()
    finished = []

    for symbol, track in list(active_tracks.items()):
        if symbol not in tickers:
            continue

        price = tickers[symbol]["price"]
        entry = track["entry_price"]

        track["peak_price"] = max(track["peak_price"], price)
        track["lowest_price"] = min(track["lowest_price"], price)

        track["mfe_pct"] = pct(track["peak_price"], entry)
        track["mae_pct"] = pct(track["lowest_price"], entry)

        elapsed_min = (now - track["start_ts"]) / 60

        for target in TRACK_MINUTES:
            if target not in track["results"] and elapsed_min >= target:
                track["results"][target] = pct(price, entry)

        # نعتبر التتبع مكتملًا بعد 24 ساعة.
        if elapsed_min >= 1440:
            track["status"] = "COMPLETED"
            finished.append(symbol)

        # حفظ snapshot بسيط عند كل دورة حتى لو لم يكتمل.
        elif int(elapsed_min) in (5, 15, 30, 60, 240):
            track["status"] = "TRACKING"

    for symbol in finished:
        save_track(active_tracks[symbol])
        del active_tracks[symbol]


def format_final(x, confirmations):
    return f"""🔥 FINAL EARLY-PUMP CANDIDATE

⚠️ هذه إشارة تحليلية وليست ضمانًا أو أمر شراء.

COIN: {x['symbol']}
SETUP: {x['setup']}
SCORE: {x['score']}/100

Price at final signal:
{x['price']:.12g}

5m:  {x['5m']:+.2f}%
15m: {x['15m']:+.2f}%
1h:  {x['1h']:+.2f}%
24h: {x['24h']:+.2f}%

Volume 1h: {x['volume_1h']:.2f}x
Volume 15m: {x['volume_15m']:.2f}x
Acceleration: {x['acceleration']:.2f}x

Breakout: {x['breakout']:+.2f}%
Liquidity: {x['liquidity']}
24h Volume: ${x['quote_volume']:,.0f}

Confirmation scans: {confirmations}

📌 لماذا ظهرت؟
السعر + الحجم + تسارع الحجم + الاختراق
اجتمعت في نفس الوقت.

📊 سيتم الآن تتبع النتيجة تلقائيًا:
5m / 15m / 30m / 1h / 4h / 24h

⚠️ لا يوجد تداول آلي في هذه النسخة.
"""


def start_final_tracking(x):
    active_tracks[x["symbol"]] = {
        "final_time_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": x["symbol"],
        "setup": x["setup"],
        "score": x["score"],
        "entry_price": x["price"],
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
        "status": "TRACKING"
    }


def maybe_final_signal(x, now):
    symbol = x["symbol"]

    if symbol in final_cooldown:
        if now - final_cooldown[symbol] < FINAL_COOLDOWN_HOURS * 3600:
            return False

    # فقط الإشارات القوية تدخل مرحلة التأكيد.
    if x["score"] < FINAL_SCORE:
        # تنظيف مرشح قديم
        c = candidates.get(symbol)
        if c and (now - c["last_ts"]) > CANDIDATE_EXPIRY_MINUTES * 60:
            del candidates[symbol]
        return False

    c = candidates.get(symbol)

    if not c:
        candidates[symbol] = {
            "count": 1,
            "first_ts": now,
            "last_ts": now,
            "best": x
        }
        return False

    # يجب أن تكون الدورة التالية قريبة زمنيًا.
    if now - c["last_ts"] <= CANDIDATE_EXPIRY_MINUTES * 60:
        c["count"] += 1
        c["last_ts"] = now
        if x["score"] > c["best"]["score"]:
            c["best"] = x
    else:
        candidates[symbol] = {
            "count": 1,
            "first_ts": now,
            "last_ts": now,
            "best": x
        }
        return False

    # تأكيد عادي: دورتان متتاليتان.
    confirmed = c["count"] >= CONFIRMATIONS_REQUIRED

    # أو إشارة استثنائية جدًا.
    exceptional = x["score"] >= EXCEPTIONAL_SCORE

    if confirmed or exceptional:
        final_x = x if x["score"] >= c["best"]["score"] else c["best"]

        final_cooldown[symbol] = now
        candidates.pop(symbol, None)

        message = format_final(
            final_x,
            c["count"]
        )

        telegram_send(message)

        print("\n" + "!" * 70)
        print(message)
        print("!" * 70)

        start_final_tracking(final_x)
        return True

    return False


# ============================================================
# START
# ============================================================

init_results_file()

print("\nتحميل قائمة Binance...")
symbols = get_symbols()
print(f"تم تحميل {len(symbols)} زوج USDT.")
print("Scanner V2 started.")
print("النظام لن يرسل كل إشارة؛ سيجمع التأكيدات ثم يرسل FINAL مرة واحدة.")
print("اترك هذه الخلية تعمل.\n")

while True:
    cycle_start = time.time()

    try:
        tickers = get_tickers()

        # تحديث تتبع الإشارات النهائية القديمة.
        update_tracking(tickers)

        universe = [
            s for s in symbols
            if s in tickers
            and tickers[s]["quote_volume"] >= MIN_24H_QUOTE_VOLUME
            and tickers[s]["change"] <= MAX_24H_GAIN_PCT
        ]

        results = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(analyze, symbol, tickers[symbol]): symbol
                for symbol in universe
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result and result["score"] >= WATCH_SCORE:
                        results.append(result)
                except Exception:
                    pass

        results.sort(
            key=lambda x: (
                x["score"],
                x["volume_1h"],
                x["acceleration"]
            ),
            reverse=True
        )

        now = time.time()

        print("\n" + "=" * 70)
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        if not results:
            print(f"No watch candidates >= {WATCH_SCORE}")

        else:
            print(f"Watch candidates: {len(results)}")

            for x in results[:10]:
                print(
                    f"{x['symbol']:12} "
                    f"Score={x['score']:3} "
                    f"15m={x['15m']:+6.2f}% "
                    f"Vol1h={x['volume_1h']:5.2f}x "
                    f"Vol15m={x['volume_15m']:5.2f}x "
                    f"Accel={x['acceleration']:4.2f}x "
                    f"Break={x['breakout']:+5.2f}% "
                    f"Liquidity={x['liquidity']}"
                )

                # لا نرسل أي شيء عند 65.
                # فقط نبدأ confirmation داخلي عند 80+.
                if x["score"] >= FINAL_SCORE:
                    maybe_final_signal(x, now)

        elapsed = time.time() - cycle_start
        sleep_time = max(10, SCAN_EVERY_SECONDS - elapsed)

        print(f"\nActive final tracks: {len(active_tracks)}")
        print(f"Pending confirmations: {len(candidates)}")
        print(f"Next scan in {sleep_time:.0f} seconds...")

        time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nScanner stopped.")
        break

    except Exception as e:
        print("\nScanner error:", e)
        time.sleep(30)
