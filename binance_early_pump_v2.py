# ============================================================
# BINANCE EARLY-PUMP SCANNER V2 — GOOGLE COLAB / TELEGRAM
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

import requests, time, math, csv, os, json, sys
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# data-api.binance.vision = عنوان Binance الرسمي لبيانات السوق العامة فقط (مفيش تداول).
# بيشتغل غالبًا من السيرفرات اللي api.binance.com بيرجعلها خطأ 451.
# لو فشل بـ 451 بنجرب العناوين التانية بالترتيب.
BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]
STATE_FILE = "scanner_state.json"   # حالة السكريبت بين التشغيلات (لوضع --once)
INTERVAL = "5m"
KLINES_LIMIT = 180

# ---------- إعدادات الفحص ----------

SCAN_EVERY_SECONDS = 300

# السيولة
MIN_24H_QUOTE_VOLUME = 3_000_000

# لا نريد مطاردة عملة ارتفعت بالفعل بقوة.
# كانت 18.0: بترفض أي عملة عدّت 24h gain المحدد، حتى لو كانت لسه في
# بداية انفجار حقيقي (زي MOVR). القفزات الكبيرة غالبًا بتبدأ من +15-30%
# مش من صفر، فرفعناها لـ35 عشان نراقب العملة حتى لو بدأت تتحرك فعلاً،
# مع إبقاء حد أقصى يمنع ملاحقة عملة خلصت حركتها تمامًا.
MAX_24H_GAIN_PCT = 35.0

# الحركة قصيرة الأجل المقبولة
MIN_15M_GAIN = -2.0
MAX_15M_GAIN = 10.0

MIN_1H_GAIN = -5.0
MAX_1H_GAIN = 18.0

# ---------- مستويات دخول/خروج مقترحة (Risk Management) ----------
# دي نسب مئوية ثابتة وبسيطة من سعر الإشارة، مش تحليل عميق لتذبذب
# العملة (زي ATR). الهدف إنها تدّيك نقطة مرجعية سريعة تحسب عليها
# حجم صفقتك ومخاطرتك بنفسك، مش توصية بشراء أو ضمان لأي ربح.
STOP_LOSS_PCT = 8.0        # وقف خسارة تحت سعر الدخول
TAKE_PROFIT_1_PCT = 25.0   # هدف أول: مكسب جيد وواقعي
TAKE_PROFIT_2_PCT = 30.0   # هدف ثاني: لو الزخم استمر

# تأكيد حركة سعر حقيقية قبل أي FINAL (بغض النظر عن الـ Score):
# لازم واحد من الاتنين يتحقق على الأقل. ده بيمنع إن حجم تداول ضخم
# لوحده (من غير حركة سعر فعلية) يوصل لـFINAL، زي ما حصل مع
# DODOUSDT (حجم 61x لكن 24h=-0.97%) وMEMEUSDT (حجم 14x لكن 1h=0.69%).
MIN_PRICE_CONFIRM_15M = 1.5
MIN_PRICE_CONFIRM_1H = 3.0


def has_price_confirmation(x):
    return x["15m"] >= MIN_PRICE_CONFIRM_15M or x["1h"] >= MIN_PRICE_CONFIRM_1H

# الدرجات
WATCH_SCORE = 65
FINAL_SCORE = 80
# كانت 90: يعني إشارة لازم توصل لدرجة شبه مثالية عشان تتبعت من غير
# تأكيدين. قللناها لـ85 عشان إشارات قوية (زي اللي شفناها فعليًا بـ87)
# تتبعت فورًا، مع إبقاء التأكيد الثنائي شغال للإشارات بين 80 و84.
EXCEPTIONAL_SCORE = 85

# تأكيد الإشارة:
# يجب أن تظهر الإشارة القوية في دورتين متتاليتين
# أو تصل إلى EXCEPTIONAL_SCORE في دورة واحدة.
CONFIRMATIONS_REQUIRED = 2

# كانت 20 دقيقة قبل كده، وده كان بيفترض إن GitHub Actions بيشغّل
# الدورة كل 5 دقائق بالظبط. الواقع إن GitHub بيتأخر فعليًا 15-60+
# دقيقة وقت الزحام، فنافذة الـ20 دقيقة كانت بتقفل قبل ما الدورة
# التانية تتشغل أصلًا — ده كان بيمنع أي تأكيد يحصل خالص (تأكدنا من
# كده: عملات وصلت score 80+ (CRVUSDT, PROMUSDT, TRBUSDT, VTHOUSDT)
# وفضلت عالقة على count=1 للأبد، ومفيش FINAL واحد اتبعت من أول
# تشغيل). رفعناها لـ90 دقيقة عشان تدّي هامش واقعي لجدولة GitHub.
CANDIDATE_EXPIRY_MINUTES = 90

# بعد إرسال FINAL لا نرسل نفس العملة مرة أخرى خلال هذه المدة.
FINAL_COOLDOWN_HOURS = 12

# ---------- تأكيد عبر بورصات تانية (Cross-Exchange Confirmation) ----------
# بنتأكد من نفس العملة على Bybit و OKX قبل إرسال FINAL بس (مش كل دورة فحص)،
# عشان نقلل الإشارات الكاذبة اللي سببها نشاط محصور على Binance بس
# (زي wash trading)، ونديك صورة أوضح هل الحركة حقيقية في السوق كله.
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"

# لو التغير في الساعة الأخيرة على بورصة تانية >= الرقم ده، نعتبرها موافقة.
CROSS_CONFIRM_AGREE_PCT = 1.0
# لو التغير <= الرقم ده (سالب)، نعتبرها تعارض واضح مع حركة Binance.
CROSS_CONFIRM_DISAGREE_PCT = -1.0

# لو True: أي عملة عليها "تعارض واضح" من البورصتين مع مفيش أي موافقة،
# ميتبعتش لها FINAL خالص (بيتسجل في الـ log بس). لو False: بتتبعت
# برضه لكن الرسالة بتوضح التعارض عشان تاخد قرارك بنفسك.
# الافتراضي False لأن فشل مؤقت في API بورصة تانية (مش نادر) مش المفروض
# يمنع إشارة حقيقية على Binance؛ التوضيح في الرسالة كافي غالبًا.
SUPPRESS_ON_DIVERGENCE = False

# ---------- تتبع النتيجة ----------

TRACK_MINUTES = [5, 15, 30, 60, 240, 1440]

# ملف النتائج (مسار نسبي — هيتحفظ في مجلد المشروع على السيرفر)
TRACK_FILE = "early_pump_signal_results.csv"

# ---------- Telegram ----------

# على VPS السكريبت بيشتغل من غير تفاعل بشري (headless)، فبنقرأ
# التوكن والـ Chat ID من environment variables بدل input() اللي كانت
# بتشتغل بس على Colab. الـ systemd service بيمررهم تلقائي.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("تحذير: TELEGRAM_BOT_TOKEN أو TELEGRAM_CHAT_ID غير موجودين — "
          "النتائج هتظهر في الـ log بس من غير إرسال لتليجرام.")

session = requests.Session()
session.headers.update({"User-Agent": "Binance-Early-Pump-Scanner-V2/1.0"})


def get_json(path, params=None):
    last_err = None
    for base in BASE_URLS:
        try:
            r = session.get(base + path, params=params, timeout=15)
            if r.status_code == 451:
                last_err = requests.HTTPError(f"451 from {base}", response=r)
                continue
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            raise
        except requests.RequestException as e:
            last_err = e
            continue
    raise last_err if last_err else RuntimeError("no base url worked")


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


# عملات مستقرة (Stablecoins) مربوطة بسعر تقريبًا ثابت (غالبًا $1).
# مينفعش "تنفجر" أصلًا، لكن نسب الحجم عندها ممكن تطلع أرقام جنونية
# (زي BFUSDUSDT اللي طلعت acceleration=173x من غير أي حركة سعر
# حقيقية) لمجرد إن حجمها الأساسي صغير. بنستبعدها من الأساس.
STABLECOIN_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "BFUSD",
    "USDE", "PYUSD", "EUR", "GBP", "AEUR", "USD1", "WBETH",
}


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

        base = symbol[:-4]  # إزالة "USDT" من آخر الاسم
        if base in STABLECOIN_BASES:
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

    # تم رفع أعلى درجة للحجم من 30 إلى 35 بناءً على تحليل backtest على آخر
    # 30 يوم: vol_ratio_1h كان أقوى مقياس فارق فعليًا قبل القفزات
    # (حجم أثر +0.98 قبل ساعة واحدة من القفزة)، وده أقوى بكتير من أي
    # مقياس تاني في السكانر.
    if volume_ratio_1h >= 6:
        score += 35
    elif volume_ratio_1h >= 4:
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

    # تم تقليل وزن الاختراق من 20/10 إلى 10/5 بناءً على تحليل backtest:
    # العملات قبل القفزة الفعلية كانت في المتوسط أبعد عن قمتها الأخيرة
    # بـ~10% (مقابل ~7% في الأوقات العادية) — يعني القرب من كسر القمة
    # مش مؤشر مبكر قوي زي ما كان مفترض، فقللنا اعتماد السكور عليه
    # لحد ما تتجمع بيانات أكتر تأكد الاتجاه ده.
    if breakout_pct >= 0:
        score += 10
    elif breakout_pct >= -1.5:
        score += 5

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

    # Smart Early Pump: حجم انفجر فجأة والسعر لسه ما تحركش كتير ولسه
    # تحت القمة — نمط "العملة جاهزة تتحرك بس الحركة الكبيرة ماحصلتش
    # بعد"، وده أقرب حالة لنوع الفرص اللي بتتفوّت حاليًا (زي MOVR وهي
    # لسه في أول حركتها). بيتفحص قبل الأنماط التانية لأنه الأكثر تحديدًا.
    if (
        volume_ratio_15m >= 8
        and acceleration >= 3
        and 1.0 <= gain_15m <= 8.0
        and gain_24h < 25.0
    ):
        setup = "SMART EARLY PUMP"
    elif (
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

# ---------- EARLY WATCH logging (CSV فقط، مفيش تليجرام) ----------
# بنسجل أول مرة عملة توصل WATCH_SCORE، عشان نجمع بيانات نراجعها بعدين
# ("هل النظام شاف العملة دي مبكرًا؟") من غير ما نزعج تليجرام بكل عملة
# بتلمس 65 في أي فحصة. نفس العملة متتسجلش تاني قبل ما EARLY_WATCH_COOLDOWN_HOURS
# تعدي، عشان عملة قاعدة على 68 لمدة ساعات ما تتسجلش عشرات المرات.
EARLY_WATCH_LOG_FILE = "early_watch_log.csv"
EARLY_WATCH_COOLDOWN_HOURS = 2
early_watch_logged = {}


def log_early_watch(x, now):
    symbol = x["symbol"]
    last = early_watch_logged.get(symbol)
    if last is not None and now - last < EARLY_WATCH_COOLDOWN_HOURS * 3600:
        return

    early_watch_logged[symbol] = now
    exists = os.path.exists(EARLY_WATCH_LOG_FILE)

    with open(EARLY_WATCH_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow([
                "logged_time_utc", "symbol", "setup", "score", "price",
                "gain_15m", "gain_1h", "gain_24h",
                "volume_1h", "volume_15m", "acceleration",
                "breakout", "liquidity", "quote_volume",
            ])
        writer.writerow([
            datetime.now(timezone.utc).isoformat(), symbol, x["setup"], x["score"], x["price"],
            x["15m"], x["1h"], x["24h"],
            x["volume_1h"], x["volume_15m"], x["acceleration"],
            x["breakout"], x["liquidity"], x["quote_volume"],
        ])


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


# ============================================================
# CROSS-EXCHANGE CONFIRMATION (Bybit + OKX)
# ============================================================

def okx_inst_id(binance_symbol):
    """RADUSDT -> RAD-USDT (كل أزواجنا مقابل USDT أصلاً)."""
    base = binance_symbol[:-4] if binance_symbol.endswith("USDT") else binance_symbol
    return f"{base}-USDT"


def get_bybit_klines_15m(symbol, limit=6):
    """آخر limit شمعة 15 دقيقة من Bybit، الأحدث أولًا. None لو مش موجودة/فشل الطلب."""
    try:
        r = session.get(
            f"{BYBIT_BASE}/v5/market/kline",
            params={"category": "spot", "symbol": symbol, "interval": "15", "limit": limit},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            return None
        rows = data.get("result", {}).get("list", [])
        return rows or None
    except Exception:
        return None


def get_okx_klines_15m(inst_id, limit=6):
    """آخر limit شمعة 15 دقيقة من OKX، الأحدث أولًا. None لو مش موجودة/فشل الطلب."""
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
        rows = data.get("data", [])
        return rows or None
    except Exception:
        return None


def klines_1h_change_pct(rows, close_index):
    """رجّع نسبة التغير خلال آخر ساعة تقريبًا (4 شمعات * 15 دقيقة)
    من قائمة شموع مرتبة الأحدث أولًا، أو None لو البيانات مش كفاية."""
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


def cross_exchange_confirmation(binance_symbol):
    """
    بيشوف نفس العملة على Bybit وOKX ويرجّع (label, detail):
    - "CONFIRMED"        : بورصة تانية على الأقل بتأكد نفس الحركة صاعدة
    - "DIVERGENCE"       : مفيش موافقة، وفي بورصة بتاعد عكس الحركة تمامًا
    - "NEUTRAL"          : موجودة في بورصة تانية بس التغير مش واضح كفاية
    - "BINANCE-EXCLUSIVE": مش لاقيينها على Bybit ولا OKX (عادي لعملات حديثة الإدراج)
    بيتنادى بس على المرشحين اللي وصلوا لعتبة FINAL، مش كل دورة فحص،
    فمكلفش أداء السكانر ككل.
    """
    notes = []
    agree = 0
    disagree = 0
    listed_anywhere = False

    bybit_rows = get_bybit_klines_15m(binance_symbol)
    if bybit_rows:
        listed_anywhere = True
        chg = klines_1h_change_pct(bybit_rows, close_index=4)
        if chg is not None:
            notes.append(f"Bybit 1h: {chg:+.2f}%")
            if chg >= CROSS_CONFIRM_AGREE_PCT:
                agree += 1
            elif chg <= CROSS_CONFIRM_DISAGREE_PCT:
                disagree += 1

    okx_rows = get_okx_klines_15m(okx_inst_id(binance_symbol))
    if okx_rows:
        listed_anywhere = True
        chg = klines_1h_change_pct(okx_rows, close_index=4)
        if chg is not None:
            notes.append(f"OKX 1h: {chg:+.2f}%")
            if chg >= CROSS_CONFIRM_AGREE_PCT:
                agree += 1
            elif chg <= CROSS_CONFIRM_DISAGREE_PCT:
                disagree += 1

    if not listed_anywhere:
        label = "BINANCE-EXCLUSIVE"
    elif agree > 0:
        label = "CONFIRMED"
    elif disagree > 0:
        label = "DIVERGENCE"
    else:
        label = "NEUTRAL"

    detail = " | ".join(notes) if notes else "لا توجد بيانات من بورصات تانية"
    return label, detail


CROSS_LABEL_EMOJI = {
    "CONFIRMED": "✅",
    "DIVERGENCE": "⚠️",
    "NEUTRAL": "➖",
    "BINANCE-EXCLUSIVE": "🔒",
}


def format_final(x, confirmations, cross_label=None, cross_detail=None):
    cross_section = ""
    if cross_label:
        emoji = CROSS_LABEL_EMOJI.get(cross_label, "")
        cross_section = f"\nCross-exchange check: {emoji} {cross_label}\n{cross_detail}\n"

    entry = x["price"]
    stop_loss = entry * (1 - STOP_LOSS_PCT / 100)
    take_profit_1 = entry * (1 + TAKE_PROFIT_1_PCT / 100)
    take_profit_2 = entry * (1 + TAKE_PROFIT_2_PCT / 100)

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
{cross_section}
💰 مستويات مقترحة (نسب ثابتة، مش تحليل تذبذب دقيق):
Entry: {entry:.12g}
🎯 Target 1 (+{TAKE_PROFIT_1_PCT:.0f}%): {take_profit_1:.12g}
🎯 Target 2 (+{TAKE_PROFIT_2_PCT:.0f}%): {take_profit_2:.12g}
🛑 Stop-loss (-{STOP_LOSS_PCT:.0f}%): {stop_loss:.12g}

📌 لماذا ظهرت؟
السعر + الحجم + تسارع الحجم + الاختراق
اجتمعت في نفس الوقت.

📊 سيتم الآن تتبع النتيجة تلقائيًا:
5m / 15m / 30m / 1h / 4h / 24h

⚠️ المستويات دي مبنية على نسبة ثابتة بسيطة، مش توصية مالية ولا
ضمان ربح. قرار الدخول والخروج ومقدار المخاطرة بتاعك إنت.
لا يوجد تداول آلي في هذه النسخة.
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
        c = {
            "count": 1,
            "first_ts": now,
            "last_ts": now,
            "best": x
        }
        candidates[symbol] = c
        # باج كان موجود هنا: كان بيرجع False فورًا حتى لو score وصل
        # EXCEPTIONAL_SCORE من أول مرة، يعني "إشارة قوية تتبعت من دورة
        # واحدة" ماكانتش بتشتغل عمليًا أبدًا (كان المفروض يستنى CONFIRMATIONS_REQUIRED
        # دايمًا، حتى لو الشرط exceptional كان True). دلوقتي أول حدوث
        # يكمّل للفحص تحت بدل ما يرجع فورًا.
    else:
        # يجب أن تكون الدورة التالية قريبة زمنيًا.
        if now - c["last_ts"] <= CANDIDATE_EXPIRY_MINUTES * 60:
            c["count"] += 1
            c["last_ts"] = now
            if x["score"] > c["best"]["score"]:
                c["best"] = x
        else:
            c = {
                "count": 1,
                "first_ts": now,
                "last_ts": now,
                "best": x
            }
            candidates[symbol] = c

    # تأكيد عادي: دورتان متتاليتان.
    confirmed = c["count"] >= CONFIRMATIONS_REQUIRED

    # أو إشارة استثنائية جدًا.
    exceptional = x["score"] >= EXCEPTIONAL_SCORE

    if confirmed or exceptional:
        final_x = x if x["score"] >= c["best"]["score"] else c["best"]

        # تأكيد عبر Bybit وOKX — بس للمرشحين اللي وصلوا هنا فعلاً،
        # مش لكل عملة بتتفحص، عشان ما يبطأش السكانر.
        cross_label, cross_detail = cross_exchange_confirmation(symbol)

        if SUPPRESS_ON_DIVERGENCE and cross_label == "DIVERGENCE":
            print(f"\n[Cross-exchange] {symbol}: DIVERGENCE — FINAL اتمنع. {cross_detail}")
            final_cooldown[symbol] = now
            candidates.pop(symbol, None)
            return False

        final_cooldown[symbol] = now
        candidates.pop(symbol, None)

        message = format_final(
            final_x,
            c["count"],
            cross_label=cross_label,
            cross_detail=cross_detail,
        )

        telegram_send(message)

        print("\n" + "!" * 70)
        print(message)
        print("!" * 70)

        start_final_tracking(final_x)
        return True

    return False


# ============================================================
# STATE (لوضع --once على GitHub Actions: كل تشغيلة بتبدأ من الصفر،
# فلازم نحفظ الحالة في ملف ونرجّعها)
# ============================================================

def save_state():
    state = {
        "candidates": candidates,
        "final_cooldown": final_cooldown,
        "active_tracks": active_tracks,
        "early_watch_logged": early_watch_logged,
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        print("Could not save state:", e)


def load_state():
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        print("Could not read state:", e)
        return

    candidates.update(state.get("candidates", {}))
    final_cooldown.update(state.get("final_cooldown", {}))
    early_watch_logged.update(state.get("early_watch_logged", {}))

    for sym, tr in state.get("active_tracks", {}).items():
        # مفاتيح JSON بتتحول لنصوص، فبنرجّعها أرقام.
        tr["results"] = {int(k): v for k, v in tr.get("results", {}).items()}
        active_tracks[sym] = tr


# ============================================================
# ONE SCAN CYCLE
# ============================================================

def run_cycle(symbols):
    cycle_start = time.time()

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
        key=lambda x: (x["score"], x["volume_1h"], x["acceleration"]),
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

            # EARLY WATCH: تسجيل في CSV بس (مفيش تليجرام)، عشان نجمع
            # بيانات نراجعها بعدين من غير ما نزعج الموبايل بكل عملة
            # بتلمس 65.
            log_early_watch(x, now)

            # لا نرسل أي شيء عند 65.
            # فقط نبدأ confirmation داخلي عند 80+، وبشرط إن السعر
            # نفسه بيتحرك فعلاً (مش بس الحجم)، عشان نتجنب حالات زي
            # DODOUSDT/MEMEUSDT اللي وصلت Score 80+ بسبب حجم ضخم
            # بينما السعر كان شبه ثابت أو نازل.
            if x["score"] >= FINAL_SCORE and has_price_confirmation(x):
                maybe_final_signal(x, now)

    print(f"\nActive final tracks: {len(active_tracks)}")
    print(f"Pending confirmations: {len(candidates)}")

    return max(10, SCAN_EVERY_SECONDS - (time.time() - cycle_start))


# ============================================================
# START
# ============================================================

ONCE = "--once" in sys.argv

init_results_file()
load_state()

print("\nتحميل قائمة Binance...")
try:
    symbols = get_symbols()
except Exception as e:
    print("Cannot load Binance symbols:", e)
    sys.exit(1)

print(f"تم تحميل {len(symbols)} زوج USDT.")
print("Scanner V2 started" + (" (single run)." if ONCE else "."))

if ONCE:
    # وضع GitHub Actions / cron: تشغيلة واحدة ثم خروج.
    try:
        run_cycle(symbols)
    except Exception as e:
        print("Scanner error:", e)
        save_state()
        sys.exit(1)
    save_state()
    sys.exit(0)

# وضع الحلقة المستمرة (Colab / VPS)
while True:
    try:
        sleep_time = run_cycle(symbols)
        save_state()
        print(f"Next scan in {sleep_time:.0f} seconds...")
        time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nScanner stopped.")
        save_state()
        break

    except Exception as e:
        print("\nScanner error:", e)
        time.sleep(30)
