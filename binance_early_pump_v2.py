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

# ---------- STRICT FINAL PUMP GATE ----------
# الـFINAL هنا مخصص للانفجار السعري المبكر، وليس لأي عملة صاعدة.
# لذلك لازم السعر نفسه يتحرك في 15m مع حجم قصير المدى وacceleration.
FINAL_MIN_15M_MOVE = 1.25
FINAL_MIN_5M_MOVE = 0.25
FINAL_MIN_VOLUME_15M = 4.50
FINAL_MIN_VOLUME_1H = 2.00
FINAL_MIN_ACCELERATION = 2.50
FINAL_MIN_24H_VOLUME = 3_000_000
FINAL_MAX_24H_GAIN = 12.0
FINAL_MAX_RSI = 79.5

# ---------- EXPLOSIVE EARLY PATH ----------
# مسار إضافي لا ينتظر 15m +1.25% إذا كان هناك تسارع سعري/حجمي
# واضح جدًا. الهدف التقاط بداية الانفجار قبل أن تتحول العملة إلى
# "late momentum"، مع إبقاء فلاتر البنية والـexhaustion.
EXPLOSIVE_MIN_5M_MOVE = 0.70
EXPLOSIVE_MIN_15M_MOVE = 0.40
EXPLOSIVE_MIN_VOLUME_15M = 7.00
EXPLOSIVE_MIN_VOLUME_1H = 2.00
EXPLOSIVE_MIN_ACCELERATION = 3.00
EXPLOSIVE_MIN_BUY_PRESSURE = 0.58
EXPLOSIVE_MIN_24H_VOLUME = 3_000_000
EXPLOSIVE_MAX_24H_GAIN = 15.0
EXPLOSIVE_MAX_1H_GAIN = 8.0
EXPLOSIVE_MAX_RSI = 78.0
EXPLOSIVE_MAX_UPPER_WICK = 0.40
EXPLOSIVE_MIN_BREAKOUT = -1.50

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI",
    "USDE", "USD1", "RLUSD", "USDD", "EUR", "EURI", "USTC"
}

def explosive_early_gate(x):
    """مسار التقاط بداية الانفجار بدون انتظار 15m +1.25%."""
    symbol = x.get("symbol", "")
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    if base in STABLE_BASES:
        return False

    price_impulse = (
        x.get("5m", -999) >= EXPLOSIVE_MIN_5M_MOVE
        and x.get("15m", -999) >= EXPLOSIVE_MIN_15M_MOVE
    )

    acceleration_ok = (
        x.get("volume_15m", 0) >= EXPLOSIVE_MIN_VOLUME_15M
        and x.get("volume_1h", 0) >= EXPLOSIVE_MIN_VOLUME_1H
        and x.get("acceleration", 0) >= EXPLOSIVE_MIN_ACCELERATION
    )

    structure_ok = (
        x.get("buy_pressure", 0) >= EXPLOSIVE_MIN_BUY_PRESSURE
        and x.get("upper_wick_ratio", 1.0) < EXPLOSIVE_MAX_UPPER_WICK
        and x.get("breakout", -999) >= EXPLOSIVE_MIN_BREAKOUT
        and x.get("rsi14") is not None
        and x.get("rsi14") < EXPLOSIVE_MAX_RSI
    )

    early_ok = (
        x.get("24h", 999) <= EXPLOSIVE_MAX_24H_GAIN
        and x.get("1h", 999) <= EXPLOSIVE_MAX_1H_GAIN
        and x.get("15m", 999) <= 4.0
    )

    liquidity_ok = x.get("quote_volume", 0) >= EXPLOSIVE_MIN_24H_VOLUME

    return bool(price_impulse and acceleration_ok and structure_ok and early_ok and liquidity_ok)


def strict_pump_gate(x):
    symbol = x.get("symbol", "")
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    if base in STABLE_BASES:
        return False

    # لازم يكون فيه حركة سعرية قصيرة حقيقية.
    price_ok = (
        x.get("15m", -999) >= FINAL_MIN_15M_MOVE
        and (
            x.get("5m", -999) >= FINAL_MIN_5M_MOVE
            or x.get("breakout", -999) >= 0.0
        )
    )

    # لازم يكون الحجم القصير + التسارع غير عاديين.
    volume_ok = (
        x.get("volume_1h", 0) >= FINAL_MIN_VOLUME_1H
        and (
            (
                x.get("volume_15m", 0) >= FINAL_MIN_VOLUME_15M
                and x.get("acceleration", 0) >= FINAL_MIN_ACCELERATION
            )
            or (
                x.get("15m", 0) >= 2.50
                and x.get("5m", 0) >= 1.00
                and x.get("volume_15m", 0) >= 8.00
            )
        )
    )

    structure_ok = (
        x.get("buy_pressure", 0) >= 0.52
        and x.get("upper_wick_ratio", 1.0) < 0.45
        and x.get("breakout", -999) >= -0.75
        and x.get("rsi14") is not None
        and x.get("rsi14") < FINAL_MAX_RSI
    )

    # منع العملات التي تحركت بالفعل بدرجة تجعلنا نطاردها.
    not_late = (
        x.get("24h", 999) <= FINAL_MAX_24H_GAIN
        and x.get("15m", 999) <= 6.0
        and x.get("1h", 999) <= 10.0
    )

    liquidity_ok = x.get("quote_volume", 0) >= FINAL_MIN_24H_VOLUME

    return bool(price_ok and volume_ok and structure_ok and not_late and liquidity_ok)

MIN_PRICE_CONFIRM_15M = 1.5
MIN_PRICE_CONFIRM_1H = 3.0


def has_price_confirmation(x):
    # مساران للـFINAL:
    # 1) STRICT: الحركة بدأت بالفعل.
    # 2) EXPLOSIVE EARLY: التسارع السعري/الحجمي قوي بما يكفي لالتقاط
    #    بداية الانفجار قبل أن تصل 15m إلى +1.25%.
    strict_ok = strict_pump_gate(x)
    explosive_ok = explosive_early_gate(x)
    return bool(
        x.get("price_confirmation")
        and x.get("continuation_quality")
        and (strict_ok or explosive_ok)
    )


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
session.headers.update({"User-Agent": "Binance-Early-Pump-Scanner-V3/1.0"})


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

# ---------- فلتر العملات المتوافقة شرعيًا (اختياري) ----------
# المصدر: Shariyah Review Bureau — https://shariyah.net/our-regulatory-status/
# دي تصنيفات المصدر نفسه، مش رأي شرعي من عندنا، وتواريخ التقييم
# متفاوتة (2021-2025) — يعني القايمة محتاجة مراجعة دورية يدوية، وأي
# عملة جديدة أو ماتقيّمتش من المصدر ده مش هتظهر هنا تلقائيًا (نهج
# "allow-list": لو العملة مش في القايمة، بنستبعدها افتراضيًا، بدل
# ما نفترض إنها حلال لغياب تقييم).
SHARIAH_COMPLIANT_BASES = {
    "BTC", "BNB", "ADA", "ETH", "XRP", "XLM", "USDT", "ALGO",
    "AVAX", "DOGE", "LTC", "DOT", "MATIC", "XTZ", "USDC", "SOL",
    "BUSD", "LINK", "ETC", "UNI", "ATOM", "FIL", "HNT", "ICP",
    "XMR", "NEAR", "THETA", "TON", "TRX", "SUI",
}

# شغّلها True عشان السكانر يبعت بس من العملات دي، أو False يرجع
# للمسح الكامل زي الأول. ملحوظة: تفعيلها هيقلل عدد الإشارات بشكل
# واضح، لأن معظم القفزات الكبيرة (30%+) بتحصل في عملات صغيرة
# مش موجودة في القايمة دي أصلًا.
SHARIAH_FILTER_ENABLED = False  # التحليل الجديد: لا نستبعد العملات غير الموجودة في القائمة تلقائيًا


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

        if SHARIAH_FILTER_ENABLED and base not in SHARIAH_COMPLIANT_BASES:
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



def ema(values, period):
    """EMA بسيط بدون مكتبات إضافية."""
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
    gains = []
    losses = []
    for i in range(1, len(values)):
        d = values[i] - values[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr_pct(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1]),
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

    opens = [float(x[1]) for x in k]
    closes = [float(x[4]) for x in k]
    highs = [float(x[2]) for x in k]
    lows = [float(x[3]) for x in k]
    quote_volumes = [float(x[7]) for x in k]
    taker_buy_quote_volumes = [float(x[10]) for x in k]

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

    # ---------- Trend / momentum / volatility ----------
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    atr14_pct = atr_pct(highs, lows, closes, 14)

    ema9_slope_15m = pct_slope(closes[-4:]) if len(closes) >= 4 else 0.0
    ema21_slope_1h = pct_slope(closes[-13:]) if len(closes) >= 13 else 0.0

    trend_bullish = (
        ema9 is not None and ema21 is not None and
        price > ema9 > ema21
    )
    trend_strong = (
        trend_bullish and ema50 is not None and price > ema50
    )

    # Buy-pressure proxy: taker-buy quote volume / total quote volume.
    current_buy = sum(taker_buy_quote_volumes[-3:])
    current_total = sum(quote_volumes[-3:])
    buy_pressure_15m = current_buy / current_total if current_total else 0.5

    prev_buy = sum(taker_buy_quote_volumes[-6:-3])
    prev_total = sum(quote_volumes[-6:-3])
    prev_buy_pressure = prev_buy / prev_total if prev_total else 0.5

    buy_pressure_delta = buy_pressure_15m - prev_buy_pressure

    # شمعة 5m الحالية: جسم قوي وإغلاق قريب من القمة = ضغط شراء أفضل.
    body_pct = pct(closes[-1], opens[-1]) if opens[-1] else 0.0
    upper_wick = highs[-1] - max(opens[-1], closes[-1])
    candle_range_abs = highs[-1] - lows[-1]
    upper_wick_ratio = upper_wick / candle_range_abs if candle_range_abs else 0.0

    # ---------- Early-opportunity score ----------
    # الهدف هنا ليس قياس "قوة الحركة الحالية"؛ بل جودة فرصة ما زالت مبكرة.
    # حجم التداول = اهتمام، وليس شراء بحد ذاته. الحركة الزائدة/الـwick الكبير
    # يعاقبان لأنهما قد يعنيان أننا وصلنا متأخرين.
    previous_15m = sum(quote_volumes[-6:-3])
    prev_prev_15m = sum(quote_volumes[-9:-6])
    prior_acceleration = previous_15m / prev_prev_15m if prev_prev_15m else 0.0
    volume_persistence = min(volume_ratio_15m, volume_ratio_1h)

    recent_high_1h = max(highs[-13:-1])
    distance_from_1h_high = pct(price, recent_high_1h) if recent_high_1h else 0.0
    efficiency = abs(gain_15m) / max(volume_ratio_15m, 1.0) if volume_ratio_15m else 0.0

    # Score من 0 إلى 100، لكن عتبة FINAL صعبة عمدًا.
    score = 0

    # 1) Abnormal volume: دليل اهتمام، وليس إشارة شراء مستقلة.
    if volume_ratio_1h >= 6:
        score += 18
    elif volume_ratio_1h >= 4:
        score += 15
    elif volume_ratio_1h >= 2.5:
        score += 11
    elif volume_ratio_1h >= 1.7:
        score += 6

    if volume_ratio_15m >= 10:
        score += 12
    elif volume_ratio_15m >= 6:
        score += 9
    elif volume_ratio_15m >= 3:
        score += 6
    elif volume_ratio_15m >= 2:
        score += 3

    # 2) استمرار الحجم أهم من spike واحد.
    if volume_persistence >= 3:
        score += 6
    elif volume_persistence >= 2:
        score += 4
    elif volume_persistence >= 1.5:
        score += 2

    if acceleration >= 3 and prior_acceleration >= 1.2:
        score += 6
    elif acceleration >= 2:
        score += 4
    elif acceleration >= 1.3:
        score += 2

    # 3) Earlyness: نكافئ الحركة المتوسطة، لا الحركة التي قطعت شوطًا كبيرًا.
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

    # 4) Price/volume efficiency: حجم كبير مع حركة سعر مضبوطة أفضل من
    # حجم كبير بعد قفزة سعرية بالفعل.
    if 0.05 <= efficiency <= 1.25:
        score += 6
    elif efficiency <= 2:
        score += 2
    elif efficiency > 3:
        score -= 5

    # 5) Trend structure.
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

    # 6) Buy pressure يدعم الاستمرار، لكنه ليس predictor منفرد.
    if buy_pressure_15m >= 0.62 and buy_pressure_delta >= 0.02:
        score += 7
    elif buy_pressure_15m >= 0.56:
        score += 4
    elif buy_pressure_15m < 0.45:
        score -= 6

    # 7) Breakout = سياق فقط. لا نكافئ كسرًا كبيرًا كأنه بداية مؤكدة.
    if -1.0 <= breakout_pct <= 1.5:
        score += 5
    elif 1.5 < breakout_pct <= 3:
        score += 2
    elif breakout_pct > 4:
        score -= 4
    elif breakout_pct < -3:
        score -= 2

    # 8) Exhaustion / rejection filter.
    if upper_wick_ratio > 0.35 and body_pct < 0.5:
        score -= 5
    elif close_location >= 0.75 and body_pct > 0:
        score += 3

    if rsi14 is not None:
        if 50 <= rsi14 <= 68:
            score += 4
        elif 68 < rsi14 <= 78:
            score += 1
        elif rsi14 > 85:
            score -= 7
        elif rsi14 < 42:
            score -= 4

    if atr14_pct is not None:
        if 0.4 <= atr14_pct <= 5:
            score += 2
        elif atr14_pct > 8:
            score -= 3

    # 9) 24h context: الصعود البسيط مقبول، الارتفاع الكبير يقلل earlyness.
    if 0 <= gain_24h <= 8:
        score += 4
    elif gain_24h < 0:
        score += 2
    elif gain_24h <= 14:
        score += 1
    elif gain_24h > 20:
        score -= 3

    # 10) Liquidity = جودة تنفيذ، وليست احتمال pump.
    if qv24 >= 20_000_000:
        liquidity = "HIGH"
        liquidity_points = 4
    elif qv24 >= 10_000_000:
        liquidity = "GOOD"
        liquidity_points = 3
    elif qv24 >= 5_000_000:
        liquidity = "MEDIUM"
        liquidity_points = 1
    else:
        liquidity = "LOW"
        liquidity_points = 0
    score += liquidity_points

    # Late-entry penalty.
    if gain_15m > 7 or gain_1h > 10:
        score -= 8
    if gain_15m > 8.5 and gain_1h > 12:
        score -= 10

    score = max(0, min(int(round(score)), 100))

    # ---------- Setup classification ----------
    if (
        volume_ratio_15m >= 5
        and volume_ratio_1h >= 2
        and acceleration >= 1.5
        and 0.5 <= gain_15m <= 6
        and gain_1h <= 8
        and buy_pressure_15m >= 0.54
        and (rsi14 is None or rsi14 < 80)
    ):
        setup = "SMART EARLY PUMP"
    elif (
        breakout_pct >= -1
        and volume_ratio_1h >= 2
        and acceleration >= 1.5
        and gain_15m <= 6
    ):
        setup = "BREAKOUT + VOLUME ACCELERATION"
    elif volume_ratio_1h >= 3 and volume_persistence >= 2:
        setup = "VOLUME EXPANSION"
    elif breakout_pct >= 0:
        setup = "BREAKOUT"
    else:
        setup = "EARLY MOMENTUM"

    # لو العملة تستوفي مسار EXPLOSIVE EARLY، نميزها صراحة في السجل
    # والـTelegram بدل خلطها مع EARLY MOMENTUM العادي.
    explosive_probe = {
        "symbol": symbol, "5m": gain_5m, "15m": gain_15m, "1h": gain_1h,
        "24h": gain_24h, "volume_15m": volume_ratio_15m,
        "volume_1h": volume_ratio_1h, "acceleration": acceleration,
        "buy_pressure": buy_pressure_15m, "upper_wick_ratio": upper_wick_ratio,
        "breakout": breakout_pct, "rsi14": rsi14, "quote_volume": qv24,
    }
    if explosive_early_gate(explosive_probe):
        setup = "EXPLOSIVE EARLY"

    # Final gate: لا يكفي score مرتفع؛ لازم السعر يؤكد الاستمرار بدون exhaustion.
    price_confirmation = (
        gain_15m >= 1.0
        or gain_1h >= 2.0
        or (breakout_pct >= -1.0 and close_location >= 0.70)
    )
    continuation_quality = (
        buy_pressure_15m >= 0.52
        and (rsi14 is None or rsi14 < 82)
        and upper_wick_ratio < 0.50
        and not (gain_15m > 8 and gain_1h > 12)
    )

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
        "liquidity": liquidity,
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50,
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

    return f"""🔥 FINAL EARLY-PUMP CANDIDATE

⚠️ إشارة تحليلية مبنية على نموذج early-opportunity، وليست ضمانًا أو أمر شراء.

COIN: {x['symbol']}
SETUP: {x['setup']}
EARLY SCORE: {x['score']}/100

Price at final signal:
{x['price']:.12g}

5m:  {x['5m']:+.2f}%
15m: {x['15m']:+.2f}%
1h:  {x['1h']:+.2f}%
24h: {x['24h']:+.2f}%

Volume 1h: {x['volume_1h']:.2f}x
Volume 15m: {x['volume_15m']:.2f}x
Acceleration: {x['acceleration']:.2f}x
Volume persistence: {x.get('volume_persistence', 0):.2f}x

Trend: {x.get('trend', 'N/A')}
EMA9/EMA21: {x.get('ema9', 0):.8g} / {x.get('ema21', 0):.8g}
RSI14: {x.get('rsi14') if x.get('rsi14') is not None else 0:.1f}
ATR14: {x.get('atr_pct') if x.get('atr_pct') is not None else 0:.2f}%
Buy pressure: {x.get('buy_pressure', 0):.2f}
Buy pressure Δ: {x.get('buy_pressure_delta', 0):+.3f}
Candle body: {x.get('body_pct', 0):+.2f}%
Upper wick: {x.get('upper_wick_ratio', 0) * 100:.1f}% of range

Breakout: {x['breakout']:+.2f}%
Distance from 1h high: {x.get('distance_from_1h_high', 0):+.2f}%
Liquidity: {x['liquidity']}
24h Volume: ${x['quote_volume']:,.0f}

Confirmation scans: {confirmations}
{cross_section}
📌 لماذا ظهرت؟
حجم غير طبيعي + استمرار حجم + حركة سعر ما زالت محدودة نسبيًا
+ اتجاه/ضغط شراء + فلتر ضد الـexhaustion.

🚫 لا يوجد Target ثابت +25%/+30% في هذه النسخة.
الهدف الثابت كان يوحي بقدرة على توقع مسافة الحركة، بينما النموذج
الحالي يقيس جودة البداية فقط. سيتم قياس MFE / MAE فعليًا بعد الإشارة.

📊 سيتم الآن تتبع النتيجة:
5m / 15m / 30m / 1h / 4h / 24h

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

    # لا ندخل confirmation إلا إذا score + price gate اجتمعوا.
    if x["score"] < FINAL_SCORE or not has_price_confirmation(x):
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
            "best": x,
            "first": x,
        }
        return False

    if now - c["last_ts"] > CANDIDATE_EXPIRY_MINUTES * 60:
        candidates[symbol] = {
            "count": 1,
            "first_ts": now,
            "last_ts": now,
            "best": x,
            "first": x,
        }
        return False

    c["count"] += 1
    c["last_ts"] = now
    if x["score"] > c["best"]["score"]:
        c["best"] = x

    first = c["first"]
    second_confirmed = (
        x["score"] >= FINAL_SCORE
        and has_price_confirmation(x)
        and (strict_pump_gate(x) or explosive_early_gate(x))
        and x.get("continuation_quality", False)
        and x["5m"] > -0.5
        and x["15m"] >= first["15m"] - 1.0
        and x["volume_1h"] >= first["volume_1h"] * 0.70
    )

    # لا يوجد bypass للـconfirmation حتى لو score = 90 أو 100.
    if c["count"] < CONFIRMATIONS_REQUIRED or not second_confirmed:
        return False

    final_x = x if x["score"] >= c["best"]["score"] else c["best"]

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

    # state القديم من V2/V3 ممكن ما يكونش فيه first/metrics المطلوبة للـconfirmation الجديد؛
    # نحتفظ بالـcooldown والتتبع، لكن نعيد المرشحات القديمة من الصفر بدل crash.
    for sym, cand in state.get("candidates", {}).items():
        if isinstance(cand, dict) and "first" in cand and "best" in cand:
            candidates[sym] = cand

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
                f"Buy={x.get('buy_pressure', 0):.2f} "
                f"RSI={x.get('rsi14') or 0:4.1f} "
                f"Trend={x.get('trend','-')} "
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
