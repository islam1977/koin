# Binance Early-Pump Scanner — GitHub Actions Setup

سكريبت مسح مجاني بيشتغل تلقائي كل 5 دقايق على سيرفرات GitHub، من غير ما تسيب
جهازك أو Colab شغالين. Signal only — مفيش أي تنفيذ صفقات حقيقي.

## 1) إنشاء الريبو

1. اعمل ريبو جديد على GitHub (**Public** أفضل — دقائق الـ Actions مجانية بلا حدود
   للريبوهات العامة، أما الخاصة فمحدودة بحصة شهرية مجانية).
2. ارفع الملفات دي في الريبو بنفس الأسماء والمسارات:
   ```
   binance_early_pump.py
   requirements.txt
   .github/workflows/scanner.yml
   ```

## 2) إنشاء بوت تليجرام والحصول على التوكن

1. افتح تليجرام وابحث عن **@BotFather**.
2. ابعتله `/newbot` واتبع التعليمات (اسم للبوت + username ينتهي بـ `bot`).
3. هيديك **API Token** شكله شبه:
   `123456789:ABCdefGhIjKlmNoPQRstuVwxYZ`
   ← ده هو `TELEGRAM_BOT_TOKEN`.

## 3) الحصول على Chat ID بتاعك

1. افتح محادثة مع البوت اللي عملته وابعتله أي رسالة (مثلاً "hi").
2. افتح الرابط ده في المتصفح (حط التوكن بتاعك مكان `<TOKEN>`):
   ```
   https://api.telegram.org/bot<TOKEN>/getUpdates
   ```
3. هتلاقي في الرد جزء زي:
   ```json
   "chat": { "id": 987654321, "first_name": "..." }
   ```
   الرقم ده هو `TELEGRAM_CHAT_ID`.

## 4) تخزين البيانات كـ Secrets في GitHub (مش في الكود أبدًا)

في الريبو بتاعك:
`Settings → Secrets and variables → Actions → New repository secret`

أضف الاتنين دول بنفس الاسم بالظبط:

| الاسم | القيمة |
|---|---|
| `TELEGRAM_BOT_TOKEN` | التوكن اللي أخدته من BotFather |
| `TELEGRAM_CHAT_ID` | الرقم اللي طلع في الـ getUpdates |

الـ workflow (`scanner.yml`) بيقرأهم تلقائي ويحطهم كـ environment variables،
والسكريبت بيقراهم بـ `os.getenv(...)` — مفيش أي تعديل تاني مطلوب.

## 5) تشغيل أول مرة يدويًا (اختياري لكن مفيد)

في تبويب **Actions** بالريبو، افتح workflow اسمه
"Binance Early-Pump Scanner" واضغط **Run workflow** يدويًا عشان تتأكد إن كل
حاجة شغالة قبل ما تستنى الجدولة التلقائية.

## إيه اللي هيحصل بعد كده؟

- كل 5 دقايق تقريبًا (GitHub مش بيضمن الدقة للثانية، ممكن تأخير بسيط وقت
  الزحام)، هيشتغل السكريبت مرة واحدة (`--once`)، يعمل مسح، ويقفل.
- أي إشارة أو ملخص أو صفقة محاكاة تتقفل → توصلك رسالة تليجرام فورًا.
- الملفات دي بتتحدث وتترفع (commit) على الريبو تلقائي بعد كل تشغيلة، عشان
  الـ cooldown والإحصائيات ما تضيعش بين تشغيلة والتانية:
  - `scanner_alerts.csv` — سجل كل الإشارات
  - `signal_outcomes.csv` — دقة الإشارات بعد مرور الوقت
  - `trailing_stop_sim.csv` — نتائج محاكاة الـ trailing stop
  - `pending_outcomes.json`, `last_alert.json`, `scan_count.json` — حالة داخلية

## تحذير مهم

GitHub بيوقف أي **scheduled workflow** تلقائيًا لو الريبو فضل من غير أي commit
لمدة 60 يوم. لو حصل كده، روح لتبويب Actions واضغط "Enable workflow" تاني —
مش هتحتاج تعمل حاجة تانية.
