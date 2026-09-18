"""
Bybit Short-Scalper Bot  (v2 — с подтверждением разворота)
=========================================================
Ловит аномальный памп на Bybit и шлёт сигнал в ШОРТ ТОЛЬКО когда видно, что
импульс выдохся и вошли продавцы: вершина сформирована, цена начала откатывать,
свеча отказа / красная свеча, перекупленный RSI, лонги в ловушке (растущий OI).
Смысл сигнала: «заходи в шорт — монета, скорее всего, откатит вниз на ~1–3%».

Публичный Bybit V5 API — ключи биржи не нужны. Нужен только Telegram-бот и chat_id.
Тейки/стопы в сигнал не пишутся (по запросу).
"""

import os
import asyncio
import time
import logging

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("shortbot")


# ------------------------------------------------------------------ #
def _f(n, d):
    try: return float(os.getenv(n, d))
    except (TypeError, ValueError): return float(d)

def _i(n, d):
    try: return int(float(os.getenv(n, d)))
    except (TypeError, ValueError): return int(d)


TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SCAN_INTERVAL      = _i("SCAN_INTERVAL", 60)
PUMP_LOOKBACK_MIN  = _i("PUMP_LOOKBACK_MIN", 7)       # окно пампа, минут
PUMP_THRESHOLD_PCT = _f("PUMP_THRESHOLD_PCT", 5.0)    # порог пампа, %
MIN_TURNOVER_24H   = _f("MIN_TURNOVER_24H", 5_000_000)
MIN_AGE_DAYS       = _i("MIN_AGE_DAYS", 30)

# --- подтверждение разворота ---
ROLLOVER_MIN_PCT   = _f("ROLLOVER_MIN_PCT", 0.3)     # цена уже ниже хая минимум на это (вершина есть)
MAX_ROLLOVER_PCT   = _f("MAX_ROLLOVER_PCT", 2.5)     # но не больше — иначе откат уже упущен
WICK_MIN_PCT       = _f("WICK_MIN_PCT", 0.5)         # верхний фитиль (свеча отказа), %
RSI_OB             = _f("RSI_OB", 70.0)              # перекупленность
MIN_REVERSAL_SCORE = _i("MIN_REVERSAL_SCORE", 2)     # мин. признаков продавца, чтобы слать
RSI_PERIOD         = _i("RSI_PERIOD", 14)
COOLDOWN_MIN       = _i("COOLDOWN_MIN", 30)
MAX_CONCURRENCY    = _i("MAX_CONCURRENCY", 12)

BYBIT_BASE = "https://api.bybit.com"

STATE = {"started_at": time.time(), "scans": 0, "last_scan": 0.0,
         "universe": 0, "alerts_total": 0, "running": True}
_last_alert = {}
_instruments = {"ts": 0.0, "data": {}}


# ================================================================== #
async def _get(session, url, params, ok, retries=3):
    for a in range(retries):
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 429:
                    await asyncio.sleep(1.5 * (a + 1)); continue
                d = await r.json()
                return ok(d)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(0.8 * (a + 1))
    return None


async def bybit_get(session, path, params):
    return await _get(session, BYBIT_BASE + path, params,
                      lambda d: d.get("result") if d.get("retCode") == 0 else None)


async def tg_send(session, text, button_url=None):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Нет TELEGRAM_TOKEN/CHAT_ID"); return
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    if button_url:
        payload["reply_markup"] = {"inline_keyboard":
                                   [[{"text": "📉 Открыть график", "url": button_url}]]}
    try:
        async with session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                                json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                log.warning("Telegram %s: %s", r.status, await r.text())
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("Telegram error: %s", e)


# ================================================================== #
def rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    g = l = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        g += d if d > 0 else 0.0
        l += -d if d < 0 else 0.0
    ag, al = g / period, l / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + (d if d > 0 else 0.0)) / period
        al = (al * (period - 1) + (-d if d < 0 else 0.0)) / period
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


# ================================================================== #
async def load_instruments(session):
    now = time.time()
    if _instruments["data"] and now - _instruments["ts"] < 6 * 3600:
        return _instruments["data"]
    data, cursor = {}, ""
    for _ in range(20):
        p = {"category": "linear", "limit": 1000}
        if cursor: p["cursor"] = cursor
        res = await bybit_get(session, "/v5/market/instruments-info", p)
        if not res: break
        for it in res.get("list", []):
            lt = it.get("launchTime")
            if lt: data[it["symbol"]] = int(lt)
        cursor = res.get("nextPageCursor", "")
        if not cursor: break
    if data:
        _instruments.update(data=data, ts=now); log.info("Инструменты: %d", len(data))
    return _instruments["data"]


async def klines(session, symbol, limit):
    res = await bybit_get(session, "/v5/market/kline",
                          {"category": "linear", "symbol": symbol, "interval": "1", "limit": limit})
    if not res: return None
    return list(reversed(res.get("list", [])))   # old -> new


async def oi_trend(session, symbol):
    res = await bybit_get(session, "/v5/market/open-interest",
                          {"category": "linear", "symbol": symbol,
                           "intervalTime": "5min", "limit": 6})
    if not res: return 0
    lst = res.get("list", [])
    if len(lst) < 3: return 0
    try:
        newest = float(lst[0]["openInterest"])
        older  = float(lst[min(3, len(lst) - 1)]["openInterest"])
    except (KeyError, ValueError):
        return 0
    if older <= 0: return 0
    ch = (newest - older) / older * 100
    return 1 if ch > 0.3 else (-1 if ch < -0.3 else 0)


# ================================================================== #
async def analyze(session, sem, symbol, ticker):
    async with sem:
        need = max(PUMP_LOOKBACK_MIN + 2, RSI_PERIOD + 5, 25)
        rows = await klines(session, symbol, need)
    if not rows or len(rows) < PUMP_LOOKBACK_MIN + 1:
        return None

    opens  = [float(r[1]) for r in rows]
    highs  = [float(r[2]) for r in rows]
    closes = [float(r[4]) for r in rows]
    last_close = closes[-1]

    # 1) был ли памп за окно
    ref_open = opens[-(PUMP_LOOKBACK_MIN + 1)]
    if ref_open <= 0: return None
    pump_pct = (max(highs[-(PUMP_LOOKBACK_MIN + 1):]) - ref_open) / ref_open * 100.0
    if pump_pct < PUMP_THRESHOLD_PCT:
        return None

    # 2) вершина сформирована? (цена уже отошла вниз от хая, но откат ещё не упущен)
    window_high = max(highs[-(PUMP_LOOKBACK_MIN + 1):])
    pullback = (window_high - last_close) / window_high * 100.0
    if not (ROLLOVER_MIN_PCT <= pullback <= MAX_ROLLOVER_PCT):
        return None

    # 3) признаки продавца
    score, reasons = 0, []

    # верхний фитиль последних 2 свечей (свеча отказа)
    wick = 0.0
    for i in (-1, -2):
        body_top = max(opens[i], closes[i])
        if body_top > 0:
            wick = max(wick, (highs[i] - body_top) / body_top * 100.0)
    if wick >= WICK_MIN_PCT:
        score += 1; reasons.append(f"фитиль {wick:.1f}%")

    # красная свеча / слом импульса
    if closes[-1] < opens[-1] or closes[-1] < closes[-2]:
        score += 1; reasons.append("свеча вниз")

    # RSI перекуплен
    r = rsi(closes, RSI_PERIOD)
    if r is not None and r >= RSI_OB:
        score += 1; reasons.append(f"RSI {r:.0f}")

    # лонги в ловушке (OI растёт при затухании)
    async with sem:
        oi = await oi_trend(session, symbol)
    if oi > 0:
        score += 1; reasons.append("OI растёт (лонги в ловушке)")

    if score < MIN_REVERSAL_SCORE:
        return None

    return {"symbol": symbol, "pump_pct": pump_pct, "pullback": pullback,
            "price": last_close, "rsi": r, "score": score, "reasons": reasons,
            "turnover": float(ticker.get("turnover24h", 0) or 0)}


def fmt(x):
    if x is None: return "—"
    if x >= 100: return f"{x:,.2f}"
    if x >= 1:   return f"{x:.4f}"
    return f"{x:.6f}".rstrip("0")


def build_alert(s):
    stars = "⭐" * s["score"]
    link = f"https://www.bybit.com/trade/usdt/{s['symbol']}"
    reasons = " • ".join(s["reasons"]) if s["reasons"] else "—"
    text = (
        f"🔴 <b>ЗАХОДИ В ШОРТ</b> — <b>{s['symbol']}</b>\n"
        f"Импульс выдохся, вошли продавцы — жду откат вниз <b>~1–3%</b>\n\n"
        f"📈 Памп был: <b>+{s['pump_pct']:.2f}%</b> за {PUMP_LOOKBACK_MIN} мин\n"
        f"📉 Уже отошла от хая: <b>-{s['pullback']:.2f}%</b>\n"
        f"💵 Цена: <b>{fmt(s['price'])}</b>\n"
        f"{stars}  Подтверждений: <b>{s['score']}</b>\n"
        f"🧩 {reasons}\n\n"
        f"<i>⚠️ Разворот — вероятность, не гарантия. Риск и объём на тебе.</i>"
    )
    return link, text


# ================================================================== #
async def scan_loop(session):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    while STATE["running"]:
        t0 = time.time()
        try:
            instruments = await load_instruments(session)
            res = await bybit_get(session, "/v5/market/tickers", {"category": "linear"})
            tickers = res.get("list", []) if res else []
            now_ms, min_age = time.time() * 1000, MIN_AGE_DAYS * 86400 * 1000

            cand = []
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                try:
                    if float(t.get("turnover24h", 0) or 0) < MIN_TURNOVER_24H:
                        continue
                except ValueError:
                    continue
                lt = instruments.get(sym)
                if lt is None or (now_ms - lt) < min_age:
                    continue
                cand.append(t)
            STATE["universe"] = len(cand)

            results = await asyncio.gather(
                *[analyze(session, sem, t["symbol"], t) for t in cand],
                return_exceptions=True)
            hits = [r for r in results if isinstance(r, dict) and r]
            hits.sort(key=lambda x: (x["score"], x["pump_pct"]), reverse=True)

            now, sent = time.time(), 0
            for s in hits:
                if now - _last_alert.get(s["symbol"], 0) < COOLDOWN_MIN * 60:
                    continue
                link, text = build_alert(s)
                await tg_send(session, text, button_url=link)
                _last_alert[s["symbol"]] = now
                STATE["alerts_total"] += 1; sent += 1
                await asyncio.sleep(0.3)

            STATE["scans"] += 1; STATE["last_scan"] = now
            log.info("Скан #%d: вселенная=%d, сигналов=%d, отправлено=%d, %.1fс",
                     STATE["scans"], len(cand), len(hits), sent, time.time() - t0)
        except Exception as e:
            log.exception("scan_loop: %s", e)
        await asyncio.sleep(max(3, SCAN_INTERVAL - (time.time() - t0)))


async def command_loop(session):
    if not TELEGRAM_TOKEN: return
    offset, base = None, f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    while STATE["running"]:
        try:
            p = {"timeout": 30}
            if offset is not None: p["offset"] = offset
            async with session.get(base + "/getUpdates", params=p,
                                   timeout=aiohttp.ClientTimeout(total=40)) as r:
                data = await r.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("channel_post") or {}
                text = (msg.get("text") or "").strip().lower()
                chat = str(msg.get("chat", {}).get("id", ""))
                if TELEGRAM_CHAT_ID and chat != TELEGRAM_CHAT_ID:
                    continue
                if text.startswith("/start"):
                    await tg_send(session,
                        "👋 Шорт-бот v2 запущен. Ловлю памп + подтверждение разворота\n"
                        "и шлю «заходи в шорт», когда продавцы уже вошли.\n"
                        f"Памп ≥ <b>{PUMP_THRESHOLD_PCT:.1f}%</b>/{PUMP_LOOKBACK_MIN}м • "
                        f"подтверждений ≥ <b>{MIN_REVERSAL_SCORE}</b>\n"
                        "Команды: /status /help")
                elif text.startswith("/status"):
                    up = int(time.time() - STATE["started_at"])
                    last = int(time.time() - STATE["last_scan"]) if STATE["last_scan"] else -1
                    await tg_send(session,
                        f"📟 <b>Статус</b>\nАптайм: {up//3600}ч {up%3600//60}м\n"
                        f"Сканов: {STATE['scans']}\nМонет в отборе: {STATE['universe']}\n"
                        f"Сигналов: {STATE['alerts_total']}\n"
                        f"Последний скан: {last if last>=0 else '—'} сек назад")
                elif text.startswith("/help"):
                    await tg_send(session,
                        "Сначала ищу памп ≥ порога, потом проверяю, что вершина сформирована\n"
                        "и вошли продавцы (фитиль, красная свеча, RSI, OI). Только тогда — сигнал.\n"
                        "Настройки — переменные окружения Railway.")
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(3)
        except Exception as e:
            log.exception("command_loop: %s", e); await asyncio.sleep(3)


async def main():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Задай TELEGRAM_TOKEN и TELEGRAM_CHAT_ID!")
    log.info("Старт шорт-бота v2: памп +%.1f%%/%sм, откат %.1f–%.1f%%, подтверждений≥%d",
             PUMP_THRESHOLD_PCT, PUMP_LOOKBACK_MIN, ROLLOVER_MIN_PCT, MAX_ROLLOVER_PCT,
             MIN_REVERSAL_SCORE)
    async with aiohttp.ClientSession() as session:
        await tg_send(session, "🚀 Шорт-бот v2 (с подтверждением разворота) запущен. /status")
        await asyncio.gather(scan_loop(session), command_loop(session))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
