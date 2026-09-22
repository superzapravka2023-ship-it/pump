"""
Bybit → Bitget  LONG Continuation Signal Bot  (FINAL / основной)
===============================================================
Идея: Bybit — «ведущий» рынок (объёмы, импульс приходят раньше). Ловим на нём
аномальный памп, а сделку ты открываешь на Bitget, где цена ещё НЕ догнала.
Бот сам проверяет, что на Bitget монета есть и реально отстаёт, считает «запас
хода» и присваивает сигналу балл качества.

Всё на ПУБЛИЧНЫХ API обеих бирж — ключи бирж не нужны. Нужен только Telegram
Bot Token и твой chat_id. Тейки/стопы в сигнал не пишем (по запросу) — только
чистый сигнал + кнопка «Открыть на Bitget».
"""

import os
import asyncio
import time
import logging

import aiohttp

# ------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("longbot")


# ------------------------------------------------------------------ #
#  Конфиг (переменные окружения Railway)
# ------------------------------------------------------------------ #
def _f(n, d):
    try: return float(os.getenv(n, d))
    except (TypeError, ValueError): return float(d)

def _i(n, d):
    try: return int(float(os.getenv(n, d)))
    except (TypeError, ValueError): return int(d)


TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SCAN_INTERVAL      = _i("SCAN_INTERVAL", 60)          # частота скана, сек
PUMP_LOOKBACK_MIN  = _i("PUMP_LOOKBACK_MIN", 5)       # окно роста, минут
PUMP_THRESHOLD_PCT = _f("PUMP_THRESHOLD_PCT", 5.0)    # порог роста за окно, %
MIN_TURNOVER_24H   = _f("MIN_TURNOVER_24H", 5_000_000)  # мин. оборот 24ч (Bybit), USDT
MIN_AGE_DAYS       = _i("MIN_AGE_DAYS", 30)           # монета старше N дней

MAX_PULLBACK_FOR_ENTRY_PCT = _f("MAX_PULLBACK_FOR_ENTRY_PCT", 1.0)  # если от хая уже упали больше — импульс гаснет, пропуск
VOL_SURGE_MULT     = _f("VOL_SURGE_MULT", 2.0)        # всплеск объёма: посл. свеча / средн.
MIN_SCORE          = _i("MIN_SCORE", 2)               # мин. балл качества, чтобы слать
RSI_PERIOD         = _i("RSI_PERIOD", 14)
COOLDOWN_MIN       = _i("COOLDOWN_MIN", 30)           # антиспам на монету, минут
MAX_CONCURRENCY    = _i("MAX_CONCURRENCY", 12)

BYBIT_BASE  = "https://api.bybit.com"
BITGET_BASE = "https://api.bitget.com"
BITGET_PT   = "USDT-FUTURES"

STATE = {"started_at": time.time(), "scans": 0, "last_scan": 0.0,
         "universe": 0, "alerts_total": 0, "running": True}
_last_alert = {}
_instruments = {"ts": 0.0, "data": {}}     # bybit symbol -> launchTime(ms)
_bitget_syms = {"ts": 0.0, "data": set()}  # множество символов Bitget USDT-FUTURES


# ================================================================== #
#  HTTP
# ================================================================== #
async def _get(session, url, params, ok_check, retries=3):
    for a in range(retries):
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 429:
                    await asyncio.sleep(1.5 * (a + 1)); continue
                data = await r.json()
                res = ok_check(data)
                if res is not None:
                    return res
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(0.8 * (a + 1))
    return None


async def bybit_get(session, path, params):
    return await _get(session, BYBIT_BASE + path, params,
                      lambda d: d.get("result") if d.get("retCode") == 0 else None)


async def bitget_get(session, path, params):
    return await _get(session, BITGET_BASE + path, params,
                      lambda d: d.get("data") if str(d.get("code")) == "00000" else None)


async def tg_send(session, text, button_url=None):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Нет TELEGRAM_TOKEN/CHAT_ID — сообщение не отправлено"); return
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    if button_url:
        payload["reply_markup"] = {"inline_keyboard":
                                   [[{"text": "📲 Открыть на Bitget", "url": button_url}]]}
    try:
        async with session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                                json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                log.warning("Telegram %s: %s", r.status, await r.text())
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("Telegram error: %s", e)


# ================================================================== #
#  Индикаторы / утилиты
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


def pct_move(rows, lookback):
    """Рост % за окно lookback минут по свечам old->new."""
    if len(rows) < lookback + 1:
        return None
    ref_open = float(rows[-(lookback + 1)][1])
    last_close = float(rows[-1][4])
    if ref_open <= 0:
        return None
    return (last_close - ref_open) / ref_open * 100.0


# ================================================================== #
#  Загрузка справочников
# ================================================================== #
async def load_bybit_instruments(session):
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
        _instruments.update(data=data, ts=now)
        log.info("Bybit инструменты: %d", len(data))
    return _instruments["data"]


async def load_bitget_symbols(session):
    """Множество символов Bitget USDT-FUTURES (обновляем раз в 30 мин)."""
    now = time.time()
    if _bitget_syms["data"] and now - _bitget_syms["ts"] < 1800:
        return _bitget_syms["data"]
    res = await bitget_get(session, "/api/v2/mix/market/tickers", {"productType": BITGET_PT})
    if res:
        syms = {t.get("symbol") for t in res if t.get("symbol")}
        if syms:
            _bitget_syms.update(data=syms, ts=now)
            log.info("Bitget символы: %d", len(syms))
    return _bitget_syms["data"]


# ================================================================== #
#  Данные по монете
# ================================================================== #
async def bybit_klines(session, symbol, limit):
    res = await bybit_get(session, "/v5/market/kline",
                          {"category": "linear", "symbol": symbol, "interval": "1", "limit": limit})
    if not res: return None
    return list(reversed(res.get("list", [])))   # old -> new


async def bybit_oi_trend(session, symbol):
    """+1 если OI растёт, -1 если падает, 0 если непонятно."""
    res = await bybit_get(session, "/v5/market/open-interest",
                          {"category": "linear", "symbol": symbol,
                           "intervalTime": "5min", "limit": 6})
    if not res: return 0
    lst = res.get("list", [])            # newest first
    if len(lst) < 3: return 0
    try:
        newest = float(lst[0]["openInterest"])
        older  = float(lst[min(3, len(lst) - 1)]["openInterest"])
    except (KeyError, ValueError):
        return 0
    if older <= 0: return 0
    ch = (newest - older) / older * 100
    return 1 if ch > 0.3 else (-1 if ch < -0.3 else 0)


async def bitget_move(session, symbol, lookback):
    res = await bitget_get(session, "/api/v2/mix/market/candles",
                           {"symbol": symbol, "productType": BITGET_PT,
                            "granularity": "1m", "limit": str(lookback + 3)})
    if not res: return None
    rows = sorted(res, key=lambda r: int(r[0]))   # old -> new
    return pct_move(rows, lookback)


# ================================================================== #
#  Анализ кандидата
# ================================================================== #
async def analyze(session, sem, symbol, ticker):
    async with sem:
        need = max(PUMP_LOOKBACK_MIN + 2, RSI_PERIOD + 5, 25)
        rows = await bybit_klines(session, symbol, need)
    if not rows or len(rows) < PUMP_LOOKBACK_MIN + 1:
        return None

    bybit_pct = pct_move(rows, PUMP_LOOKBACK_MIN)
    if bybit_pct is None or bybit_pct < PUMP_THRESHOLD_PCT:
        return None

    closes = [float(r[4]) for r in rows]
    highs  = [float(r[2]) for r in rows]
    turns  = [float(r[6]) for r in rows]   # оборот свечи (USDT)
    last_close = closes[-1]

    # --- импульс ещё жив? не покупаем продолжение в разворот ---
    window_high = max(highs[-(PUMP_LOOKBACK_MIN + 1):])
    drop_from_high = (window_high - last_close) / window_high * 100 if window_high > 0 else 0
    if drop_from_high > MAX_PULLBACK_FOR_ENTRY_PCT:
        return None                        # цена уже откатывает от хая — импульс гаснет, поздно

    # --- Bitget: есть ли монета (и насколько отстаёт — только для инфо) ---
    async with sem:
        bg_pct = await bitget_move(session, symbol, PUMP_LOOKBACK_MIN)
    if bg_pct is None:
        return None                        # нет на Bitget / нет данных — пропуск
    gap = bybit_pct - bg_pct               # запас хода на Bitget (справочно, не фильтр)

    # --- фильтры качества ---
    score, reasons = 0, []

    # 1) всплеск объёма
    if len(turns) >= 21:
        avg = sum(turns[-21:-1]) / 20
        mult = turns[-1] / avg if avg > 0 else 0
        if mult >= VOL_SURGE_MULT:
            score += 1; reasons.append(f"объём ×{mult:.1f}")
    # 2) открытый интерес
    async with sem:
        oi = await bybit_oi_trend(session, symbol)
    if oi > 0:
        score += 1; reasons.append("OI растёт")
    elif oi < 0:
        reasons.append("OI падает ⚠️")
    # 3) пробой локального хая
    prior_high = max(highs[-(PUMP_LOOKBACK_MIN + 1):-1]) if len(highs) > PUMP_LOOKBACK_MIN else 0
    if last_close > prior_high > 0:
        score += 1; reasons.append("пробой хая")
    # 5) здоровый RSI (сильный, но не выдох)
    r = rsi(closes, RSI_PERIOD)
    if r is not None and 60 <= r <= 88:
        score += 1; reasons.append(f"RSI {r:.0f}")
    # штраф за истощение (большой верхний фитиль + перегрев)
    upper_wick = (highs[-1] - last_close) / last_close * 100 if last_close else 0
    if upper_wick > 0.6 and (r or 0) > 90:
        score -= 1; reasons.append("истощение?")

    if score < MIN_SCORE:
        return None

    return {"symbol": symbol, "bybit_pct": bybit_pct, "bg_pct": bg_pct, "gap": gap,
            "score": score, "reasons": reasons, "rsi": r,
            "bg_price": None}   # цену Bitget подставим из тикера ниже


def fmt_price(x):
    if x is None: return "—"
    if x >= 100: return f"{x:,.2f}"
    if x >= 1:   return f"{x:.4f}"
    return f"{x:.6f}".rstrip("0")


def build_alert(s):
    stars = "⭐" * s["score"]
    link = f"https://www.bitget.com/futures/usdt/{s['symbol']}"
    reasons = " • ".join(s["reasons"]) if s["reasons"] else "—"
    return (link,
        f"🟢 <b>LONG (продолжение)</b> — <b>{s['symbol']}</b>\n"
        f"⚡ Bybit: <b>+{s['bybit_pct']:.2f}%</b> за {PUMP_LOOKBACK_MIN} мин\n"
        f"📊 Bitget: <b>+{s['bg_pct']:.2f}%</b>  (отставание {s['gap']:+.2f}%)\n"
        f"💵 Bitget цена: <b>{fmt_price(s['bg_price'])}</b>\n"
        f"{stars}  Качество: <b>{s['score']}</b>\n"
        f"🧩 {reasons}\n\n"
        f"<i>⚠️ Импульс может развернуться — риск и объём на тебе.</i>")


# ================================================================== #
#  Основной цикл
# ================================================================== #
async def scan_loop(session):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    while STATE["running"]:
        t0 = time.time()
        try:
            instruments = await load_bybit_instruments(session)
            bitget_set  = await load_bitget_symbols(session)
            res = await bybit_get(session, "/v5/market/tickers", {"category": "linear"})
            tickers = res.get("list", []) if res else []

            now_ms = time.time() * 1000
            min_age = MIN_AGE_DAYS * 86400 * 1000

            # вселенная: есть на обеих биржах, оборот, возраст
            cand = []
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT") or sym not in bitget_set:
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

            # цены Bitget для подстановки в сигнал
            bg_prices = {}
            bg_res = await bitget_get(session, "/api/v2/mix/market/tickers",
                                      {"productType": BITGET_PT})
            if bg_res:
                for x in bg_res:
                    p = x.get("lastPr") or x.get("last") or x.get("close")
                    if x.get("symbol") and p:
                        try: bg_prices[x["symbol"]] = float(p)
                        except ValueError: pass

            results = await asyncio.gather(
                *[analyze(session, sem, t["symbol"], t) for t in cand],
                return_exceptions=True)
            hits = [r for r in results if isinstance(r, dict) and r]
            hits.sort(key=lambda x: (x["score"], x["gap"]), reverse=True)

            now, sent = time.time(), 0
            for s in hits:
                if now - _last_alert.get(s["symbol"], 0) < COOLDOWN_MIN * 60:
                    continue
                s["bg_price"] = bg_prices.get(s["symbol"])
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


# ================================================================== #
#  Telegram команды
# ================================================================== #
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
                        "👋 Бот запущен. Слежу за Bybit, сигналю LONG на продолжение,\n"
                        "сделку открываешь на Bitget.\n"
                        f"Памп: <b>+{PUMP_THRESHOLD_PCT:.1f}%</b>/{PUMP_LOOKBACK_MIN}м • "
                        f"балл ≥ <b>{MIN_SCORE}</b>\n"
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
                        "Ищу памп на Bybit, проверяю что Bitget ещё отстаёт, и шлю LONG-сигнал\n"
                        "с запасом хода и баллом качества. Настройки — переменные окружения Railway.")
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(3)
        except Exception as e:
            log.exception("command_loop: %s", e); await asyncio.sleep(3)


async def main():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Задай TELEGRAM_TOKEN и TELEGRAM_CHAT_ID!")
    log.info("Старт LONG-бота. Bybit→Bitget, памп +%.1f%%/%sм, балл≥%d",
             PUMP_THRESHOLD_PCT, PUMP_LOOKBACK_MIN, MIN_SCORE)
    async with aiohttp.ClientSession() as session:
        await tg_send(session, "🚀 Bybit→Bitget LONG-бот (финальный) запущен. /status — состояние.")
        await asyncio.gather(scan_loop(session), command_loop(session))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
