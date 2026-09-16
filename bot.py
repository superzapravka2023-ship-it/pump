"""
Bybit Short-Scalper Signal Bot
==============================
Сканирует все USDT-перпетуалы Bybit, ищет аномальный рост (памп) за короткое
окно и присылает в Telegram сигнал на ШОРТ с уровнями.

Данные берутся из ПУБЛИЧНОГО Bybit V5 API — ключи биржи НЕ нужны.
Нужен только Telegram Bot Token и твой chat_id.

Автор логики: сигнальный бот (вход подтверждаешь руками). Как докрутить до
автоторговли — см. README.
"""

import os
import asyncio
import time
import logging

import aiohttp

# ------------------------------------------------------------------ #
#  Логирование
# ------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("shortbot")


# ------------------------------------------------------------------ #
#  Конфиг (всё настраивается через переменные окружения на Railway)
# ------------------------------------------------------------------ #
def _f(name, default):        # float из env
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


def _i(name, default):        # int из env
    try:
        return int(float(os.getenv(name, default)))
    except (TypeError, ValueError):
        return int(default)


TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SCAN_INTERVAL       = _i("SCAN_INTERVAL", 60)          # как часто сканировать, сек (30-60)
PUMP_LOOKBACK_MIN   = _i("PUMP_LOOKBACK_MIN", 7)       # окно роста, минут
PUMP_THRESHOLD_PCT  = _f("PUMP_THRESHOLD_PCT", 5.0)    # порог роста за окно, %
MIN_TURNOVER_24H    = _f("MIN_TURNOVER_24H", 5_000_000)  # мин. оборот за 24ч, USDT
MIN_AGE_DAYS        = _i("MIN_AGE_DAYS", 30)           # монета старше N дней
COOLDOWN_MIN        = _i("COOLDOWN_MIN", 30)           # антиспам на монету, минут
RSI_PERIOD          = _i("RSI_PERIOD", 14)
MAX_CONCURRENCY     = _i("MAX_CONCURRENCY", 12)        # одновременных запросов к Bybit
STOP_BUFFER_PCT     = _f("STOP_BUFFER_PCT", 1.5)       # буфер стопа над хаем, %

BYBIT_BASE = "https://api.bybit.com"

# ------------------------------------------------------------------ #
#  Глобальное состояние (для команды /status)
# ------------------------------------------------------------------ #
STATE = {
    "started_at": time.time(),
    "scans": 0,
    "last_scan": 0.0,
    "universe": 0,          # монет проходит фильтры объёма/возраста
    "alerts_total": 0,
    "running": True,
}
_last_alert = {}            # symbol -> ts последнего алерта
_instruments_cache = {"ts": 0.0, "data": {}}   # symbol -> launchTime(ms)


# ================================================================== #
#  HTTP helpers
# ================================================================== #
async def bybit_get(session, path, params, retries=3):
    """GET к Bybit V5 с ретраями и обработкой 429."""
    url = BYBIT_BASE + path
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                data = await r.json()
                if data.get("retCode") == 0:
                    return data.get("result", {})
                # retCode != 0 — логируем и не ретраим бессмысленно
                log.debug("Bybit retCode=%s msg=%s (%s)", data.get("retCode"), data.get("retMsg"), path)
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            await asyncio.sleep(0.8 * (attempt + 1))
    return None


async def tg_send(session, text):
    """Отправить сообщение в Telegram (HTML)."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("TELEGRAM_TOKEN/CHAT_ID не заданы — сообщение не отправлено")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                log.warning("Telegram send failed: %s %s", r.status, await r.text())
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("Telegram send error: %s", e)


# ================================================================== #
#  Индикаторы
# ================================================================== #
def rsi(closes, period=14):
    """Классический RSI по списку close (старые -> новые)."""
    if len(closes) < period + 1:
        return None
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = diff if diff > 0 else 0.0
        loss = -diff if diff < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ================================================================== #
#  Bybit data
# ================================================================== #
async def load_instruments(session):
    """Кэш launchTime по всем linear-символам (обновляем раз в 6 часов)."""
    now = time.time()
    if _instruments_cache["data"] and now - _instruments_cache["ts"] < 6 * 3600:
        return _instruments_cache["data"]

    data = {}
    cursor = ""
    for _ in range(20):  # пагинация
        params = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        res = await bybit_get(session, "/v5/market/instruments-info", params)
        if not res:
            break
        for it in res.get("list", []):
            lt = it.get("launchTime")
            if lt:
                data[it["symbol"]] = int(lt)
        cursor = res.get("nextPageCursor", "")
        if not cursor:
            break
    if data:
        _instruments_cache["data"] = data
        _instruments_cache["ts"] = now
        log.info("Инструменты загружены: %d символов", len(data))
    return _instruments_cache["data"]


async def get_tickers(session):
    """Все linear-тикеры одним запросом."""
    res = await bybit_get(session, "/v5/market/tickers", {"category": "linear"})
    if not res:
        return []
    return res.get("list", [])


async def get_klines(session, symbol, limit):
    """1-мин свечи. Bybit отдаёт newest-first, разворачиваем в old->new."""
    res = await bybit_get(session, "/v5/market/kline", {
        "category": "linear", "symbol": symbol, "interval": "1", "limit": limit,
    })
    if not res:
        return None
    rows = res.get("list", [])
    rows = list(reversed(rows))  # теперь старые -> новые
    return rows


# ================================================================== #
#  Анализ одной монеты
# ================================================================== #
async def analyze(session, sem, symbol, ticker):
    async with sem:
        need = max(PUMP_LOOKBACK_MIN + 2, RSI_PERIOD + 5, 30)
        rows = await get_klines(session, symbol, need)
    if not rows or len(rows) < PUMP_LOOKBACK_MIN + 1:
        return None

    closes = [float(r[4]) for r in rows]
    highs  = [float(r[2]) for r in rows]

    last_close = closes[-1]
    ref_open   = float(rows[-(PUMP_LOOKBACK_MIN + 1)][1])  # open N минут назад
    if ref_open <= 0:
        return None

    change_pct = (last_close - ref_open) / ref_open * 100.0
    if change_pct < PUMP_THRESHOLD_PCT:
        return None

    # локальный хай окна — для стопа
    window_high = max(highs[-(PUMP_LOOKBACK_MIN + 1):])
    stop_price  = window_high * (1 + STOP_BUFFER_PCT / 100.0)

    # тейки: откат части памп-движения
    take1 = last_close - (last_close - ref_open) * 0.5   # ~50% отката
    take2 = ref_open                                     # к точке старта пампа

    # верхний фитиль последней свечи (признак откупа) — доп. уверенность
    last_high = highs[-1]
    upper_wick = (last_high - last_close) / last_close * 100.0 if last_close else 0.0

    r = rsi(closes, RSI_PERIOD)
    funding = ticker.get("fundingRate")
    try:
        funding_pct = float(funding) * 100 if funding not in (None, "") else None
    except ValueError:
        funding_pct = None

    return {
        "symbol": symbol,
        "change_pct": change_pct,
        "price": last_close,
        "turnover": float(ticker.get("turnover24h", 0) or 0),
        "stop": stop_price,
        "take1": take1,
        "take2": take2,
        "rsi": r,
        "funding_pct": funding_pct,
        "upper_wick": upper_wick,
    }


def build_alert(s):
    """Собрать текст сигнала (HTML)."""
    def fmt(x):
        if x is None:
            return "—"
        if x >= 100:
            return f"{x:,.2f}"
        if x >= 1:
            return f"{x:.4f}"
        return f"{x:.6f}".rstrip("0")

    conf = []
    if s["rsi"] is not None and s["rsi"] >= 70:
        conf.append(f"RSI перекуплен ({s['rsi']:.0f})")
    if s["funding_pct"] is not None and s["funding_pct"] > 0.03:
        conf.append(f"фандинг + ({s['funding_pct']:.3f}%)")
    if s["upper_wick"] > 0.4:
        conf.append(f"верхний фитиль ({s['upper_wick']:.1f}%)")
    conf_line = " • ".join(conf) if conf else "нет доп. подтверждений"

    link = f"https://www.bybit.com/trade/usdt/{s['symbol']}"
    return (
        f"🔴 <b>SHORT сигнал</b> — <b>{s['symbol']}</b>\n"
        f"📈 Памп: <b>+{s['change_pct']:.2f}%</b> за {PUMP_LOOKBACK_MIN} мин\n"
        f"💵 Цена: <b>{fmt(s['price'])}</b>\n"
        f"📊 Оборот 24ч: <b>${s['turnover']/1e6:.1f}M</b>\n"
        f"🧭 RSI(1m): <b>{'%.0f' % s['rsi'] if s['rsi'] is not None else '—'}</b>\n\n"
        f"🎯 <b>План шорта:</b>\n"
        f"• Вход: по рынку / лимиткой у {fmt(s['price'])}\n"
        f"• Стоп: <b>{fmt(s['stop'])}</b> (над хаем)\n"
        f"• Тейк 1: {fmt(s['take1'])}\n"
        f"• Тейк 2: {fmt(s['take2'])}\n\n"
        f"✅ Подтверждения: {conf_line}\n"
        f"🔗 <a href=\"{link}\">Открыть график</a>\n\n"
        f"<i>Не финансовый совет. Шорт пампа — высокий риск, ставь стоп.</i>"
    )


# ================================================================== #
#  Основной цикл сканирования
# ================================================================== #
async def scan_loop(session):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    while STATE["running"]:
        t0 = time.time()
        try:
            instruments = await load_instruments(session)
            tickers = await get_tickers(session)

            now_ms = time.time() * 1000
            min_age_ms = MIN_AGE_DAYS * 86400 * 1000

            # фильтр вселенной: USDT-перп, оборот, возраст
            candidates = []
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
                if lt is None or (now_ms - lt) < min_age_ms:
                    continue
                candidates.append(t)

            STATE["universe"] = len(candidates)

            # анализируем кандидатов
            tasks = [analyze(session, sem, t["symbol"], t) for t in candidates]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            hits = [r for r in results if isinstance(r, dict) and r]
            hits.sort(key=lambda x: x["change_pct"], reverse=True)

            now = time.time()
            sent = 0
            for s in hits:
                last = _last_alert.get(s["symbol"], 0)
                if now - last < COOLDOWN_MIN * 60:
                    continue
                await tg_send(session, build_alert(s))
                _last_alert[s["symbol"]] = now
                STATE["alerts_total"] += 1
                sent += 1
                await asyncio.sleep(0.3)  # мягко к Telegram

            STATE["scans"] += 1
            STATE["last_scan"] = now
            log.info("Скан #%d: вселенная=%d, пампов=%d, отправлено=%d, за %.1fс",
                     STATE["scans"], len(candidates), len(hits), sent, time.time() - t0)

        except Exception as e:
            log.exception("Ошибка в scan_loop: %s", e)

        # выдержать интервал
        elapsed = time.time() - t0
        await asyncio.sleep(max(3, SCAN_INTERVAL - elapsed))


# ================================================================== #
#  Telegram команды (long polling getUpdates)
# ================================================================== #
async def command_loop(session):
    if not TELEGRAM_TOKEN:
        return
    offset = None
    base = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    while STATE["running"]:
        try:
            params = {"timeout": 30}
            if offset is not None:
                params["offset"] = offset
            async with session.get(base + "/getUpdates", params=params,
                                   timeout=aiohttp.ClientTimeout(total=40)) as r:
                data = await r.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("channel_post") or {}
                text = (msg.get("text") or "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if TELEGRAM_CHAT_ID and chat_id != TELEGRAM_CHAT_ID:
                    continue  # реагируем только на своего владельца
                if text.startswith("/start"):
                    await tg_send(session,
                        "👋 Бот запущен и сканирует Bybit.\n"
                        f"Порог пампа: <b>+{PUMP_THRESHOLD_PCT:.1f}%</b> за {PUMP_LOOKBACK_MIN} мин\n"
                        f"Мин. оборот: <b>${MIN_TURNOVER_24H/1e6:.1f}M</b> • "
                        f"Возраст: <b>&gt;{MIN_AGE_DAYS}д</b>\n"
                        "Команды: /status /help")
                elif text.startswith("/status"):
                    up = int(time.time() - STATE["started_at"])
                    last = int(time.time() - STATE["last_scan"]) if STATE["last_scan"] else -1
                    await tg_send(session,
                        f"📟 <b>Статус</b>\n"
                        f"Аптайм: {up//3600}ч {up%3600//60}м\n"
                        f"Сканов: {STATE['scans']}\n"
                        f"Монет в отборе: {STATE['universe']}\n"
                        f"Сигналов всего: {STATE['alerts_total']}\n"
                        f"Последний скан: {last if last>=0 else '—'} сек назад")
                elif text.startswith("/help"):
                    await tg_send(session,
                        "Я ищу аномальный рост фьючерсов Bybit и шлю сигнал на ШОРТ.\n"
                        "/start — приветствие и настройки\n"
                        "/status — состояние\n"
                        "Настройки меняются переменными окружения на Railway.")
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(3)
        except Exception as e:
            log.exception("command_loop: %s", e)
            await asyncio.sleep(3)


# ================================================================== #
#  Entry point
# ================================================================== #
async def main():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Задай переменные окружения TELEGRAM_TOKEN и TELEGRAM_CHAT_ID!")
    log.info("Старт. Интервал=%ss, порог=+%.1f%%/%sм, оборот>=$%.0f, возраст>%sд",
             SCAN_INTERVAL, PUMP_THRESHOLD_PCT, PUMP_LOOKBACK_MIN, MIN_TURNOVER_24H, MIN_AGE_DAYS)

    async with aiohttp.ClientSession() as session:
        await tg_send(session, "🚀 Bybit Short-Scalper запущен. /status — проверить состояние.")
        await asyncio.gather(scan_loop(session), command_loop(session))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
