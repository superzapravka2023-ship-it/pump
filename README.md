# Bybit Short-Scalper Signal Bot

Сканирует все USDT-перпетуалы **Bybit** каждые 30–60 сек, ловит аномальный
рост (памп) и присылает в Telegram **сигнал на ШОРТ** с уровнями входа, стопа
и тейков.

Использует **публичный** Bybit V5 API — ключи биржи не нужны. Нужен только
Telegram-бот (токен от @BotFather) и твой chat_id.

## Что именно ищет

Монета попадает в сигнал, если **одновременно**:
- это USDT-перпетуал Bybit;
- оборот за 24ч ≥ `MIN_TURNOVER_24H` (по умолч. 5 000 000 USDT);
- монета листнута ≥ `MIN_AGE_DAYS` дней назад (по умолч. 30);
- цена выросла ≥ `PUMP_THRESHOLD_PCT`% (по умолч. 5%) за `PUMP_LOOKBACK_MIN` минут (по умолч. 7).

В сигнал добавляются подтверждения: RSI(1m) перекупленность, положительный
funding, верхний фитиль последней свечи. Антиспам: одна монета не чаще раза
в `COOLDOWN_MIN` минут.

## Деплой: GitHub → Railway

1. **GitHub.** Создай новый репозиторий и залей туда эти файлы:
   ```bash
   git init
   git add .
   git commit -m "bybit short scalper bot"
   git branch -M main
   git remote add origin https://github.com/USERNAME/REPO.git
   git push -u origin main
   ```

2. **Telegram.** Получи токен у [@BotFather](https://t.me/BotFather) (`/newbot`)
   и свой chat_id у [@userinfobot](https://t.me/userinfobot). Напиши своему боту
   `/start` хотя бы один раз, чтобы он мог тебе писать.

3. **Railway.**
   - [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo** → выбери репозиторий.
   - Railway сам увидит `requirements.txt` (Nixpacks) и `railway.json` со стартовой командой `python bot.py`.
   - Вкладка **Variables** → добавь переменные из `.env.example`
     (минимум `TELEGRAM_TOKEN` и `TELEGRAM_CHAT_ID`).
   - Деплой стартует автоматически. В логах должно появиться «Старт…» и в
     Telegram придёт «🚀 … запущен».

> Это **worker** (фоновый процесс без веб-порта) — HTTP-порт открывать не нужно.
> `restartPolicyType: ALWAYS` поднимет бота после падения.

## Команды бота
- `/start` — приветствие + текущие настройки
- `/status` — аптайм, число сканов, монет в отборе, сигналов
- `/help` — краткая справка

## Настройка под себя
Всё меняется переменными окружения на Railway без правки кода. Хочешь агрессивнее —
`PUMP_THRESHOLD_PCT=4`, `SCAN_INTERVAL=30`. Хочешь строже — подними порог/оборот.

## Как докрутить до автоторговли (по желанию)
Сейчас бот только сигналит. Чтобы он сам открывал шорт:
1. Добавь `pybit` в `requirements.txt`.
2. Заведи API-ключи Bybit (с правами на торговлю фьючерсами) в переменные `BYBIT_API_KEY` / `BYBIT_API_SECRET`.
3. В `analyze()`/после отправки алерта вызывай `session.place_order(category="linear", symbol=..., side="Sell", orderType="Market", qty=...)` через `pybit.unified_trading.HTTP`.
4. Обязательно выставляй stop-loss (`stopLoss=`) и считай `qty` от риска на сделку, а не от всего депозита.

**Дисклеймер.** Это не финансовый совет. Шорт вертикальных пампов — высокий
риск (памп может продолжиться, ликвидация). Тестируй на малом объёме и всегда
со стопом.
