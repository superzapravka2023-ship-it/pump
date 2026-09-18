# Bybit Short-Scalper Bot v2 — с подтверждением разворота

Ловит памп на Bybit и шлёт сигнал в ШОРТ **только когда импульс выдохся и вошли
продавцы**: вершина сформирована, цена начала откатывать, свеча отказа/красная,
RSI перекуплен, лонги в ловушке (растущий OI). Смысл: «заходи в шорт — жду откат
вниз ~1–3%». Тейки/стопы в сигнал не пишутся.

Публичный Bybit V5 API, ключи биржи не нужны. Нужен только Telegram-бот и chat_id.

## Как отбирается сигнал
1. Памп ≥ `PUMP_THRESHOLD_PCT`% за `PUMP_LOOKBACK_MIN` мин, оборот ≥ `MIN_TURNOVER_24H`, монета старше `MIN_AGE_DAYS`.
2. **Вершина сформирована:** цена уже ниже хая на `ROLLOVER_MIN_PCT`…`MAX_ROLLOVER_PCT`% (не шортим вертикаль и не входим слишком поздно).
3. **Признаки продавца (нужно ≥ `MIN_REVERSAL_SCORE`):** верхний фитиль ≥ `WICK_MIN_PCT`%, красная свеча/слом импульса, RSI ≥ `RSI_OB`, растущий открытый интерес.

## Деплой GitHub → Railway
```bash
git init && git add . && git commit -m "short bot v2"
git branch -M main
git remote add origin https://github.com/USERNAME/REPO.git
git push -u origin main
```
Railway → New Project → Deploy from GitHub repo → Variables: впиши переменные из
`.env.example` (минимум TELEGRAM_TOKEN, TELEGRAM_CHAT_ID). Worker, без веб-порта.

## Тюнинг
- Меньше и точнее: `MIN_REVERSAL_SCORE=3`, `WICK_MIN_PCT=0.7`.
- Больше сигналов: `MIN_REVERSAL_SCORE=1`, `PUMP_THRESHOLD_PCT=4`.
- Ловить откат раньше/позже: двигай `ROLLOVER_MIN_PCT` и `MAX_ROLLOVER_PCT`.

## Честно
Разворот после пампа — вероятность, а не гарантия: часть пампов продолжит рост
(тогда шорт против тренда). Подтверждения снижают долю таких входов, но не убирают.
Риск и объём — на твоей стороне (стопы намеренно не в сигнале).
