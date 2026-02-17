# BI / Analytics Demo: near-real-time витрина + Data Quality мониторинг (dbt + Postgres + Redash)

Проект — мини-версия аналитического контура “как в проде”: генерация событий → очистка/дедупликация → витрины (KPI + funnel) → слой Data Quality (снапшоты + дневные метрики) → дашборды в Redash.
Собирается одним `docker compose up` и каждые 5 минут пересчитывает витрины/проверки.

> Цель: показать подход, который ожидают от аналитика/инженера данных в Avito: понятная архитектура, воспроизводимость, DQ как часть пайплайна, мониторинг в BI.


## Архитектура пайплайна

![alt text](<docs/images/Снимок экрана 2026-02-17 в 20.22.11.png>)
*(Смысл схемы: `raw → stg(view) → mart + dq`, dbt запускается по расписанию, Redash читает mart/dq.)*

### Поток данных (в двух словах)

1. **data_generator** пишет события в `raw.events` (append-only).
2. **dbt**:

   * строит `stg.stg_events` как **view** (всегда актуальная очистка/дедуп от raw),
   * обновляет витрины `mart.*` (таблицы) инкрементально/по окну,
   * обновляет `dq.*` (таблицы-счетчики и снапшоты).
3. **Redash** показывает:

   * продуктовые метрики из `mart.*`,
   * качество данных из `dq.*`.


## Что есть в базе (слои)

### `raw`

* `raw.events` — “сырые” события, **append-only**.

### `stg`

* `stg.stg_events` — **view**: чистка, нормализация, дедупликация.

  * Важно: view **не “обновляется” по расписанию** — она **всегда** возвращает актуальный результат запроса к raw на момент чтения (и dbt, и Redash видят “текущую правду”).

### `mart`

* `mart.daily_metrics` — DAU, sessions, orders, revenue, conversion_rate по дням.
* `mart.funnel_daily` — воронка (view → click → cart → purchase) по дням.

### `dq`

* `dq.status_snapshots` — “пульс” качества за короткое окно (например, последние 10 минут) + общий pass/fail.
* `dq.dq_daily` — дневные счетчики проблем (unknown_*, отрицательные цены, purchase без price, и т.д.) + pass/fail по дню.


## Дашборды

### Product Overview (KPI + funnel)

![alt text](<docs/images/Снимок экрана 2026-02-17 в 19.46.14.png>)

Что показывает:

* KPI за последний день (revenue / conversion / orders / sessions / dau)
* тренды
* funnel по шагам
* таблицы с дневными значениями

### Data Quality

![alt text](<docs/images/Снимок экрана 2026-02-17 в 19.46.03.png>)

Что показывает:

* events за последние 10 минут (индикатор “живости” потока)
* динамику ошибок (invalid_event_name, negative_price, purchase_without_price и т.д.)
* карточки-счетчики проблем за день
* итоговый overall pass


## Технологии

* PostgreSQL — хранение данных (raw/stg/mart/dq)
* dbt — трансформации + тесты + DQ-слой
* Docker / docker-compose — воспроизводимая среда
* Redash — BI/дашборды


## Как запустить

### 0 Предусловия

* установлен Docker + docker compose
* свободны порты (в проекте обычно: Postgres 5432, Redash 5001/5000 — зависит от твоего compose)

### 1 Старт

```bash
docker compose up --build
```

После старта:

* генератор начинает писать в `raw.events`
* dbt по расписанию каждые ~5 минут прогоняет модели и тесты
* Redash доступен по адресу из `docker-compose.yml` (например, `http://localhost:5001`)

### 2 Проверка, что данные появились

Подключиться к базе:

```bash
docker exec -it postgres psql -U appuser appdb
```

Быстрые проверки:

```sql
select count(*) from raw.events;
select count(*) from public_mart.mart_daily_metrics;
select count(*) from public_dq.dq_daily;
select * from public_dq.status_snapshots order by check_ts desc limit 5;
```


## Что именно делает dbt в проекте

### Модели

* `stg_events.sql` — view (очистка/дедуп)
* `mart_daily_metrics.sql` — таблица (upsert по окну последних N дней)
* `mart_funnel_daily.sql` — таблица (upsert по окну последних N дней)
* `dq_status_snapshots.sql` — таблица (снапшоты “последние 10 минут”)
* `dq_daily.sql` — таблица (агрегация DQ по дням)

### Тесты (schema.yml)

* `not_null`, `unique`, `accepted_values` и т.п.
* эти тесты — “контракт” на данные и быстрый сигнал при регрессиях


## Про “каждые 5 минут” (важный момент)

* `raw` пополняется каждые 5 минут генератором.
* `stg` — это **view**, поэтому “сама по себе” она **не пересчитывается**, а вычисляется **при чтении**.
* `mart` и `dq` — это **таблицы**, они реально обновляются **по расписанию** (dbt run).
* Дашборды в Redash можно настроить на авто-refresh (например, тоже раз в 5 минут), чтобы видеть “почти стриминг”.


## Data Quality: как задумано (практика из индустрии)

В индустрии обычно делают 2 уровня DQ:

1. **Онлайн-сигналы (near-real-time)**
   `dq.status_snapshots`: короткое окно (10м/30м) → быстро понять “поток жив?” и “ошибки пошли?”.

2. **Исторические метрики качества**
   `dq.dq_daily`: агрегаты по дням → тренды качества, алерты по SLA, отчеты.

Дашборд DQ строится на **таблицах dq** (не на “сырых тестах”), потому что:

* метрики можно хранить и сравнивать во времени,
* можно строить графики/алерты,
* можно дебажить конкретный тип проблемы (счетчики + drill-down запросы).


## Структура репозитория (логика сервисов)

* `db/` — инициализация базы (schema raw + таблицы сырья и индексы)
* `data_generator/` — генератор событий (backfill + live)
* `dbt/` — проект dbt (models, schema.yml, macros при необходимости)
* `redash/` (если есть) — конфиг/infra Redash (метаданные/redis/postgres)


## Что можно улучшить (если развивать как “прод”)

* **Incremental стратегии dbt** (по `event_date`, watermark, partitioning) для ускорения.
* **DQ алерты**: Redash Alerts / webhook / Telegram (pass→fail).
* **SLA/latency метрики**: lag между `raw.ingested_at` и “последним dt в mart”.
* **Drill-down таблицы ошибок** (например, top источники unknown_platform).
* **Orchestration**: вынести расписание из контейнера в Airflow/Prefect/Argo (если нужно ближе к enterprise).


## Где смотреть результат

* **Product метрики**: `public_mart.mart_daily_metrics`, `public_mart.mart_funnel_daily` + Product Overview dashboard.
* **DQ мониторинг**: `public_dq.dq_daily`, `public_dq.status_snapshots` + DQ dashboard.

