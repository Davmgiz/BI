"""
Генератор событий для raw.events:
каждые 5 минут вставляет 100 строк в PostgreSQL.

Переменные окружения (обязательные):
  DB_HOST, DB_NAME, DB_USER, DB_PASS

Опциональные:
  BATCH_SIZE=100
  INTERVAL_SEC=300
  DIRTY_RATE=0.01
"""

from __future__ import annotations

import os
import time
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import execute_values


# Конфигурация генерации
EVENTS = ("view", "click", "add_to_cart", "purchase", "signup")
PLATFORMS = ("web", "ios", "android")
REGIONS = ("msk", "spb", "kzn", "nsk", "ekb")
SOURCES = ("organic", "ads", "referral")


@dataclass(frozen=True)
class RawEvent:
    event_time: datetime
    user_id: int
    session_id: str
    event_name: str
    item_id: int | None
    price: Decimal | None
    platform: str | None
    region: str | None
    source: str | None


def _money(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def generate_one_raw_event(
    *,
    now: datetime | None = None,
    user_id_range: tuple[int, int] = (1, 200_000),
    item_id_range: tuple[int, int] = (1, 2_000_000),
    dirty_rate: float = 0.0,
    seed: int | None = None,
) -> RawEvent:
    """
    Генерирует 1 кортеж для raw.events (кроме event_id и ingested_at).

    dirty_rate:
      - 0.0 -> "чистые" данные (NULL только там, где это логично: item_id/price)
      - >0  -> добавляет редкие пропуски только в некритичных полях:
              platform/region/source и иногда item_id/price.
              Никогда не портит event_time/user_id/event_name/session_id.
    """
    if seed is not None:
        random.seed(seed)

    if not (0.0 <= dirty_rate <= 1.0):
        raise ValueError("dirty_rate must be between 0.0 and 1.0")

    ts = now or datetime.now(timezone.utc)

    user_id = random.randint(*user_id_range)
    session_id = str(uuid.uuid4())

    platform: str | None = random.choice(PLATFORMS)
    region: str | None = random.choice(REGIONS)
    source: str | None = random.choices(SOURCES, weights=(0.65, 0.25, 0.10), k=1)[0]

    event_name = random.choices(
        EVENTS,
        weights=(0.62, 0.22, 0.10, 0.04, 0.02),
        k=1,
    )[0]

    item_id: int | None = None
    price: Decimal | None = None

    # Базовая логика
    if event_name == "signup":
        item_id = None
        price = None
    elif event_name == "purchase":
        item_id = random.randint(*item_id_range)
        price = _money(random.uniform(199.0, 15_999.0))
    else:
        if random.random() < 0.92:
            item_id = random.randint(*item_id_range)
            if random.random() < (0.35 if event_name == "view" else 0.55):
                price = _money(random.uniform(99.0, 29_999.0))

    # Контролируемая "грязь"
    if dirty_rate > 0:
        if random.random() < dirty_rate:
            platform = None
        if random.random() < dirty_rate:
            region = None
        if random.random() < dirty_rate:
            source = None

        if event_name != "purchase":
            if item_id is not None and random.random() < dirty_rate:
                item_id = None
                price = None
            elif price is not None and random.random() < dirty_rate:
                price = None

    return RawEvent(
        event_time=ts,
        user_id=user_id,
        session_id=session_id,
        event_name=event_name,
        item_id=item_id,
        price=price,
        platform=platform,
        region=region,
        source=source,
    )



# Вставка в БД
INSERT_SQL = """
INSERT INTO raw.events (
  event_time,
  user_id,
  session_id,
  event_name,
  item_id,
  price,
  platform,
  region,
  source
)
VALUES %s
"""


def generate_rows(n: int, *, dirty_rate: float = 0.0) -> list[tuple]:
    rows: list[tuple] = []
    for _ in range(n):
        e = generate_one_raw_event(dirty_rate=dirty_rate)
        rows.append(
            (
                e.event_time,
                e.user_id,
                e.session_id,
                e.event_name,
                e.item_id,
                e.price,
                e.platform,
                e.region,
                e.source,
            )
        )
    return rows


def main() -> None:
    batch_size = int(os.getenv("BATCH_SIZE", "100"))
    interval_sec = int(os.getenv("INTERVAL_SEC", str(5 * 60)))  # 5 минут
    dirty_rate = float(os.getenv("DIRTY_RATE", "0.01"))

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        port=5432,
    )
    conn.autocommit = False

    print(
        f"Started generator: every {interval_sec}s insert {batch_size} rows "
        f"(dirty_rate={dirty_rate})"
    )

    try:
        with conn.cursor() as cur:
            while True:
                rows = generate_rows(batch_size, dirty_rate=dirty_rate)
                execute_values(cur, INSERT_SQL, rows, page_size=batch_size)
                conn.commit()
                print(
                    f"Inserted {batch_size} events at "
                    f"{time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                time.sleep(interval_sec)

    except KeyboardInterrupt:
        print("Stopped by user.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
