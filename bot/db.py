"""
Модуль для работы с базой данных PostgreSQL, содержащей объявления (listings).

- Устанавливает соединение с базой данных по адресу DATABASE_URL.
- Создает таблицу listings, если она не существует, с полями:
    id, name, price, district, img_url, description, last_seen_dt, upload_dt, created_at_dt.
- Функция is_new_listing(listing_id):
    Проверяет, является ли объявление с данным id новым.
    Если объявления нет в базе, добавляет его с текущей меткой времени и возвращает True.
    Если объявление уже есть, обновляет поле last_seen_dt и возвращает False.
- Логирует важные события, такие как создание таблицы и добавление новых объявлений.
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Загружаем переменные из .env (файл должен лежать в корне проекта)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL is not set! Please check your .env file.")
    raise Exception("DATABASE_URL not found in environment variables")

logger.info(f"Using DATABASE_URL: {DATABASE_URL[:30]}...")  # не выводим всю строку из соображений безопасности

try:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        id TEXT PRIMARY KEY,
        name TEXT,
        price TEXT,
        district TEXT,
        img_url TEXT,
        description TEXT,
        last_seen_dt TIMESTAMPTZ,
        upload_dt TIMESTAMPTZ,
        created_at_dt TIMESTAMPTZ
    );
    """)
    logger.info("Connected to PostgreSQL and ensured table exists.")
except Exception as e:
    logger.error(f"Failed to connect to PostgreSQL or create table: {e}")
    raise


def is_new_listing(listing_id):
    from datetime import datetime, timezone

    cursor.execute("SELECT last_seen_dt FROM listings WHERE id = %s", (listing_id,))
    row = cursor.fetchone()
    now = datetime.now(timezone.utc)

    if row is None:
        cursor.execute("INSERT INTO listings (id, last_seen_dt) VALUES (%s, %s)", (listing_id, now))
        logger.info(f"New listing: {listing_id}")
        return True
    else:
        cursor.execute("UPDATE listings SET last_seen_dt = %s WHERE id = %s", (now, listing_id))
        return False
