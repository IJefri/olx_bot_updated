import os
import time
import locale
import psycopg2
from datetime import datetime, timezone
from urllib.parse import urlencode
import logging
import re
import requests
from io import BytesIO
import psutil
import gc

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import telebot

# Логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def log_memory(stage):
    mem = psutil.Process().memory_info().rss / 1024 / 1024
    logger.info(f"[MEMORY] {stage}: {mem:.1f} MB")

# Токен та чат
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
bot = telebot.TeleBot(BOT_TOKEN)

# База даних
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor()
logger.info("Connected to PostgreSQL.")

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

# Локаль для дат
try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
except locale.Error:
    logger.warning("Locale en_US.UTF-8 not supported.")

def is_new_listing(listing_id):
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

def build_url(params):
    BASE_URL = "https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/kiev/"
    return BASE_URL + "?" + urlencode(params)

def parse_ukr_date(date_str):
    MONTHS = {
        "січня": "01", "лютого": "02", "березня": "03", "квітня": "04", "травня": "05", "червня": "06",
        "липня": "07", "серпня": "08", "вересня": "09", "жовтня": "10", "листопада": "11", "грудня": "12",
        "января": "01", "февраля": "02", "марта": "03", "апреля": "04", "мая": "05", "июня": "06",
        "июля": "07", "августа": "08", "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12"
    }

    logger.info(f"Вхідний рядок дати: {date_str}")
    date_str = date_str.replace("р.", "").strip()
    logger.info(f"Рядок після очищення: {date_str}")

    # Сьогодні / Сегодня
    if date_str.startswith("Сьогодні") or date_str.startswith("Сегодня"):
        logger.info("Рядок починається з 'Сьогодні' або 'Сегодня'")
        time_match = re.search(r'(\d{1,2}:\d{2})', date_str)
        if time_match:
            time_part = time_match.group(1)
            logger.info(f"Знайдений час: {time_part}")
        else:
            time_part = "00:00"
            logger.info("Час не знайдений, використовую за замовчуванням: 00:00")
    
        dt_local = datetime.strptime(f"{datetime.now().strftime('%Y-%m-%d')} {time_part}", "%Y-%m-%d %H:%M")
        dt_utc = dt_local.astimezone(timezone.utc)
        logger.info(f"Повертаю datetime у UTC: {dt_utc}")
        return dt_utc

    # Формат "30 липня 2024"
    parts = date_str.split()
    logger.info(f"Розбиття рядка на частини: {parts}")
    if len(parts) >= 3:
        day = parts[0]
        month_name = parts[1].lower()
        year = parts[2]
        month = MONTHS.get(month_name)
        logger.info(f"Визначено день: {day}, місяць: {month_name} -> {month}, рік: {year}")
        if month:
            dt = datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
            dt_utc = dt.replace(tzinfo=timezone.utc)
            logger.info(f"Повертаю datetime у UTC: {dt_utc}")
            return dt_utc

    logger.warning("Не вдалося розпізнати дату, повертаю None")
    return None

def parse_card(card):
    try:
        listing_id = card.get("id")  # берем id из div[data-cy='l-card']
        if not listing_id or not is_new_listing(listing_id):
            return

        title_tag = card.select_one("a.css-1tqlkj0 h4")
        title = title_tag.get_text(strip=True) if title_tag else ""

        price_tag = card.select_one('[data-testid="ad-price"]')
        price = price_tag.get_text(strip=True) if price_tag else ""

        district_tag = card.select_one('[data-testid="location-date"]')
        district = district_tag.get_text(strip=True) if district_tag else ""

        img_tag = card.select_one("img.css-8wsg1m")
        img_url = img_tag.get("src") if img_tag else None

        now = datetime.now(timezone.utc)

        # Парсим дату из district, если есть, например "Київ, Дарницький - Сьогодні о 12:08"
        created_at_dt = now
        if district and " - " in district:
            _, date_part = district.split(" - ", 1)
            parsed_date = parse_ukr_date(date_part)
            if parsed_date:
                created_at_dt = parsed_date

        cursor.execute("""
            INSERT INTO listings (id, name, price, district, img_url, description, last_seen_dt, upload_dt, created_at_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                last_seen_dt = EXCLUDED.last_seen_dt,
                price = EXCLUDED.price,
                name = EXCLUDED.name,
                district = EXCLUDED.district,
                img_url = EXCLUDED.img_url,
                upload_dt = EXCLUDED.upload_dt,
                created_at_dt = EXCLUDED.created_at_dt
        """, (
            listing_id,
            title,
            price,
            district,
            img_url,
            None,  # description
            now,
            now,
            created_at_dt
        ))
        logger.info(f"Processed: {listing_id} - {title}")
    except Exception as e:
        logger.error(f"Error parsing card: {e}")

def get_links(pages):
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    })

    for page_num in range(1, pages + 1):
        logger.info(f"Fetching page {page_num}")
        PARAMS = {
            "currency": "UAH",
            "search[order]": "created_at:desc",
            "search[filter_float_price:from]": "12000",
            "search[filter_float_price:to]": "20000",
            "search[filter_float_total_area:from]": "30",
            "page": page_num
        }
        url = build_url(PARAMS)
        logger.info(f"Loading URL: {url}")

        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Находи все карточки объявлений
            cards = soup.select("div[data-cy='l-card']")
            logger.info(f"Found {len(cards)} cards on page {page_num}")

            for card in cards:
                parse_card(card)

            #log_memory(f"After page {page_num}")
            gc.collect()
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error on page {page_num}: {e}")
            break

if __name__ == "__main__":
    try:
        logger.info("Starting scraping process")
        get_links(25)
        logger.info("Finished scraping.")
    finally:
        cursor.close()
        conn.close()
        logger.info("DB connection closed.")
