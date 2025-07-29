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
from PIL import Image

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import telebot

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Получаем токен и чат из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
bot = telebot.TeleBot(BOT_TOKEN)

# Подключение к PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL")
try:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cursor = conn.cursor()
    logger.info("Connected to PostgreSQL database.")
except Exception as e:
    logger.error(f"Error connecting to database: {e}")
    raise e

# Создание таблицы, если не существует
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
logger.info("Ensured listings table exists.")

locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')

def is_new_listing(listing_id):
    try:
        cursor.execute("SELECT last_seen_dt FROM listings WHERE id = %s", (listing_id,))
        row = cursor.fetchone()
        now = datetime.now(timezone.utc)
        if row is None:
            cursor.execute("INSERT INTO listings (id, last_seen_dt) VALUES (%s, %s)", (listing_id, now))
            logger.info(f"New listing added: {listing_id}")
            return True
        else:
            cursor.execute("UPDATE listings SET last_seen_dt = %s WHERE id = %s", (now, listing_id))
            logger.info(f"Listing updated last_seen_dt: {listing_id}")
            return False
    except Exception as e:
        logger.error(f"Database error in is_new_listing for {listing_id}: {e}")
        return False

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--single-process")
chrome_options.add_argument("--remote-debugging-port=9222")

driver = webdriver.Chrome(options=chrome_options)
logger.info("Initialized headless Chrome driver.")

BASE_URL = "https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/kiev/"
PARAMS = {
    "currency": "UAH",
    "search[order]": "created_at:desc",
    "search[filter_float_price:from]": "12000",
    "search[filter_float_price:to]": "20000",
    "search[filter_float_total_area:from]": "30",
    "page": 1
}

def build_url(params):
    url = BASE_URL + "?" + urlencode(params)
    logger.debug(f"Built URL: {url}")
    return url

def parse_ukr_date(date_str):
    MONTHS = {
        "січня": "01", "лютого": "02", "березня": "03", "квітня": "04", "травня": "05",
        "червня": "06", "липня": "07", "серпня": "08", "вересня": "09", "жовтня": "10",
        "листопада": "11", "грудня": "12",
        "января": "01", "февраля": "02", "марта": "03", "апреля": "04", "мая": "05",
        "июня": "06", "июля": "07", "августа": "08", "сентября": "09", "октября": "10",
        "ноября": "11", "декабря": "12",
    }
    date_str = date_str.replace("р.", "").strip()

    if date_str.startswith("Сьогодні") or date_str.startswith("Сегодня"):
        time_match = re.search(r'о\s*(\d{1,2}:\d{2})', date_str)
        time_part = time_match.group(1) if time_match else "00:00"
        result = datetime.now().strftime(f"%Y-%m-%d {time_part}:00")
        logger.debug(f"Parsed date (today): {result}")
        return result

    parts = date_str.split()
    if len(parts) >= 3:
        day = parts[0]
        month = MONTHS.get(parts[1].lower())
        year = parts[2]
        if month:
            result = datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y").isoformat()
            logger.debug(f"Parsed date: {result}")
            return result
    logger.warning(f"Could not parse date: {date_str}")
    return None

def parse_card(card):
    try:
        listing_id = card.get_attribute('id')
        if not listing_id:
            logger.warning("Card without id skipped")
            return
        if not is_new_listing(listing_id):
            return

        title = card.find_element(By.CSS_SELECTOR, "a.css-1tqlkj0 h4").text
        link = card.find_element(By.CSS_SELECTOR, "a.css-1tqlkj0").get_attribute('href')
        price = card.find_element(By.CSS_SELECTOR, '[data-testid="ad-price"]').text
        district = card.find_element(By.CSS_SELECTOR, '[data-testid="location-date"]').text
        img_url = card.find_element(By.CSS_SELECTOR, 'img.css-8wsg1m').get_attribute('src')

        if ' - ' in district:
            _, date_str = district.split(' - ', 1)
            created_at_dt = parse_ukr_date(date_str)
        else:
            created_at_dt = None
        
        now = datetime.now(timezone.utc)

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
            None,
            now,
            now,
            created_at_dt
        ))
        logger.info(f"Processed listing: {listing_id} - {title}")
    except NoSuchElementException:
        logger.warning("NoSuchElementException in parse_card")
    except Exception as e:
        logger.error(f"Error parsing card: {e}")

def get_links(pages=None):
    page_num = 1
    logger.info("Starting to scrape listing pages.")
    while True:
        if pages is not None and page_num > pages:
            logger.info("Reached max pages limit.")
            break
        PARAMS["page"] = page_num
        url = build_url(PARAMS)
        try:
            driver.get(url)
            time.sleep(3)
            cards = driver.find_elements(By.CSS_SELECTOR, "div[data-cy='l-card']")
            if not cards:
                logger.info(f"No cards found on page {page_num}, stopping.")
                break
            for card in cards:
                parse_card(card)
            logger.info(f"Processed page {page_num} with {len(cards)} cards.")
            page_num += 1
        except Exception as e:
            logger.error(f"Error fetching/parsing page {page_num}: {e}")
            break

def send_message(name, district, price, description, link, collage_img=None):
    message = (
        f"🏠 **{name}**\n"
        f"📍 **Район**: {district}\n\n"
        f"💰 **Ціна**: {price}\n"
        f"📝 **Опис**: {description[:500]}\n"
        f"🔗 **Посилання**: {link}"
    )
    try:
        if collage_img:
            bio = BytesIO()
            bio.name = 'collage.jpg'
            collage_img.save(bio, 'JPEG')
            bio.seek(0)
            bot.send_photo(CHAT_ID, photo=bio, caption=message, parse_mode='Markdown')
        else:
            bot.send_message(CHAT_ID, message, parse_mode='Markdown')
        logger.info(f"Sent Telegram message for listing: {name}")
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")

# (Остальной код функций get_all_slider_images, download_images, create_collage, update_missing_descriptions_and_images
# также можно обернуть в try/except и логировать по необходимости)

# В самом конце:
if __name__ == "__main__":
    try:
        get_links()
        update_missing_descriptions_and_images()
        logger.info("Script finished successfully.")
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}")
    finally:
        driver.quit()
        cursor.close()
        conn.close()
        logger.info("Cleaned up resources.")
