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

# Chrome options
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless=new")  # новий headless режим в Chrome 112+
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1024,768")  # зменшене вікно для економії пам'яті

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
    return BASE_URL + "?" + urlencode(params)

def parse_card(card):
    try:
        listing_id = card.get_attribute('id')
        if not listing_id or not is_new_listing(listing_id):
            return
        title = card.find_element(By.CSS_SELECTOR, "a.css-1tqlkj0 h4").text
        price = card.find_element(By.CSS_SELECTOR, '[data-testid="ad-price"]').text
        district = card.find_element(By.CSS_SELECTOR, '[data-testid="location-date"]').text
        img_url = card.find_element(By.CSS_SELECTOR, 'img.css-8wsg1m').get_attribute('src')
        now = datetime.now(timezone.utc)
        cursor.execute("""
            INSERT INTO listings (id, name, price, district, img_url, last_seen_dt, upload_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                last_seen_dt = EXCLUDED.last_seen_dt,
                price = EXCLUDED.price,
                name = EXCLUDED.name,
                district = EXCLUDED.district,
                img_url = EXCLUDED.img_url,
                upload_dt = EXCLUDED.upload_dt
        """, (listing_id, title, price, district, img_url, now, now))
        logger.info(f"Processed: {listing_id} - {title}")
    except Exception as e:
        logger.error(f"Error parsing card: {e}")

def get_links(pages):
    driver = webdriver.Chrome(options=chrome_options)
    log_memory("Driver started")
    try:
        for page_num in range(1, pages + 1):
            PARAMS["page"] = page_num
            url = build_url(PARAMS)
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            cards = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-cy='l-card']")))
            for card in cards:
                parse_card(card)
                del card
            logger.info(f"Page {page_num} processed ({len(cards)} cards).")
            
            # Очищуємо кеш і куки браузера для зменшення пам’яті
            try:
                driver.execute_cdp_cmd("Network.clearBrowserCache", {})
                driver.execute_cdp_cmd("Network.clearBrowserCookies", {})
            except Exception as e:
                logger.warning(f"Failed to clear cache/cookies: {e}")

            gc.collect()
            log_memory(f"After page {page_num}")
            time.sleep(1)  # Невелика пауза для стабільності
    finally:
        driver.quit()
        log_memory("Driver quit")

if __name__ == "__main__":
    try:
        get_links(3)
        logger.info("Finished scraping.")
    finally:
        cursor.close()
        conn.close()
        logger.info("DB connection closed.")
