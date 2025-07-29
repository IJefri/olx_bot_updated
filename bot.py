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
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import telebot
import gc

# Логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Токен і чат
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
bot = telebot.TeleBot(BOT_TOKEN)

# Підключення до PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL")
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

# Локаль для дат
try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
except locale.Error:
    pass

# Selenium options
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

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
        return datetime.now().isoformat()
    parts = date_str.split()
    if len(parts) >= 3:
        day, month_name, year = parts[:3]
        month = MONTHS.get(month_name.lower())
        if month:
            return datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y").isoformat()
    return None

def is_new_listing(listing_id):
    cursor.execute("SELECT last_seen_dt FROM listings WHERE id = %s", (listing_id,))
    row = cursor.fetchone()
    now = datetime.now(timezone.utc)
    if row is None:
        cursor.execute("INSERT INTO listings (id, last_seen_dt) VALUES (%s, %s)", (listing_id, now))
        logger.info(f"New listing added: {listing_id}")
        return True
    else:
        cursor.execute("UPDATE listings SET last_seen_dt = %s WHERE id = %s", (now, listing_id))
        return False

def parse_card(card):
    try:
        listing_id = card.get_attribute('id')
        if not listing_id or not is_new_listing(listing_id):
            return
        title = card.find_element(By.CSS_SELECTOR, "a.css-1tqlkj0 h4").text
        link = card.find_element(By.CSS_SELECTOR, "a.css-1tqlkj0").get_attribute('href')
        price = card.find_element(By.CSS_SELECTOR, '[data-testid="ad-price"]').text
        district = card.find_element(By.CSS_SELECTOR, '[data-testid="location-date"]').text
        img_url = card.find_element(By.CSS_SELECTOR, 'img.css-8wsg1m').get_attribute('src')
        created_at_dt = None
        if ' - ' in district:
            _, date_str = district.split(' - ', 1)
            created_at_dt = parse_ukr_date(date_str)
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
        """, (listing_id, title, price, district, img_url, None, now, now, created_at_dt))
        logger.info(f"Processed listing: {listing_id} - {title}")
    except Exception as e:
        logger.error(f"Error parsing card: {e}")

def send_message(name, district, price, description, link, first_img_url=None):
    msg = f"🏠 **{name}**\n📍 **Район**: {district}\n\n💰 **Ціна**: {price}\n📝 **Опис**: {description[:500]}\n🔗 **Посилання**: {link}"
    try:
        if first_img_url:
            r = requests.get(first_img_url, timeout=10)
            bio = BytesIO(r.content)
            bio.name = 'image.jpg'
            bot.send_photo(CHAT_ID, photo=bio, caption=msg, parse_mode='Markdown')
            bio.close()
        else:
            bot.send_message(CHAT_ID, msg, parse_mode='Markdown')
        logger.info(f"Sent Telegram message for {name}")
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")

def update_missing_descriptions_and_images(driver):
    cursor.execute("""
    SELECT id, name, district, price
    FROM listings
    WHERE (description IS NULL OR description = '')
      AND upload_dt >= NOW() - INTERVAL '30 minutes'
      AND created_at_dt >= NOW() - INTERVAL '2 days'
    """)
    rows = cursor.fetchall()
    logger.info(f"Found {len(rows)} listings missing description/images to update.")
    for listing_id, name, district, price in rows:
        try:
            link = f"https://www.olx.ua/{listing_id}"
            driver.get(link)
            wait = WebDriverWait(driver, 15)
            desc_elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.css-19duwlz")))
            description_html = desc_elem.get_attribute("innerHTML").strip()
            description_text = re.sub(r'<[^>]+>', '', description_html)
            imgs = driver.find_elements(By.CSS_SELECTOR, "div.swiper-slide img")
            img_urls = list({img.get_attribute("src") for img in imgs})
            first_img_url = img_urls[0] if img_urls else None
            cursor.execute("UPDATE listings SET description = %s, img_url = %s WHERE id = %s",
                           (description_text, first_img_url, listing_id))
            send_message(name, district, price, description_text, link, first_img_url)
        except Exception as e:
            logger.error(f"Error updating listing {listing_id}: {e}")
            time.sleep(2)

def get_links(pages=3):
    driver = webdriver.Chrome(options=chrome_options)
    for page in range(1, pages + 1):
        PARAMS["page"] = page
        url = build_url(PARAMS)
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-cy='l-card']")))
            cards = driver.find_elements(By.CSS_SELECTOR, "div[data-cy='l-card']")
            for card in cards:
                parse_card(card)
            logger.info(f"Processed page {page} with {len(cards)} cards.")
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            break
    driver.quit()
    gc.collect()

if __name__ == "__main__":
    get_links(3)
    driver = webdriver.Chrome(options=chrome_options)
    update_missing_descriptions_and_images(driver)
    driver.quit()
    cursor.close()
    conn.close()
