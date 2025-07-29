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
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import telebot

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Больше логов — DEBUG уровень
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
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

# Установка локали для парсинга дат
try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
    logger.debug("Locale set to en_US.UTF-8")
except locale.Error:
    logger.warning("Locale en_US.UTF-8 not supported, skipping locale setting.")

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
            logger.debug(f"Listing updated last_seen_dt: {listing_id}")
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
            try:
                result = datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y").isoformat()
                logger.debug(f"Parsed date: {result}")
                return result
            except Exception as e:
                logger.error(f"Error parsing date {date_str}: {e}")
                return None
    logger.warning(f"Could not parse date: {date_str}")
    return None

def parse_card(card, cursor):
    try:
        listing_id = card.get_attribute('id')
        if not listing_id:
            logger.warning("Card without id skipped")
            return
        if not is_new_listing(listing_id):
            logger.debug(f"Listing {listing_id} already seen, skipping insert.")
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
    except NoSuchElementException as e:
        logger.warning(f"NoSuchElementException in parse_card: {e}")
    except Exception as e:
        logger.error(f"Error parsing card: {e}")

def send_message(name, district, price, description, link, first_img_url=None):
    message = (
        f"🏠 *{name}*\n"
        f"📍 *Район*: {district}\n\n"
        f"💰 *Ціна*: {price}\n"
        f"📝 *Опис*: {description[:500]}\n"
        f"🔗 *Посилання*: {link}"
    )
    try:
        if first_img_url:
            logger.debug(f"Fetching image from {first_img_url}")
            response = requests.get(first_img_url, timeout=10)
            response.raise_for_status()
            bio = BytesIO(response.content)
            bio.name = 'image.jpg'
            bio.seek(0)
            bot.send_photo(CHAT_ID, photo=bio, caption=message, parse_mode='Markdown')
            bio.close()
            response.close()
        else:
            bot.send_message(CHAT_ID, message, parse_mode='Markdown')
        logger.info(f"Sent Telegram message for listing: {name}")
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")

def update_missing_descriptions_and_images(cursor, conn, driver):
    logger.info("Starting update of missing descriptions and images.")
    cursor.execute("""
    SELECT id, name, district, price
    FROM listings 
    WHERE (description IS NULL OR description = '') 
      AND upload_dt >= NOW() - INTERVAL '30 minutes'
      AND created_at_dt >= NOW() - INTERVAL '2 days'
      AND (
        district LIKE '%Оболонський%' OR
        district LIKE '%Подільський%' OR
        district LIKE '%Шевченківський%' OR
        district LIKE '%Печерський%' OR
        district LIKE '%Солом''янський%' OR
        district LIKE '%Голосіївський%' OR
        district LIKE '%Оболонский%' OR
        district LIKE '%Подольский%' OR
        district LIKE '%Шевченковский%' OR
        district LIKE '%Печерский%' OR
        district LIKE '%Соломенский%' OR
        district LIKE '%Голосеевский%'
      )
    """)
    rows = cursor.fetchall()
    logger.info(f"Found {len(rows)} listings missing description/images to update.")

    for idx, (listing_id, name, district, price) in enumerate(rows):
        try:
            link = f"https://www.olx.ua/{listing_id}"
            logger.debug(f"Updating listing {listing_id}: opening {link}")
            driver.get(link)

            # Проверяем, доступно ли объявление
            try:
                inactive_div = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="ad-inactive-msg"]')
                if "Це оголошення більше не доступне" in inactive_div.text:
                    logger.info(f"Listing {listing_id} is inactive, marking as NOT AVAILABLE.")
                    cursor.execute(
                        "UPDATE listings SET description = 'NOT AVAILABLE', img_url = NULL WHERE id = %s",
                        (listing_id,)
                    )
                    conn.commit()
                    continue
            except NoSuchElementException:
                pass

            wait = WebDriverWait(driver, 20)
            desc_elem = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.css-19duwlz"))
            )
            description_html = desc_elem.get_attribute("innerHTML").strip()
            description_text = re.sub(r'<[^>]+>', '', description_html)
            description_text = ' '.join(description_text.split())

            img_elements = driver.find_elements(By.CSS_SELECTOR, "div.swiper-slide img")
            img_urls = list({img.get_attribute("src") for img in img_elements})

            first_img_url = img_urls[0] if img_urls else None

            cursor.execute(
                "UPDATE listings SET description = %s, img_url = %s WHERE id = %s",
                (description_text, first_img_url, listing_id)
            )
            conn.commit()
            logger.info(f"Updated listing {listing_id} with description and image.")

            send_message(name, district, price, description_text, link, first_img_url)

            # Перезапуск драйвера после каждых 10 обработанных объявлений (для контроля памяти)
            if idx > 0 and idx % 10 == 0:
                logger.info("Restarting webdriver to avoid memory leaks (after 10 updates).")
                driver.quit()
                time.sleep(2)
                global driver
                driver = webdriver.Chrome(options=chrome_options)

        except TimeoutException:
            logger.error(f"Timeout loading description for listing {listing_id}")
        except Exception as e:
            logger.error(f"Error updating listing {listing_id}: {e}")
            time.sleep(3)

def get_links(pages=None):
    page_num = 1
    logger.info("Starting to scrape listing pages.")
    global driver
    while True:
        if pages is not None and page_num > pages:
            logger.info("Reached max pages limit.")
            break
        PARAMS["page"] = page_num
        url = build_url(PARAMS)
        try:
            logger.debug(f"Loading page {page_num}: {url}")
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-cy='l-card']")))

            cards = driver.find_elements(By.CSS_SELECTOR, "div[data-cy='l-card']")
            if not cards:
                logger.info(f"No cards found on page {page_num}, stopping.")
                break

            logger.debug(f"Found {len(cards)} cards on page {page_num}")

            for card in cards:
                parse_card(card, cursor)

            logger.info(f"Processed page {page_num} with {len(cards)} cards.")

            # Перезапуск драйвера каждые 5 страниц для контроля памяти
            if page_num % 5 == 0:
                logger.info("Restarting webdriver to avoid memory leaks (after 5 pages).")
                driver.quit()
                time.sleep(2)
                driver = webdriver.Chrome(options=chrome_options)

            page_num += 1
        except Exception as e:
            logger.error(f"Error fetching/parsing page {page_num}: {e}")
            break

if __name__ == "__main__":
    logger.info("Starting main script execution.")
    driver = webdriver.Chrome(options=chrome_options)
    logger.info("Initialized headless Chrome driver.")
    try:
        get_links(3)  # Можно менять количество страниц
        # Чтобы обновить описания и отправить в телеграм, раскомментируй ниже:
        update_missing_descriptions_and_images(cursor, conn, driver)
        logger.info("Script finished successfully.")
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}")
    finally:
        logger.info("Cleaning up resources.")
        driver.quit()
        cursor.close()
        conn.close()
        logger.info("Cleaned up resources and exiting.")
