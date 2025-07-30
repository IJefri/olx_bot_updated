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
import requests

from bs4 import BeautifulSoup
from PIL import Image

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
# BOT_TOKEN = os.getenv("BOT_TOKEN")
# CHAT_ID = int(os.getenv("CHAT_ID"))

# # База даних
# DATABASE_URL = os.getenv("DATABASE_URL")

BOT_TOKEN = "7176877320:AAGLMiDHUVe3J6fpqyzFrxBKLEYyJgLndkE"
CHAT_ID = 430697715  # Your Telegram ID
DATABASE_URL = 'postgresql://postgres.elotsajrevfqherenjtj:%23Hr%40i7uS8E.%21cTn@aws-0-eu-central-1.pooler.supabase.com:6543/postgres'

bot = telebot.TeleBot(BOT_TOKEN)




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
            logger.info(f"Sending photo with collage for listing '{name}'")
            bio = BytesIO()
            bio.name = 'collage.jpg'
            collage_img.save(bio, 'JPEG')
            bio.seek(0)
            bot.send_photo(CHAT_ID, photo=bio, caption=message, parse_mode='Markdown')
            bio.close()
        else:
            logger.info(f"Sending message without photo for listing '{name}'")
            bot.send_message(CHAT_ID, message, parse_mode='Markdown')
        logger.info(f"Sent Telegram message for: {name}")
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")

def resize_image_url(url, new_size="600x300"):
    pattern = r"(s=\d+x\d+)$"
    if re.search(pattern, url):
        return re.sub(pattern, f"s={new_size}", url)
    return url

def get_all_slider_images(soup):
    img_elements = soup.select('div.swiper-zoom-container img')
    img_urls = []
    for img in img_elements:
        src = img.get('src')
        if src:
            src_resized = resize_image_url(src)
            if src_resized not in img_urls:
                img_urls.append(src_resized)
    logger.info(f"Found {len(img_urls)} image URLs in slider")
    return img_urls

def parse_description(soup):
    desc_container = soup.select_one('div[data-testid="ad_description"]')
    if desc_container:
        desc_elem = desc_container.select_one('div.css-19duwlz')
        if desc_elem:
            return desc_elem.get_text(separator=' ', strip=True)
    return "Опис не знайдено"

def download_images(img_urls, timeout=10, max_images=7, thumb_size=(300, 400)):
    images = []
    for url in img_urls[:max_images]:
        try:
            logger.info(f"Downloading image: {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert('RGB')
            img.thumbnail(thumb_size)  # уменьшение размера сразу
            images.append(img)
            logger.info(f"Successfully downloaded and resized image: {url}")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout while downloading: {url}")
        except Exception as e:
            logger.warning(f"Error downloading {url}: {e}")
    logger.info(f"Downloaded {len(images)} images out of {len(img_urls)} URLs")
    return images


def create_collage(images, cols=3, margin=5):
    if not images:
        logger.warning("No images to create collage")
        return None

    thumb_width, thumb_height = images[0].size
    rows = (len(images) + cols - 1) // cols

    collage_width = cols * thumb_width + (cols + 1) * margin
    collage_height = rows * thumb_height + (rows + 1) * margin
    collage_img = Image.new('RGB', (collage_width, collage_height), (0, 0, 0))

    for idx, img in enumerate(images):
        x = margin + (idx % cols) * (thumb_width + margin)
        y = margin + (idx // cols) * (thumb_height + margin)
        collage_img.paste(img, (x, y))

    # Очистка памяти
    del images
    gc.collect()

    logger.info(f"Created collage image with size: {collage_img.size}")
    return collage_img

def update_missing_descriptions_and_images():
    cursor.execute("""
    SELECT id, name, district, price
    FROM listings 
    WHERE (description IS NULL OR description = '')
      AND created_at_dt >= NOW() - INTERVAL '1 days'
      AND (
        district ILIKE '%Оболонський%' OR district ILIKE '%Оболонский%' OR
        district ILIKE '%Подільський%' OR district ILIKE '%Подольский%' OR
        district ILIKE '%Шевченківський%' OR district ILIKE '%Шевченковский%' OR
        district ILIKE '%Печерський%' OR district ILIKE '%Печерский%' OR
        district ILIKE '%Солом''янський%' OR district ILIKE '%Соломенский%' OR
        district ILIKE '%Голосіївський%' OR district ILIKE '%Голосеевский%'
      )
    ORDER BY upload_dt
    """)
    rows = cursor.fetchall()
    logger.info(f"Found {len(rows)} listings missing description/images")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    for listing_id, name, district, price in rows:
        try:
            logger.info(f"Processing listing ID {listing_id}")
            url = f"https://www.olx.ua/{listing_id}"

            for attempt in range(2):
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")

                    inactive_div = soup.select_one('div[data-testid="ad-inactive-msg"]')
                    if inactive_div and "Це оголошення більше не доступне" in inactive_div.text:
                        logger.warning(f"Listing ID {listing_id} no longer available")
                        cursor.execute(
                            "UPDATE listings SET description = 'NOT AVAILABLE', img_url = NULL WHERE id = %s",
                            (listing_id,)
                        )
                        conn.commit()
                        break

                    description_text = parse_description(soup)
                    img_urls = get_all_slider_images(soup)
                    logger.info(f"Found {len(img_urls)} images for listing {listing_id}")

                    images = download_images(img_urls, max_images=3)
                    collage_img = create_collage(images) if images else None

                    del soup
                    del images
                    gc.collect()

                    first_img_url = img_urls[0] if img_urls else None
                    cursor.execute(
                        "UPDATE listings SET description = %s, img_url = %s WHERE id = %s",
                        (description_text, first_img_url, listing_id)
                    )
                    conn.commit()

                    logger.info(f"Updated description and images for ID {listing_id}")

                    send_message(name, district, price, description_text, url, collage_img)
                    break
                except Exception as e:
                    logger.error(f"Attempt {attempt+1} failed for ID {listing_id}: {e}")
                    if attempt == 0:
                        time.sleep(3)
                    else:
                        logger.error(f"Failed to update listing ID {listing_id} after 2 attempts")
        except Exception as e:
            logger.error(f"General error processing ID {listing_id}: {e}")

        gc.collect()

          

if __name__ == "__main__":
    try:
        logger.info("STARTING | Scrapping")
        get_links(1)
        logger.info("FINISHED | Scraping.")
        logger.info("STARTING | UPLOAD DESCRIPTION - IMG - TELEGRAM")
        update_missing_descriptions_and_images()
        logger.info("FINISHED | UPLOAD DESCRIPTION - IMG - TELEGRAM")
    finally:
        cursor.close()
        conn.close()
        logger.info("DB connection closed.")
