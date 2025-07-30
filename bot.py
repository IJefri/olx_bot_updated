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
from PIL import Image
from bs4 import BeautifulSoup
import telebot

# Логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
bot = telebot.TeleBot(BOT_TOKEN)

# PostgreSQL
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

try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
except locale.Error:
    logger.warning("Locale en_US.UTF-8 not supported.")

def send_message(name, district, price, description, link, collage_img=None):
    message = (
        f"\U0001F3E0 **{name}**\n"
        f"\U0001F4CD **Район**: {district}\n\n"
        f"\U0001F4B0 **Ціна**: {price}\n"
        f"\U0001F4DD **Опис**: {description[:500]}\n"
        f"\U0001F517 **Посилання**: {link}"
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

def download_images(img_urls, timeout=10, max_images=10):
    images = []
    for url in img_urls[:max_images]:
        try:
            logger.info(f"Downloading image: {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert('RGB')
            images.append(img)
            logger.info(f"Successfully downloaded image: {url}")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout while downloading: {url}")
        except Exception as e:
            logger.warning(f"Error downloading {url}: {e}")
    logger.info(f"Downloaded {len(images)} images out of {len(img_urls)} URLs")
    return images

def create_collage(images, cols=3, thumb_width=300, margin=5):
    if not images:
        logger.warning("No images to create collage")
        return None
    rows = (len(images) + cols - 1) // cols
    thumb_height = int(thumb_width * 4 / 3)
    collage_width = cols * thumb_width + (cols + 1) * margin
    collage_height = rows * thumb_height + (rows + 1) * margin
    collage_img = Image.new('RGB', (collage_width, collage_height), (0, 0, 0))
    for idx, img in enumerate(images):
        img_thumb = img.copy()
        img_thumb.thumbnail((thumb_width, thumb_height))
        x = margin + (idx % cols) * (thumb_width + margin)
        y = margin + (idx // cols) * (thumb_height + margin)
        collage_img.paste(img_thumb, (x, y))
    logger.info(f"Created collage image with size: {collage_img.size}")
    return collage_img

def update_missing_descriptions_and_images():
    cursor.execute("""
    SELECT id, name, district, price FROM listings 
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

                    desc_container = soup.find('div', attrs={'data-testid': 'ad_description'})
                    if desc_container:
                        desc_div = desc_container.find('div', class_='css-19duwlz')
                        if desc_div:
                            for br in desc_div.find_all('br'):
                                br.replace_with('\n')
                            description_text = desc_div.get_text(strip=True)
                        else:
                            description_text = "Опис не знайдено (css-19duwlz відсутній)"
                    else:
                        description_text = "Опис не знайдено (data-testid='ad_description' відсутній)"

                    img_elements = soup.select('div.swiper-zoom-container img')
                    img_urls = [img.get('src') for img in img_elements if img.get('src')]
                    img_urls = list(dict.fromkeys(img_urls))
                    logger.info(f"Found {len(img_urls)} image URLs for listing {listing_id}")

                    images = download_images(img_urls)
                    collage_img = create_collage(images) if images else None

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
        except Exception as e:
            logger.error(f"General error processing ID {listing_id}: {e}")

if __name__ == "__main__":
    try:
        while True:
            logger.info("Starting scraping process")
            get_links(25)
            update_missing_descriptions_and_images()
            #logger.info("Finished scraping cycle. Sleeping for 3 minutes.")
            #time.sleep(180)
    finally:
        cursor.close()
        conn.close()
        logger.info("DB connection closed.")
