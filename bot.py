import os
import time
import locale
import sqlite3
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

# Получаем токен и чат из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
bot = telebot.TeleBot(BOT_TOKEN)

locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')

# --- Настройка базы данных на persistent диске Render ---
DB_PATH = "/data/listings.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    name TEXT,
    price TEXT,
    district TEXT,
    img_url TEXT,
    description TEXT,
    last_seen_dt DATETIME,
    upload_dt DATETIME,
    created_at_dt DATETIME
)
""")
conn.commit()

def is_new_listing(listing_id):
    cursor.execute("SELECT last_seen_dt FROM listings WHERE id = ?", (listing_id,))
    row = cursor.fetchone()
    now = datetime.now(timezone.utc).isoformat()
    if row is None:
        cursor.execute("INSERT INTO listings (id, last_seen_dt) VALUES (?, ?)", (listing_id, now))
        conn.commit()
        return True
    else:
        cursor.execute("UPDATE listings SET last_seen_dt = ? WHERE id = ?", (now, listing_id))
        conn.commit()
        return False

# --- Настройка Selenium с headless Chrome ---
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--single-process")
chrome_options.add_argument("--remote-debugging-port=9222")

driver = webdriver.Chrome(options=chrome_options)

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
        time_match = re.search(r'о\s*(\d{1,2}:\d{2})', date_str)
        time_part = time_match.group(1) if time_match else "00:00"
        return datetime.now().strftime(f"%Y-%m-%d {time_part}:00")

    parts = date_str.split()
    if len(parts) >= 3:
        day = parts[0]
        month = MONTHS.get(parts[1].lower())
        year = parts[2]
        if month:
            return datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y").isoformat()
    return None

def parse_card(card):
    try:
        listing_id = card.get_attribute('id')
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
        
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute("""
            INSERT INTO listings (id, name, price, district, img_url, description, last_seen_dt, upload_dt, created_at_dt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                last_seen_dt=excluded.last_seen_dt,
                price=excluded.price,
                name=excluded.name,
                district=excluded.district,
                img_url=excluded.img_url,
                upload_dt=excluded.upload_dt,
                created_at_dt=excluded.created_at_dt
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
        conn.commit()
    except NoSuchElementException:
        pass

def get_links(pages=None):
    page_num = 1
    while True:
        if pages is not None and page_num > pages:
            break
        PARAMS["page"] = page_num
        url = build_url(PARAMS)
        driver.get(url)
        time.sleep(3)
        cards = driver.find_elements(By.CSS_SELECTOR, "div[data-cy='l-card']")
        if not cards:
            break
        for card in cards:
            parse_card(card)
        page_num += 1

def send_message(name, district, price, description, link, collage_img=None):
    message = (
        f"🏠 **{name}**\n"
        f"📍 **Район**: {district}\n\n"
        f"💰 **Ціна**: {price}\n"
        f"📝 **Опис**: {description[:500]}\n"
        f"🔗 **Посилання**: {link}"
    )
    if collage_img:
        bio = BytesIO()
        bio.name = 'collage.jpg'
        collage_img.save(bio, 'JPEG')
        bio.seek(0)
        bot.send_photo(CHAT_ID, photo=bio, caption=message, parse_mode='Markdown')
    else:
        bot.send_message(CHAT_ID, message, parse_mode='Markdown')

def get_all_slider_images(driver):
    img_elements = driver.find_elements(By.CSS_SELECTOR, "div.swiper-slide img")
    img_urls = [img.get_attribute("src") for img in img_elements]
    img_urls = list(dict.fromkeys(img_urls))
    return img_urls

def download_images(img_urls, timeout=10, max_images=10):
    images = []
    for url in img_urls[:max_images]:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert('RGB')
            images.append(img)
        except:
            continue
    return images

def create_collage(images, cols=3, thumb_width=300, margin=5):
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
    return collage_img

def update_missing_descriptions_and_images():
    cursor.execute("""
    SELECT id, name, district, price
    FROM listings 
    WHERE (description IS NULL OR description = '') 
      AND upload_dt >= datetime('now', '-30 minutes')
      AND created_at_dt >= datetime('now', '-2 days')
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

    for listing_id, name, district, price in rows:
        try:
            link = f"https://www.olx.ua/{listing_id}"
            for attempt in range(2):
                try:
                    driver.get(link)
                    try:
                        inactive_div = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="ad-inactive-msg"]')
                        if "Це оголошення більше не доступне" in inactive_div.text:
                            cursor.execute(
                                "UPDATE listings SET description = 'NOT AVAILABLE', img_url = NULL WHERE id = ?",
                                (listing_id,)
                            )
                            conn.commit()
                            break
                    except:
                        pass
                    wait = WebDriverWait(driver, 20)
                    desc_elem = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.css-19duwlz"))
                    )
                    description_html = desc_elem.get_attribute("innerHTML").strip()
                    description_text = re.sub(r'<[^>]+>', '', description_html)
                    description_text = ' '.join(description_text.split())
                    img_urls = get_all_slider_images(driver)
                    images = download_images(img_urls)
                    collage_img = create_collage(images) if images else None
                    first_img_url = img_urls[0] if img_urls else None
                    cursor.execute(
                        "UPDATE listings SET description = ?, img_url = ? WHERE id = ?",
                        (description_text, first_img_url, listing_id)
                    )
                    conn.commit()
                    send_message(name, district, price, description_text, link, collage_img)
                    break
                except Exception as e:
                    time.sleep(3)
        except Exception as e:
            pass

get_links()
update_missing_descriptions_and_images()
