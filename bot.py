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

BOT_TOKEN = "7176877320:AAGLMiDHUVe3J6fpqyzFrxBKLEYyJgLndkE"
CHAT_ID = 430697715  # Your Telegram ID

bot = telebot.TeleBot(BOT_TOKEN)

locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')

# База на persistent-диске
conn = sqlite3.connect("/data/listings.db")
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

# Selenium настройки для Render
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

def get_links(pages=1):
    print('Старт бота')
    page_num = 1
    while page_num <= pages:
        PARAMS["page"] = page_num
        url = build_url(PARAMS)
        driver.get(url)
        time.sleep(3)
        cards = driver.find_elements(By.CSS_SELECTOR, "div[data-cy='l-card']")
        if not cards:
            break
        print(f"Знайдено {len(cards)} оголошень на сторінці {page_num}")
        for card in cards:
            listing_id = card.get_attribute('id')
            if is_new_listing(listing_id):
                bot.send_message(CHAT_ID, f"New listing: {listing_id}", parse_mode='Markdown')
        page_num += 1
    print("Парсинг завершено.")

get_links()
