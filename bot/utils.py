"""
Модуль для обработки строк с датами на украинском и русском языках, а также изменения размера изображений по URL.

Функции:

- parse_ukr_date(date_str):
    Принимает строку с датой в формате на украинском или русском языке, включая варианты "Сьогодні" / "Сегодня" с временем.
    Парсит дату и возвращает объект datetime в UTC.
    Если дата не распознана — возвращает None.
    Логирует ключевые шаги парсинга.

- resize_image_url(url, new_size="600x300"):
    Принимает URL изображения и меняет параметр размера (например, "s=800x600") на новый размер.
    Если параметр размера отсутствует — возвращает URL без изменений.
"""


import re
import locale
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
except locale.Error:
    logger.warning("Locale en_US.UTF-8 not supported.")

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

    if date_str.startswith("Сьогодні") or date_str.startswith("Сегодня"):
        time_match = re.search(r'(\d{1,2}:\d{2})', date_str)
        if time_match:
            time_part = time_match.group(1)
        else:
            time_part = "00:00"
    
        dt_local = datetime.strptime(f"{datetime.now().strftime('%Y-%m-%d')} {time_part}", "%Y-%m-%d %H:%M")
        dt_utc = dt_local.astimezone(timezone.utc)
        logger.info(f"Повертаю datetime у UTC: {dt_utc}")
        return dt_utc

    parts = date_str.split()
    if len(parts) >= 3:
        day = parts[0]
        month_name = parts[1].lower()
        year = parts[2]
        month = MONTHS.get(month_name)
        if month:
            dt = datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
            dt_utc = dt.replace(tzinfo=timezone.utc)
            logger.info(f"Повертаю datetime у UTC: {dt_utc}")
            return dt_utc

    logger.warning("Не вдалося розпізнати дату, повертаю None")
    return None


def resize_image_url(url, new_size="600x300"):
    pattern = r"(s=\d+x\d+)$"
    if re.search(pattern, url):
        return re.sub(pattern, f"s={new_size}", url)
    return url
