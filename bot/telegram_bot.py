"""
Модуль для отправки сообщений и изображений в Telegram чат с использованием Telebot.

Функция:

- send_message(name, district, price, description, link, collage_img=None):
    Формирует и отправляет сообщение о недвижимости с такими данными, как название, район, цена, описание и ссылка.
    В качестве тега для района автоматически создаётся хештег.
    Если передано изображение (collage_img), отправляет его как фотографию с подписью.
    Если изображения нет — отправляет обычное текстовое сообщение.
    Использует HTML-разметку для форматирования сообщения.
    Логирует процесс отправки и ошибки.
"""


import telebot
from io import BytesIO
import re
from html import escape as hesc  # импорт функции экранирования HTML
import logging

logger = logging.getLogger(__name__)

from bot.config import BOT_TOKEN, CHAT_ID, CHAT_ID_15_20K, CHAT_ID_20_25K  # импорт токена и ID чата из конфигурации

bot = telebot.TeleBot(BOT_TOKEN)  # создаём объект бота с токеном


def parse_price(price_str):
    """
    Достаёт цену из строки вида:
    '15 000 грн.Договірна'
    '9 000 грн.'
    '9000грн'
    """

    if not price_str:
        return None

    # ищем первое число с пробелами или без
    match = re.search(r"(\d[\d\s]*)", price_str)

    if not match:
        return None

    # убираем пробелы внутри числа
    number = match.group(1).replace(" ", "")

    return int(number)


def send_message(name, district, price, description, link, collage_img=None):
    # определяем куда отправлять: либо CHAT_ID, либо публичный канал по умолчанию
    price_value = parse_price(price)

    if price_value is not None:
        if price_value < 15000:
            DEST_CHAT = CHAT_ID
        elif 15000 <= price_value <= 20000:
            DEST_CHAT = CHAT_ID_15_20K
        elif 20000 < price_value <= 25000:
            DEST_CHAT = CHAT_ID_20_25K
        else:
            DEST_CHAT = CHAT_ID  # можно позже сделать отдельный чат 25k+
    else:
        DEST_CHAT = CHAT_ID
        logger.info(f"Price not parsed for listing '{name}': {price}")

    # берём первую часть района (до " - ") и обрезаем пробелы
    loc_text = district.split(" - ", 1)[0].strip()
    # формируем тег, заменяя все не буквы и цифры на подчеркивания
    tag = re.sub(r"[^\wА-Яа-яІіЇїЄєҐґ0-9]+", "_", loc_text, flags=re.UNICODE)
    # объединяем подряд идущие подчеркивания и обрезаем лишние с концов
    tag = re.sub(r"_+", "_", tag).strip("_")
    # формируем хештег если тег не пустой
    hashtag = f"#{tag}" if tag else ""

    # экранируем HTML для безопасной вставки в сообщение
    name_html = hesc(name)
    # loc_html = hesc(loc_text)
    price_html = hesc(price)
    desc_html = hesc((description or "")[:500])  # обрезаем описание до 500 символов
    link_html = hesc(link)

    # формируем HTML-сообщение с нужными данными
    message = (
        f"🏠 <b>{name_html}</b>\n"
        f"📍 <b>Район</b>: {hashtag}\n\n"
        f"💰 <b>Ціна</b>: {price_html}\n"
        f"📝 <b>Опис</b>: {desc_html}\n"
        f"🔗 <a href=\"{link_html}\">Посилання</a>"
    )

    try:
        if collage_img:
            logger.info(f"Sending photo with collage for listing '{name}'")  # логируем отправку фото
            bio = BytesIO()  # создаём байтовый поток для хранения картинки в памяти
            bio.name = 'collage.jpg'  # указываем имя для корректной обработки API
            collage_img.save(bio, 'JPEG')  # сохраняем изображение в байтовый поток
            bio.seek(0)  # сбрасываем курсор в начало потока
            bot.send_photo(DEST_CHAT, photo=bio, caption=message, parse_mode='HTML')  # отправляем фото с подписью
            bio.close()  # закрываем поток для освобождения ресурсов
        else:
            logger.info(f"Sending message without photo for listing '{name}'")  # логируем отправку текста
            bot.send_message(DEST_CHAT, message, parse_mode='HTML', disable_web_page_preview=False)  # отправляем текстовое сообщение
        logger.info(f"Sent Telegram message for: {name}")  # подтверждаем успешную отправку
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")  # логируем ошибку при отправке
