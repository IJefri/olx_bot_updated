"""
Модуль для парсинга объявлений с сайта OLX и управления ими в базе данных.

Функционал:
- Формирует URL для запросов с параметрами.
- Получает карточки объявлений со страниц и парсит ключевые данные.
- Сохраняет/обновляет объявления в базе PostgreSQL.
- Парсит подробное описание и изображения из страницы объявления.
- Загружает и обрабатывает изображения, создавая коллаж.
- Обновляет объявления без описания и изображений, фильтруя по районам и исключая определённые ключевые слова.
- Отправляет обновлённые данные и коллажи в Telegram через бота.
- Логирует ключевые события и ошибки.
- Использует сборку мусора и задержки для экономии ресурсов и корректной работы с сетью.
"""

import os
import sys

LOCK_FILE = "/tmp/olx_scraper.lock"

if os.path.exists(LOCK_FILE):
    print("Another instance is running. Exiting.")
    sys.exit()

with open(LOCK_FILE, "w") as f:
    f.write(str(os.getpid()))

try:
    import time
    import gc
    import logging
    import requests
    from bs4 import BeautifulSoup
    from datetime import datetime, timezone
    from bot.db import conn, cursor, is_new_listing  # <- добавлен импорт conn
    from bot.utils import parse_ukr_date, resize_image_url
    from bot.telegram_bot import send_message
    from bot.config import OLX_BASE_URL
    from PIL import Image
    from io import BytesIO

    logger = logging.getLogger(__name__)

    def build_url(params):
        from urllib.parse import urlencode
        # Формируем URL с параметрами для запроса к OLX
        return OLX_BASE_URL + "?" + urlencode(params)

    def parse_card(card):
        try:
            listing_id = card.get("id")
            # Проверяем наличие ID и является ли объявление новым
            if not listing_id or not is_new_listing(listing_id):
                return  # Пропускаем уже обработанные или без ID

            # Получаем название объявления
            title_tag = card.select_one("a.css-1tqlkj0 h4")
            title = title_tag.get_text(strip=True) if title_tag else ""

            # Получаем цену объявления
            price_tag = card.select_one('[data-testid="ad-price"]')
            price = price_tag.get_text(strip=True) if price_tag else ""

            # Получаем информацию о районе и дате публикации
            district_tag = card.select_one('[data-testid="location-date"]')
            district = district_tag.get_text(strip=True) if district_tag else ""

            # Получаем URL изображения
            img_tag = card.select_one("img.css-8wsg1m")
            img_url = img_tag.get("src") if img_tag else None

            now = datetime.now(timezone.utc)
            created_at_dt = now  # По умолчанию текущая дата

            if district and " - " in district:
                # Разделяем район и дату, парсим дату из строки
                _, date_part = district.split(" - ", 1)
                parsed_date = parse_ukr_date(date_part)
                if parsed_date:
                    created_at_dt = parsed_date  # Используем распарсенную дату

            # Вставляем или обновляем объявление в базе с помощью UPSERT
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
            logger.info(f"Processed: {listing_id} - {title}")
        except Exception as e:
            logger.error(f"Error parsing card: {e}")

    def get_links(pages):
        session = requests.Session()
        # Обновляем заголовок User-Agent для эмуляции браузера
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        })

        for page_num in range(1, pages + 1):
            logger.info(f"Fetching page {page_num}")
            PARAMS = {
                "currency": "UAH",
                "search[order]": "created_at:desc",
                "search[filter_float_price:from]": "12000",
                "search[filter_float_price:to]": "25000",
                "search[filter_float_total_area:from]": "30",
                "page": page_num
            }
            url = build_url(PARAMS)
            logger.info(f"Loading URL: {url}")

            try:
                # Отправляем GET-запрос на страницу
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")

                # Извлекаем карточки объявлений
                cards = soup.select("div[data-cy='l-card']")
                logger.info(f"Found {len(cards)} cards on page {page_num}")

                # Парсим каждую карточку
                for card in cards:
                    parse_card(card)

                gc.collect()  # Явный вызов сборщика мусора
                time.sleep(2)  # Задержка между запросами для снижения нагрузки
            except Exception as e:
                logger.error(f"Error on page {page_num}: {e}")
                break  # При ошибке прекращаем парсинг дальше

    def get_all_slider_images(soup):
        img_elements = soup.select('div.swiper-zoom-container img')
        img_urls = []
        for img in img_elements:
            src = img.get('src')
            if src:
                # Меняем URL изображения на нужный размер
                src_resized = resize_image_url(src)
                # Добавляем только уникальные URL
                if src_resized not in img_urls:
                    img_urls.append(src_resized)
        logger.info(f"Found {len(img_urls)} image URLs in slider")
        return img_urls

    def parse_description(soup):
        desc_container = soup.select_one('div[data-testid="ad_description"]')
        if desc_container:
            desc_elem = desc_container.select_one('div.css-19duwlz')
            if desc_elem:
                # Возвращаем текст описания с пробелами между элементами
                return desc_elem.get_text(separator=' ', strip=True)
        return "Опис не знайдено"  # Если описание не найдено

    def download_images(img_urls, timeout=10, max_images=7, thumb_size=(300, 400)):
        images = []
        for url in img_urls[:max_images]:
            try:
                logger.info(f"Downloading image: {url}")
                # Загружаем изображение с таймаутом
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                # Открываем изображение и конвертируем в RGB
                img = Image.open(BytesIO(response.content)).convert('RGB')
                # Создаем уменьшенную копию изображения (thumbnail)
                img.thumbnail(thumb_size)
                images.append(img)
                logger.info(f"Downloaded and resized image: {url}")
            except Exception as e:
                logger.warning(f"Error downloading {url}: {e}")
        logger.info(f"Downloaded {len(images)} images out of {len(img_urls)} URLs")
        return images

    def create_collage(images, cols=3, margin=5):
        if not images:
            logger.warning("No images to create collage")
            return None  # Нет изображений — коллаж не создаём

        thumb_width, thumb_height = images[0].size
        # Вычисляем количество рядов с учётом количества колонок
        rows = (len(images) + cols - 1) // cols

        # Рассчитываем размеры итогового изображения с учётом отступов
        collage_width = cols * thumb_width + (cols + 1) * margin
        collage_height = rows * thumb_height + (rows + 1) * margin
        # Создаём пустое изображение с чёрным фоном
        collage_img = Image.new('RGB', (collage_width, collage_height), (0, 0, 0))

        # Вставляем каждое изображение в коллаж по координатам с учётом отступов
        for idx, img in enumerate(images):
            x = margin + (idx % cols) * (thumb_width + margin)
            y = margin + (idx // cols) * (thumb_height + margin)
            collage_img.paste(img, (x, y))

        # Очищаем список изображений и вызываем сборщик мусора
        del images
        gc.collect()

        logger.info(f"Created collage image with size: {collage_img.size}")
        return collage_img

    def update_missing_descriptions_and_images():
        # Выбираем объявления без описания, которые созданы за последние сутки,
        # с районами из заданного списка и исключаем по ключевым словам
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
          AND lower(name) NOT LIKE '%ракетна%'
          AND lower(name) NOT LIKE '%світлопарк%'
          AND lower(name) NOT LIKE '%svitlopark%'
          AND lower(name) NOT LIKE '%навігатор%'
          AND lower(name) NOT LIKE '%навигатор%'
          AND lower(name) NOT LIKE '%паркове місто%'
          AND lower(name) NOT LIKE '%медовий%'
          AND lower(name) NOT LIKE '%новомост%'
          AND lower(name) NOT LIKE '%варшавс%'
          AND lower(name) NOT LIKE '%англія%'
          AND lower(name) NOT LIKE '%англия%'
          AND lower(name) NOT LIKE '%караває%'
          AND lower(name) NOT LIKE '%каравае%'
          AND lower(name) NOT LIKE '%британс%'
          AND lower(name) NOT LIKE '%orange%'
          AND lower(name) NOT LIKE '%нау%'
          AND lower(name) NOT LIKE '%швидкісни%'
          AND lower(name) NOT LIKE '%виноградар%'
        ORDER BY upload_dt;
        """)
        rows = cursor.fetchall()
        logger.info(f"Found {len(rows)} listings missing description/images")

        headers = {
            # Заголовок для имитации браузера при запросах
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

        for listing_id, name, district, price in rows:
            try:
                logger.info(f"Processing listing ID {listing_id}")
                url = f"https://www.olx.ua/{listing_id}"

                for attempt in range(2):
                    try:
                        # Запрашиваем страницу объявления
                        response = requests.get(url, headers=headers, timeout=10)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, "html.parser")

                        # Проверяем, доступно ли объявление (не снято ли с публикации)
                        inactive_div = soup.select_one('div[data-testid="ad-inactive-msg"]')
                        if inactive_div and "Це оголошення більше не доступне" in inactive_div.text:
                            logger.warning(f"Listing ID {listing_id} no longer available")
                            cursor.execute(
                                "UPDATE listings SET description = 'NOT AVAILABLE', img_url = NULL WHERE id = %s",
                                (listing_id,)
                            )
                            conn.commit()
                            break  # Прекращаем попытки, объявление недоступно

                        # Парсим описание и получаем URL изображений
                        description_text = parse_description(soup)
                        img_urls = get_all_slider_images(soup)
                        logger.info(f"Found {len(img_urls)} images for listing {listing_id}")

                        # Загружаем изображения и создаём коллаж
                        images = download_images(img_urls, max_images=6)
                        collage_img = create_collage(images) if images else None

                        # Чистим память
                        del soup
                        del images
                        gc.collect()

                        first_img_url = img_urls[0] if img_urls else None
                        # Обновляем описание и URL первого изображения в базе
                        cursor.execute(
                            "UPDATE listings SET description = %s, img_url = %s WHERE id = %s",
                            (description_text, first_img_url, listing_id)
                        )
                        conn.commit()

                        logger.info(f"Updated description and images for ID {listing_id}")

                        # Отправляем сообщение с данными и коллажем в Telegram
                        send_message(name, district, price, description_text, url, collage_img)
                        break  # Успешно обработали — выходим из цикла попыток
                    except Exception as e:
                        logger.error(f"Attempt {attempt+1} failed for ID {listing_id}: {e}")
                        if attempt == 0:
                            # При первой ошибке даём время на восстановление соединения
                            time.sleep(3)
                        else:
                            logger.error(f"Failed to update listing ID {listing_id} after 2 attempts")
            except Exception as e:
                logger.error(f"General error processing ID {listing_id}: {e}")

            # Освобождаем память после каждой итерации
            gc.collect()

finally:
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
