from bot import scraper, telegram_bot

def test_scraper():
    print("=== Тест: scraper.get_links() ===")
    try:
        scraper.get_links(1)
        print("scraper.get_links(1) выполнена успешно.")
    except Exception as e:
        print(f"Ошибка в scraper.get_links: {e}")

def test_update():
    print("\n=== Тест: scraper.update_missing_descriptions_and_images() ===")
    try:
        scraper.update_missing_descriptions_and_images()
        print("scraper.update_missing_descriptions_and_images() выполнена успешно.")
    except Exception as e:
        print(f"Ошибка в scraper.update_missing_descriptions_and_images: {e}")

# def test_send_message():
#     print("\n=== Тест: telegram_bot.send_message() ===")

#     name = "Тестова квартира"
#     district = "Оболонський - Сьогодні о 12:00"
#     price = "15000 UAH"
#     description = "Тестовий опис квартири"
#     link = "https://www.olx.ua/test-listing"
#     collage_img = None  # Можно передать изображение PIL.Image, если есть

#     print(f"Параметры для send_message:\n"
#           f" name={name}\n"
#           f" district={district}\n"
#           f" price={price}\n"
#           f" description={description}\n"
#           f" link={link}\n"
#           f" collage_img={collage_img}")

#     try:
#         telegram_bot.send_message(name, district, price, description, link, collage_img)
#         print("telegram_bot.send_message() выполнена успешно.")
#     except Exception as e:
#         print(f"Ошибка в telegram_bot.send_message: {e}")

if __name__ == "__main__":
    test_scraper()
    test_update()
    #test_send_message()
