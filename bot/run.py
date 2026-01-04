import logging
from bot import scraper, db

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
)

if __name__ == "__main__":
    logging.info("STARTING | Scraping")
    scraper.get_links(1)
    logging.info("FINISHED | Scraping.")

    logging.info("STARTING | Updating descriptions, images, sending Telegram messages")
    scraper.update_missing_descriptions_and_images()
    logging.info("FINISHED | Updating descriptions, images, sending Telegram messages")

    db.cursor.close()
    db.conn.close()
    logging.info("DB connection closed.")
