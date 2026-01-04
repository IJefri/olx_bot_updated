from dotenv import load_dotenv
import os

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
OLX_BASE_URL = "https://www.olx.ua/d/uk/nedvizhimost/kvartiry/"
