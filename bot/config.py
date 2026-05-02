from dotenv import load_dotenv
import os

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
CHAT_ID_15_20K = os.getenv("CHAT_ID_15_20K")
CHAT_ID_20_25K = os.getenv("CHAT_ID_20_25K")
DATABASE_URL = os.getenv("DATABASE_URL")
OLX_BASE_URL = "https://www.olx.ua/uk/nedvizhimost/kvartiry/kiev/"
