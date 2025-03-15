
import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin
from langdetect import detect
from deep_translator import GoogleTranslator
import re
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Глобальная переменная для кэширования клиента Google Sheets
_client = None

def authenticate_google_sheets():
    global _client
    if (_client is not None):
        return _client

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
        _client = gspread.authorize(creds)
        logging.info("✅ Успешная авторизация в Google Sheets.")
        return _client
    except Exception as e:
        logging.error(f"❌ Ошибка авторизации: {e}")
        raise

