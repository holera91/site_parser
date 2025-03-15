# Тестує чи правильні credits.json
import logging
from google.oauth2.service_account import Credentials
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

try:
    creds = Credentials.from_service_account_file("credentials.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    logging.info("✅ Файл credentials.json прочитан успешно.")
    logging.info(f"✅ Email сервисного аккаунта: {creds.service_account_email}")
except FileNotFoundError:
    logging.error("❌ Файл credentials.json не найден.")
except Exception as e:
    logging.error(f"❌ Ошибка при чтении credentials.json: {e}")