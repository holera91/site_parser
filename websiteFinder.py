import gspread
from google.oauth2.service_account import Credentials
from googlesearch import search
import logging
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Авторизация в Google Sheets
def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
        client = gspread.authorize(creds)
        logging.info("✅ Успешная авторизация в Google Sheets.")
        return client
    except Exception as e:
        logging.error(f"❌ Ошибка авторизации: {e}")
        raise

client = authenticate_google_sheets()

# Открываем таблицу и лист
SHEET_NAME = "Parser"
WORKSHEET_NAME = "WebsiteFinder"

try:
    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    logging.info(f"📄 Открыта таблица: {SHEET_NAME} / {WORKSHEET_NAME}")
except Exception as e:
    logging.error(f"❌ Ошибка открытия таблицы: {e}")
    raise

# Получаем список компаний из первого столбца (A)
companies = sheet.col_values(1)[1:]  # Пропускаем заголовок

def find_website(company_name):
    """Ищет сайт компании в Google"""
    query = f"{company_name} официальный сайт"
    try:
        results = list(search(query, num_results=1, lang="ru"))  # Исправленный вызов
        return results[0] if results else "Не найдено"
    except Exception as e:
        logging.error(f"❌ Ошибка при поиске {company_name}: {e}")
        return "Не найдено"

# Обновляем Google Sheets
for i, company in enumerate(companies, start=2):  # Начинаем со 2-й строки (1 - заголовок)
    if not company.strip():
        continue  # Пропускаем пустые строки
    
    website = find_website(company)
    sheet.update_cell(i, 2, website)  # Записываем в колонку B (Website)
    logging.info(f"🔍 {company} → {website}")
    
    time.sleep(2)  # Задержка для избежания блокировки Google

logging.info("✅ Готово! Данные обновлены в Google Sheets.")
