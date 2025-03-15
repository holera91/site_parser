import gspread
from google.oauth2.service_account import Credentials
import logging
import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, urljoin
from langdetect import detect, DetectorFactory

# Уніфікованість результатів визначення мови
DetectorFactory.seed = 0

# Настройка логування
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

_client = None

def authenticate_google_sheets():
    """Авторизація в Google Sheets"""
    global _client
    if _client is not None:
        return _client

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
        _client = gspread.authorize(creds)
        logging.info("✅ Успішна авторизація в Google Sheets.")
        return _client
    except Exception as e:
        logging.error(f"❌ Помилка авторизації: {e}")
        raise

def get_sheet_data(spreadsheet_name):
    """Отримує дані з листа 'SiteInfo' (Website, Site Language, Тип сайту)"""
    try:
        client = authenticate_google_sheets()
        sheet = client.open(spreadsheet_name).worksheet("SiteInfo")
        data = sheet.get_all_records()

        websites = []
        site_language_col = None
        site_type_col = None

        for i, row in enumerate(data):
            if "Website" in row and row["Website"].strip():
                formatted_url = format_url(row["Website"])
                websites.append((i + 2, formatted_url))  # +2 через заголовки

            if site_language_col is None and "Site Language" in row:
                site_language_col = list(row.keys()).index("Site Language") + 1

            if site_type_col is None and "Тип сайту" in row:
                site_type_col = list(row.keys()).index("Тип сайту") + 1

        logging.info(f"📥 Знайдено {len(websites)} сайтів для обробки.")
        return sheet, websites, site_language_col, site_type_col
    except Exception as e:
        logging.error(f"❌ Помилка отримання даних з Google Sheets: {e}")
        return None, [], None, None

def format_url(url):
    """Форматує URL у вигляді https://domain.com"""
    parsed = urlparse(url)
    if parsed.scheme in ["http", "https"]:
        return f"{parsed.scheme}://{parsed.netloc}"
    return f"https://{parsed.path.split('/')[0]}"

def detect_language(text):
    """Визначає мову тексту"""
    try:
        return detect(text)
    except Exception:
        return "unknown"

def get_site_language(url):
    """Визначає мову сайту (meta description або <p>)"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return "unknown"

        soup = BeautifulSoup(response.text, "html.parser")

        # Пробуємо взяти мову з метатегу description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        text = meta_desc["content"] if meta_desc else ""

        # Якщо мета-тег не дав результату, беремо перший <p>
        if not text:
            paragraph = soup.find("p")
            text = paragraph.get_text(strip=True) if paragraph else ""

        return detect_language(text) if text else "unknown"
    except Exception as e:
        logging.error(f"❌ Помилка визначення мови {url}: {e}")
        return "unknown"

def get_site_type(url):
    """Визначає тип сайту: 'візитка' або 'багато сторінок'"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return "unknown"

        soup = BeautifulSoup(response.text, "html.parser")
        domain = urlparse(url).netloc
        internal_links = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            full_url = urljoin(url, href)
            if domain in urlparse(full_url).netloc and full_url != url:
                internal_links.add(full_url)

        return "багато сторінок" if len(internal_links) > 5 else "візитка"
    except Exception as e:
        logging.error(f"❌ Помилка визначення типу сайту {url}: {e}")
        return "unknown"

def get_next_empty_column(sheet):
    """Знаходить наступну вільну колонку після 'Website'"""
    try:
        headers = sheet.row_values(1)
        if "Website" in headers:
            website_index = headers.index("Website")
            return website_index + 2  # Наступна колонка після Website
        return None
    except Exception as e:
        logging.error(f"❌ Помилка пошуку вільної колонки: {e}")
        return None

def scrape_about_page(url):
    """Парсить сайт і шукає інформацію про компанію"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return "❌ Сайт недоступний"

        soup = BeautifulSoup(response.text, "html.parser")
        paragraphs = soup.find_all("p")
        about_text = ""

        for p in paragraphs:
            text = p.get_text(strip=True)
            if 50 < len(text) < 500:  # Відсіюємо короткі та довгі тексти
                about_text = text
                break

        return about_text if about_text else "Не знайдено"
    except Exception as e:
        logging.error(f"❌ Помилка парсингу {url}: {e}")
        return "❌ Помилка парсингу"

def update_sheet(sheet, row, col, value):
    """Оновлює конкретну ячейку в Google Sheets"""
    try:
        sheet.update_cell(row, col, value)
        logging.info(f"✅ Записано в {row}:{col}")
    except Exception as e:
        logging.error(f"❌ Помилка запису в Google Sheets: {e}")

def main(spreadsheet_name):
    sheet, websites, site_language_col, site_type_col = get_sheet_data(spreadsheet_name)
    if not sheet or not websites or not site_language_col or not site_type_col:
        logging.error("❌ Помилка отримання даних.")
        return

    next_column = get_next_empty_column(sheet)
    if not next_column:
        logging.error("❌ Не вдалося знайти наступну колонку для даних.")
        return

    for row, website in websites:
        update_sheet(sheet, row, 2, website)

        site_language = get_site_language(website)
        update_sheet(sheet, row, site_language_col, site_language)

        site_type = get_site_type(website)
        update_sheet(sheet, row, site_type_col, site_type)

        about_info = scrape_about_page(website)
        update_sheet(sheet, row, next_column, about_info)

        time.sleep(2)  # Антибан

    logging.info("✅ Парсинг завершено.")

# Запуск коду
spreadsheet_name = "Parser"
main(spreadsheet_name)
