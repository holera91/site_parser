import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin
from langdetect import detect
from deep_translator import GoogleTranslator

def translate_keywords(keywords, target_lang):
    try:
        translated = [GoogleTranslator(source='auto', target=target_lang).translate(keyword) for keyword in keywords]
        return translated
    except Exception as e:
        logging.error(f"❌ Ошибка перевода ключевых слов: {e}")
        return keywords
from concurrent.futures import ThreadPoolExecutor

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def detect_page_language(text):
    try:
        return detect(text)
    except Exception as e:
        logging.error(f"❌ Ошибка определения языка страницы: {e}")
        return "unknown"

# Глобальная переменная для кэширования клиента Google Sheets
_client = None

# HTTP-заголовки для обхода блокировок
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"
}

def authenticate_google_sheets():
    global _client
    if _client is not None:
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

def find_job_pages(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        page_lang = detect_page_language(response.text)
        keywords = ["job", "career", "careers", "jobs", "hiring", "employment", "join us", "work with us", "vacancies", "karriere"]
        
        job_links = set()
        base_links = set()
        
        for link in soup.find_all("a", href=True):
            href = link["href"].lower()
            if any(keyword in href for keyword in keywords):
                full_url = urljoin(url, link["href"])
                
                is_subpath = False
                for base_link in base_links:
                    if full_url.startswith(base_link + "/"):
                        is_subpath = True
                        break
                
                if not is_subpath:
                    job_links.add(full_url)
                    if not any(c in full_url for c in ["/", "?", "#"]):
                        base_links.add(full_url)
        
        if not job_links and page_lang != "en":
            logging.info(f"🌍 Переводим ключевые слова на {page_lang} и повторяем поиск для {url}.")
            translated_keywords = translate_keywords(keywords, page_lang)
            for link in soup.find_all("a", href=True):
                href = link["href"].lower()
                if any(keyword in href for keyword in translated_keywords):
                    full_url = urljoin(url, link["href"])
                    
                    is_subpath = False
                    for base_link in base_links:
                        if full_url.startswith(base_link + "/"):
                            is_subpath = True
                            break
                    
                    if not is_subpath:
                        job_links.add(full_url)
                        if not any(c in full_url for c in ["/", "?", "#"]):
                            base_links.add(full_url)
        
        if job_links:
            logging.info(f"🔍 Найдено {len(job_links)} страниц с вакансиями на {url}.")
        else:
            logging.warning(f"⚠️ На {url} страницы с вакансиями не найдены.")
        
        return list(job_links) if job_links else None
    except Exception as e:
        logging.error(f"❌ Ошибка при поиске вакансий на {url}: {e}")
        return None

def parse_job_page(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        page_lang = detect_page_language(response.text)
        keywords = [
            "Software Developer", "Data Engineer", "Software Architect",
            "Designer", "Data Scientist", "IT Manager", "DevOps"
        ]
        
        if page_lang != "en":
            translated_keywords = translate_keywords(keywords, page_lang)
            keywords.extend(translated_keywords)
        
        found_positions = set()
        for keyword in keywords:
            if keyword.lower() in soup.get_text().lower():
                found_positions.add(keyword)
        
        return ", ".join(found_positions) if found_positions else "No relevant positions found"
    except Exception as e:
        logging.error(f"❌ Ошибка при парсинге страницы {url}: {e}")
        return "Error parsing page"

def main():
    sheet_name = "Parser"
    logging.info("🚀 Запуск парсинга...")
    websites = ["https://www.codigi.com/"]  # Заглушка, замените на получение сайтов из Google Sheets
    
    logging.info("🔎 Начинаем поиск страниц с вакансиями...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        job_urls = list(executor.map(find_job_pages, websites))
    
    logging.info("🔍 Парсим информацию с найденных ссылок...")
    for urls in job_urls:
        if urls:
            for job_url in urls:
                parse_job_page(job_url)
    
    logging.info("🎯 Парсинг завершен!")

if __name__ == "__main__":
    main()
