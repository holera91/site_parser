import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin
from langdetect import detect
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Глобальная переменная для кэширования клиента Google Sheets
_client = None

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

def get_business_websites(sheet_name):
    try:
        client = authenticate_google_sheets()
        sheet = client.open(sheet_name).sheet1
        websites = sheet.col_values(1)
        num_sites = len(websites) - 1
        logging.info(f"📌 Получено {num_sites} сайтов из таблицы.")
        return websites[1:]  # Пропускаем заголовок
    except Exception as e:
        logging.error(f"❌ Ошибка при чтении данных из таблицы: {e}")
        return []

def detect_page_language(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        lang_tag = soup.find("html").get("lang")
        if lang_tag:
            return lang_tag.split("-")[0]
        return detect(html)
    except Exception as e:
        logging.warning(f"⚠️ Не удалось определить язык страницы: {e}")
        return "en"

def translate_keywords(keywords, target_lang):
    try:
        translated_keywords = [GoogleTranslator(source="en", target=target_lang).translate(word) for word in keywords]
        logging.info(f"🌍 Ключевые слова переведены на {target_lang}: {translated_keywords}")
        return translated_keywords
    except Exception as e:
        logging.error(f"❌ Ошибка перевода ключевых слов: {e}")
        return keywords

def find_job_pages(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        page_lang = detect_page_language(response.text)
        keywords = ["job", "career", "careers", "jobs", "hiring", "employment", "join us", "work with us", "vacancies", "karriere"]
        
        job_links = set()
        base_links = set()  # Для хранения базовых ссылок
        
        for link in soup.find_all("a", href=True):
            href = link["href"].lower()
            if any(keyword in href for keyword in keywords):
                full_url = urljoin(url, link["href"])
                
                # Проверяем, является ли ссылка базовой или подкаталогом
                is_subpath = False
                for base_link in base_links:
                    if full_url.startswith(base_link + "/"):
                        is_subpath = True
                        break
                
                # Если это не подкаталог, добавляем в список
                if not is_subpath:
                    job_links.add(full_url)
                    # Если это базовая ссылка, добавляем её в base_links
                    if not any(c in full_url for c in ["/", "?", "#"]):  # Простая проверка на базовую ссылку
                        base_links.add(full_url)
        
        if not job_links and page_lang != "en":
            logging.info(f"🌍 Переводим ключевые слова на {page_lang} и повторяем поиск для {url}.")
            translated_keywords = translate_keywords(keywords, page_lang)
            for link in soup.find_all("a", href=True):
                href = link["href"].lower()
                if any(keyword in href for keyword in translated_keywords):
                    full_url = urljoin(url, link["href"])
                    
                    # Проверяем, является ли ссылка подкаталогом
                    is_subpath = False
                    for base_link in base_links:
                        if full_url.startswith(base_link + "/"):
                            is_subpath = True
                            break
                    
                    # Если это не подкаталог, добавляем в список
                    if not is_subpath:
                        job_links.add(full_url)
                        # Если это базовая ссылка, добавляем её в base_links
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

def write_job_urls_to_sheet(sheet_name, websites, job_urls):
    try:
        client = authenticate_google_sheets()
        sheet = client.open(sheet_name).sheet1
        headers = sheet.row_values(1)
        
        if "Job Url" not in headers:
            sheet.update_cell(1, 2, "Job Url")
        
        for i, urls in enumerate(job_urls, start=2):
            if urls:
                sheet.update_cell(i, 2, ", ".join(urls))
            else:
                sheet.update_cell(i, 2, "нет URL")
        
        logging.info("✅ Данные успешно записаны в Google Sheets.")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи данных в таблицу: {e}")

# Настройка Selenium для работы с динамическим контентом
def setup_selenium_driver():
    options = Options()
    options.headless = True  # Запуск без UI
    service = Service('C:\chromedriver\chromedriver.exe')
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def check_for_jobs_in_url(url, driver):
    try:
        driver.get(url)
        time.sleep(5)  # Даем время на загрузку динамического контента

        # Получаем исходный HTML страницы
        page_html = driver.page_source

        # Ключевые слова для поиска
        job_keywords = [
            "Software Developer", "Data Engineer", "Software Architect", "Designer", 
            "Data Scientist", "IT Manager", "DevOps", "Tester", "Back End", 
            "Backend", "Back-End", "Front End", "Frontend", "Front-End", 
            "Data", "iOS", "Android", "Developer", "Project", "Product"
        ]
        
        # Проверка наличия ключевых слов на странице
        for keyword in job_keywords:
            if keyword.lower() in page_html.lower():
                logging.info(f"🔍 Вакансии найдены на {url} для ключевого слова '{keyword}'.")
                return True
        logging.info(f"⚠️ Вакансии не найдены на {url}.")
        return False
    except Exception as e:
        logging.error(f"❌ Ошибка при проверке вакансий на {url}: {e}")
        return False

def check_job_urls_for_keywords(sheet_name):
    try:
        client = authenticate_google_sheets()
        sheet = client.open(sheet_name).sheet1
        job_urls = sheet.col_values(2)[1:]  # Чтение URL из второй колонки

        # Настройка Selenium
        driver = setup_selenium_driver()

        # Проход по всем ссылкам
        for url in job_urls:
            has_job = check_for_jobs_in_url(url, driver)
            # Записать результаты проверки (например, в новый столбец или строку)
            row_index = job_urls.index(url) + 2  # Индекс строки
            sheet.update_cell(row_index, 3, "Найдены вакансии" if has_job else "Нет вакансий")

        driver.quit()
        logging.info("✅ Проверка завершена.")
    except Exception as e:
        logging.error(f"❌ Ошибка при проверке ссылок: {e}")

def main():
    sheet_name = "Parser"
    logging.info("🚀 Запуск парсинга...")
    websites = get_business_websites(sheet_name)
    
    if not websites:
        logging.warning("⚠️ Нет сайтов для парсинга.")
        return
    
    logging.info("🔎 Начинаем поиск страниц с вакансиями...")
    
    # Используем многопоточность для ускорения
    with ThreadPoolExecutor(max_workers=5) as executor:
        job_urls = list(executor.map(find_job_pages, websites))
    
    logging.info("✍️ Записываем результаты в таблицу...")
    write_job_urls_to_sheet(sheet_name, websites, job_urls)
    
    # Проверяем ссылки на наличие вакансий
    logging.info("🔍 Проверяем все URL на наличие вакансий...")
    check_job_urls_for_keywords(sheet_name)
    
    logging.info("🎯 Парсинг завершен!")

if __name__ == "__main__":
    main()
