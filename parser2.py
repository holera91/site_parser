import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin
from langdetect import detect
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
import re

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
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        page_lang = detect_page_language(response.text)
        keywords = ["job", "career", "careers", "jobs", "hiring", "employment", "join us", "work with us", "vacancies", "karriere"]
        
        job_links = set()
        base_links = set()  # Для хранения базовых ссылок
        
        for link in soup.find_all("a", href=True):
            href = link["href"].lower()
            if href.startswith("mailto:"):
                continue  # Пропускаем ссылки, начинающиеся с mailto:
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
                if href.startswith("mailto:"):
                    continue  # Пропускаем ссылки, начинающиеся с mailto:
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

def find_emails(html):
    """
    Ищет email-адреса в HTML-коде страницы.
    """
    try:
        email_patterns = [
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            r"[a-zA-Z0-9._%+-]+\s@\s[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            r"[a-zA-Z0-9._%+-]+\s\(at\)\s[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            r"[a-zA-Z0-9._%+-]+\s*\(at\)\s*[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        ]
        emails = set()
        for pattern in email_patterns:
            matches = re.findall(pattern, html)
            for match in matches:
                cleaned_email = match.replace(" ", "").replace("(at)", "@")
                emails.add(cleaned_email)
        logging.info(f"📧 Найдено {len(emails)} email-адресов.")
        return list(emails)
    except Exception as e:
        logging.error(f"❌ Ошибка при поиске email-адресов: {e}")
        return []

def write_job_urls_and_emails_to_sheet(sheet_name, websites, job_urls, emails):
    try:
        client = authenticate_google_sheets()
        sheet = client.open(sheet_name).sheet1
        
        for i, (urls, email_list) in enumerate(zip(job_urls, emails), start=2):
            if urls:
                sheet.update_cell(i, 2, ", ".join(urls))
            else:
                sheet.update_cell(i, 2, "нет URL")
            
            existing_emails = sheet.cell(i, 3).value
            if existing_emails:
                email_list = list(set(existing_emails.split(", ") + email_list))
            if email_list:
                sheet.update_cell(i, 3, ", ".join(email_list))
            else:
                sheet.update_cell(i, 3, "нет email")
        
        logging.info("✅ Данные успешно записаны в Google Sheets.")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи данных в таблицу: {e}")

def parse_job_page(url):
    """
    Парсит страницу с вакансиями и ищет ключевые слова.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Определяем язык страницы
        page_lang = detect_page_language(response.text)
        
        # Ключевые слова для поиска
        keywords = [
            "Software Developer", "Data Engineer", "Software Architect",
            "Designer", "Data Scientist", "IT Manager", "DevOps"
        ]
        
        # Если язык не английский, переводим ключевые слова
        if page_lang != "en":
            translated_keywords = translate_keywords(keywords, page_lang)
            keywords.extend(translated_keywords)
        
        # Ищем ключевые слова на странице
        found_positions = set()
        for keyword in keywords:
            if keyword.lower() in soup.get_text().lower():
                if detect(keyword) != "en":  # Если ключевое слово не на английском, переводим
                    translated = GoogleTranslator(source=page_lang, target="en").translate(keyword)
                    found_positions.add(translated)
                else:
                    found_positions.add(keyword)
        
        if found_positions:
            return ", ".join(found_positions)
        else:
            return "No relevant positions found"
    except Exception as e:
        logging.error(f"❌ Ошибка при парсинге страницы {url}: {e}")
        return "Error parsing page"

def update_open_positions(sheet_name):
    """
    Читает данные из колонки Job Url и обновляет колонку Open Position.
    """
    try:
        client = authenticate_google_sheets()
        sheet = client.open(sheet_name).sheet1
        
        # Читаем данные из колонки Job Url
        job_urls = sheet.col_values(2)
        for i, cell_value in enumerate(job_urls[1:], start=2):  # Пропускаем заголовок
            if cell_value.strip().lower() == "нет url":
                sheet.update_cell(i, 4, "check manually")
                continue
            
            # Если есть ссылки, парсим их
            urls = [url.strip() for url in cell_value.split(",")]
            results = []
            for url in urls:
                if url:  # Пропускаем пустые строки
                    result = parse_job_page(url)
                    results.append(result)
            
            # Записываем результат в колонку Open Position
            if results:
                sheet.update_cell(i, 4, ", ".join(results))
            else:
                sheet.update_cell(i, 4, "No relevant positions found")
        
        logging.info("✅ Колонка Open Position успешно обновлена.")
    except Exception as e:
        logging.error(f"❌ Ошибка при обновлении колонки Open Position: {e}")

def main():
    sheet_name = "Parser"
    logging.info("🚀 Запуск парсинга...")
    websites = get_business_websites(sheet_name)
    
    if not websites:
        logging.warning("⚠️ Нет сайтов для парсинга.")
        return
    
    logging.info("🔎 Начинаем поиск страниц с вакансиями и email-адресов...")
    
    job_urls = []
    emails = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        for website in websites:
            job_urls.append(executor.submit(find_job_pages, website).result())
            response = requests.get(website)
            emails.append(find_emails(response.text))
    
    logging.info("✍️ Записываем результаты в таблицу...")
    write_job_urls_and_emails_to_sheet(sheet_name, websites, job_urls, emails)
    
    logging.info("🔍 Парсим информацию с найденных ссылок...")
    update_open_positions(sheet_name)
    
    logging.info("🎯 Парсинг завершен!")

if __name__ == "__main__":
    main()