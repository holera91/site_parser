import logging
import os
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
from bs4 import BeautifulSoup

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

_client = None  # Переменная для хранения клиента Google Sheets

# Проверяем, есть ли credentials.json
if not os.path.exists("credentials.json"):
    logging.error("❌ Файл credentials.json не найден. Авторизация невозможна!")
    exit(1)

# Функция авторизации в Google Sheets
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
        exit(1)

# Настройка ChromeDriver для Selenium
def init_driver():
    options = Options()
    options.add_argument("--headless")  # Запуск в фоновом режиме
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# Поиск страниц с вакансиями
def parse_job_pages():
    client = authenticate_google_sheets()
    sheet = client.open("Parser").sheet1  # Открываем таблицу
    business_websites = sheet.col_values(1)  # Читаем колонку с сайтами компаний

    for row_index, urls in enumerate(business_websites, start=1):
        try:
            if not urls:
                continue
            links = [link.strip() for link in urls.split(",")]  # Разделяем ссылки по запятой
            job_urls = []

            for url in links:
                driver = init_driver()
                driver.get(url)
                time.sleep(3)  # Ожидание загрузки

                soup = BeautifulSoup(driver.page_source, "html.parser")
                job_links = find_job_links(soup, url)
                job_urls.extend(job_links)
                driver.quit()

            if job_urls:
                job_urls_str = ", ".join(job_urls)
                sheet.update_cell(row_index, 2, job_urls_str)  # Запись в колонку "Job URL"
                logging.info(f"✅ Записаны ссылки в строку {row_index}: {job_urls_str}")

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге {urls}: {e}")

# Функция поиска страниц с вакансиями
def find_job_links(soup, url):
    job_keywords = ["job", "career", "careers", "jobs", "hiring", "employment", "join us", "work with us", "vacancies", "job opening"]
    job_links = []
    
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.text.strip().lower()

        if any(keyword in text for keyword in job_keywords) or any(keyword in href for keyword in job_keywords):
            full_link = href if href.startswith("http") else url + href
            job_links.append(full_link)

    return job_links

# Поиск вакансий на страницах
def parse_vacancies():
    client = authenticate_google_sheets()
    sheet = client.open("Parser").sheet1
    job_urls_list = sheet.col_values(2)  # Читаем колонку "Job URL"

    job_titles = ["Software Developer", "Data Engineer", "Software Architect", "Designer", "Data Scientist", 
                  "IT Manager", "DevOps", "Tester", "Back End", "Backend", "Back-End", "Front End", "Frontend", 
                  "Front-End", "Data", "iOS", "Android", "Developer", "Project", "Product", "IT"]  # Добавил "IT"

    for row_index, job_urls in enumerate(job_urls_list, start=1):
        try:
            if not job_urls:
                continue
            links = [link.strip() for link in job_urls.split(",")]  # Разделяем ссылки

            found_vacancies = []
            for job_url in links:
                response = requests.get(job_url, timeout=10)
                soup = BeautifulSoup(response.text, "html.parser")

                vacancies = find_vacancy_titles(soup, job_titles)
                if vacancies:
                    found_vacancies.extend(vacancies)

            if found_vacancies:
                vacancies_str = ", ".join(found_vacancies)
                sheet.update_cell(row_index, 3, vacancies_str)  # Запись в колонку "Job Openings"
                logging.info(f"✅ Записаны вакансии в строку {row_index}: {vacancies_str}")

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге {job_urls}: {e}")

# Поиск названий вакансий на странице
def find_vacancy_titles(soup, job_titles):
    found_vacancies = []
    for element in soup.find_all(text=True):
        text = element.strip()
        for title in job_titles:
            if title.lower() in text.lower():
                found_vacancies.append(text)
    return found_vacancies

if __name__ == "__main__":
    try:
        parse_job_pages()  # Этап 1: поиск страниц с вакансиями
        parse_vacancies()  # Этап 2: поиск вакансий на этих страницах
    except Exception as e:
        logging.error(f"🚨 Ошибка выполнения скрипта: {e}")
