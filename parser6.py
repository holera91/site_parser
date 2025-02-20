import logging
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
from bs4 import BeautifulSoup

_client = None  # Инициализируем переменную для хранения клиента

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
        raise

# Настройка ChromeDriver для работы с Selenium
def init_driver():
    options = Options()
    options.add_argument("--headless")  # Для запуска без интерфейса
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# Парсинг сайтов для поиска вакансий
def parse_job_pages():
    client = authenticate_google_sheets()
    sheet = client.open('Parser').sheet1  # Открываем таблицу с данными
    business_websites = sheet.col_values(1)  # Колонка с сайтами компаний

    for row_index, urls in enumerate(business_websites, start=1):
        try:
            if not urls:
                continue
            links = [link.strip() for link in urls.split(",")]  # Разделяем ссылки по запятой

            job_urls = []
            for url in links:
                driver = init_driver()
                driver.get(url)
                time.sleep(3)  # Ожидание загрузки страницы

                # Ищем страницы с вакансиями на сайте
                soup = BeautifulSoup(driver.page_source, "html.parser")
                job_links = find_job_links(soup, url)
                job_urls.extend(job_links)
                driver.quit()

            # Записываем найденные ссылки через запятую в колонку "Job URL"
            if job_urls:
                sheet.update_cell(row_index, 2, ", ".join(job_urls))  

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге сайта {urls}: {e}")

# Функция для поиска страниц с вакансиями
def find_job_links(soup, url):
    job_keywords = ["job", "career", "careers", "jobs", "hiring", "employment", "join us", "work with us", "vacancies", "job opening"]
    job_links = []
    for keyword in job_keywords:
        links = soup.find_all('a', href=True, text=lambda text: text and keyword.lower() in text.lower())
        for link in links:
            job_link = link['href']
            if job_link.startswith('http'):
                job_links.append(job_link)
            elif job_link.startswith('/'):
                job_links.append(url + job_link)
    return job_links

# Парсинг вакансий на найденных страницах
def parse_vacancies():
    client = authenticate_google_sheets()
    sheet = client.open('Parser').sheet1
    job_urls_list = sheet.col_values(2)  # Колонка с URL вакансий

    job_titles = ["Software Developer", "Data Engineer", "Software Architect", "Designer", "Data Scientist", 
                  "IT Manager", "DevOps", "Tester", "Back End", "Backend", "Back-End", "Front End", "Frontend", 
                  "Front-End", "Data", "iOS", "Android", "Developer", "Project", "Product", "IT"]  # Добавлено "IT"

    for row_index, job_urls in enumerate(job_urls_list, start=1):
        try:
            if not job_urls:
                continue
            links = [link.strip() for link in job_urls.split(",")]  # Разделяем ссылки

            found_vacancies = []
            for job_url in links:
                page = requests.get(job_url)
                soup = BeautifulSoup(page.content, "html.parser")
                found_vacancies.extend(find_vacancy_titles(soup, job_titles))

            # Записываем вакансии через запятую в колонку "Job Openings"
            if found_vacancies:
                sheet.update_cell(row_index, 3, ", ".join(found_vacancies))  

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге вакансий на {job_urls}: {e}")

# Поиск вакансий на странице
def find_vacancy_titles(soup, job_titles):
    found_vacancy = []
    for title in job_titles:
        if any(title.lower() in element.text.lower() for element in soup.find_all(text=True)):
            found_vacancy.append(title)
    return found_vacancy

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        parse_job_pages()  # Этап 1: поиск страниц с вакансиями
        parse_vacancies()  # Этап 2: поиск вакансий на этих страницах
    except Exception as e:
        logging.error(f"Ошибка при выполнении парсинга: {e}")
