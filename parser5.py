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

    job_urls = []
    for url in business_websites:
        try:
            if not url:
                continue
            # Открываем сайт с Selenium
            driver = init_driver()
            driver.get(url)
            time.sleep(3)  # Ожидание загрузки страницы

            # Ищем страницы с вакансиями на сайте
            soup = BeautifulSoup(driver.page_source, "html.parser")
            job_links = find_job_links(soup, url)
            job_urls.extend(job_links)
            driver.quit()

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге сайта {url}: {e}")

    # Запись найденных ссылок в таблицу
    update_job_urls(job_urls)

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

# Обновление найденных ссылок в Google Sheets
def update_job_urls(job_urls):
    client = authenticate_google_sheets()
    sheet = client.open('Parser').sheet1
    existing_urls = sheet.col_values(2)  # Колонка с URL вакансий
    new_urls = [url for url in job_urls if url not in existing_urls]
    
    if new_urls:
        logging.info(f"Записываем {len(new_urls)} новых ссылок на вакансии.")
        for url in new_urls:
            sheet.append_row([url])  # Добавляем новую строку с URL

# Парсинг вакансий на найденных страницах
def parse_vacancies():
    client = authenticate_google_sheets()
    sheet = client.open('Parser').sheet1
    job_urls = sheet.col_values(2)  # Колонка с URL вакансий

    job_titles = ["Software Developer", "Data Engineer", "Software Architect", "Designer", "Data Scientist", 
                  "IT Manager", "DevOps", "Tester", "Back End", "Backend", "Back-End", "Front End", "Frontend", 
                  "Front-End", "Data", "iOS", "Android", "Developer", "Project", "Product"]

    for job_url in job_urls:
        try:
            page = requests.get(job_url)
            soup = BeautifulSoup(page.content, "html.parser")
            found_vacancy = find_vacancy_titles(soup, job_titles)
            if found_vacancy:
                update_vacancy_titles(job_url, found_vacancy)

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге вакансий на {job_url}: {e}")

# Поиск вакансий на странице
def find_vacancy_titles(soup, job_titles):
    found_vacancy = []
    for title in job_titles:
        if any(title.lower() in element.text.lower() for element in soup.find_all(text=True)):
            found_vacancy.append(title)
    return found_vacancy

# Обновление найденных вакансий в Google Sheets
def update_vacancy_titles(job_url, found_vacancy):
    client = authenticate_google_sheets()
    sheet = client.open('Parser').sheet1
    for position in found_vacancy:
        sheet.append_row([job_url, position])  # Добавляем URL и название вакансии

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        parse_job_pages()  # Этап 1: поиск страниц с вакансиями
        parse_vacancies()  # Этап 2: поиск вакансий на этих страницах
    except Exception as e:
        logging.error(f"Ошибка при выполнении парсинга: {e}")
