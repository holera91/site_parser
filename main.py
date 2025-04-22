import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import List, Dict
import json
from datetime import datetime
import re
from urllib.parse import urlparse
from tqdm import tqdm
import sys
import random

# Конфигурация
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = '1984k6gru7k9WI8FYIUg9hA6HG80b4J5hacYFiIGbP5Y'  # Замените на ID вашей таблицы
RANGE_NAME = 'Website_check!A2:A'  # Диапазон с URL сайтов

# Заголовки для имитации браузера
BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}

class SiteChecker:
    def __init__(self):
        self.creds = None
        self.service = None
        self.results = []
        self.domain_free_patterns = [
            r'domain.*free',
            r'domain.*available',
            r'domain.*for sale',
            r'domain.*parked',
            r'domain.*not configured',
            r'domain.*not found',
            r'domain.*not connected',
            r'domain.*not active',
            r'domain.*not registered',
            r'domain.*not assigned',
            r'domain.*not pointing',
            r'domain.*not resolving',
            r'domain.*not set up',
            r'domain.*not working',
            r'domain.*not responding',
            r'domain.*not available',
            r'domain.*not valid',
            r'domain.*not active',
            r'domain.*not configured',
            r'domain.*not connected',
            r'domain.*not found',
            r'domain.*not registered',
            r'domain.*not assigned',
            r'domain.*not pointing',
            r'domain.*not resolving',
            r'domain.*not set up',
            r'domain.*not working',
            r'domain.*not responding',
            r'domain.*not available',
            r'domain.*not valid'
        ]

    async def setup_google_sheets(self):
        """Настройка доступа к Google Sheets"""
        print("🔐 Авторизація в Google Sheets...")
        self.creds = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=SCOPES)
        self.service = build('sheets', 'v4', credentials=self.creds)
        print("✅ Авторизація успішна")

    async def normalize_url(self, url: str) -> str:
        """Нормализация URL - добавление протокола если отсутствует"""
        if not url:
            return url
            
        # Проверяем, есть ли уже протокол
        parsed = urlparse(url)
        if not parsed.scheme:
            # Пробуем сначала https
            try:
                async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as session:
                    async with session.get(f'https://{url}', timeout=5, ssl=False) as response:
                        if response.status == 200:
                            return f'https://{url}'
            except:
                pass
            # Если https не работает, используем http
            return f'http://{url}'
        return url

    async def get_sites_from_sheet(self) -> List[str]:
        """Получение списка сайтов из Google Sheets"""
        print("📊 Отримання даних з таблиці...")
        result = self.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        urls = [row[0] for row in result.get('values', []) if row]
        print(f"✅ Знайдено {len(urls)} сайтів")
        normalized_urls = []
        for url in urls:
            normalized_url = await self.normalize_url(url)
            normalized_urls.append(normalized_url)
        return normalized_urls

    def check_domain_free(self, html: str) -> bool:
        """Проверка, является ли домен свободным или не настроенным"""
        html_lower = html.lower()
        for pattern in self.domain_free_patterns:
            if re.search(pattern, html_lower):
                return True
        return False

    async def check_final_url(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """Проверка конечного URL после редиректа"""
        result = {
            'final_url': url,
            'status': 'error',
            'response_time': 0,
            'error_type': None,
            'is_domain_free': False
        }

        try:
            start_time = datetime.now()
            async with session.get(url, timeout=30, ssl=False) as response:
                result['response_time'] = (datetime.now() - start_time).total_seconds()
                result['status'] = response.status
                
                if response.status == 200:
                    html = await response.text()
                    result['is_domain_free'] = self.check_domain_free(html)
                    if result['is_domain_free']:
                        result['error_type'] = 'domain_free'
                    else:
                        result['error_type'] = 'available'
                elif response.status == 403:
                    result['error_type'] = 'blocked_by_ip'
                elif response.status == 404:
                    result['error_type'] = 'not_found'
                elif response.status == 500:
                    result['error_type'] = 'server_error'
                else:
                    result['error_type'] = f'http_error_{response.status}'

        except Exception as e:
            result['error_type'] = 'unknown_error'
            result['error_message'] = str(e)

        return result

    async def check_site(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """Проверка одного сайта"""
        result = {
            'url': url,
            'status': 'error',
            'response_time': 0,
            'error_type': None,
            'redirect_url': None,
            'final_url_check': None,
            'is_domain_free': False,
            'check_time': datetime.now().isoformat()
        }

        try:
            start_time = datetime.now()
            async with session.get(url, timeout=30, allow_redirects=False, ssl=False) as response:
                result['response_time'] = (datetime.now() - start_time).total_seconds()
                result['status'] = response.status

                # Проверка редиректов
                if response.status in (301, 302, 303, 307, 308):
                    redirect_url = response.headers.get('Location')
                    result['redirect_url'] = redirect_url
                    result['error_type'] = 'redirect'
                    
                    # Проверка конечного URL
                    if redirect_url:
                        result['final_url_check'] = await self.check_final_url(session, redirect_url)
                    return result

                # Проверка доступности
                if response.status == 200:
                    html = await response.text()
                    result['is_domain_free'] = self.check_domain_free(html)
                    if result['is_domain_free']:
                        result['error_type'] = 'domain_free'
                    else:
                        result['error_type'] = 'available'
                elif response.status == 403:
                    result['error_type'] = 'blocked_by_ip'
                elif response.status == 404:
                    result['error_type'] = 'not_found'
                elif response.status == 500:
                    result['error_type'] = 'server_error'
                else:
                    result['error_type'] = f'http_error_{response.status}'

        except aiohttp.ClientError as e:
            result['error_type'] = 'connection_error'
            result['error_message'] = str(e)
        except asyncio.TimeoutError:
            result['error_type'] = 'timeout'
        except Exception as e:
            result['error_type'] = 'unknown_error'
            result['error_message'] = str(e)

        return result

    async def check_all_sites(self):
        """Проверка всех сайтов"""
        sites = await self.get_sites_from_sheet()
        print("\n🔄 Початок перевірки сайтів...")
        
        # Создаем сессию с настройками браузера
        connector = aiohttp.TCPConnector(ssl=False, force_close=True)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            headers=BROWSER_HEADERS,
            connector=connector,
            timeout=timeout
        ) as session:
            # Создаем прогресс-бар
            pbar = tqdm(total=len(sites), desc="Перевірка", 
                       bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} {elapsed}<{remaining}',
                       ncols=100)
            
            for i, url in enumerate(sites, 1):
                # Обновляем описание прогресс-бара
                pbar.set_description(f"{i}/{len(sites)} {url}")
                result = await self.check_site(session, url)
                self.results.append(result)
                pbar.update(1)
                
                # Добавляем небольшую задержку между запросами
                await asyncio.sleep(random.uniform(0.5, 1.5))
            
            pbar.close()
            print("\n✅ Перевірка завершена")

    def save_results(self):
        """Сохранение результатов в JSON файл"""
        filename = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"💾 Результати збережено у файл: {filename}")

async def main():
    checker = SiteChecker()
    await checker.setup_google_sheets()
    await checker.check_all_sites()
    checker.save_results()

if __name__ == '__main__':
    asyncio.run(main())
