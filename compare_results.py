import json
import sys
from datetime import datetime
from typing import Dict, List
import difflib

def load_results(file_path: str) -> List[Dict]:
    """Загрузка результатов из JSON файла"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def compare_results(old_file: str, new_file: str):
    """Сравнение результатов двух проверок"""
    print("🔍 Порівняння результатів...")
    
    # Загрузка результатов
    old_results = load_results(old_file)
    new_results = load_results(new_file)
    
    # Создаем словари для быстрого поиска по URL
    old_by_url = {result['url']: result for result in old_results}
    new_by_url = {result['url']: result for result in new_results}
    
    # Находим общие URL
    common_urls = set(old_by_url.keys()) & set(new_by_url.keys())
    
    # Статистика
    print(f"\n📊 Статистика:")
    print(f"Загальна кількість сайтів: {len(common_urls)}")
    
    # Сравниваем результаты для каждого URL
    changes = []
    for url in common_urls:
        old_result = old_by_url[url]
        new_result = new_by_url[url]
        
        if old_result['error_type'] != new_result['error_type']:
            changes.append({
                'url': url,
                'old_status': old_result['error_type'],
                'new_status': new_result['error_type'],
                'old_time': old_result['check_time'],
                'new_time': new_result['check_time']
            })
    
    # Выводим изменения
    if changes:
        print("\n🔄 Зміни в статусах:")
        for change in changes:
            print(f"\nURL: {change['url']}")
            print(f"Старий статус: {change['old_status']} ({change['old_time']})")
            print(f"Новий статус: {change['new_status']} ({change['new_time']})")
    else:
        print("\nℹ️ Змін в статусах не виявлено")
    
    # Проверяем новые сайты
    new_urls = set(new_by_url.keys()) - set(old_by_url.keys())
    if new_urls:
        print(f"\n➕ Нові сайти ({len(new_urls)}):")
        for url in new_urls:
            print(f"- {url}")
    
    # Проверяем удаленные сайты
    removed_urls = set(old_by_url.keys()) - set(new_by_url.keys())
    if removed_urls:
        print(f"\n➖ Видалені сайти ({len(removed_urls)}):")
        for url in removed_urls:
            print(f"- {url}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Використання: python compare_results.py <старий_файл.json> <новий_файл.json>")
        sys.exit(1)
    
    old_file = sys.argv[1]
    new_file = sys.argv[2]
    
    try:
        compare_results(old_file, new_file)
    except Exception as e:
        print(f"❌ Помилка: {str(e)}")
        sys.exit(1) 