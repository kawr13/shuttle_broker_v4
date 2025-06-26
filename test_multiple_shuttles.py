#!/usr/bin/env python3
"""
Тестовый скрипт для проверки исправления проблемы с записью множественных неизвестных шаттлов
"""
import threading
import time
import yaml
from core.config import add_shuttle_to_config

def test_concurrent_shuttle_addition():
    """Тестирует одновременное добавление нескольких шаттлов"""
    
    # Список шаттлов для добавления
    shuttles_to_add = [
        ("test_shuttle_100", "10.181.80.100"),
        ("test_shuttle_101", "10.181.80.101"), 
        ("test_shuttle_102", "10.181.80.102"),
        ("test_shuttle_103", "10.181.80.103"),
        ("test_shuttle_104", "10.181.80.104"),
    ]
    
    # Функция для добавления шаттла в отдельном потоке
    def add_shuttle_thread(shuttle_id, shuttle_ip):
        print(f"Поток {threading.current_thread().name}: Добавляем шаттл {shuttle_id} с IP {shuttle_ip}")
        result = add_shuttle_to_config(shuttle_id, shuttle_ip, "Главный")
        if result:
            print(f"Поток {threading.current_thread().name}: Шаттл {shuttle_id} успешно добавлен")
        else:
            print(f"Поток {threading.current_thread().name}: Ошибка при добавлении шаттла {shuttle_id}")
    
    # Создаем и запускаем потоки
    threads = []
    for shuttle_id, shuttle_ip in shuttles_to_add:
        thread = threading.Thread(
            target=add_shuttle_thread, 
            args=(shuttle_id, shuttle_ip),
            name=f"Thread-{shuttle_id}"
        )
        threads.append(thread)
    
    # Запускаем все потоки одновременно
    print("Запускаем одновременное добавление шаттлов...")
    for thread in threads:
        thread.start()
    
    # Ждем завершения всех потоков
    for thread in threads:
        thread.join()
    
    print("Все потоки завершены. Проверяем результат...")
    
    # Проверяем результат
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        print("\nРезультат проверки:")
        shuttles_in_config = config_data.get('shuttles', {})
        stock_to_shuttle = config_data.get('stock_to_shuttle', {})
        
        print(f"Всего шаттлов в конфигурации: {len(shuttles_in_config)}")
        
        # Проверяем каждый добавленный шаттл
        for shuttle_id, shuttle_ip in shuttles_to_add:
            if shuttle_id in shuttles_in_config:
                print(f"✓ Шаттл {shuttle_id} найден в конфигурации")
                if shuttles_in_config[shuttle_id]['host'] == shuttle_ip:
                    print(f"  ✓ IP адрес корректный: {shuttle_ip}")
                else:
                    print(f"  ✗ IP адрес некорректный: ожидался {shuttle_ip}, получен {shuttles_in_config[shuttle_id]['host']}")
                
                # Проверяем, что шаттл добавлен на склад
                if 'Главный' in stock_to_shuttle and shuttle_id in stock_to_shuttle['Главный']:
                    print(f"  ✓ Шаттл {shuttle_id} добавлен на склад 'Главный'")
                else:
                    print(f"  ✗ Шаттл {shuttle_id} НЕ добавлен на склад 'Главный'")
            else:
                print(f"✗ Шаттл {shuttle_id} НЕ найден в конфигурации")
        
        print(f"\nСодержимое stock_to_shuttle['Главный']: {stock_to_shuttle.get('Главный', [])}")
        
    except Exception as e:
        print(f"Ошибка при проверке результата: {e}")

if __name__ == "__main__":
    print("Тестирование исправления проблемы с записью множественных шаттлов")
    print("=" * 70)
    test_concurrent_shuttle_addition()
    print("=" * 70)
    print("Тест завершен")