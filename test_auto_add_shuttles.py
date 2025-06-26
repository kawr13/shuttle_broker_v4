#!/usr/bin/env python3
"""
Тестовый скрипт для проверки автоматического добавления шаттлов при подключении
"""
import asyncio
import yaml
from core.config import add_shuttle_to_config

async def simulate_shuttle_connection():
    """Симулирует подключение нескольких шаттлов"""
    
    # Список шаттлов для симуляции подключения
    shuttles_to_connect = [
        ("10.181.80.141", "shuttle_141"),
        ("10.181.80.146", "shuttle_146"), 
        ("10.181.80.150", "shuttle_150"),
        ("10.181.80.155", "shuttle_155"),
    ]
    
    print("Симулируем автоматическое добавление шаттлов при подключении...")
    
    # Добавляем каждый шаттл
    for shuttle_ip, shuttle_id in shuttles_to_connect:
        print(f"Добавляем шаттл {shuttle_id} с IP {shuttle_ip}")
        result = add_shuttle_to_config(shuttle_id, shuttle_ip, "Главный")
        if result:
            print(f"✓ Шаттл {shuttle_id} успешно добавлен")
        else:
            print(f"✗ Ошибка при добавлении шаттла {shuttle_id}")
    
    # Проверяем результат
    print("\nПроверяем результат...")
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        shuttles_in_config = config_data.get('shuttles', {})
        stock_to_shuttle = config_data.get('stock_to_shuttle', {})
        
        print(f"Всего шаттлов в конфигурации: {len(shuttles_in_config)}")
        
        # Проверяем каждый добавленный шаттл
        for shuttle_ip, shuttle_id in shuttles_to_connect:
            if shuttle_id in shuttles_in_config:
                print(f"✓ Шаттл {shuttle_id} найден в конфигурации")
                if shuttles_in_config[shuttle_id]['host'] == shuttle_ip:
                    print(f"  ✓ IP адрес корректный: {shuttle_ip}")
                else:
                    print(f"  ✗ IP адрес некорректный")
                
                # Проверяем привязку к складу
                if 'Главный' in stock_to_shuttle and shuttle_id in stock_to_shuttle['Главный']:
                    print(f"  ✓ Шаттл {shuttle_id} привязан к складу 'Главный'")
                else:
                    print(f"  ✗ Шаттл {shuttle_id} НЕ привязан к складу 'Главный'")
            else:
                print(f"✗ Шаттл {shuttle_id} НЕ найден в конфигурации")
        
        print(f"\nШаттлы на складе 'Главный': {stock_to_shuttle.get('Главный', [])}")
        
    except Exception as e:
        print(f"Ошибка при проверке результата: {e}")

if __name__ == "__main__":
    print("Тестирование автоматического добавления шаттлов")
    print("=" * 60)
    asyncio.run(simulate_shuttle_connection())
    print("=" * 60)
    print("Тест завершен")