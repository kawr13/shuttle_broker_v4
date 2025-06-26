#!/usr/bin/env python3
"""
Скрипт для отображения всех доступных шаттлов из конфигурации
"""
import yaml
import sys

def list_shuttles():
    """Выводит список всех шаттлов из конфигурации"""
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        shuttles = config_data.get('shuttles', {})
        
        if not shuttles:
            print("❌ Шаттлы не найдены в конфигурации")
            return
        
        print(f"🚀 Найдено {len(shuttles)} шаттлов в конфигурации:")
        print("=" * 60)
        
        # Сортируем шаттлы по ID
        for shuttle_id in sorted(shuttles.keys()):
            shuttle_config = shuttles[shuttle_id]
            host = shuttle_config['host']
            command_port = shuttle_config.get('command_port', 2000)
            response_port = shuttle_config.get('response_port', 5000)
            
            print(f"🔹 {shuttle_id}")
            print(f"   IP: {host}")
            print(f"   Порт команд: {command_port}")
            print(f"   Порт ответов: {response_port}")
            print()
        
        print("=" * 60)
        print("💡 Для отправки команды используйте:")
        print("   python send_command.py <shuttle_id> <command>")
        print("   python shuttle_direct_client.py <shuttle_id> <command>")
        print()
        print("📋 Примеры команд: STATUS, HOME, BATTERY, LOC, MRCD")
        
    except FileNotFoundError:
        print("❌ Файл config.yaml не найден")
    except Exception as e:
        print(f"❌ Ошибка при чтении конфигурации: {e}")

if __name__ == "__main__":
    list_shuttles()