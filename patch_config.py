#!/usr/bin/env python3
"""
Скрипт для обновления конфигурации шаттлов.
Добавляет автоматически обнаруженные шаттлы в config.yaml.
"""
import argparse
import yaml
import os
import sys
from typing import Dict, Any


def load_yaml(file_path: str) -> Dict[str, Any]:
    """Загружает YAML файл"""
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file) or {}
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")
        return {}


def save_yaml(file_path: str, data: Dict[str, Any]) -> bool:
    """Сохраняет данные в YAML файл"""
    try:
        with open(file_path, 'w') as file:
            yaml.dump(data, file, default_flow_style=False)
        return True
    except Exception as e:
        print(f"Ошибка при сохранении файла {file_path}: {e}")
        return False


def add_shuttle_to_config(config_path: str, shuttle_id: str, ip: str) -> bool:
    """Добавляет шаттл в конфигурацию"""
    # Загружаем конфигурацию
    config = load_yaml(config_path)
    if not config:
        return False
    
    # Проверяем наличие секции shuttles
    if 'shuttles' not in config:
        config['shuttles'] = {}
    
    # Проверяем, есть ли уже такой шаттл
    if shuttle_id in config['shuttles']:
        print(f"Шаттл {shuttle_id} уже существует в конфигурации")
        return True
    
    # Добавляем шаттл
    config['shuttles'][shuttle_id] = {
        'host': ip,
        'command_port': 2000,
        'response_port': 5000
    }
    
    # Проверяем наличие секции stock_to_shuttle
    if 'stock_to_shuttle' not in config:
        config['stock_to_shuttle'] = {}
    
    # Проверяем наличие склада "Главный"
    if 'Главный' not in config['stock_to_shuttle']:
        config['stock_to_shuttle']['Главный'] = []
    
    # Добавляем шаттл к складу "Главный", если его там еще нет
    if shuttle_id not in config['stock_to_shuttle']['Главный']:
        config['stock_to_shuttle']['Главный'].append(shuttle_id)
    
    # Сохраняем конфигурацию
    if save_yaml(config_path, config):
        print(f"Шаттл {shuttle_id} ({ip}) успешно добавлен в конфигурацию")
        return True
    else:
        return False


def add_discovered_shuttles(config_path: str) -> bool:
    """Добавляет все обнаруженные шаттлы в конфигурацию"""
    # Получаем список всех шаттлов из логов
    shuttles = {}
    try:
        with open("logs/gateway.log", 'r') as log_file:
            for line in log_file:
                if "Неизвестный шаттл с IP" in line and "Назначен ID: shuttle_" in line:
                    parts = line.split("Неизвестный шаттл с IP")[1].split("Назначен ID: shuttle_")
                    if len(parts) == 2:
                        ip = parts[0].strip().rstrip(".")
                        shuttle_id = "shuttle_" + parts[1].strip()
                        shuttles[shuttle_id] = ip
    except Exception as e:
        print(f"Ошибка при чтении лог-файла: {e}")
        return False
    
    if not shuttles:
        print("Не найдено автоматически обнаруженных шаттлов в логах")
        return False
    
    # Добавляем каждый шаттл в конфигурацию
    success = True
    for shuttle_id, ip in shuttles.items():
        if not add_shuttle_to_config(config_path, shuttle_id, ip):
            success = False
    
    return success


def main():
    parser = argparse.ArgumentParser(description="Утилита для обновления конфигурации шаттлов")
    parser.add_argument("--config", default="config.yaml", help="Путь к файлу конфигурации")
    parser.add_argument("--shuttle", help="ID шаттла (например, shuttle_138)")
    parser.add_argument("--ip", help="IP-адрес шаттла")
    parser.add_argument("--auto", action="store_true", help="Автоматически добавить все обнаруженные шаттлы из логов")
    
    args = parser.parse_args()
    
    if args.auto:
        success = add_discovered_shuttles(args.config)
    elif args.shuttle and args.ip:
        success = add_shuttle_to_config(args.config, args.shuttle, args.ip)
    else:
        parser.print_help()
        return 1
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())