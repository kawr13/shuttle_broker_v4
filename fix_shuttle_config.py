#!/usr/bin/env python3
"""
Скрипт для исправления конфигурации шаттлов
"""
import yaml
import sys
import argparse

def load_config() -> dict:
    """Загружает конфигурацию из файла"""
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)

def save_config(config: dict) -> bool:
    """Сохраняет конфигурацию в файл"""
    try:
        with open('config.yaml', 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print("✅ Конфигурация успешно сохранена")
        return True
    except Exception as e:
        print(f"❌ Ошибка сохранения конфигурации: {e}")
        return False

def fix_shuttle_config(config: dict, shuttle_id: str, new_ip: str) -> dict:
    """Исправляет IP-адрес шаттла в конфигурации"""
    if shuttle_id in config['shuttles']:
        old_ip = config['shuttles'][shuttle_id]['host']
        config['shuttles'][shuttle_id]['host'] = new_ip
        print(f"✅ IP-адрес шаттла {shuttle_id} изменен с {old_ip} на {new_ip}")
    else:
        print(f"❌ Шаттл {shuttle_id} не найден в конфигурации")
    
    return config

def add_shuttle(config: dict, shuttle_id: str, ip: str) -> dict:
    """Добавляет новый шаттл в конфигурацию"""
    if shuttle_id in config['shuttles']:
        print(f"❌ Шаттл {shuttle_id} уже существует в конфигурации")
        return config
    
    config['shuttles'][shuttle_id] = {
        'host': ip,
        'command_port': 2000,
        'response_port': 5000,
        'shuttle_health_check_interval': 10
    }
    
    # Добавляем шаттл в список шаттлов склада
    if 'Главный' in config['stock_to_shuttle']:
        config['stock_to_shuttle']['Главный'].append(shuttle_id)
    
    print(f"✅ Добавлен новый шаттл: {shuttle_id} с IP {ip}")
    return config

def main():
    parser = argparse.ArgumentParser(description='Исправление конфигурации шаттлов')
    subparsers = parser.add_subparsers(dest='action', help='Действие')
    
    # Команда fix
    fix_parser = subparsers.add_parser('fix', help='Исправить IP-адрес шаттла')
    fix_parser.add_argument('shuttle_id', help='ID шаттла')
    fix_parser.add_argument('new_ip', help='Новый IP-адрес')
    
    # Команда add
    add_parser = subparsers.add_parser('add', help='Добавить новый шаттл')
    add_parser.add_argument('shuttle_id', help='ID шаттла')
    add_parser.add_argument('ip', help='IP-адрес')
    
    # Команда auto-fix
    auto_fix_parser = subparsers.add_parser('auto-fix', help='Автоматически исправить конфигурацию')
    
    args = parser.parse_args()
    
    if not args.action:
        parser.print_help()
        return 1
    
    # Загружаем конфигурацию
    config = load_config()
    
    if args.action == 'fix':
        config = fix_shuttle_config(config, args.shuttle_id, args.new_ip)
        save_config(config)
    
    elif args.action == 'add':
        config = add_shuttle(config, args.shuttle_id, args.ip)
        save_config(config)
    
    elif args.action == 'auto-fix':
        # Проверяем, есть ли шаттл с IP 10.181.80.132
        ip_132_exists = False
        for shuttle_id, shuttle_config in config['shuttles'].items():
            if shuttle_config['host'] == '10.181.80.132':
                ip_132_exists = True
                print(f"✅ Шаттл с IP 10.181.80.132 уже существует: {shuttle_id}")
        
        # Если нет, добавляем новый шаттл
        if not ip_132_exists:
            shuttle_id = "shuttle_132"
            config = add_shuttle(config, shuttle_id, '10.181.80.132')
        
        # Проверяем шаттл shuttle_135
        if 'shuttle_135' in config['shuttles']:
            # Проверяем, правильный ли IP
            if config['shuttles']['shuttle_135']['host'] != '10.181.80.132':
                print(f"⚠️ Шаттл shuttle_135 имеет неправильный IP: {config['shuttles']['shuttle_135']['host']}")
                print("Исправляем на 10.181.80.132...")
                config = fix_shuttle_config(config, 'shuttle_135', '10.181.80.132')
        
        save_config(config)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())