#!/usr/bin/env python3
"""
Скрипт для проверки доступности шаттлов и исправления конфигурации
"""
import asyncio
import yaml
import sys
import socket
import argparse
from typing import Dict, List, Tuple

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

async def check_tcp_port(host: str, port: int, timeout: float = 2.0) -> bool:
    """Проверяет доступность TCP порта"""
    try:
        # Создаем футуру для подключения
        future = asyncio.open_connection(host, port)
        # Ждем подключения с таймаутом
        reader, writer = await asyncio.wait_for(future, timeout=timeout)
        # Закрываем соединение
        writer.close()
        await writer.wait_closed()
        return True
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return False

def ping_host(host: str, timeout: float = 1.0) -> bool:
    """Проверяет доступность хоста с помощью сокета"""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, 80))
        return True
    except (socket.timeout, socket.error):
        try:
            # Пробуем другой порт
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, 443))
            return True
        except (socket.timeout, socket.error):
            try:
                # Пробуем еще один порт
                socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, 22))
                return True
            except (socket.timeout, socket.error):
                return False

async def check_shuttles(config: dict) -> List[Tuple[str, str, bool, bool]]:
    """Проверяет доступность всех шаттлов"""
    results = []
    
    for shuttle_id, shuttle_config in config['shuttles'].items():
        host = shuttle_config['host']
        port = shuttle_config.get('command_port', 2000)
        
        # Проверяем доступность хоста
        host_available = ping_host(host)
        
        # Проверяем доступность порта
        port_available = False
        if host_available:
            port_available = await check_tcp_port(host, port)
        
        results.append((shuttle_id, host, host_available, port_available))
    
    return results

async def discover_shuttles_on_network(subnet: str) -> List[Tuple[str, bool]]:
    """Ищет доступные шаттлы в указанной подсети"""
    results = []
    base_ip = subnet.rsplit('.', 1)[0]
    
    # Проверяем IP-адреса в диапазоне
    for i in range(1, 255):
        ip = f"{base_ip}.{i}"
        available = ping_host(ip, timeout=0.5)
        if available:
            port_available = await check_tcp_port(ip, 2000, timeout=0.5)
            results.append((ip, port_available))
    
    return results

async def main():
    parser = argparse.ArgumentParser(description='Проверка доступности шаттлов')
    parser.add_argument('--fix', action='store_true', help='Исправить конфигурацию')
    parser.add_argument('--discover', action='store_true', help='Обнаружить шаттлы в сети')
    parser.add_argument('--subnet', default='10.181.80.0', help='Подсеть для поиска (например: 10.181.80.0)')
    args = parser.parse_args()
    
    # Загружаем конфигурацию
    config = load_config()
    
    # Проверяем доступность шаттлов
    print("🔍 Проверка доступности шаттлов...")
    results = await check_shuttles(config)
    
    # Выводим результаты
    print("\n📊 Результаты проверки:")
    print("=" * 60)
    print(f"{'ID шаттла':<20} {'IP-адрес':<15} {'Хост доступен':<15} {'Порт доступен':<15}")
    print("-" * 60)
    
    for shuttle_id, host, host_available, port_available in results:
        host_status = "✅" if host_available else "❌"
        port_status = "✅" if port_available else "❌"
        print(f"{shuttle_id:<20} {host:<15} {host_status:<15} {port_status:<15}")
    
    print("=" * 60)
    
    # Обнаруживаем шаттлы в сети
    if args.discover:
        print("\n🔍 Поиск шаттлов в сети...")
        print(f"Сканирование подсети {args.subnet}/24...")
        
        discovered = await discover_shuttles_on_network(args.subnet)
        
        print("\n📊 Обнаруженные хосты:")
        print("=" * 40)
        print(f"{'IP-адрес':<15} {'Порт 2000 открыт':<20}")
        print("-" * 40)
        
        for ip, port_available in discovered:
            port_status = "✅" if port_available else "❌"
            print(f"{ip:<15} {port_status:<20}")
        
        print("=" * 40)
    
    # Исправляем конфигурацию
    if args.fix:
        print("\n🔧 Исправление конфигурации...")
        
        # Проверяем, есть ли шаттлы с IP 10.181.80.132
        ip_132_exists = False
        for shuttle_id, shuttle_config in config['shuttles'].items():
            if shuttle_config['host'] == '10.181.80.132':
                ip_132_exists = True
                print(f"✅ Шаттл с IP 10.181.80.132 уже существует: {shuttle_id}")
        
        # Если нет, добавляем новый шаттл
        if not ip_132_exists:
            shuttle_id = "shuttle_132"
            config['shuttles'][shuttle_id] = {
                'host': '10.181.80.132',
                'command_port': 2000,
                'response_port': 5000,
                'shuttle_health_check_interval': 10
            }
            
            # Добавляем шаттл в список шаттлов склада
            if 'Главный' in config['stock_to_shuttle']:
                config['stock_to_shuttle']['Главный'].append(shuttle_id)
            
            print(f"✅ Добавлен новый шаттл: {shuttle_id} с IP 10.181.80.132")
        
        # Сохраняем конфигурацию
        save_config(config)

if __name__ == "__main__":
    asyncio.run(main())