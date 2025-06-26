#!/usr/bin/env python3
"""
Скрипт для прямого подключения к шаттлу с учетом возможных проблем с IP-адресами
"""
import asyncio
import sys
import yaml
import argparse
import socket
from typing import Optional, Tuple, List

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

async def find_alternative_ips(subnet: str) -> List[str]:
    """Ищет доступные IP-адреса в подсети"""
    results = []
    base_ip = subnet.rsplit('.', 1)[0]
    
    # Проверяем IP-адреса в диапазоне
    for i in range(1, 255):
        ip = f"{base_ip}.{i}"
        available = ping_host(ip, timeout=0.5)
        if available:
            port_available = await check_tcp_port(ip, 2000, timeout=0.5)
            if port_available:
                results.append(ip)
    
    return results

async def send_command_to_shuttle(shuttle_id: str, command: str, ip: Optional[str] = None, port: int = 2000, timeout: float = 5.0) -> bool:
    """Отправляет команду шаттлу"""
    # Если IP не указан, загружаем из конфигурации
    if not ip:
        try:
            with open('config.yaml', 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if shuttle_id not in config['shuttles']:
                print(f"❌ Шаттл {shuttle_id} не найден в конфигурации")
                return False
            
            ip = config['shuttles'][shuttle_id]['host']
            port = config['shuttles'][shuttle_id].get('command_port', 2000)
        except Exception as e:
            print(f"❌ Ошибка загрузки конфигурации: {e}")
            return False
    
    # Проверяем доступность IP
    print(f"🔍 Проверка доступности {ip}...")
    host_available = ping_host(ip)
    
    if not host_available:
        print(f"❌ Хост {ip} недоступен")
        
        # Ищем альтернативные IP
        print("🔍 Поиск альтернативных IP-адресов...")
        subnet = '.'.join(ip.split('.')[:3]) + '.0'
        alternative_ips = await find_alternative_ips(subnet)
        
        if alternative_ips:
            print(f"✅ Найдены альтернативные IP-адреса с открытым портом 2000: {alternative_ips}")
            ip = alternative_ips[0]
            print(f"🔄 Используем альтернативный IP: {ip}")
        else:
            print("❌ Альтернативные IP-адреса не найдены")
            return False
    
    # Проверяем доступность порта
    print(f"🔍 Проверка доступности порта {port} на {ip}...")
    port_available = await check_tcp_port(ip, port)
    
    if not port_available:
        print(f"❌ Порт {port} на {ip} недоступен")
        return False
    
    # Отправляем команду
    try:
        print(f"📤 Подключаемся к шаттлу {ip}:{port}")
        
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port),
            timeout=timeout
        )
        
        # Добавляем терминатор CRLF если его нет
        if not command.endswith('\r\n'):
            command = command.rstrip('\r\n') + '\r\n'
        
        print(f"📤 Отправляем команду: '{command.strip()}'")
        print(f"📤 Команда в байтах: {command.encode('utf-8')!r}")
        
        writer.write(command.encode('utf-8'))
        await writer.drain()
        
        # Закрываем соединение
        writer.close()
        await writer.wait_closed()
        
        print(f"✅ Команда отправлена шаттлу {shuttle_id} ({ip}:{port})")
        return True
    except asyncio.TimeoutError:
        print(f"❌ Таймаут подключения к шаттлу {ip}:{port}")
        return False
    except Exception as e:
        print(f"❌ Ошибка при отправке команды: {e}")
        return False

async def listen_for_response(shuttle_id: str, ip: Optional[str] = None, port: int = 8181, timeout: float = 10.0) -> Optional[str]:
    """Слушает ответ от шаттла"""
    # Если IP не указан, загружаем из конфигурации
    if not ip:
        try:
            with open('config.yaml', 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if shuttle_id not in config['shuttles']:
                print(f"❌ Шаттл {shuttle_id} не найден в конфигурации")
                return None
            
            ip = config['shuttles'][shuttle_id]['host']
        except Exception as e:
            print(f"❌ Ошибка загрузки конфигурации: {e}")
            return None
    
    print(f"👂 Слушаем ответ от шаттла {shuttle_id} ({ip}) на порту {port}")
    
    response_received = False
    response_data = None
    
    async def handle_connection(reader, writer):
        nonlocal response_received, response_data
        peer_name = writer.get_extra_info('peername')
        client_ip = peer_name[0] if peer_name else 'unknown'
        
        print(f"📥 Получено соединение от {client_ip}")
        
        # Принимаем соединения от любого IP, так как IP в конфигурации может быть неправильным
        try:
            # Читаем данные
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            if data:
                message = data.decode('utf-8').strip()
                print(f"📨 Ответ от {client_ip}: '{message}'")
                print(f"📨 Ответ в байтах: {data!r}")
                response_data = message
                response_received = True
            
        except asyncio.TimeoutError:
            print(f"⏰ Таймаут чтения данных от {client_ip}")
        except Exception as e:
            print(f"❌ Ошибка при чтении данных: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    server = None
    try:
        server = await asyncio.start_server(
            handle_connection,
            '0.0.0.0',
            port
        )
        
        print(f"🔊 Сервер запущен на порту {port}")
        
        # Ждем ответ с таймаутом
        for _ in range(int(timeout * 10)):
            if response_received:
                break
            await asyncio.sleep(0.1)
        
        if not response_received:
            print(f"⏰ Таймаут ожидания ответа от шаттла {shuttle_id}")
            
    except Exception as e:
        print(f"❌ Ошибка при прослушивании ответа: {e}")
    finally:
        if server:
            server.close()
            await server.wait_closed()
            print("🔇 Сервер остановлен")
    
    return response_data

async def main():
    parser = argparse.ArgumentParser(description='Прямое подключение к шаттлу')
    parser.add_argument('shuttle_id', help='ID шаттла (например: shuttle_135)')
    parser.add_argument('command', help='Команда для отправки (например: STATUS)')
    parser.add_argument('--ip', help='IP-адрес шаттла (если не указан, берется из конфигурации)')
    parser.add_argument('--port', type=int, default=2000, help='Порт для отправки команд')
    parser.add_argument('--listen-port', type=int, default=8181, help='Порт для прослушивания ответов')
    parser.add_argument('--timeout', '-t', type=float, default=10.0, help='Таймаут ожидания ответа в секундах')
    parser.add_argument('--no-listen', action='store_true', help='Не слушать ответ')
    
    args = parser.parse_args()
    
    print(f"🚀 Запуск прямого подключения к шаттлу {args.shuttle_id}")
    print("=" * 60)
    
    # Отправляем команду
    success = await send_command_to_shuttle(
        args.shuttle_id, 
        args.command, 
        args.ip, 
        args.port, 
        args.timeout
    )
    
    if not success:
        print("❌ Не удалось отправить команду")
        return 1
    
    # Слушаем ответ если нужно
    if not args.no_listen:
        response = await listen_for_response(
            args.shuttle_id, 
            args.ip, 
            args.listen_port, 
            args.timeout
        )
        
        if response:
            print(f"✅ Получен ответ: '{response}'")
        else:
            print("❌ Ответ не получен")
    
    print("=" * 60)
    print("✅ Завершено")
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)