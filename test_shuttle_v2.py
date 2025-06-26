#!/usr/bin/env python3
"""
Тестирование шлюза для шаттлов с фиксированной длиной сообщений
"""
import asyncio
import argparse
import sys
import logging
import time
from datetime import datetime

from shuttle_module.shuttle_client_v2 import ShuttleClientV2
from shuttle_module.shuttle_listener_v2 import get_shuttle_listener
from shuttle_module.shuttle_manager_v2 import get_shuttle_manager
from core.config import get_config

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('shuttle_test.log')
    ]
)
logger = logging.getLogger("test_shuttle_v2")

async def test_direct_command(ip, port, command, use_fixed_length=True):
    """Тестирует прямую отправку команды шаттлу"""
    try:
        # Создаем временный клиент
        shuttle = ShuttleClientV2("test_shuttle")
        shuttle.config = type('ShuttleConfig', (), {'host': ip, 'command_port': port})
        
        # Подключаемся к шаттлу
        connected = await shuttle.connect()
        if not connected:
            logger.error(f"Не удалось подключиться к шаттлу {ip}:{port}")
            return False
        
        # Отправляем команду
        success = await shuttle.send_command(command, use_fixed_length=use_fixed_length)
        
        # Отключаемся от шаттла
        await shuttle.disconnect()
        
        return success
    except Exception as e:
        logger.error(f"Ошибка при тестировании прямой команды: {e}")
        return False

async def test_manager(shuttle_id, commands):
    """Тестирует отправку команд через менеджер шаттлов"""
    try:
        # Получаем менеджер шаттлов
        manager = get_shuttle_manager()
        
        # Запускаем менеджер
        await manager.start()
        
        # Проверяем, что шаттл добавлен
        if shuttle_id not in manager.shuttles:
            logger.error(f"Шаттл {shuttle_id} не найден в менеджере")
            return False
        
        # Отправляем команды
        for cmd in commands:
            logger.info(f"Отправка команды {cmd} шаттлу {shuttle_id}")
            success = await manager.send_command(shuttle_id, cmd)
            if not success:
                logger.error(f"Не удалось отправить команду {cmd} шаттлу {shuttle_id}")
            
            # Ждем 3 секунды между командами
            await asyncio.sleep(3)
        
        # Останавливаем менеджер
        await manager.stop()
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при тестировании менеджера: {e}")
        return False

async def test_listener(duration=30):
    """Тестирует прослушивание сообщений от шаттлов"""
    try:
        # Запускаем слушатель
        listener = get_shuttle_listener()
        await listener.start()
        
        # Регистрируем обработчик для всех сообщений
        async def global_handler(message, message_hex):
            logger.info(f"Глобальный обработчик: '{message}', HEX: {message_hex}")
        
        listener.register_message_handler("global", global_handler)
        
        # Ждем указанное время
        logger.info(f"Слушатель запущен, ожидание {duration} секунд...")
        await asyncio.sleep(duration)
        
        # Останавливаем слушатель
        await listener.stop()
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при тестировании слушателя: {e}")
        return False

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование шлюза для шаттлов")
    parser.add_argument("--ip", help="IP-адрес шаттла для прямого тестирования")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--command", help="Команда для прямого тестирования")
    parser.add_argument("--fixed", action="store_true", help="Использовать фиксированную длину сообщений")
    parser.add_argument("--shuttle", help="ID шаттла для тестирования через менеджер")
    parser.add_argument("--listen", action="store_true", help="Только прослушивать сообщения")
    parser.add_argument("--duration", type=int, default=30, help="Продолжительность прослушивания в секундах")
    
    args = parser.parse_args()
    
    if args.listen:
        # Тестируем только прослушивание
        await test_listener(args.duration)
    elif args.ip and args.command:
        # Тестируем прямую отправку команды
        await test_direct_command(args.ip, args.port, args.command, args.fixed)
    elif args.shuttle:
        # Тестируем отправку команд через менеджер
        commands = ["STATUS", "LOC"]
        await test_manager(args.shuttle, commands)
    else:
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))