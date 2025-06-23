#!/usr/bin/env python3
import argparse
import asyncio
import sys
from typing import Optional, List

from shuttle_module.commands import ShuttleCommandEnum, ShuttleCommand
from core.config import load_config, get_config, ShuttleConfig
from core.logging import setup_logging, get_logger


async def send_command_direct(shuttle_id: str, command: str, params: Optional[str] = None):
    """Отправляет команду шаттлу напрямую по IP"""
    logger = get_logger()
    
    # Проверяем, существует ли такая команда
    try:
        command_enum = ShuttleCommandEnum[command]
    except KeyError:
        logger.error(f"Неизвестная команда: {command}")
        logger.info(f"Доступные команды: {[cmd.name for cmd in ShuttleCommandEnum]}")
        return False
    
    # Определяем IP по имени шаттла
    if not shuttle_id.startswith("shuttle_"):
        logger.error(f"Неверный формат ID шаттла: {shuttle_id}. Ожидается формат shuttle_XXX")
        return False
    
    try:
        last_octet = shuttle_id.split("_")[1]
        ip = f"10.181.80.{last_octet}"
    except (IndexError, ValueError):
        logger.error(f"Не удалось извлечь номер из ID шаттла: {shuttle_id}")
        return False
    
    logger.info(f"Отправка команды {command} шаттлу {shuttle_id} по IP {ip}")
    
    # Создаем конфигурацию и клиент
    from shuttle_module.shuttle_client import ShuttleClient
    shuttle_config = ShuttleConfig(host=ip, command_port=2000, response_port=5000)
    shuttle_client = ShuttleClient(shuttle_id, shuttle_config)
    
    # Подключаемся к шаттлу
    connected = await shuttle_client.connect()
    if not connected:
        logger.error(f"Не удалось подключиться к шаттлу {shuttle_id} ({ip})")
        return False
    
    # Создаем команду
    shuttle_command = ShuttleCommand(
        command_type=command_enum,
        shuttle_id=shuttle_id,
        params=params
    )
    
    # Отправляем команду
    success = await shuttle_client.send_command(shuttle_command)
    
    if success:
        logger.info(f"Команда {command} успешно отправлена шаттлу {shuttle_id} ({ip})")
        
        # Ждем ответа от шаттла
        logger.info(f"Ожидание ответа от шаттла {shuttle_id}...")
        
        # Добавляем обработчик сообщений для вывода ответа
        response_received = asyncio.Event()
        response_message = None
        
        def message_handler(message):
            nonlocal response_message
            response_message = message
            logger.info(f"Получен ответ от шаттла {shuttle_id}: '{message}'")
            response_received.set()
        
        shuttle_client.add_message_handler(message_handler)
        
        # Ждем ответа максимум 10 секунд
        try:
            await asyncio.wait_for(response_received.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут ожидания ответа от шаттла {shuttle_id}")
        
        # Удаляем обработчик
        shuttle_client.remove_message_handler(message_handler)
        
        # Закрываем соединение
        await shuttle_client.disconnect()
        return True
    else:
        logger.error(f"Не удалось отправить команду {command} шаттлу {shuttle_id}")
        await shuttle_client.disconnect()
        return False


async def main():
    """Основная функция CLI для прямого взаимодействия с шаттлами"""
    # Настраиваем парсер аргументов
    parser = argparse.ArgumentParser(description="Утилита для прямого управления шаттлами по IP")
    
    # Команда для отправки команды шаттлу
    parser.add_argument("shuttle_id", help="ID шаттла (например, shuttle_138)")
    parser.add_argument("command", help="Команда для шаттла (STATUS, HOME, и т.д.)")
    parser.add_argument("--params", help="Параметры команды")
    
    # Парсим аргументы
    args = parser.parse_args()
    
    # Настраиваем логирование
    logger = setup_logging()
    
    # Выполняем команду
    success = await send_command_direct(args.shuttle_id, args.command, args.params)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))