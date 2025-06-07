#!/usr/bin/env python3
import argparse
import asyncio
import sys
from typing import Optional, List

from shuttle_module.commands import ShuttleCommandEnum
from shuttle_module.shuttle_manager import get_shuttle_manager
from shuttle_module.shuttle_client import ShuttleClient
from shuttle_module.shuttle_listener import get_shuttle_listener
from core.config import load_config, get_config
from core.logging import setup_logging, get_logger


CONFYG: str = 'config.yaml'

async def send_command(shuttle_id: str, command: str, params: Optional[str] = None):
    """Отправляет команду шаттлу"""
    logger = get_logger()
    
    # Проверяем, существует ли такая команда
    try:
        command_enum = ShuttleCommandEnum[command]
    except KeyError:
        logger.error(f"Неизвестная команда: {command}")
        logger.info(f"Доступные команды: {[cmd.name for cmd in ShuttleCommandEnum]}")
        return False
    
    # Создаем клиент для шаттла
    try:
        shuttle_client = ShuttleClient(shuttle_id)
    except ValueError as e:
        logger.error(f"Ошибка: {e}")
        return False
    
    # Подключаемся к шаттлу
    connected = await shuttle_client.connect()
    if not connected:
        logger.error(f"Не удалось подключиться к шаттлу {shuttle_id}")
        return False
    
    # Создаем команду
    from shuttle_module.commands import ShuttleCommand
    shuttle_command = ShuttleCommand(
        command_type=command_enum,
        shuttle_id=shuttle_id,
        params=params
    )
    
    # Отправляем команду
    success = await shuttle_client.send_command(shuttle_command)
    
    # Закрываем соединение
    await shuttle_client.disconnect()
    
    if success:
        logger.info(f"Команда {command} успешно отправлена шаттлу {shuttle_id}")
        return True
    else:
        logger.error(f"Не удалось отправить команду {command} шаттлу {shuttle_id}")
        return False


async def list_shuttles():
    """Выводит список доступных шаттлов"""
    logger = get_logger()
    config = get_config()
    
    logger.info("Доступные шаттлы:")
    for shuttle_id, shuttle_config in config.shuttles.items():
        logger.info(f"  {shuttle_id}: {shuttle_config.host}:{shuttle_config.command_port}")
    
    return True


async def get_shuttle_status(shuttle_id: str):
    """Получает статус шаттла"""
    logger = get_logger()
    
    # Получаем менеджер шаттлов
    shuttle_manager = get_shuttle_manager()
    
    # Получаем состояние шаттла
    state = await shuttle_manager.get_shuttle_state(shuttle_id)
    if not state:
        logger.error(f"Шаттл {shuttle_id} не найден")
        return False
    
    # Выводим информацию о шаттле
    logger.info(f"Статус шаттла {shuttle_id}:")
    logger.info(f"  Статус: {state.status}")
    logger.info(f"  Текущая команда: {state.current_command}")
    logger.info(f"  Последнее сообщение: {state.last_message}")
    logger.info(f"  Последняя активность: {state.last_seen}")
    logger.info(f"  Уровень батареи: {state.battery_level}")
    logger.info(f"  Местоположение: {state.location_data}")
    logger.info(f"  Ошибка: {state.error_code}")
    
    return True


async def main():
    """Основная функция CLI"""
    # Настраиваем парсер аргументов
    parser = argparse.ArgumentParser(description="Утилита для управления шаттлами")
    subparsers = parser.add_subparsers(dest="command", help="Команда")
    
    # Команда для отправки команды шаттлу
    send_parser = subparsers.add_parser("send", help="Отправить команду шаттлу")
    send_parser.add_argument("shuttle_id", help="ID шаттла")
    send_parser.add_argument("shuttle_command", help="Команда для шаттла")
    send_parser.add_argument("--params", help="Параметры команды")
    
    # Команда для вывода списка шаттлов
    subparsers.add_parser("list", help="Вывести список доступных шаттлов")
    
    # Команда для получения статуса шаттла
    status_parser = subparsers.add_parser("status", help="Получить статус шаттла")
    status_parser.add_argument("shuttle_id", help="ID шаттла")
    
    # Команда для перемещения шаттла между ячейками
    move_parser = subparsers.add_parser("move", help="Переместить шаттл между ячейками")
    move_parser.add_argument("shuttle_id", help="ID шаттла")
    move_parser.add_argument("from_cell", help="Исходная ячейка")
    move_parser.add_argument("to_cell", help="Целевая ячейка")
    move_parser.add_argument("--stock", help="Название склада", default="Главный")
    
    # Общие аргументы
    parser.add_argument("--config", help="Путь к файлу конфигурации", default="config.yaml")
    
    # Парсим аргументы
    args = parser.parse_args()
    
    # Настраиваем логирование
    logger = setup_logging()
    
    # Загружаем конфигурацию
    try:
        config = load_config(args.config)
        logger.info(f"Загружена конфигурация из {args.config}")
        logger.info(f"Доступные шаттлы: {list(config.shuttles.keys())}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке конфигурации из {args.config}: {e}")
        return 1
    
    # Выполняем команду
    if args.command == "send":
        success = await send_command(args.shuttle_id, args.shuttle_command, args.params)
    elif args.command == "list":
        success = await list_shuttles()
    elif args.command == "status":
        success = await get_shuttle_status(args.shuttle_id)
    elif args.command == "move":
        success = await move_shuttle(args.shuttle_id, args.from_cell, args.to_cell, args.stock)
    else:
        parser.print_help()
        success = False
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
async def move_shuttle(shuttle_id: str, from_cell: str, to_cell: str, stock_name: str):
    """Перемещает шаттл между ячейками"""
    logger = get_logger()
    
    # Создаем клиент для шаттла
    try:
        shuttle_client = ShuttleClient(shuttle_id)
    except ValueError as e:
        logger.error(f"Ошибка: {e}")
        return False
    
    # Подключаемся к шаттлу
    connected = await shuttle_client.connect()
    if not connected:
        logger.error(f"Не удалось подключиться к шаттлу {shuttle_id}")
        return False
    
    # Получаем текущее местоположение шаттла
    logger.info(f"Запрашиваем текущее местоположение шаттла {shuttle_id}...")
    
    # Отправляем команду STATUS для получения текущего местоположения
    from shuttle_module.commands import ShuttleCommand
    status_command = ShuttleCommand(
        command_type=ShuttleCommandEnum.STATUS,
        shuttle_id=shuttle_id
    )
    
    await shuttle_client.send_command(status_command)
    
    # Ждем немного, чтобы получить ответ
    await asyncio.sleep(1)
    
    # Получаем состояние шаттла
    state = shuttle_client.get_state()
    current_location = state.location_data
    
    logger.info(f"Текущее местоположение шаттла {shuttle_id}: {current_location}")
    logger.info(f"Перемещаем шаттл {shuttle_id} из ячейки {from_cell} в ячейку {to_cell} на складе {stock_name}...")
    
    # Отправляем команду HOME для возврата в домашнюю позицию (если нужно)
    if from_cell != to_cell:
        # Сначала отправляем команду HOME
        home_command = ShuttleCommand(
            command_type=ShuttleCommandEnum.HOME,
            shuttle_id=shuttle_id
        )
        
        await shuttle_client.send_command(home_command)
        
        # Ждем, пока шаттл вернется в домашнюю позицию
        await asyncio.sleep(2)
        
        # Затем отправляем команду PALLET_IN для перемещения в целевую ячейку
        move_command = ShuttleCommand(
            command_type=ShuttleCommandEnum.PALLET_IN,
            shuttle_id=shuttle_id,
            params=to_cell,
            cell_id=to_cell,
            stock_name=stock_name
        )
        
        await shuttle_client.send_command(move_command)
        
        logger.info(f"Команды для перемещения шаттла {shuttle_id} отправлены")
    else:
        logger.info(f"Шаттл {shuttle_id} уже находится в ячейке {to_cell}")
    
    # Закрываем соединение
    await shuttle_client.disconnect()
    
    return True