#!/usr/bin/env python3
"""
Основной файл шлюза для шаттлов с поддержкой фиксированной длины сообщений
"""
import asyncio
import logging
import signal
import sys
import os
from datetime import datetime

from core.config import get_config
from shuttle_module.shuttle_manager_v2 import get_shuttle_manager

# Настройка логирования
def setup_logging():
    config = get_config()
    log_level = getattr(logging, config.logging.level)
    log_file = config.logging.file_path
    
    # Создаем директорию для логов, если она не существует
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )
    
    logger = logging.getLogger("shuttle_gateway")
    logger.info(f"Логирование настроено с уровнем {config.logging.level}")
    return logger

# Обработчик сигналов для корректного завершения
async def shutdown(signal, loop, manager):
    """Корректно завершает работу шлюза при получении сигнала"""
    logger = logging.getLogger("shuttle_gateway")
    logger.info(f"Получен сигнал {signal.name}, завершение работы...")
    
    # Останавливаем менеджер шаттлов
    await manager.stop()
    
    # Останавливаем все задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()
    logger.info("Шлюз остановлен")

async def main():
    """Основная функция шлюза"""
    # Настраиваем логирование
    logger = setup_logging()
    logger.info("Запуск шлюза для шаттлов...")
    
    try:
        # Получаем менеджер шаттлов
        manager = get_shuttle_manager()
        
        # Запускаем менеджер
        await manager.start()
        
        # Настраиваем обработчики сигналов для корректного завершения
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(shutdown(s, loop, manager))
            )
        
        logger.info("Шлюз запущен и готов к работе")
        
        # Бесконечный цикл для поддержания работы шлюза
        while True:
            await asyncio.sleep(3600)  # Спим 1 час
    except Exception as e:
        logger.error(f"Критическая ошибка в шлюзе: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))